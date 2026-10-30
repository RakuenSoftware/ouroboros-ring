package ring

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// D16: the host writes one content-free record per seq-stamped event into a
// single-producer ring in its own region, and one telemetry process holds the
// consumer end. These mirror c/include/ouroboros/telemetry.h.
const (
	telemetryMagic   uint32 = 0x594d4c54 // "TLMY"
	telemetryVersion uint32 = 1

	// TelemetryRecordLen is the size of every record.
	TelemetryRecordLen = 128

	telemetryOffType         = 0
	telemetryOffFlags        = 4
	telemetryOffRealtimeNS   = 8
	telemetryOffLost         = 16
	telemetryOffReserved     = 24
	telemetryOffFrame        = 32
	telemetryOffReservedTail = 96

	telemetryRegionOffMagic      = 0
	telemetryRegionOffVersion    = 4
	telemetryRegionOffRecordSize = 8
	telemetryRegionOffFlags      = 12
	telemetryRegionOffHostEpoch  = 16
	telemetryRegionOffDropped    = 24
	telemetryRegionOffWaiting    = 64
	telemetryRegionOffRing       = 128
)

// TelemetryRecordType is a record's type field.
type TelemetryRecordType uint32

const (
	// TelemetryEvent describes one seq-stamped event.
	TelemetryEvent TelemetryRecordType = 0
	// TelemetryGap says Lost records were dropped just before this one.
	TelemetryGap TelemetryRecordType = 1
)

// ErrTelemetryRecord is returned for a record that disagrees with D16.
var ErrTelemetryRecord = errors.New("ring: malformed telemetry record")

// TelemetryRecord is a decoded record. For a gap, Frame is zero and Lost is
// the count.
type TelemetryRecord struct {
	Type       TelemetryRecordType
	RealtimeNS uint64
	Lost       uint64
	Frame      Frame
}

// DecodeTelemetryRecord decodes one TelemetryRecordLen-byte record.
func DecodeTelemetryRecord(b []byte) (TelemetryRecord, error) {
	var r TelemetryRecord
	if len(b) < TelemetryRecordLen {
		return r, ErrTelemetryRecord
	}
	r.Type = TelemetryRecordType(binary.LittleEndian.Uint32(b[telemetryOffType:]))
	r.RealtimeNS = binary.LittleEndian.Uint64(b[telemetryOffRealtimeNS:])
	r.Lost = binary.LittleEndian.Uint64(b[telemetryOffLost:])
	if binary.LittleEndian.Uint32(b[telemetryOffFlags:]) != 0 {
		return r, ErrTelemetryRecord
	}
	switch r.Type {
	case TelemetryGap:
		if r.Lost == 0 {
			return r, ErrTelemetryRecord
		}
		return r, nil
	case TelemetryEvent:
		if r.Lost != 0 || Decode(b[telemetryOffFrame:telemetryOffFrame+HdrLen], &r.Frame) != OK {
			return r, ErrTelemetryRecord
		}
		return r, nil
	}
	return r, ErrTelemetryRecord
}

// TelemetryReader is the consumer end of the host's telemetry ring. One
// goroutine at a time, like Poll.
type TelemetryReader struct {
	mem     []byte
	ring    *Ring
	waiting *uint32
	dropped *uint64
	bell    int
	host    int // D15: a pidfd on the host, -1 where none could be opened
}

// hostProcess opens a process handle on the peer of the attach socket (D15),
// so a consumer asleep on the doorbell wakes when the host exits.
func hostProcess(sock int) int {
	cred, err := unix.GetsockoptUcred(sock, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil || cred.Pid <= 0 {
		return -1
	}
	fd, err := unix.PidfdOpen(int(cred.Pid), 0)
	if err != nil {
		return -1
	}
	return fd
}

func openTelemetryReader(regionFd, bellFd int) (*TelemetryReader, error) {
	fail := func(err error) (*TelemetryReader, error) {
		unix.Close(regionFd)
		unix.Close(bellFd)
		return nil, err
	}
	size, err := fdSize(regionFd)
	if err != nil {
		return fail(err)
	}
	if size <= telemetryRegionOffRing || size > maxRegionBytes {
		return fail(ErrRegion)
	}
	mem, err := unix.Mmap(regionFd, 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fail(err)
	}
	unix.Close(regionFd) // the mapping holds its own reference
	// Validate once; afterwards only head, tail and the waiting flag are read.
	if atomic.LoadUint32(u32ptr(mem, telemetryRegionOffMagic)) != telemetryMagic ||
		atomic.LoadUint32(u32ptr(mem, telemetryRegionOffVersion)) != telemetryVersion ||
		atomic.LoadUint32(u32ptr(mem, telemetryRegionOffRecordSize)) != TelemetryRecordLen {
		unix.Munmap(mem)
		unix.Close(bellFd)
		return nil, ErrRegion
	}
	r, err := AttachRing(mem[telemetryRegionOffRing:])
	if err != nil || r.SlotSize() != TelemetryRecordLen {
		unix.Munmap(mem)
		unix.Close(bellFd)
		if err == nil {
			err = ErrRegion
		}
		return nil, err
	}
	return &TelemetryReader{
		mem: mem, ring: r, bell: bellFd, host: -1,
		waiting: u32ptr(mem, telemetryRegionOffWaiting),
		dropped: u64ptr(mem, telemetryRegionOffDropped),
	}, nil
}

func u32ptr(b []byte, off int) *uint32 { return (*uint32)(unsafe.Pointer(&b[off])) }

// Available reports the records written and not yet released.
func (t *TelemetryReader) Available() uint64 {
	n := t.ring.Count()
	if n > uint64(t.ring.Capacity()) {
		return 0 // a tail the host never wrote is not a ring state
	}
	return n
}

// Peek returns the i-th unreleased record's bytes, valid until Release.
func (t *TelemetryReader) Peek(i uint64) []byte {
	if i >= t.Available() {
		return nil
	}
	return t.ring.slot(atomic.LoadUint64(t.ring.tail) + i)
}

// Release hands the oldest n records back to the host once they are exported.
func (t *TelemetryReader) Release(n uint64) {
	if avail := t.Available(); n > avail {
		n = avail
	}
	if n > 0 {
		atomic.StoreUint64(t.ring.tail, atomic.LoadUint64(t.ring.tail)+n)
	}
}

// Dropped is every record the host has lost since the region was created.
func (t *TelemetryReader) Dropped() uint64 { return atomic.LoadUint64(t.dropped) }

// Wait sleeps until a record is available or timeout passes (negative waits
// indefinitely), under the D13 protocol. It reports whether records are
// available, and returns ErrEpoch once the host has exited (D15).
func (t *TelemetryReader) Wait(timeout time.Duration) (bool, error) {
	atomic.StoreUint32(t.waiting, 1)
	defer atomic.StoreUint32(t.waiting, 0)
	if t.Available() > 0 {
		return true, nil
	}
	ms := -1
	if timeout >= 0 {
		ms = int((timeout + time.Millisecond - 1) / time.Millisecond)
	}
	fds := []unix.PollFd{{Fd: int32(t.bell), Events: unix.POLLIN}}
	if t.host >= 0 {
		fds = append(fds, unix.PollFd{Fd: int32(t.host), Events: unix.POLLIN})
	}
	n, err := unix.Poll(fds, ms)
	if err != nil && err != unix.EINTR {
		return false, err
	}
	if n > 0 && fds[0].Revents&unix.POLLIN != 0 {
		var count [8]byte
		_, _ = unix.Read(t.bell, count[:])
	}
	if t.Available() > 0 {
		return true, nil
	}
	if n > 0 && len(fds) > 1 && fds[1].Revents != 0 {
		return false, ErrEpoch
	}
	return false, nil
}

// Interrupt wakes Wait from another goroutine.
func (t *TelemetryReader) Interrupt() { ringDoorbell(t.bell) }

// Close unmaps the region. The host keeps it, and a later consumer resumes at
// the same tail.
func (t *TelemetryReader) Close() {
	if t.mem != nil {
		unix.Munmap(t.mem)
		t.mem = nil
	}
	if t.bell >= 0 {
		unix.Close(t.bell)
		t.bell = -1
	}
	if t.host >= 0 {
		unix.Close(t.host)
		t.host = -1
	}
}
