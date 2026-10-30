package ring

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A record built byte by byte from the layout table decodes, and one with an
// unknown flag, a zero-count gap or a bad frame is refused.
func TestTelemetryRecordDecode(t *testing.T) {
	f := Frame{HdrFlags: FNotification | FInline, WireVersion: WireVersion, EventKind: 0x2a01,
		PrincipalRef: 9, Seq: 41, PayloadLen: 5, PayloadRef: HdrLen, SrcHandle: 2}
	rec := make([]byte, TelemetryRecordLen)
	binary.LittleEndian.PutUint32(rec[telemetryOffType:], uint32(TelemetryEvent))
	binary.LittleEndian.PutUint64(rec[telemetryOffRealtimeNS:], 1234)
	if f.Encode(rec[telemetryOffFrame:]) != HdrLen {
		t.Fatal("encode")
	}
	got, err := DecodeTelemetryRecord(rec)
	if err != nil || got.Type != TelemetryEvent || got.RealtimeNS != 1234 || got.Frame.Seq != 41 ||
		got.Frame.EventKind != 0x2a01 || got.Frame.SrcHandle != 2 || got.Frame.PayloadLen != 5 {
		t.Fatalf("decoded %+v, %v", got, err)
	}
	bad := bytes.Clone(rec)
	bad[telemetryOffFlags] = 1
	if _, err := DecodeTelemetryRecord(bad); err == nil {
		t.Error("an unknown flag was accepted")
	}
	gap := make([]byte, TelemetryRecordLen)
	binary.LittleEndian.PutUint32(gap[telemetryOffType:], uint32(TelemetryGap))
	if _, err := DecodeTelemetryRecord(gap); err == nil {
		t.Error("a gap of zero was accepted")
	}
	binary.LittleEndian.PutUint64(gap[telemetryOffLost:], 3)
	if g, err := DecodeTelemetryRecord(gap); err != nil || g.Lost != 3 {
		t.Errorf("gap: %+v, %v", g, err)
	}
}

// The Go client, granted the consumer end by the C host, reads the record of
// every event it caused: its own notification, and the C side's ack, in seq
// order, with no payload byte in either.
func TestTelemetryConformance(t *testing.T) {
	harness := os.Getenv("OURO_CONFORMANCE_HOST")
	if harness == "" {
		t.Skip("OURO_CONFORMANCE_HOST not set; see README.md, Building and testing")
	}
	sock := filepath.Join(t.TempDir(), "bus.sock")
	cmd := exec.Command(harness, sock, "10000")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("harness output:\n%s", out.String())
		}
	}()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("harness socket did not appear")
		}
	}

	attachAs := func(ref uint32) (*Client, *TelemetryReader, int) {
		fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := unix.Connect(fd, &unix.SockaddrUnix{Name: sock}); err != nil {
			t.Fatal(err)
		}
		c, r, err := AttachTelemetry(fd, 1, ref)
		if err != nil {
			t.Fatalf("attach as %#x: %v", ref, err)
		}
		return c, r, fd
	}

	// A principal the host did not grant is attached without the consumer end.
	c, r, fd := attachAs(0x1234)
	if r != nil {
		t.Fatal("an ungranted principal was handed the telemetry region")
	}
	c.Detach()
	unix.Close(fd)
	time.Sleep(50 * time.Millisecond) // the host reaps it and accepts the next

	c, r, fd = attachAs(0x7E1E)
	defer unix.Close(fd)
	if r == nil {
		t.Fatal("the granted principal was not handed the telemetry region")
	}
	defer r.Close()
	defer c.Detach()
	// Whatever the earlier attach caused is not this test's.
	r.Release(r.Available())

	payload := []byte("payload-never-exported")
	if err := c.Publish(1000, payload); err != nil { // KIND_NOTIFY
		t.Fatal(err)
	}
	var notify, ack *TelemetryRecord
	deadline := time.Now().Add(5 * time.Second)
	var lastSeq uint64
	for (notify == nil || ack == nil) && time.Now().Before(deadline) {
		if ok, err := r.Wait(time.Second); err != nil {
			t.Fatal(err)
		} else if !ok {
			continue
		}
		n := r.Available()
		for i := uint64(0); i < n; i++ {
			raw := r.Peek(i)
			if bytes.Contains(raw, []byte("payload")) {
				t.Fatal("a payload byte reached a telemetry record")
			}
			rec, err := DecodeTelemetryRecord(raw)
			if err != nil {
				t.Fatalf("record %d: %v", i, err)
			}
			if rec.Type != TelemetryEvent {
				continue
			}
			if rec.Frame.Seq <= lastSeq {
				t.Fatalf("seq %d after %d", rec.Frame.Seq, lastSeq)
			}
			lastSeq = rec.Frame.Seq
			switch rec.Frame.EventKind {
			case 1000:
				notify = &rec
			case 1001:
				ack = &rec
			}
		}
		r.Release(n)
	}
	if notify == nil || ack == nil {
		t.Fatalf("records: notify %v ack %v", notify != nil, ack != nil)
	}
	if notify.Frame.SrcHandle != c.Handle || notify.Frame.PayloadLen != uint32(len(payload)) ||
		notify.RealtimeNS == 0 {
		t.Errorf("notify record %+v", notify)
	}
	if ack.Frame.Seq <= notify.Frame.Seq {
		t.Errorf("the ack (seq %d) is recorded after the notify (seq %d)", ack.Frame.Seq, notify.Frame.Seq)
	}
	if r.Dropped() != 0 {
		t.Errorf("dropped %d", r.Dropped())
	}

	// D15: a consumer asleep on the doorbell wakes when the host exits.
	start := time.Now()
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = cmd.Process.Kill()
	}()
	if _, err := r.Wait(10 * time.Second); !errors.Is(err, ErrEpoch) {
		t.Fatalf("wait after the host exited: %v", err)
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Errorf("woke %v after the host exited, not at once", waited)
	}
}
