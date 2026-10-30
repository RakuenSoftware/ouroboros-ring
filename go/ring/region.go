package ring

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"unsafe"
)

// Control region layout, mirroring ouro_control_t (96 bytes).
const (
	ctlMagic             uint32 = 0x4c544342 // "BCTL"
	ctlOffMagic                 = 0
	ctlOffSpec                  = 4
	ctlOffLayout                = 8
	ctlOffSlot                  = 16
	ctlOffInline                = 20
	ctlOffQueueCap              = 24
	ctlOffArena                 = 32
	ctlOffFlags                 = 12
	ctlOffEpoch                 = 40
	ctlOffHeartbeat             = 48
	ctlOffLeaseViewSlots        = 28 // D12, formerly reserved0
	ctlOffLeaseViewOff          = 56 // D12, formerly reserved1[0]
	ctlOffHostWaiting           = 64 // D13, formerly the low half of reserved1[0]
	ctlOffReserved2             = 68
	ctlOffReserved1             = 72
	ctlBytes                    = 96

	// D12: the lease view follows the control header when the host sets
	// ctlFlagLeaseView.
	ctlFlagLeaseView uint32 = 0x1
	// D13: the host keeps host_waiting and signals client doorbells.
	ctlFlagDoorbell  uint32 = 0x2
	leaseViewSlots          = 4096
	leaseViewEntry          = 16 // generation u32, len u32, offset u64
	ctlBytesWithView        = ctlBytes + leaseViewSlots*leaseViewEntry

	specVersion   uint32 = 1
	layoutVersion uint32 = 1
)

// Queue-pair region layout, mirroring ouro_qpair_hdr_t (96 bytes) followed by the
// inbound and outbound rings.
const (
	qpMagic            uint32 = 0x52504251 // "QPBR"
	qpOffMagic                = 0
	qpOffSlot                 = 4
	qpOffCap                  = 8
	qpOffDataCredits          = 12
	qpOffCtlCredits           = 16
	qpOffCtlLost              = 20
	qpOffInboundOff           = 24
	qpOffOutboundOff          = 28
	qpOffHeartbeat            = 32
	qpOffClientWaiting        = 40 // D13, formerly the low half of reserved[0]
	qpOffReserved0            = 44
	qpOffReserved             = 48
	qpBytes                   = 96
	// Each ring starts on a 64-byte boundary after the header.
	qpRingAlign = 64
)

// Arena region header (ouro_arena_hdr_t, 16 bytes) before the usable bytes.
const (
	arenaMagic       uint32 = 0x4e524142 // "BARN"
	arenaOffMagic           = 0
	arenaOffReserved        = 4
	arenaOffSize            = 8
	arenaHdrBytes           = 16

	// Lease-view entry fields (D12).
	leaseEntryOffGeneration = 0
	leaseEntryOffLen        = 4
	leaseEntryOffOffset     = 8

	// Lease-release payload fields (D12).
	leaseReleaseOffLease      = 0
	leaseReleaseOffGeneration = 4

	// Overflow payload fields (D6), decoded by subscribers since D14.
	overflowLen         = 16
	overflowOffShedSeq  = 0
	overflowOffShedKind = 8
	overflowOffDstSlot  = 12
)

// ErrRegion is returned when a mapped region fails validation.
var ErrRegion = errors.New("ring: region invalid")

// Control is a read-only view of the control region.
type Control struct {
	mem []byte
}

// AttachControl validates a mapped control region.
func AttachControl(mem []byte) (*Control, error) {
	if len(mem) < ctlBytes {
		return nil, ErrRegion
	}
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(&mem[ctlOffMagic]))) != ctlMagic {
		return nil, ErrRegion
	}
	if binary.LittleEndian.Uint32(mem[ctlOffSpec:]) != specVersion ||
		binary.LittleEndian.Uint32(mem[ctlOffLayout:]) != layoutVersion {
		return nil, ErrRegion
	}
	slot := binary.LittleEndian.Uint32(mem[ctlOffSlot:])
	inl := binary.LittleEndian.Uint32(mem[ctlOffInline:])
	arena := binary.LittleEndian.Uint64(mem[ctlOffArena:])
	if slot == 0 || inl > slot || arena == 0 {
		return nil, ErrRegion
	}
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(&mem[ctlOffFlags])))&ctlFlagLeaseView != 0 &&
		(binary.LittleEndian.Uint32(mem[ctlOffLeaseViewSlots:]) != leaseViewSlots ||
			binary.LittleEndian.Uint64(mem[ctlOffLeaseViewOff:]) != ctlBytes) {
		return nil, ErrRegion
	}
	return &Control{mem: mem}, nil
}

// leaseView returns the D12 lease view, or nil when the host does not
// advertise one or the mapping is only the header (a host that predates it).
func (c *Control) leaseView() []byte {
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(&c.mem[ctlOffFlags])))&ctlFlagLeaseView == 0 ||
		len(c.mem) < ctlBytesWithView {
		return nil
	}
	return c.mem[ctlBytes:ctlBytesWithView]
}

// attachArena checks the arena region's header (bus_arena_hdr_t): the host's
// magic, and the usable size the attach reply announced, as the C client does
// (ouro_arena_region_attach). It returns the usable bytes.
func attachArena(mem []byte, announced uint64) ([]byte, error) {
	if len(mem) < arenaHdrBytes {
		return nil, ErrRegion
	}
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(&mem[arenaOffMagic]))) != arenaMagic {
		return nil, ErrRegion
	}
	size := atomic.LoadUint64(u64ptr(mem, arenaOffSize))
	if size != announced || uint64(len(mem)-arenaHdrBytes) != size {
		return nil, ErrRegion
	}
	return mem[arenaHdrBytes:], nil
}

// resolveLease maps an arena frame's (lease id, generation, length) to an
// offset into the arena's usable bytes. Every value is a claim: the entry must
// be live with the frame's generation, the payload must fit the span, and the
// span must lie in the arena. The acquire load of generation pairs with the
// host's release store, made after offset and len.
func resolveLease(view []byte, lease, generation, length uint32, arenaSize uint64) (uint64, bool) {
	if view == nil || generation == 0 || length == 0 || lease >= leaseViewSlots {
		return 0, false
	}
	e := int(lease) * leaseViewEntry
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(&view[e+leaseEntryOffGeneration]))) != generation {
		return 0, false
	}
	span := atomic.LoadUint32((*uint32)(unsafe.Pointer(&view[e+leaseEntryOffLen])))
	off := atomic.LoadUint64((*uint64)(unsafe.Pointer(&view[e+leaseEntryOffOffset])))
	// Seqlock read (the host's writer is view_publish): if the entry was
	// retired or reused while the span was read, the generation has changed.
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(&view[e+leaseEntryOffGeneration]))) != generation {
		return 0, false
	}
	if length > span || off > arenaSize || arenaSize-off < uint64(length) {
		return 0, false
	}
	return off, true
}

func (c *Control) SlotSize() uint32     { return binary.LittleEndian.Uint32(c.mem[ctlOffSlot:]) }
func (c *Control) InlineBudget() uint32 { return binary.LittleEndian.Uint32(c.mem[ctlOffInline:]) }
func (c *Control) QueueCapacity() uint32 {
	return binary.LittleEndian.Uint32(c.mem[ctlOffQueueCap:])
}
func (c *Control) ArenaSize() uint64 { return binary.LittleEndian.Uint64(c.mem[ctlOffArena:]) }

// Epoch is the current host epoch (bumped on restart).
func (c *Control) Epoch() uint64 {
	return atomic.LoadUint64(u64ptr(c.mem, ctlOffEpoch))
}

// QueuePair is a view of a client's queue-pair region: two rings plus metadata.
type QueuePair struct {
	mem      []byte
	Inbound  *Ring
	Outbound *Ring
}

// AttachQueuePair validates a mapped queue-pair region and resolves both rings.
func AttachQueuePair(mem []byte) (*QueuePair, error) {
	if len(mem) < qpBytes {
		return nil, ErrRegion
	}
	if atomic.LoadUint32((*uint32)(unsafe.Pointer(&mem[qpOffMagic]))) != qpMagic {
		return nil, ErrRegion
	}
	inOff := binary.LittleEndian.Uint32(mem[qpOffInboundOff:])
	outOff := binary.LittleEndian.Uint32(mem[qpOffOutboundOff:])
	if int(inOff) >= len(mem) || int(outOff) >= len(mem) || inOff == outOff {
		return nil, ErrRegion
	}
	inbound, err := AttachRing(mem[inOff:])
	if err != nil {
		return nil, err
	}
	outbound, err := AttachRing(mem[outOff:])
	if err != nil {
		return nil, err
	}
	return &QueuePair{mem: mem, Inbound: inbound, Outbound: outbound}, nil
}

// flags is the control region's feature-flag word.
func (c *Control) flags() uint32 {
	return atomic.LoadUint32((*uint32)(unsafe.Pointer(&c.mem[ctlOffFlags])))
}

// hostWaiting reports the host's D13 waiting flag.
func (c *Control) hostWaiting() bool {
	return atomic.LoadUint32((*uint32)(unsafe.Pointer(&c.mem[ctlOffHostWaiting]))) != 0
}

// setClientWaiting stores this client's D13 waiting flag. Go's atomics are
// sequentially consistent, which is the ordering the protocol needs.
func (q *QueuePair) setClientWaiting(waiting bool) {
	var v uint32
	if waiting {
		v = 1
	}
	atomic.StoreUint32((*uint32)(unsafe.Pointer(&q.mem[qpOffClientWaiting])), v)
}

// ControlLost reports whether the host set the sticky control_lost flag.
func (q *QueuePair) ControlLost() bool {
	return atomic.LoadUint32((*uint32)(unsafe.Pointer(&q.mem[qpOffCtlLost]))) != 0
}

// SetHeartbeat writes the client heartbeat the host reads for liveness.
func (q *QueuePair) SetHeartbeat(now uint64) {
	atomic.StoreUint64(u64ptr(q.mem, qpOffHeartbeat), now)
}
