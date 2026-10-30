package ring

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

// D13 and D14 surfaces the runtimes look for on their ring. *Client has both;
// a ring without them (an in-memory test double) keeps the polling behaviour.
type doorbellRing interface {
	Wait(timeout time.Duration) (bool, error)
	Interrupt()
}

type publishingRing interface {
	Publish(kind uint32, payload []byte) error
}

// Publisher sends notifications on the slot it was handed by (D14). It is safe
// for concurrent use.
type Publisher interface {
	Publish(ctx context.Context, kind uint32, payload []byte) error
}

// ErrPublishUnsupported is returned when the ring under a runtime cannot
// publish.
var ErrPublishUnsupported = errors.New("ring: this ring cannot publish")

// publishRetry is how often a publish that found the outbound ring full is
// retried. A full ring is backpressure from the host, which is rare and brief.
const publishRetry = time.Millisecond

type ringPublisher struct{ ring any }

func (p ringPublisher) Publish(ctx context.Context, kind uint32, payload []byte) error {
	publisher, ok := p.ring.(publishingRing)
	if !ok {
		return ErrPublishUnsupported
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		err := publisher.Publish(kind, payload)
		if !errors.Is(err, ErrWouldBlock) {
			return err
		}
		timer := time.NewTimer(publishRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// notifyQueueMax bounds notifications accepted but not yet handled. Past it a
// notification is dropped and its kind reported as missed, which is the same
// thing the host does for a full ring (D6): the subscriber is told, and
// reconciles, rather than the process growing without bound.
const notifyQueueMax = 4096

type notification struct {
	kind    uint32
	payload []byte
}

// notifier delivers notifications and overflow to a process's handlers on one
// goroutine, in the order the ring delivered them, so a slow handler delays
// the next notification and never the ring.
type notifier struct {
	notify   func(kind uint32, payload []byte)
	overflow func(kind uint32)

	mu    sync.Mutex
	cond  *sync.Cond
	queue []notification
	// inflight counts the batch taken from queue and not yet handled, so the
	// bound covers everything accepted, not only what is still waiting.
	inflight int
	missed   map[uint32]bool
	closed   bool
	done     chan struct{}
}

func newNotifier(notify func(uint32, []byte), overflow func(uint32)) *notifier {
	n := &notifier{notify: notify, overflow: overflow, missed: map[uint32]bool{},
		done: make(chan struct{})}
	n.cond = sync.NewCond(&n.mu)
	go n.run()
	return n
}

// offer takes a polled event if it is a notification or an overflow, and
// reports whether it did. The payload is copied: the event's is valid only
// until the next Poll.
func (n *notifier) offer(event Event) bool {
	f := event.Frame
	if f.HdrFlags&FNotification == 0 {
		return false
	}
	if f.HdrFlags&FControl != 0 {
		if f.EventKind != KindOverflow {
			return false
		}
		if len(event.Payload) >= overflowLen {
			n.miss(binary.LittleEndian.Uint32(event.Payload[overflowOffShedKind:]))
		}
		return true
	}
	if f.EventKind < KindModuleBase {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || n.notify == nil {
		return true
	}
	if len(n.queue)+n.inflight >= notifyQueueMax {
		n.missed[f.EventKind] = true
	} else {
		n.queue = append(n.queue, notification{kind: f.EventKind,
			payload: append([]byte(nil), event.Payload...)})
	}
	n.cond.Signal()
	return true
}

func (n *notifier) miss(kind uint32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || n.overflow == nil {
		return
	}
	n.missed[kind] = true
	n.cond.Signal()
}

func (n *notifier) run() {
	defer close(n.done)
	for {
		n.mu.Lock()
		for !n.closed && len(n.queue) == 0 && len(n.missed) == 0 {
			n.cond.Wait()
		}
		if n.closed {
			n.mu.Unlock()
			return
		}
		queue := n.queue
		n.queue = nil
		n.inflight = len(queue)
		var missed []uint32
		for kind := range n.missed {
			missed = append(missed, kind)
		}
		clear(n.missed)
		n.mu.Unlock()
		for _, note := range queue {
			n.notify(note.kind, note.payload)
			n.mu.Lock()
			n.inflight--
			n.mu.Unlock()
		}
		if n.overflow != nil {
			for _, kind := range missed {
				n.overflow(kind)
			}
		}
	}
}

// close stops delivery and waits for a handler in progress to return.
func (n *notifier) close() {
	if n == nil {
		return
	}
	n.mu.Lock()
	n.closed = true
	n.cond.Signal()
	n.mu.Unlock()
	<-n.done
}
