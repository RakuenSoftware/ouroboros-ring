package ring

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

func notificationEvent(kind uint32, payload string) Event {
	return Event{Frame: Frame{HdrFlags: FNotification | FInline, EventKind: kind,
		PayloadLen: uint32(len(payload))}, Payload: []byte(payload)}
}

func overflowEvent(shedKind uint32) Event {
	body := make([]byte, overflowLen)
	binary.LittleEndian.PutUint64(body[overflowOffShedSeq:], 41)
	binary.LittleEndian.PutUint32(body[overflowOffShedKind:], shedKind)
	return Event{Frame: Frame{HdrFlags: FNotification | FControl | FInline, EventKind: KindOverflow,
		PayloadLen: overflowLen}, Payload: body}
}

// recorder collects what a process's handlers were given.
type recorder struct {
	mu       sync.Mutex
	notes    []string
	overflow []uint32
}

func (r *recorder) notify(kind uint32, payload []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notes = append(r.notes, string(payload))
}

func (r *recorder) missed(kind uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.overflow = append(r.overflow, kind)
}

func (r *recorder) waitFor(t *testing.T, notes, overflow int) ([]string, []uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		if len(r.notes) >= notes && len(r.overflow) >= overflow {
			n, o := append([]string(nil), r.notes...), append([]uint32(nil), r.overflow...)
			r.mu.Unlock()
			return n, o
		}
		r.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d notifications and %d overflows", notes, overflow)
	return nil, nil
}

func TestModuleRuntimeDeliversNotificationsAndOverflow(t *testing.T) {
	const kind uint32 = 6501
	fake := &fakeModuleRing{budget: 256}
	fake.input = []Event{
		notificationEvent(900, "first"),
		// A control frame that is not an overflow is the runtime's, not the
		// module's: it must not reach Notify.
		{Frame: Frame{HdrFlags: FNotification | FControl, EventKind: KindCapabilityAbsent}},
		overflowEvent(901),
		notificationEvent(900, "second"),
	}
	rec := &recorder{}
	config := ModuleProcessConfig{SocketPath: "/unused", ModuleName: "go-notify",
		PrincipalClass: 1, PrincipalRef: 9, Stages: []ModuleStage{{EventKind: kind, StageID: 1}},
		Handler: func(ModuleInvocation, []byte) ([]byte, ModuleStatus) {
			return nil, ModuleStatusOK
		},
		Notify: rec.notify, Overflow: rec.missed}
	stages, err := validateModuleConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runModuleClient(ctx, config, stages, fake) }()
	notes, overflow := rec.waitFor(t, 2, 1)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(notes) != 2 || notes[0] != "first" || notes[1] != "second" {
		t.Fatalf("notifications = %q, want first then second", notes)
	}
	if len(overflow) != 1 || overflow[0] != 901 {
		t.Fatalf("overflow = %v, want the shed kind 901", overflow)
	}
}

// publishingModuleRing is a module ring that can publish, and is full for the
// first few attempts.
type publishingModuleRing struct {
	fakeModuleRing
	full      int
	published []Event
}

func (p *publishingModuleRing) Publish(kind uint32, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.full > 0 {
		p.full--
		return ErrWouldBlock
	}
	p.published = append(p.published, notificationEvent(kind, string(payload)))
	return nil
}

func TestModuleRuntimeStartedPublisherRetriesBackpressure(t *testing.T) {
	const kind uint32 = 6502
	fake := &publishingModuleRing{fakeModuleRing: fakeModuleRing{budget: 256}, full: 3}
	started := make(chan Publisher, 1)
	config := ModuleProcessConfig{SocketPath: "/unused", ModuleName: "go-publish",
		PrincipalClass: 1, PrincipalRef: 9, Stages: []ModuleStage{{EventKind: kind, StageID: 1}},
		Handler: func(ModuleInvocation, []byte) ([]byte, ModuleStatus) {
			return nil, ModuleStatusOK
		},
		Started: func(p Publisher) { started <- p }}
	stages, err := validateModuleConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runModuleClient(ctx, config, stages, fake) }()
	publisher := <-started
	if err := publisher.Publish(ctx, 950, []byte("changed")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	fake.mu.Lock()
	published := append([]Event(nil), fake.published...)
	fake.mu.Unlock()
	if len(published) != 1 || published[0].Frame.EventKind != 950 ||
		string(published[0].Payload) != "changed" {
		t.Fatalf("published = %+v, want one kind-950 notification after the ring drained", published)
	}
	full, stop := context.WithCancel(context.Background())
	fake.mu.Lock()
	fake.full = 1 << 30
	fake.mu.Unlock()
	time.AfterFunc(20*time.Millisecond, stop)
	if err := publisher.Publish(full, 950, nil); err == nil {
		t.Fatal("a publish that never finds room must give up when its context ends")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPublisherWithoutAPublishingRing(t *testing.T) {
	if err := (ringPublisher{ring: &fakeModuleRing{}}).Publish(context.Background(), 1, nil); err !=
		ErrPublishUnsupported {
		t.Fatalf("err = %v, want ErrPublishUnsupported", err)
	}
}

func TestNotifierReportsWhatItHadToDrop(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	delivered := 0
	var missed []uint32
	n := newNotifier(func(uint32, []byte) {
		<-release
		mu.Lock()
		delivered++
		mu.Unlock()
	}, func(kind uint32) {
		mu.Lock()
		missed = append(missed, kind)
		mu.Unlock()
	})
	const total = notifyQueueMax + 50
	for i := 0; i < total; i++ {
		if !n.offer(notificationEvent(777, "x")) {
			t.Fatal("a data notification must be taken")
		}
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got, gotMissed := delivered, len(missed)
		mu.Unlock()
		if gotMissed > 0 && got >= notifyQueueMax {
			break
		}
		time.Sleep(time.Millisecond)
	}
	n.close()
	mu.Lock()
	defer mu.Unlock()
	if delivered >= total || delivered < notifyQueueMax {
		t.Fatalf("delivered %d of %d; want the queue's worth and no more", delivered, total)
	}
	if len(missed) != 1 || missed[0] != 777 {
		t.Fatalf("missed = %v, want kind 777 reported once", missed)
	}
}

func TestNotifierLeavesRepliesAndRequestsAlone(t *testing.T) {
	n := newNotifier(func(uint32, []byte) {}, func(uint32) {})
	defer n.close()
	for _, ev := range []Event{
		{Frame: Frame{HdrFlags: FReply, EventKind: 900, CorrelationID: 3}},
		{Frame: Frame{HdrFlags: FRequest, EventKind: 900, CorrelationID: 4}},
		{Frame: Frame{HdrFlags: FNotification | FControl, EventKind: KindCapabilityDenied}},
		{Frame: Frame{HdrFlags: FNotification, EventKind: KindEpochChange}},
	} {
		if n.offer(ev) {
			t.Fatalf("offer took %+v, which is not a notification for the module", ev.Frame)
		}
	}
}

func TestConcurrentCallerDeliversNotificationsBesideCalls(t *testing.T) {
	const kind uint32 = 6601
	fake := &fakeCallerRing{budget: 256}
	fake.replyTo = func(f *fakeCallerRing, kind uint32, correlation uint64, body []byte) {
		f.mu.Lock()
		f.pending = append(f.pending, notificationEvent(910, "beside"))
		f.mu.Unlock()
		f.queueResult(kind, correlation, ModuleStatusOK, body, 1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	caller := newConcurrentModuleCaller(ctx, fake)
	rec := &recorder{}
	caller.OnNotification(910, func(payload []byte) { rec.notify(910, payload) })
	caller.OnOverflow(rec.missed)
	body, err := caller.Call(ctx, kind, 1, 0, time.Second, []byte("ping"))
	if err != nil || string(body) != "ping" {
		t.Fatalf("call = %q, %v; a notification must not get in the way of the reply", body, err)
	}
	fake.mu.Lock()
	fake.pending = append(fake.pending, overflowEvent(910))
	fake.mu.Unlock()
	notes, overflow := rec.waitFor(t, 1, 1)
	if notes[0] != "beside" || overflow[0] != 910 {
		t.Fatalf("notes %q overflow %v", notes, overflow)
	}
	caller.CloseAndWait()
}
