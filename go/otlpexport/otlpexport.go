// Package otlpexport is the reference D16 telemetry exporter: it sends every
// record a host writes to its telemetry ring off the host, as an OTLP log
// record, as soon as it is woken, and hands records back to the host only once
// the collector has accepted them.
//
// It has no batching timer. A wakeup drains everything available into one
// request; records that arrive while that request is in flight go in the next.
// Its only timer is the retry backoff while the collector refuses or cannot be
// reached, and while it waits the ring fills and the host counts what it cannot
// hold, which reaches the collector as a gap record.
//
// cmd/ouroboros-ring-otlp is the command around it. An embedding that applies
// its own collector policy or identity runs Exporter.Run itself.
package otlpexport

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/RakuenSoftware/ouroboros-ring/go/ring"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

const scope = "github.com/RakuenSoftware/ouroboros-ring/go/otlpexport"

// maxBatch bounds one request. A larger backlog is sent as several requests,
// back to back.
const maxBatch = 2048

// Source is the consumer end of the telemetry ring; *ring.TelemetryReader is one.
type Source interface {
	Available() uint64
	Peek(i uint64) []byte
	Release(n uint64)
}

// collect is a processor that holds what one drain emits, so the drain sends it
// as one request. It never exports on its own: no timer, no queue.
type collect struct{ records []sdklog.Record }

func (c *collect) OnEmit(_ context.Context, r *sdklog.Record) error {
	c.records = append(c.records, r.Clone())
	return nil
}
func (c *collect) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (c *collect) Shutdown(context.Context) error                         { return nil }
func (c *collect) ForceFlush(context.Context) error                       { return nil }

// Exporter turns telemetry records into OTLP log records.
type Exporter struct {
	buf    *collect
	logger log.Logger
	out    sdklog.Exporter
	now    func() time.Time
}

// New sends through out, under the resource res (service.name and the like).
func New(out sdklog.Exporter, res *resource.Resource) *Exporter {
	buf := &collect{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(buf), sdklog.WithResource(res))
	return &Exporter{buf: buf, logger: provider.Logger(scope), out: out, now: time.Now}
}

// drain sends up to maxBatch available records in one request and releases
// them once it is accepted. It reports how many it released.
func (e *Exporter) drain(ctx context.Context, src Source) (uint64, error) {
	n := src.Available()
	if n == 0 {
		return 0, nil
	}
	if n > maxBatch {
		n = maxBatch
	}
	e.buf.records = e.buf.records[:0]
	observed := e.now()
	for i := uint64(0); i < n; i++ {
		e.logger.Emit(ctx, toLog(src.Peek(i), observed))
	}
	if err := e.out.Export(ctx, e.buf.records); err != nil {
		return 0, err
	}
	src.Release(n)
	return n, nil
}

func pattern(flags uint16) string {
	switch {
	case flags&ring.FNotification != 0:
		return "notification"
	case flags&ring.FRequest != 0:
		return "request"
	case flags&ring.FReply != 0:
		return "reply"
	case flags&ring.FCancel != 0:
		return "cancel"
	}
	return "invalid"
}

func placement(flags uint16) string {
	switch {
	case flags&ring.FInline != 0:
		return "inline"
	case flags&ring.FArena != 0:
		return "arena"
	}
	return "none"
}

// toLog maps one record. Every attribute is a header field: the ring never
// puts a payload byte in a record, so none can reach the collector.
func toLog(raw []byte, observed time.Time) log.Record {
	var out log.Record
	out.SetObservedTimestamp(observed)
	rec, err := ring.DecodeTelemetryRecord(raw)
	if err != nil {
		out.SetTimestamp(observed)
		out.SetEventName("ouro.ring.malformed")
		out.SetSeverity(log.SeverityError)
		out.SetBody(attribute.StringValue("malformed ring telemetry record"))
		return out
	}
	out.SetTimestamp(time.Unix(0, int64(rec.RealtimeNS)))
	if rec.Type == ring.TelemetryGap {
		out.SetEventName("ouro.ring.gap")
		out.SetSeverity(log.SeverityWarn)
		out.SetBody(attribute.StringValue("ring telemetry lost records"))
		out.AddAttributes(attribute.Int64("ouro.lost", int64(rec.Lost)))
		return out
	}
	f := rec.Frame
	control := f.HdrFlags&ring.FControl != 0
	out.SetEventName("ouro.ring.event")
	out.SetBody(attribute.StringValue("ring event"))
	out.SetSeverity(log.SeverityInfo)
	switch f.EventKind {
	case ring.KindOverflow, ring.KindProducerReaped, ring.KindCapabilityDenied:
		out.SetSeverity(log.SeverityWarn)
	}
	out.AddAttributes(
		attribute.Int64("ouro.seq", int64(f.Seq)),
		attribute.Int64("ouro.event.kind", int64(f.EventKind)),
		attribute.String("ouro.message.type", pattern(f.HdrFlags)),
		attribute.String("ouro.payload.placement", placement(f.HdrFlags)),
		attribute.Int64("ouro.payload.bytes", int64(f.PayloadLen)),
		attribute.Int64("ouro.principal_ref", int64(f.PrincipalRef)),
		attribute.Int64("ouro.source.handle", int64(f.SrcHandle)),
		attribute.Bool("ouro.control", control),
	)
	if f.CorrelationID != 0 {
		out.AddAttributes(attribute.Int64("ouro.correlation_id", int64(f.CorrelationID)))
	}
	if control {
		out.AddAttributes(attribute.Int64("ouro.destination.handle", int64(f.DstHandle)))
	}
	return out
}

// Reader is what Run drains and sleeps on; *ring.TelemetryReader is one.
type Reader interface {
	Source
	Wait(time.Duration) (bool, error)
	Interrupt()
}

// Run drains whenever records are available, sleeping on the doorbell between.
// report receives export and wait errors; Run retries after them. It returns
// nil when ctx ends, or ring.ErrEpoch when the host has gone.
func (e *Exporter) Run(ctx context.Context, r Reader, report func(error)) error {
	stop := context.AfterFunc(ctx, r.Interrupt)
	defer stop()
	backoff := time.Duration(0)
	for ctx.Err() == nil {
		n, err := e.drain(ctx, r)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			report(fmt.Errorf("export: %w", err))
			backoff = min(max(2*backoff, 100*time.Millisecond), 5*time.Second)
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			continue
		}
		backoff = 0
		if n > 0 {
			continue // drain the rest before sleeping
		}
		if _, err := r.Wait(-1); err != nil {
			if errors.Is(err, ring.ErrEpoch) {
				return err
			}
			report(fmt.Errorf("wait: %w", err))
		}
	}
	return nil
}
