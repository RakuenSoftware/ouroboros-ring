package otlpexport

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/RakuenSoftware/ouroboros-ring/go/ring"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/sdk/resource"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

// collector is a fake OTLP/HTTP logs endpoint. status is what it answers.
type collector struct {
	mu       sync.Mutex
	status   int
	requests int
	records  []*logspb.LogRecord
	services []string
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests++
	if c.status != http.StatusOK {
		w.WriteHeader(c.status)
		return
	}
	var req collogs.ExportLogsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for _, rl := range req.ResourceLogs {
		for _, a := range rl.GetResource().GetAttributes() {
			if a.Key == "service.name" {
				c.services = append(c.services, a.Value.GetStringValue())
			}
		}
		for _, sl := range rl.ScopeLogs {
			c.records = append(c.records, sl.LogRecords...)
		}
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	out, _ := proto.Marshal(&collogs.ExportLogsServiceResponse{})
	_, _ = w.Write(out)
}

func (c *collector) snapshot() (int, []*logspb.LogRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requests, append([]*logspb.LogRecord(nil), c.records...)
}

func newCollector(t *testing.T) (*collector, *Exporter) {
	t.Helper()
	col := &collector{status: http.StatusOK}
	srv := httptest.NewServer(col)
	t.Cleanup(srv.Close)
	out, err := otlploghttp.New(context.Background(), otlploghttp.WithEndpointURL(srv.URL+"/v1/logs"),
		otlploghttp.WithRetry(otlploghttp.RetryConfig{Enabled: false}))
	if err != nil {
		t.Fatal(err)
	}
	res := resource.NewSchemaless()
	return col, New(out, res)
}

func attr(r *logspb.LogRecord, key string) (int64, string, bool) {
	for _, a := range r.Attributes {
		if a.Key == key {
			return a.Value.GetIntValue(), a.Value.GetStringValue(), true
		}
	}
	return 0, "", false
}

// memSource is an in-memory ring of records.
type memSource struct {
	records  [][]byte
	released uint64
}

func (m *memSource) Available() uint64    { return uint64(len(m.records)) - m.released }
func (m *memSource) Peek(i uint64) []byte { return m.records[m.released+i] }
func (m *memSource) Release(n uint64)     { m.released += n }

func eventRecord(seq uint64, kind uint32, plen uint32) []byte {
	b := make([]byte, ring.TelemetryRecordLen)
	binary.LittleEndian.PutUint64(b[8:], 1_700_000_000_000_000_000+seq)
	f := ring.Frame{HdrFlags: ring.FNotification | ring.FInline, WireVersion: ring.WireVersion,
		EventKind: kind, Seq: seq, PayloadLen: plen, PayloadRef: ring.HdrLen, SrcHandle: 3, PrincipalRef: 77}
	f.Encode(b[32:])
	return b
}

func gapRecord(lost uint64) []byte {
	b := make([]byte, ring.TelemetryRecordLen)
	binary.LittleEndian.PutUint32(b[0:], uint32(ring.TelemetryGap))
	binary.LittleEndian.PutUint64(b[8:], 1_700_000_000_000_000_000)
	binary.LittleEndian.PutUint64(b[16:], lost)
	return b
}

func TestDrainIsOneRequestReleasedOnAccept(t *testing.T) {
	col, e := newCollector(t)
	src := &memSource{records: [][]byte{eventRecord(1, 900, 16), gapRecord(4), eventRecord(6, 901, 0)}}

	col.status = http.StatusServiceUnavailable
	if _, err := e.drain(context.Background(), src); err == nil {
		t.Fatal("a refused export reported success")
	}
	if src.released != 0 {
		t.Fatalf("released %d records the collector refused", src.released)
	}

	col.mu.Lock()
	col.status = http.StatusOK
	col.mu.Unlock()
	n, err := e.drain(context.Background(), src)
	if err != nil || n != 3 || src.released != 3 {
		t.Fatalf("drain: n %d released %d err %v", n, src.released, err)
	}
	requests, recs := col.snapshot()
	if requests != 2 || len(recs) != 3 {
		t.Fatalf("requests %d records %d: one drain must be one request", requests, len(recs))
	}
	if recs[0].EventName != "ouro.ring.event" || recs[1].EventName != "ouro.ring.gap" {
		t.Errorf("event names %q %q", recs[0].EventName, recs[1].EventName)
	}
	if seq, _, _ := attr(recs[0], "ouro.seq"); seq != 1 {
		t.Errorf("seq %d", seq)
	}
	if kind, _, _ := attr(recs[0], "ouro.event.kind"); kind != 900 {
		t.Errorf("kind %d", kind)
	}
	if lost, _, _ := attr(recs[1], "ouro.lost"); lost != 4 {
		t.Errorf("lost %d", lost)
	}
	if pr, _, _ := attr(recs[0], "ouro.principal_ref"); pr != 77 {
		t.Errorf("principal %d", pr)
	}
	if _, p, _ := attr(recs[0], "ouro.message.type"); p != "notification" {
		t.Errorf("pattern %q", p)
	}
	if recs[0].TimeUnixNano != 1_700_000_000_000_000_001 {
		t.Errorf("timestamp %d is not the host's", recs[0].TimeUnixNano)
	}
	if n, _ := e.drain(context.Background(), src); n != 0 {
		t.Errorf("an empty ring drained %d", n)
	}
}

// End to end against the real C host: the exporter, granted the consumer end,
// delivers the record of a published notification and of the C side's ack to
// the collector, and nothing of the payload.
func TestExportFromCHost(t *testing.T) {
	harness := os.Getenv("OURO_CONFORMANCE_HOST")
	if harness == "" {
		t.Skip("OURO_CONFORMANCE_HOST not set")
	}
	sock := filepath.Join(t.TempDir(), "bus.sock")
	cmd := exec.Command(harness, sock, "10000")
	var hout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &hout, &hout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("harness socket did not appear")
		}
	}
	client, reader, err := ring.ConnectTelemetry(context.Background(), sock, 1, 0x7E1E)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Detach()
	defer reader.Close()

	col, e := newCollector(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, reader, func(err error) { t.Log(err) }) }()

	payload := []byte("payload-never-exported")
	if err := client.Publish(1000, payload); err != nil {
		t.Fatal(err)
	}
	var notify, ack bool
	for deadline := time.Now().Add(5 * time.Second); !(notify && ack) && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		_, recs := col.snapshot()
		for _, r := range recs {
			wire, _ := proto.Marshal(r)
			if bytes.Contains(wire, []byte("payload-never")) {
				t.Fatal("a payload byte reached the collector")
			}
			switch kind, _, _ := attr(r, "ouro.event.kind"); kind {
			case 1000:
				notify = true
				if h, _, _ := attr(r, "ouro.source.handle"); uint32(h) != client.Handle {
					t.Errorf("source handle %d, client %d", h, client.Handle)
				}
			case 1001:
				ack = true
			}
		}
	}
	if !notify || !ack {
		t.Fatalf("collector saw notify %v ack %v\nharness:\n%s", notify, ack, hout.String())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop when its context ended: the doorbell wait was not interrupted")
	}
}
