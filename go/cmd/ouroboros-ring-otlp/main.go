package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/RakuenSoftware/ouroboros-ring/go/ring"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Exit codes: 0 stopped by signal, 1 setup failed, 3 the host went away (a
// supervisor restarts the exporter, which attaches to the new host).
func main() {
	socket := flag.String("socket", "", "the host's ring socket")
	class := flag.Uint("principal-class", 0, "attach principal class")
	ref := flag.Uint("principal-ref", 0, "attach principal ref the host grants telemetry to")
	service := flag.String("service-name", "ouroboros-ring", "OTLP service.name, unless OTEL_SERVICE_NAME is set")
	flag.Parse()
	if *socket == "" {
		fmt.Fprintln(os.Stderr, "ouroboros-ring-otlp: -socket is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Endpoint, headers, TLS and timeout come from the standard
	// OTEL_EXPORTER_OTLP_* variables. Retries are this process's own: records
	// stay in the ring until accepted, so the SDK must not also queue them.
	out, err := otlploghttp.New(ctx, otlploghttp.WithRetry(otlploghttp.RetryConfig{Enabled: false}))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ouroboros-ring-otlp: exporter: %v\n", err)
		os.Exit(1)
	}
	defer out.Shutdown(context.Background())
	name := os.Getenv("OTEL_SERVICE_NAME")
	if name == "" {
		name = *service
	}
	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", name)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ouroboros-ring-otlp: resource: %v\n", err)
		os.Exit(1)
	}

	client, reader, err := ring.ConnectTelemetry(ctx, *socket, uint32(*class), uint32(*ref))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ouroboros-ring-otlp: attach %s: %v\n", *socket, err)
		os.Exit(1)
	}
	defer client.Detach()
	defer reader.Close()

	report := func(err error) { fmt.Fprintf(os.Stderr, "ouroboros-ring-otlp: %v\n", err) }
	if err := newExporter(out, res).run(ctx, reader, report); errors.Is(err, ring.ErrEpoch) {
		fmt.Fprintln(os.Stderr, "ouroboros-ring-otlp: the host went away")
		os.Exit(3)
	}
}
