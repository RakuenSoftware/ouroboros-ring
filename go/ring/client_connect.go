package ring

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

// ConnectClient attaches to a daemon's module ring as a requesting principal.
//
// This is the counterpart to a module process attaching to serve: the daemon
// hosting the ring admits this client under a grant naming the kinds it may
// request, so a caller is a principal of that ring rather than a second ring.
//
// A daemon restart can leave its old socket pathname in place until the new
// host replaces it, so a refused connection is retried until ctx ends. A
// missing socket or a policy refusal fails immediately: those are configuration
// mistakes and waiting only hides them.
func ConnectClient(ctx context.Context, socketPath string, principalClass, principalRef uint32) (*Client, error) {
	if socketPath == "" {
		return nil, ErrModuleConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		err = unix.Connect(fd, &unix.SockaddrUnix{Name: socketPath})
		if err == nil {
			client, attachErr := AttachAs(fd, principalClass, principalRef)
			unix.Close(fd)
			// D15: a host that watches this process needs no heartbeat.
			if attachErr == nil && !client.HasProcessLiveness() {
				keepAlive(ctx, client, clientHeartbeatInterval)
			}
			return client, attachErr
		}
		unix.Close(fd)
		if !errors.Is(err, unix.ECONNREFUSED) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(moduleConnectRetry):
		}
	}
}

// heartbeater is the one thing keepAlive needs, so the liveness contract can be
// exercised without a live host.
type heartbeater interface{ Heartbeat(now uint64) }

// keepAlive advances this client's heartbeat until ctx ends.
//
// The host reaps a slot whose heartbeat stops advancing, which is how it
// notices a client that died. A serving module gets this for free because its
// poll loop heartbeats every pass, but a caller is idle between calls -- so it
// was reaped shortly after attaching, and every later request was dropped by a
// host that no longer had a slot to route from. Nothing reported that: the
// caller sat waiting for a reply to a request nobody received, and only its own
// deadline ended the call.
func keepAlive(ctx context.Context, client heartbeater, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if now := monotonicNowNS(); now != 0 {
					client.Heartbeat(now)
				}
			}
		}
	}()
}

// ErrTelemetryNotGranted is returned when the host attached the client but
// did not hand it the telemetry consumer end (D16): telemetry is off, the
// embedding did not grant this principal, or another consumer holds it.
var ErrTelemetryNotGranted = errors.New("ring: telemetry consumer end not granted")

// ConnectTelemetry attaches to a daemon's ring as its telemetry consumer
// (D16). Refused connections are retried as ConnectClient retries them.
func ConnectTelemetry(ctx context.Context, socketPath string, principalClass, principalRef uint32) (*Client, *TelemetryReader, error) {
	if socketPath == "" {
		return nil, nil, ErrModuleConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			return nil, nil, err
		}
		err = unix.Connect(fd, &unix.SockaddrUnix{Name: socketPath})
		if err == nil {
			client, reader, attachErr := AttachTelemetry(fd, principalClass, principalRef)
			unix.Close(fd)
			if attachErr != nil {
				return nil, nil, attachErr
			}
			if reader == nil {
				client.Detach()
				return nil, nil, ErrTelemetryNotGranted
			}
			if !client.HasProcessLiveness() {
				keepAlive(ctx, client, clientHeartbeatInterval)
			}
			return client, reader, nil
		}
		unix.Close(fd)
		if !errors.Is(err, unix.ECONNREFUSED) {
			return nil, nil, err
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(moduleConnectRetry):
		}
	}
}
