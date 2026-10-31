# Changelog

What each release changed for a consumer. Every release is tagged `v<VERSION>`
and `go/v<VERSION>`, and the pull requests named here carry the full evidence.
Within 0.x a minor release may break consumers
([versions and releases](docs/versions-and-releases.md)).

## 0.4.0 (2026-10-31)

The telemetry consumer is paced, so a busy host no longer wakes it once per
pump (#15).

- **Pacing.** The telemetry region publishes `drain_interval_ns` and
  `idle_after_ns`, taken from reserved space. A consumer that found records
  recently drains on the interval without asking to be woken; an idle one
  sleeps on the doorbell. Defaults 10 µs and 1 ms, set with
  `OURO_TELEMETRY_DRAIN_INTERVAL_US`, `OURO_TELEMETRY_IDLE_AFTER_US` or
  `ouro_host_telemetry_pacing`. The C and Go readers follow them, and so does
  the exporter, unchanged.
- **Cost.** On a Threadripper 3970X with pinned threads, telemetry now adds 5–6
  ns per event instead of 17–30, and the host rings the doorbell 0–2 times in
  2 million events instead of about 30,000.
- **Breaking, C ABI.** `ouro_telemetry_reader_t` gained a field. The wire,
  layout and contract versions are unchanged, and a 0.3.0 host's zeroed fields
  give 0.3.0 behaviour.
- **Gated.** `scripts/check_perf_budget.sh` holds the doorbell-write count of a
  busy consumer under 400. The bench pins its threads and prints
  `telemetry_wakes=`; the host counts `telemetry_wakes`.
- **Fixed.** A `Release` build defined `NDEBUG`, which compiled every `assert`
  out of the C tests and harnesses. They now keep their assertions, and CI
  builds and runs `Release` too.

## 0.3.0 (2026-10-30)

Every event the host stamps is described off the host the moment it is stamped
([D16](spec/decisions.md#d16-tap-telemetry-leaves-the-host-the-moment-an-event-is-stamped), #14).

- **The record and region.** A 128-byte, content-free record per stamped event,
  written before the tap and routing into a ring in its own region. A full ring
  is counted and reported by a gap record, never by blocking.
- **The grant.** `ouro_host_telemetry_enable`, `ouro_host_grant_telemetry`, and
  `telemetry=yes` in a runtime grant file. Attach capability bit 2 hands one
  slot two more descriptors.
- **Readers.** C `ouro_client_attach_telemetry` and `ouro_telemetry_*`; Go
  `ring.AttachTelemetry`, `ConnectTelemetry` and `TelemetryReader`. Rust holds
  the layout only.
- **The reference exporter.** `go/cmd/ouroboros-ring-otlp`, with its core in
  `go/otlpexport`, sends OTLP/HTTP logs and releases records only once the
  collector accepts them. `go/ring` still depends on nothing beyond the
  standard library and `golang.org/x/sys`.
- **Compatibility.** `layout_version` stays 1. A grant file whose `telemetry`
  value is neither `yes` nor `no` is now refused as malformed.

Also: the module-call docs say a service stop must interrupt the client (#13).

## 0.2.0 (2026-10-30)

An idle ring makes no wakeups (#12).

- **Doorbells ([D13](spec/decisions.md#d13-doorbells-idle-ends-sleep-in-the-kernel)).**
  Each end of a queue pair sleeps on an `eventfd` and is signalled only while
  asleep. `ouro_host_wait` (split into `_begin` and `_end` for a locked
  embedding), `ouro_client_wait`, `Client.Wait` and `Client::wait`. The module
  runtimes and callers in all three languages sleep on them, and the runtime's
  accept loop blocks in `poll(2)`.
- **Notifications ([D14](spec/decisions.md#d14-notifications-reach-module-processes-and-callers)).**
  Module processes and concurrent callers deliver subscribed notifications and
  `overflow` to handlers, and can publish on their own slot. The Go concurrent
  caller takes per-kind overflow handlers.
- **Liveness ([D15](spec/decisions.md#d15-liveness-is-the-process-not-a-timer)).**
  A host holding a pidfd on a client releases its slot when the process exits
  and grants it process liveness, so it need not heartbeat. Detach is announced.
  A client watches the host's process too.
- **Compatibility.** The attach request grew to 20 bytes with a `capabilities`
  word, negotiated so either side may be older. `layout_version` stays 1.

## 0.1.3 (2026-10-29)

- **Module client pool.** `ouro_module_client_pool_*` makes many module calls
  at once over a pool of attached clients, waiting no longer than each call's
  deadline (#10). A convenience of the C implementation, not part of the
  contract.
- **Documentation.** `docs/`, with the architecture and a page for each part of
  the ring (#9, #11).

## 0.1.2 (2026-10-28)

Machinery aimee had built on top of the ring moved into it (#8).

- **`ouro_host_attach_local`** attaches a client to a host in the same process.
- **`ouro_capture_flush_fd`** appends a capture stream to a file.
- **Flow-control tests** for block, shed, `control_lost` and `producer_reaped`,
  which until then were tested only in aimee.
- **The dispatch budget:** `c/bench/dispatch.c`, `c/bench/baseline.json` and
  `scripts/check_perf_budget.sh`, now run in CI.

## 0.1.1 (2026-10-27)

- **GCC workaround.** The ring's own build turns off GCC's IPA bit-CP pass,
  which in GCC 15.2 was observed to pass a live count as 0 in a consumer built
  `-Os -flto` (#7). A consumer using the ring's CMake build gets this without
  knowing about it.

## 0.1.0 (2026-10-27)

The ring moved out of aimee as a contract with three reference implementations
held to it (#1).

- **The contract.** `spec/` (requirements R1 to R6, decisions D1 to D12) and
  `vectors/`, generated from the spec and checked against every
  implementation.
- **C.** The host, the client and the module runtime, as `libouroboros-ring`
  and `libouroboros-ring-client`, with an installed CMake package and
  pkg-config files. Owned in-process module services (#3).
- **Go and Rust.** Clients, module callers and module runtimes; pure Go with no
  cgo, and Rust depending only on `libc`.
- **Releases** are cut from `VERSION` on `main` (#6). Nothing in the repository
  builds, tests or runs against another project (#5).
