# Configuration

The ring's libraries read two environment variables. Everything else is set by
the embedding through the C API, because the embedding owns policy. This page
lists every environment variable, flag and build option the reference
implementations read, and the API settings an operator is most likely to ask
about.

## Host operator

| Variable | Default | Read by | Sets |
|---|---|---|---|
| `OURO_TELEMETRY_DRAIN_INTERVAL_US` | 10 | `ouro_host_telemetry_enable` | how often a busy telemetry consumer looks for records, in microseconds; 0 makes it sleep on the doorbell after every drain |
| `OURO_TELEMETRY_IDLE_AFTER_US` | 1000 | `ouro_host_telemetry_enable` | how long a consumer goes without records before it sleeps on the doorbell, in microseconds |

Both are read once, when telemetry is enabled, and published in the telemetry
region, so every consumer in any language follows the host's values. A value
that is not a plain decimal number keeps the default. An embedding can set
them instead with `ouro_host_telemetry_pacing`, in nanoseconds. What they trade
is in [telemetry: pacing](telemetry.md#pacing).

## The reference exporter

`ouroboros-ring-otlp` takes its ring settings as flags and its collector
settings from the standard OpenTelemetry variables.

| Setting | Default | Sets |
|---|---|---|
| `-socket` | none, required | the host's ring socket |
| `-principal-class` | 0 | the principal class it attaches as |
| `-principal-ref` | 0 | the principal ref the host grants telemetry to |
| `-service-name` | `ouroboros-ring` | OTLP `service.name`, unless `OTEL_SERVICE_NAME` is set |
| `OTEL_EXPORTER_OTLP_*` | the OpenTelemetry defaults | endpoint, headers, TLS and timeouts |

It exits 3 when the host goes away, so a supervisor can restart it against the
new host.

## Set through the API

These are not environment variables, and the reference host reads none of them
from one. They are listed because they are the settings that change how a
deployment behaves.

| Setting | Where | Default |
|---|---|---|
| telemetry ring size | `ouro_host_telemetry_enable(h, capacity)` | 0 means 65,536 records, 8 MiB |
| shed or block per kind | `ouro_host_set_kind_policy` | block |
| stale heartbeat window | `ouro_runtime_config_t.stale_after_ns` | none; the embedding chooses, and 0 turns heartbeat reaping off |
| socket path, mode and backlog | `ouro_runtime_config_t` | none; the embedding chooses |
| who may attach, and with which kinds | grant files, `ouro_runtime_policy_load_dir` | a missing directory refuses everyone ([attach and admission](attach-and-admission.md#the-runtime-listener)) |

## Benchmarks and tests

| Variable | Read by | Sets |
|---|---|---|
| `OURO_BENCH_TELEMETRY` | `c/bench/dispatch.c` | 1 runs the benchmark with the telemetry hand-off on |
| `OURO_BENCH_HOST_CPU`, `OURO_BENCH_DRAIN_CPU` | `c/bench/dispatch.c` | the CPUs the host and telemetry drain threads are pinned to ([performance](performance.md)) |
| `OURO_CONFORMANCE_HOST` | the Go and Rust conformance tests | path to the built `ouroboros-ring-conformance-host`; the Go tests skip without it, and the Rust test is `#[ignore]`d and requires it |
| `OURO_TEST_MODULE_CALLER` | the module-runtime harness | path to another language's module caller, which must reach the same outcomes as the C caller against the C runtime's module |
| `TMPDIR` | the C and Rust socket tests | where test sockets go; keep it short, because a Unix socket path is limited to 108 bytes |

## Build options

| CMake option | Default | Does |
|---|---|---|
| `OURO_TSAN` | `OFF` | builds the arena ThreadSanitizer test |
| `CMAKE_BUILD_TYPE` | none | `Release` defines `NDEBUG` for the libraries; tests and harnesses keep their assertions regardless |

The ring's own build also turns off GCC's IPA bit-CP for its sources, a
compiler workaround, so a consumer that compiles the sources itself instead of
using that build has to apply it too
([versions and releases](versions-and-releases.md#release-versions)).
