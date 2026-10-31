# Performance

Speed is a guardrail for the ring, not the point of it. The budget is a set of
absolute ceilings that catch a gross regression, such as a system call or a
lock on the per-event path, rather than a comparison with the last build. This
page says what is measured, how to measure it so the number means something,
and what the budget enforces.

## What is measured

`c/bench/dispatch.c` measures **dispatch overhead**: a producer writing its
outbound ring, the host routing, and a subscriber reading its inbound ring,
with a 16-byte inline payload and no work on it. It is the cost the ring adds
to move one event. It reports the median per-event time over batched samples,
and the p99.

With `OURO_BENCH_TELEMETRY=1` the host also writes a telemetry record per event
([telemetry](telemetry.md)), and a second thread drains them through the
reader's own paced wait, as an exporter would. That run also prints how many
times the host wrote the telemetry doorbell.

```sh
cmake -S c -B build/c && cmake --build build/c --target ouroboros-ring-dispatch-bench
build/c/ouroboros-ring-dispatch-bench 2000000
# dispatch_overhead_ns=...
# dispatch_overhead_p99_ns=...
OURO_BENCH_TELEMETRY=1 build/c/ouroboros-ring-dispatch-bench 2000000
# ... and telemetry_wakes=...
```

The benchmark target is always built `-O2`, whatever the rest of the build
uses.

## A number belongs to its machine

**A result compares only with results from the same machine, with the same
pinning.** This repository's own record shows why: the benchmark reported
83 ns per event in PRs #8 and #12 and 127 ns in PR #15, on different machines.
The code changed in between too, and nothing in the record separates the two
effects, so the pair says nothing about the ring.

Placement matters as much as the machine. On a Threadripper 3970X with pacing
off (the 0.3.0 behaviour), telemetry cost 144 ns per event with both threads on
one CCX and 157 ns with them on different CCXs, because each record's cache
lines crossed further. So pin both threads, and say where they are:

| Variable | Pins |
|---|---|
| `OURO_BENCH_HOST_CPU` | the host thread (producer, routing and subscriber) |
| `OURO_BENCH_DRAIN_CPU` | the telemetry drain thread |

To compare two builds, run both on one idle machine, pinned the same way,
several times each, and report the machine and the cores with every figure.
Judge "idle" by CPU use, not load average, which also counts processes waiting
on I/O: a host can have a high load average and idle cores.

## The budget

`scripts/check_perf_budget.sh BUILD_DIR` runs the benchmark from an optimized
build and fails if a metric is over its ceiling in `c/bench/baseline.json`. CI
runs it on the `-O2` build of every change.

| Metric | Ceiling | Last observed | Catches |
|---|---|---|---|
| `dispatch_overhead_ns` | **1000 ns** | 134 ns | a system call or lock on the hot path |
| `dispatch_overhead_telemetry_ns` | **1000 ns** | 132 ns | the same, with the telemetry hand-off on |
| `telemetry_wakes` | **400** doorbell writes | 0–2 | consumer pacing that has stopped working |

The time ceilings carry about seven times headroom, so a slow or shared CI
runner does not fail them, while a per-event system call still does. That also
means they cannot catch a small regression; nothing in CI does.

The wake count is the one check that means the same on every machine, because
it is a count, not a time. A busy, paced consumer is never asleep when a
record lands, so the host rings it for the first record and rarely after.
Pacing that stops working rings once per pump, about 8,000 times in the
budget's run. The ceiling leaves room for a consumer descheduled past its idle
threshold on a busy runner.

The observed values are notes, not checks. The 134 ns was measured when the
benchmark still lived in aimee, on a machine the record does not name; the
132 ns is from the Threadripper above, pinned. Changing a ceiling is a reviewed
change to `c/bench/baseline.json`, so "within budget" always names a number
someone agreed to.
