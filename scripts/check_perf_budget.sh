#!/usr/bin/env bash
# check_perf_budget.sh BUILD_DIR -- run the dispatch benchmark from an optimized
# build and fail if it is over the ceiling committed in c/bench/baseline.json.
set -euo pipefail

build="${1:?usage: check_perf_budget.sh BUILD_DIR}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
baseline="$root/c/bench/baseline.json"
bench="$build/ouroboros-ring-dispatch-bench"

[ -x "$bench" ] || { echo "FAIL: $bench not built (cmake --build $build --target ouroboros-ring-dispatch-bench)" >&2; exit 1; }
ceiling=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["metrics"]["dispatch_overhead_ns"]["ceiling_ns"])' "$baseline")

measured=$("$bench" 500000 | sed -n 's/^dispatch_overhead_ns=//p')
[ -n "$measured" ] || { echo "FAIL: the benchmark printed no dispatch_overhead_ns" >&2; exit 1; }

echo "dispatch overhead: ${measured} ns/event (ceiling ${ceiling} ns)"
if [ "$measured" -gt "$ceiling" ]; then
   echo "FAIL: ${measured} ns is over the ${ceiling} ns ceiling. Either a regression" >&2
   echo "      landed or the ceiling needs a reviewed change in c/bench/baseline.json." >&2
   exit 1
fi
