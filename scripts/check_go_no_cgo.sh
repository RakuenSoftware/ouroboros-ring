#!/usr/bin/env bash
# The Go reference client builds without cgo: it never links the C reference
# implementation, so the two agreeing is evidence rather than one codec agreeing
# with itself.
#
# Three independent assertions: the package has no cgo files, no `import "C"`,
# and it builds with CGO_ENABLED=0.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gomod="$repo_root/go"
pkg="./ring"

cd "$gomod"
cgo_files=$(go list -f '{{join .CgoFiles " "}}' "$pkg")
if [ -n "$cgo_files" ]; then
   echo "FAIL: go/ring has cgo files: $cgo_files" >&2
   exit 1
fi
if grep -rln '^import "C"' ring/ >/dev/null 2>&1; then
   echo "FAIL: go/ring imports \"C\"" >&2
   grep -rln '^import "C"' ring/ >&2
   exit 1
fi

if ! CGO_ENABLED=0 go build "$pkg" >/dev/null 2>&1; then
   echo "FAIL: go/ring does not build with CGO_ENABLED=0" >&2
   exit 1
fi

# The client depends on the standard library and golang.org/x/sys alone. The
# telemetry exporter (cmd/ouroboros-ring-otlp, D16) shares the module and brings
# OpenTelemetry with it, so this keeps that from ever reaching the client.
extra=$(go list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' "$pkg" |
   grep -v -e '^github.com/RakuenSoftware/ouroboros-ring/go/ring$' -e '^golang.org/x/sys/' || true)
if [ -n "$extra" ]; then
   echo "FAIL: go/ring depends on more than the standard library and golang.org/x/sys:" >&2
   echo "$extra" >&2
   exit 1
fi

echo "check_go_no_cgo: ok — go/ring is pure Go and depends only on golang.org/x/sys"
