#!/usr/bin/env bash
# check_single_host.sh — enforce D8: there is exactly one ring host, and the
# Go client is a client only.
#
# The conformance suite's credibility rests on two INDEPENDENT client
# implementations agreeing about ONE host. If a second host implementation
# existed, or if the Go "client" actually created regions and admitted peers, the
# agreement would be a codec agreeing with itself. Four assertions, each
# mechanical:
#
#   1. ouro_host_create is defined in exactly one translation unit.
#   2. memfd_create (region creation) appears only in the ring's host-only region
#      code, never in a second would-be host.
#   3. no Go file in go/ring creates regions or accepts attaches — it maps
#      what it is handed and connects; it never listens or memfds.
#   4. no Go test regenerates or shadows the committed wire vectors, so a drift
#      in the C host's frame bytes is caught by the C<->Go vector agreement rather
#      than papered over by a Go-local override.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail=0
note() { printf '%s\n' "$*" >&2; }

# 1. exactly one definition of ouro_host_create.
defs=$({ grep -rlE 'ouro_host_result_t[[:space:]]+ouro_host_create[[:space:]]*\(' \
   c/src/*.c 2>/dev/null || true; })
n=$(printf '%s\n' "$defs" | grep -c . || true)
if [ "$n" != "1" ]; then
   note "FAIL: ouro_host_create is defined in $n translation units (want 1):"
   printf '%s\n' "$defs" >&2
   fail=1
fi

# 2. Within the C ring, a region (memfd) is created in exactly one place —
#    region_host.c. This is about a second ring host, not global memfd policy:
#    other subsystems (git credential fds, ssh-agent) use memfd_create for their
#    own reasons and are none of the ring's business. So the scan is scoped to
#    c/src/ and matches the CALL (with a paren), not comments.
while IFS= read -r hit; do
   [ -z "$hit" ] && continue
   file="${hit%%:*}"
   case "$file" in
   c/src/region_host.c) continue ;;
   esac
   note "FAIL: a second region creator — memfd_create called outside region_host.c: $hit"
   fail=1
done < <(grep -rn 'memfd_create[[:space:]]*(' c/src/*.c 2>/dev/null || true)

# 3. the Go client is a client only: it must not create regions (memfd) or
#    accept/listen (a host does that).
if [ -d go/ring ]; then
   for forbidden in 'MemfdCreate' 'unix.Listen' 'unix.Accept' 'unix.Bind'; do
      if grep -rn "$forbidden" go/ring/*.go 2>/dev/null | grep -v '_test.go' >/dev/null; then
         note "FAIL: go/ring uses '$forbidden' — a client does not host"
         grep -rn "$forbidden" go/ring/*.go | grep -v '_test.go' >&2
         fail=1
      fi
   done

   # 4. no Go file writes the committed vector table (only the Python generator
   #    may). A Go-local vector override would hide a real wire drift.
   if grep -rn 'wire_vectors\.tsv' go/ 2>/dev/null | grep -iE 'Create|OpenFile|WriteFile|os\.O_WRONLY|os\.O_CREATE' >/dev/null; then
      note "FAIL: a Go file writes wire_vectors.tsv — the vectors are the single authority"
      fail=1
   fi
fi

# 5-7. The Rust reference client (rust/) is held to the same rules,
#      plus independence: it may not link the C implementation, so it has no
#      build script, no #[link] attribute, and no dependency but libc.
rust_crate=rust
if [ -d "$rust_crate" ]; then
   # 5. client only: no region creation, no listening or accepting.
   for forbidden in 'memfd_create' 'libc::bind' 'libc::listen' 'libc::accept'; do
      if grep -rn --include='*.rs' "$forbidden" "$rust_crate/src" >/dev/null 2>&1; then
         note "FAIL: $rust_crate uses '$forbidden' — a client does not host"
         grep -rn --include='*.rs' "$forbidden" "$rust_crate/src" >&2
         fail=1
      fi
   done

   # 6. no link edge with the C reference.
   if [ -e "$rust_crate/build.rs" ] || grep -rn --include='*.rs' '#\[link' "$rust_crate" >/dev/null 2>&1; then
      note "FAIL: $rust_crate has a build script or #[link] attribute — the Rust client must not link the C ring"
      fail=1
   fi
   deps=$(awk '/^\[/{in_deps = ($0 == "[dependencies]"); next} in_deps && /^[A-Za-z0-9_-]+[[:space:]]*=/ {print $1}' \
      "$rust_crate/Cargo.toml" | sort -u | tr '\n' ' ')
   if [ "$deps" != "libc " ]; then
      note "FAIL: $rust_crate/Cargo.toml [dependencies] must be exactly libc, found: ${deps:-none}"
      fail=1
   fi

   # 7. no Rust file writes the committed vector table.
   if grep -rn 'wire_vectors\.tsv' "$rust_crate" 2>/dev/null | grep -E 'File::create|OpenOptions|fs::write|\.write\(' >/dev/null; then
      note "FAIL: a Rust file writes wire_vectors.tsv — the vectors are the single authority"
      fail=1
   fi
fi

if [ "$fail" -ne 0 ]; then
   note ""
   note "D8: exactly one host, and the Go and Rust sides are clients only. See"
   note "spec/decisions.md."
   exit 1
fi

echo "check_single_host: ok — one host_create, memfd only in the host, Go and Rust are client-only"
