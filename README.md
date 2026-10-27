# ouroboros-ring

A local shared-memory message ring between separate programs on one host. One
**host** owns the memory, admits clients and routes typed events; each admitted
**client** maps only its own queue pair and a shared payload arena. It is a
contract first: anyone may implement it in any language. It stands on its own:
nothing here builds, tests or runs against another project.
[aimee](https://github.com/RakuenSoftware/aimee), where it began, is one consumer
and pins a release of it like any other.

- **The contract** is [`spec/`](spec/README.md) plus the frozen numbers in
  [`vectors/`](vectors/). It states what every implementation must do, with
  numbered requirements (R1 to R6).
- **The reference implementations** below are usable as they are and are held
  to the contract by the same vectors and conformance checks a new
  implementation would be. None links, imports or generates from another.

## Implementations

| Path | Language | What it is | Proven by |
|---|---|---|---|
| [`c/`](c/) | C11, POSIX/Linux | The host and a client, the module runtime, and the conformance host harness. `libouroboros-ring` (all of it) and `libouroboros-ring-client` (attach, wire, region, ring, module protocol: what a module process links) | `c/tests/` and the module runtime harness (13 tests, also under ASan/UBSan; the arena under TSan) |
| [`go/`](go/) | Go, no cgo | Client, module caller and module runtime: `github.com/RakuenSoftware/ouroboros-ring/go/ring` | `go test ./...` with `CGO_ENABLED=0`; `scripts/check_go_no_cgo.sh` |
| [`rust/`](rust/) | Rust 2024 | Client, module caller and module runtime: crate `ouroboros-ring`, depending only on `libc` | `cargo test`; `rust/tests/c_host_conformance.rs` against the C host |

The host exists only in C. The Go and Rust implementations are clients
(`scripts/check_single_host.sh`), so their agreement with the C host is evidence
about the contract rather than one codebase agreeing with itself.

## Versions

`VERSION` is the release of this repository, in semver: within 0.x a minor bump
may break consumers, from 1.0 only a major bump may. A change to `VERSION` on
`main` is tested and tagged `v<VERSION>` (and `go/v<VERSION>` for the Go module)
by `.github/workflows/release.yml`; a tag is never moved. Consumers name a
release or a compatible range, never a commit. The contract carries its own
versions, frozen in `vectors/layout_vectors.tsv`: `version.spec`,
`version.layout`, `version.wire` and `version.amod`. A host and client negotiate
the wire version at attach.

## Building and testing

```sh
# C: libraries, the conformance host and the tests
cmake -S c -B build/c && cmake --build build/c && ctest --test-dir build/c
cmake -S c -B build/c-tsan -DOURO_TSAN=ON && cmake --build build/c-tsan && ctest --test-dir build/c-tsan -R arena_tsan

# Go
(cd go && CGO_ENABLED=0 go test ./...)
scripts/check_go_no_cgo.sh

# Rust, then the Rust client against the C host
(cd rust && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test)
(cd rust && OURO_CONFORMANCE_HOST=$PWD/../build/c/ouroboros-ring-conformance-host \
    cargo test --test c_host_conformance -- --ignored --test-threads=1)

# The contract's own checks
python3 vectors/check_vectors_current.py
scripts/check_single_host.sh
```

Unix socket paths are limited to 108 bytes; run the tests with a short `TMPDIR`.

## Using it

- **C:** `cmake --install` provides `ouroboros-ring` and `ouroboros-ring-client`
  as CMake packages (`find_package(ouroboros-ring)`, targets `ouroboros::...`)
  and pkg-config files. Headers are `<ouroboros/*.h>`; every public name is
  prefixed `ouro_` / `OURO_`.
- **Go:** `go get github.com/RakuenSoftware/ouroboros-ring/go@v<VERSION>`, then
  import `github.com/RakuenSoftware/ouroboros-ring/go/ring`.
- **Rust:** `ouroboros-ring = { git = "https://github.com/RakuenSoftware/ouroboros-ring", tag = "v<VERSION>" }`
  (crate path `rust/`).

## License

AGPL-3.0. See [LICENSE](LICENSE).
