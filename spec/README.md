# The ouroboros ring contract

The ouroboros ring is a local, shared-memory message ring between separate
programs on one host. One participant, the **host**, owns the memory, admits
clients and routes events. Each admitted **client** maps only its own queue pair
and the shared payload arena, and exchanges typed events with other clients only
through the host.

This directory is the contract. It is language-neutral: the C, Go and Rust code
in this repository are reference implementations of it, usable as they are, and
anyone may implement the ring in another language. An implementation conforms
when it passes the vectors and the conformance suite described below; nothing
here requires linking, importing or generating from a reference implementation.

| Document | What it fixes |
|---|---|
| [`wire-and-segment.md`](wire-and-segment.md) | Regions, rings, the 64-byte frame, attach and admission, routing, flow control, ordering, versioning, payloads, capture |
| [`decisions.md`](decisions.md) | Why the layout is what it is (D1 to D12), including the threat model |
| [`../vectors/`](../vectors/) | The frozen numbers: `wire_vectors.tsv` (frame bytes) and `layout_vectors.tsv` (every offset, size, magic and version a client needs) |

Where prose and vectors disagree, the vectors are authoritative and the prose is
the bug. A behaviour that no vector or conformance check covers is not yet part
of the contract.

## What the ring owns

- the wire frame and version negotiation;
- the control, private queue-pair and arena layouts;
- single-producer/single-consumer rings and bounded credits;
- client admission (through the host's admission hook) and reaping;
- publish, subscribe, request/reply, cancellation and typed absence;
- the host sequence, observer routing and the full-stream tap hook;
- the capture format and observational replay;
- local `SOCK_SEQPACKET` attachment and descriptor grant;
- the AMOD module message envelope (`module_protocol`), fragmentation of large
  module calls and replies, and the module process runtime built on it.

## What it does not own

Whoever embeds the ring owns these, through the host's hooks and grants:

- which event kinds exist and what their payloads mean;
- who may attach and with what grants (identity, attestation, installation and
  approval policy), beyond enforcing the grants it is given;
- durability: the ring is not a persistence boundary, and a host restart drops
  events that were not yet sequenced;
- network transport between hosts, and anything crossing machines;
- sandboxing hostile code.

## Requirements

Numbered so tests, reviews and other implementations can cite them. Each came
from aimee's system invariants (number in brackets) when the ring was part of
aimee.

- **R1. Participants share no code.** [12] Participants meet only as ring
  events. No implementation requires a shared header, symbol, link edge or
  foreign-function boundary with another, so a client in any language is built
  from the contract alone. The Go and Rust reference clients are checked for this
  (`scripts/check_go_no_cgo.sh`, `scripts/check_single_host.sh`).
- **R2. One tap sees everything, once, in order.** [13] Every event the host
  accepts is stamped with the host sequence and offered to the tap exactly once,
  in sequence order, before delivery. That stream is what capture records and
  replay reproduces. A refused or would-block event was never accepted and is
  never tapped.
- **R3. The fast path is memory, not syscalls.** [15] Events move through
  lock-free SPSC rings in shared memory, with inline payloads or arena leases,
  and no per-message syscall on the fast path.
- **R4. Admission is the host's; nothing is open.** [17] There is no named
  segment. An unadmitted process holds no descriptor and cannot map, enumerate or
  inject. An admitted client reaches only the event kinds its grant names.
  Admission fails closed.
- **R5. Delivery is observer routing, enforced by memory layout.** [18] Each
  event goes only to clients authorized to observe its kind. A client maps only
  its own queue pair and the arena, so it cannot read another client's traffic or
  enumerate the queue directory: the kernel enforces this, not the client
  library.
- **R6. Isolation is by process.** [19] Host and clients are separate programs
  with independent fault domains. A crashed or wedged client is reaped, and its
  slot and leases are reclaimed, without affecting other clients.

The fixed numbers behind R3 to R5 (capacities, ring index rules, slot and lease
limits) are in the vectors. Index rules the implementations enforce, such as "a
consumer never passes its producer: an impossible head/tail pair reads as
empty", are part of the ring semantics in
[`wire-and-segment.md`](wire-and-segment.md#rings).

## Conforming

An implementation in a new language conforms when:

1. its frame codec reproduces every row of `vectors/wire_vectors.tsv`, and every
   layout constant it uses has a matching row in `vectors/layout_vectors.tsv`,
   checked in both directions;
2. its client attaches to the C host conformance harness
   (`c/conformance/host.c`, built as `ouroboros-ring-conformance-host`) and
   passes the scenarios the Rust suite runs against it
   (`rust/tests/c_host_conformance.rs`); and
3. if it provides a module runtime, its module process passes the C harness
   (`c/conformance/module_runtime.c`, built as
   `ouroboros-ring-module-runtime-harness MODULE_BINARY`), and its module caller
   reaches the same outcomes as the C caller (`OURO_TEST_MODULE_CALLER`); and
4. it is a client only (R1): it creates no regions and accepts no attaches.

The vectors are generated from this specification, never read from an
implementation, so agreement between implementations is evidence rather than a
copy. Regenerating them is a deliberate, reviewed act
(`vectors/gen_wire_vectors.py`, `vectors/gen_layout_vectors.py`);
`vectors/check_vectors_current.py` fails when the committed tables and their
generators disagree.
