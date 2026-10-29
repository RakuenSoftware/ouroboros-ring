# Ouroboros ring: wire and segment specification (v0)

This is the normative wire and segment contract of the ouroboros ring. Exact byte
offsets, sizes, magics and versions are frozen by the language-neutral vectors in
[`../vectors/`](../vectors/): `wire_vectors.tsv` for the 64-byte frame and
`layout_vectors.tsv` for the regions, rings, attach handshake and AMOD envelope.
Every implementation, in any language, is held to both; the C, Go and Rust
reference implementations in this repository are tested against them in both
directions. Where this text and the vectors disagree, the vectors win and this
text is the bug.

Design rationale and its history are in [`decisions.md`](decisions.md) (D1 to D12).
This specification began as aimee's v0 event-bus spec (2026-07-23, implemented
2026-07-24) and moved here with the implementations on 2026-10-27; amendment notes
below are kept as the record of why the layout is what it is.

## Scope

This spec defines the intra-service ouroboros ring: the segment layout, the ring buffers,
the event wire encoding, the attach/admission handshake, routing, flow control, ordering, versioning,
the governance/audit tap, and capture/replay format. It is what a ring client in any language
implements to interoperate with the single in-source C ring host.

Out of scope (owned by whoever embeds the ring): the descriptor and **event-contract schema**, which
event kinds a module publishes, subscribes to or requests; **admission policy** (identity,
installation, approval), invoked at attach through the host's admission hook; **cross-service**
transport, which is a network path and not this ring; and module business logic.

## Terminology

- **Ring host**: the participant that owns the segment, admits clients, routes events, and runs the
  tap. Exactly one per ring; a deployment may run several independent rings.
- **Ring client**: a per-language library a module uses to attach and publish/subscribe/request.
- **Segment**: the shared-memory region host and admitted clients map.
- **Ring**: a single-producer/single-consumer (SPSC) lock-free queue in the segment.
- **Queue pair**: one client's inbound ring (host→client) and outbound ring (client→host).
- **Event**: one typed message: a fixed header plus a payload reference.
- **Handle**: the opaque per-client identity core grants at admission; indexes the client's queue
  pair and its authorization.

## Topology

Per ring: one host, N admitted clients. The host is the only participant that reads or writes more
than one client's queues; each client maps **only its own queue pair** and the shared payload arena.
There is no client↔client shared ring. All routing goes through the host, which is what makes
observer routing and the tap total (R2, R5). A request that crosses to another
ring does not use this segment; it leaves on whatever network transport the embedding uses.

## Segment layout

> **Amendment requested 2026-07-23 (implementation feedback).** This section originally described
> one shared segment containing the control block, the queue directory, every client's rings, and
> the arena. Implementing it that way was found to make this spec's own Security section untrue:
> once a client is admitted it holds the segment, so it can map and read every other client's rings
> and enumerate the whole queue directory. "A client cannot read another client's traffic, matching
> observer routing at the memory level, not only the API level" then holds only by the client
> library's good manners. The revision below is what the layout has to be for that sentence to be a
> guarantee. It changes no frame bytes, so the conformance vectors are untouched. Rationale and the
> resulting v0 threat model are in [`decisions.md`](decisions.md)
> (D1, D2).
>
> **Amendment status: ACCEPTED.**

The ring is realized as **several fd-backed regions**, not one segment. A client receives file
descriptors only for what it is allowed to map, so isolation is enforced by the kernel rather than
by convention.

```
  Control region — one memfd, mapped PROT_READ by clients
+===========================================================+
|  magic | spec_version | layout_version | flags             |
|  slot_size | inline_budget | arena_size                    |
|  host_epoch | host_heartbeat (monotonic)                   |
+===========================================================+

  Queue-pair region — one memfd PER ADMITTED SLOT,
  mapped read/write by that client and no other
+===========================================================+
|  inbound ring (host -> client)                             |
|  outbound ring (client -> host)                            |
|  credit counters | reserved control-class credits          |
|  client_heartbeat | control_lost flag                      |
+===========================================================+

  Arena region — one memfd, mapped read/write by all
+===========================================================+
|  payload regions referenced by (offset, len)               |
+===========================================================+

  Queue directory — HOST-PRIVATE ordinary memory, not shared
+===========================================================+
|  per slot: handle_id | state | principal_ref | geometry     |
+===========================================================+
```

Three consequences, each of which was the point:

- **A client cannot map another client's rings**, because it holds no descriptor for them. This is
  an MMU fault, not a policy violation.
- **A client cannot enumerate slots**, because the queue directory is never shared. It learns its own
  geometry from the attach reply and has no address through which to observe that another slot
  exists.
- **The control region is read-only to clients**, so a client cannot forge the geometry its peers
  read.

What this does *not* close is the arena: zero-copy fan-out requires every admitted client to map it
read-write, so arena lease bounds remain a cooperative contract validated by conformance rather than
enforced by hardware. That limit is stated rather than papered over, and it is consistent with this
spec's existing note that a hostile native client in the trusted tier is outside this boundary's
threat model. Untrusted code does not run as a native admitted client; it runs sandboxed under
`module-loader`, whose tier will need per-lease arena sub-regions or a copy-on-deliver mode.

Attach carries three descriptors (control, arena, and that client's queue pair) in a single
`sendmsg` over the `SOCK_SEQPACKET` attach socket, via `SCM_RIGHTS`, only after admission succeeds.
An unadmitted process receives no descriptors and, because the regions are anonymous `memfd`s with
no filesystem name, has nothing to open. The host's descriptor table grows by one per admitted slot
plus the two shared regions.

The control region is versioned and read-mostly. `host_epoch` changes on host restart and invalidates
every prior handle and mapping (clients must re-attach). Heartbeats give liveness both ways: a client
whose heartbeat stalls is reaped by the host; a stalled `host_heartbeat`/`host_epoch` change tells a
client the host is gone.

`slot_size` and `inline_budget` are region *parameters*, read by a client at attach and used to
choose inline-versus-arena placement. They are not frame fields and they do not participate in
`layout_version`, which changes only when the *structure* of a region changes. Re-tuning them
therefore never re-issues a conformance vector.

## Rings

Each ring is a lock-free **SPSC** queue with a producer index and a consumer index on separate cache
lines to avoid false sharing. Because every ring has exactly one writer (the client for its outbound,
the host for the client's inbound) and one reader, no lock is needed, publication uses release
stores and consumption uses acquire loads. A ring is a power-of-two slot array; `full` and `empty`
are distinguished by the producer/consumer index pair (not by a count that could alias). Slot size is
fixed per segment and carries the event header plus a small inline payload budget; larger payloads go
to the arena.

The indices are free-running 64-bit counters (`head` written only by the producer, `tail` only by the
consumer), so the ring holds `head - tail` events and `0 <= head - tail <= capacity` always holds for
a correct pair. A consumer **never passes its producer**: when it reads `head == tail`, or a pair with
`head - tail > capacity` (which no correct producer can create: a stray, doubled or corrupted
commit), the ring reads as empty, and a commit at such a pair does not move `tail`. Without this, one
bad commit left `tail` ahead of `head` and the unsigned difference made the consumer read the whole
slot array as pending, forever: a production host wedged this way on 2026-10-26. Every
implementation applies the guard on both the read and the commit side.

## Event encoding

Fixed-size little-endian header (illustrative field set; exact offsets frozen by vectors):

| Field | Purpose |
|---|---|
| `magic` / `hdr_flags` | frame sync + flags (inline-payload, arena-payload, request, reply, notification, cancel) |
| `wire_version` | encoding version (matches negotiated version) |
| `event_kind` | numeric id resolved against the event-contract kind registry |
| `correlation_id` | ties a reply to its request; zero for one-way notifications |
| `src_handle` / `dst_handle` | source client and (host-filled) destination; clients set src, host sets dst on routing |
| `principal_ref` | the attested principal for this event (for the tap/policy) |
| `seq` | host-assigned monotonic dispatch sequence (authoritative order for capture/replay) |
| `logical_ts` | logical clock for ordering across sources without wall-clock trust |
| `payload_len` | length of payload |
| `payload_ref` | inline (in-slot) offset, or arena `(offset,len)` when the arena flag is set |

Payload bytes are the IR event body; their per-kind schema is owned by the event-contract schema, not
this spec. This spec defines only the framing.

## Message patterns

- **Notification**: one-way; `correlation_id = 0`; no reply expected.
- **Request/reply**: the requester sets a fresh `correlation_id`; the serving module replies with the
  same `correlation_id`; the host routes the reply point-to-point back to the requester only.
- **capability_absent**: if the target kind has no ready authorized server, the host synthesizes a
  typed `capability_absent` reply rather than dropping the request.
- **cancel**: a requester may cancel an outstanding `correlation_id`; delivery of cancel is
  best-effort and idempotent.
- **fragmentation**: wire version 3 sets `OURO_F_MORE` on every non-final inline fragment of one
  correlated request or reply. Fragments remain ordered under the correlation ID; the first frame
  without `OURO_F_MORE` completes the message. The host does not close or reuse the pending route
  while either side is fragmented, and cancel retires the partial stream.

## Attach and admission handshake

Attach is **not** a free mmap. A client requests attach over a small control channel; the host invokes
its embedding's admission hook (`ouro_admit_fn`: peer identity, attestation, installation policy,
whatever the embedding requires; not this spec), and only on success allocates the client a
queue-directory slot, maps **only** that
slot's queue pair and the arena into the client, and returns the `handle_id` and negotiated version.
An unadmitted process gets no slot, no handle, and no mapping. Detach (graceful or by reaped
heartbeat) frees the slot; `host_epoch` bumps invalidate all handles at once.

## Routing and the tap

The host drains each client's outbound ring, and for each event: stamps `seq`, offers it to the
**tap** (`ouro_tap_fn`, the embedding's full-stream hook, called once per accepted event),
resolves authorized observers of `event_kind` (observer routing, R5), and writes the
event into each authorized subscriber's inbound ring (or the single requester's ring for a reply). A
client never sees an event for a kind it is not an authorized observer of. The tap is the sole
full-stream reader and belongs to the host's embedding, not to a client. Per-event policy (an
embedding that must approve some kinds before delivery) is the embedding's, applied through its
admission grants and its tap; the ring itself only enforces the grants.

## Flow control and backpressure

Rings are bounded, so backpressure is explicit. v0 uses **credit-based** flow control: a producer may
publish only up to the consumer's advertised free slots; when credits are exhausted it blocks with a
bounded wait or returns `would_block` (the client library chooses per call). The host protects itself
from a slow client by bounding per-client in-flight and, past a threshold, applying the descriptor's
declared overflow policy for that client's kinds (block the producer, or shed with a typed
`overflow` event), never an unbounded host-side queue. A wedged client is reaped by heartbeat; its
slot is reclaimed.

## Ordering and delivery

- Per source ring: strict FIFO.
- Global order is defined only by the host's `seq`; there is no cross-ring order before the host
  stamps it. The **capture/replay** stream is exactly the host's `seq` order.
- Delivery within a live segment is exactly-once per destination; on `host_epoch` change (restart),
  in-flight events not yet `seq`-stamped are lost and clients re-attach. The durable audit chain,
  not the ring, is the persistence boundary.

## Versioning and negotiation

`spec_version`/`layout_version` are in the control block; `wire_version` is per event. A client
declares its supported version range at attach; the host accepts the highest common version or refuses
the attach with a typed reason. Within a major version, fields may be added only in reserved space and
ignored by older readers; a layout-incompatible change bumps `layout_version` and requires re-attach.

## Payloads and zero-copy

Small payloads inline in the ring slot (a copy, but cheap). Payloads above the inline budget go to the
shared **arena** and are referenced by `(offset, len)`. Zero-copy across a process boundary needs an
ownership/lifetime discipline: v0's baseline is **host-mediated arena leases**. The producer requests
an arena region, fills it, publishes the ref; the consumer reads and releases; the host tracks the
lease and reclaims on release or on client reap. Arena allocation remains limited to trusted
co-located publishers. Module processes instead use the wire-v3 ordered fragmentation contract for
requests and replies, with a 16 MiB assembled-message limit and endpoint reassembly. Direct
producer→consumer arena allocation across that process boundary remains outside this contract.

## Security

- The regions are anonymous `memfd`s with no filesystem name, so there is nothing for an unadmitted
  process to open; descriptors are passed only after admission succeeds. Each client receives
  descriptors for the control region (read-only), the arena, and its own queue pair, and for
  nothing else, so "never another client's rings" is enforced by the descriptor it was not given
  rather than by permission bits on a shared object.
- The host is the only multi-queue reader/writer; a client cannot read another client's traffic,
  matching observer routing at the memory level, not only the API level.
- Arena access is bounded to leased regions; a client cannot read arbitrary arena bytes outside its
  active leases (enforced by the client library and validated by conformance; a hostile native client
  in the trusted tier is out of this boundary's threat model, untrusted clients run sandboxed per
  by the embedding's module loader).

## Conformance

The spec is validated, not defined, by its implementations. All of the following are built and green
(see [Implementation status](#implementations)):

- **Wire vectors**: shared encode/decode fixtures for headers, each message pattern, version
  negotiation, and error frames; the C, Go and Rust reference clients all produce and accept the
  exact bytes. Committed in `vectors/wire_vectors.tsv`, generated independently of every
  codec by `vectors/gen_wire_vectors.py` (10 positive + 13 negative rows).
- **Interop**: the single C host (`c/conformance/host.c`) driven with an internal C client and a
  real Go or Rust client across a process boundary, exchanging notifications and request/replies in
  both directions, including `capability_absent` and arena payloads in all three patterns.
  Backpressure/credit exhaustion is a language-independent host and client-ring property, proven
  in-process (`c/tests/test_flow.c`, and the clients' `would_block` paths).
- **No-second-host**: exactly one host implementation; `scripts/check_single_host.sh` fails a
  second `ouro_host_create` and confirms the Go and Rust sides are client-only.
- **Third-language proof**: the Rust client ([D11](decisions.md#d11-the-rust-reference-client-is-a-third-independent-client)),
  written from this spec and the vectors, attaches to the C host and interoperates, the
  credibility test for "any language". A fourth needs no change here.

## Non-goals

- Cross-service transport (network path between Runtime and Control Plane).
- The event-contract/kind schema and descriptor graph (owned by the embedding).
- Admission policy semantics (owned by core; invoked here at attach).
- Freezing exact byte offsets in prose. The vectors are authoritative.

## Open questions

Answered in v0 (settled by the decisions in
[`decisions.md`](decisions.md) and the implementation):

- ~~Direct zero-copy vs host-mediated arena leases; arena allocation/fragmentation strategy.~~
  **Host-mediated leases** (D3): generation + consumer refcount, first-fit with coalescing, a
  synchronous per-client cap. Direct producer→consumer allocation is overruled for v0.
- ~~Final backpressure policy (credit-based confirmed for v0; shed-vs-block defaults per kind).~~
  **Block by default, shed opt-in per kind** (D5/D7), with a control-class reserve and a sticky
  `control_lost` backstop.
- ~~Exact slot size and inline-payload budget.~~ **Control-region parameters** read at attach, not
  wire fields (D4); provisional 256/192/1024 set in slice 3, the dispatch-overhead ceiling committed
  in `c/bench/baseline.json`.

Still open for a later version (out of v0 scope):

- **Huge and streamed payloads** (chunking over the ring vs a dedicated arena stream).
- **Arena-payload routing**: the host publishing a lease to the resolved observer set before
  forwarding the reference. The allocator exists (slice 4) and inline routing works; the composition
  is a focused slice-4/6 follow-up. Arena-flagged frames are currently dropped-with-count, not
  delivered to a consumer that could not read them.
- **NUMA/placement** of the regions on multi-socket hosts.
- **`shm_open` portability fallback**: v0 is Linux-only (`memfd_create`) by D1; a fallback carries
  its own threat-model note.

## Implementations

The reference implementations, their tests and the conformance harnesses are listed in the
repository [README](../README.md#implementations).
