# Arena payloads (C)

How to send and receive payloads too large for a ring slot with the C reference
implementation. The normative rules are D3 (leases) and D12 (cross-process reads)
in [`spec/decisions.md`](../spec/decisions.md); this is the practical guide.

If a payload fits the inline budget you need none of this: use
`ouro_client_publish`, `ouro_client_request` or `ouro_client_reply`, and read
`ev.payload` from `ouro_client_poll`.

## When to use the arena

A frame's payload is either **inline** (it rides in the slot after the 64-byte
header and must fit `inline_budget`) or **arena** (the bytes live in the shared
arena region and the frame carries only a lease id and generation). Choose per
event: a small payload of the same kind still goes inline.

## Producing: only next to the host

The lease table is host-private (D3), so a client cannot forge a lease's
lifetime. The consequence is that **only code in the host's process can allocate
a lease**: it needs the host's `ouro_arena_t` (`h->arena`). A client in another
process can read arena payloads (below) but cannot produce one.

```c
/* 1. Allocate a lease on the host arena, owned by the producer's slot. */
uint32_t lease;
ouro_arena_result_t r = ouro_arena_alloc(&h->arena, producer->reply.handle_id, len, &lease);
/* OURO_ARENA_ERR_CAP, _NOSPACE and _NOLEASE are backpressure: a consumer's
   release frees room. Retry or drop. */

/* 2. Fill the span. The write happens outside any lock; the span is yours until
      the frame is sent. */
uint8_t *p;
ouro_arena_fill_ptr(&h->arena, lease, &p);
memcpy(p, bytes, len);

/* 3. Read the generation and send the reference frame. No bytes cross the ring. */
ouro_arena_ref_t ref;
ouro_arena_ref(&h->arena, lease, &ref);
ouro_client_publish_arena(producer, KIND, lease, ref.generation, len);
```

**Once the frame is sent the lease is the host's**; do not touch it again. If you
allocate a lease and then decide not to send it, call `ouro_arena_cancel` to drop
the producer reference. `len` is bounded by the arena, not by `inline_budget`.

## Consuming: from any process

`ouro_client_poll` resolves an arena frame through the host's lease view (D12),
so `ev.payload` points at the span in place, in this process or any other that
attached. The pointer is valid until the next poll, and **that next poll returns
the reference to the host**: a consumer never calls `ouro_arena_release` itself (a
second release is refused and counted as dropped). Copy the bytes if you need
them past the next poll.

```c
ouro_event_t ev;
if (ouro_client_poll(consumer, &ev) == OURO_CLIENT_OK && ev.payload)
   handle(ev.payload, ev.payload_len);
```

`ouro_client_arena_readable(c)` says whether the host advertised the lease view.
A client that stops polling can return what it owes with
`ouro_client_flush_releases`.

## What the host does with it

This happens inside `ouro_host_pump`:

- **Notification:** published to a snapshot of the kind's observers (the refcount
  is that set) and delivered by reference under BLOCK or SHED like an inline
  fan-out. A shed observer's reference is released so the lease still drains. No
  observers: reclaimed at once.
- **Request:** published to the kind's server, delivered point to point, and the
  correlation registered. No server: the lease is reclaimed and the requester gets
  `capability_absent`.
- **Reply:** only the kind's server may answer; delivered to the original
  requester and the correlation retired. An unmatched reply is reclaimed and
  dropped.
- **Cancel:** carries no meaningful payload; its lease is reclaimed and the frame
  dropped.

Every arena frame is checked against the authoritative lease (generation, and
`payload_len` within the span) before routing; a mismatch is dropped and counted.

## Threading

The lease table is guarded by an in-process mutex, so a producer thread may
`alloc` and `fill` while another thread pumps and a consumer reads. The lock
covers only the bookkeeping; the copy into the span and the read out of it happen
outside it, kept safe by the refcount. `c/tests/test_arena_tsan.c` exercises this
under ThreadSanitizer (`-DOURO_TSAN=ON`).

## Capture

The capture tap materializes arena payload bytes into the record (D10), so a
captured arena event replays byte-exact without its long-gone lease.

## Quick reference

| Call | Who | When |
| --- | --- | --- |
| `ouro_arena_alloc` | producer, in the host's process | reserve a span |
| `ouro_arena_fill_ptr` | producer | get the writable pointer and fill it |
| `ouro_arena_ref` | producer | read the generation for the frame |
| `ouro_client_publish_arena` | producer | send the reference frame |
| `ouro_arena_cancel` | producer | abandon a lease that was never sent |
| `ouro_client_poll` | consumer, any process | read the payload in place; returns the previous ref |
| `ouro_client_flush_releases` | consumer | return owed refs without polling |

The declarations are in [`arena.h`](../c/include/ouroboros/arena.h) and
[`client.h`](../c/include/ouroboros/client.h); [`test_route.c`](../c/tests/test_route.c)
and [`test_client.c`](../c/tests/test_client.c) are worked examples.
