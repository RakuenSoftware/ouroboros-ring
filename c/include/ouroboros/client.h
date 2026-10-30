/* ouro_client.h: the C reference ring client.
 *
 * A per-language client library is what a module uses to attach the ring and
 * publish/subscribe/request (suite terminology). This is the C reference
 * implementation; slice 9 is the Go one, and the two — held to the same wire
 * vectors — are what keep the spec honest.
 *
 * The client attaches over the host's SOCK_SEQPACKET channel, receives its three
 * descriptors, and maps the control region (read-only), the arena, and its own
 * queue pair. After that it never touches a socket: it publishes by writing its
 * outbound ring and receives by reading its inbound ring, both zero-copy in the
 * shared mapping.
 *
 * The host and the client are separate processes; the client never drives the
 * host. It writes outbound and reads inbound, and the host's routing thread does
 * the rest. A request therefore completes in two steps a caller pumps at its own
 * pace: send the request, then poll for the reply.
 *
 * Threading: a ouro_client_t is used by one thread at a time. The outbound ring
 * has a single producer, and poll is one of its writers: it returns arena refs
 * there (D12). A caller that polls on one thread and sends on another must
 * serialise every poll and send itself.
 */
#ifndef OUROBOROS_CLIENT_H
#define OUROBOROS_CLIENT_H 1

#include <stdint.h>

#include <ouroboros/attach.h>
#include <ouroboros/region.h>
#include <ouroboros/wire.h>

typedef enum
{
   OURO_CLIENT_OK = 0,
   OURO_CLIENT_WOULD_BLOCK, /* the outbound ring is full; retry later */
   OURO_CLIENT_EMPTY,       /* poll found no event */
   OURO_CLIENT_DENIED,      /* attach was refused (see .attach_status) */
   OURO_CLIENT_EPOCH,       /* the host restarted; this client must re-attach */
   OURO_CLIENT_ERR_ARG,
   OURO_CLIENT_ERR_PAYLOAD, /* payload does not fit the inline budget */
   OURO_CLIENT_ERR_OS,      /* a socket/map failure during attach */
   OURO_CLIENT_ERR_PROTOCOL
} ouro_client_result_t;

/* Arena refs owed back to the host but not yet sent because the outbound ring
 * was full (D12). Bounded: a client this far behind stops taking arena frames
 * until its outbound drains. */
#define OURO_CLIENT_MAX_PENDING_RELEASES 64

typedef struct
{
   uint32_t lease_id;
   uint32_t generation;
} ouro_client_lease_t;

typedef struct
{
   ouro_attach_reply_t reply;
   ouro_attach_status_t attach_status;
   ouro_region_t control, arena, qpair;
   ouro_control_t *ctl;
   ouro_qpair_t qp;
   uint64_t attached_epoch;
   int have_pending_read; /* an inbound slot handed out by poll, not yet released */

   /* D12: arena reads across the process boundary. lease_view is NULL when the
    * host predates it; arena frames then arrive with no payload, as before. */
   const ouro_lease_view_t *lease_view;
   uint8_t *arena_base;
   uint64_t arena_size;
   int have_pending_lease; /* the event poll last returned held an arena ref */
   ouro_client_lease_t pending_lease;
   uint32_t release_count;
   ouro_client_lease_t releases[OURO_CLIENT_MAX_PENDING_RELEASES];

   /* D13: the doorbell pair, valid iff have_doorbell. The host doorbell is
    * written to wake the host; the client doorbell is read to sleep. */
   int have_doorbell;
   int host_doorbell_fd;
   int doorbell_fd;
} ouro_client_t;

/* A received event. `payload` points into the shared inbound slot, or for an
 * arena-placed payload into the arena span its lease names (D12), and is valid
 * until the next ouro_client_poll call — zero-copy read in place. The next poll
 * also returns the arena ref to the host, for every client, co-located or not:
 * a consumer reads arena payloads through the event and never releases the
 * lease itself (a second release is refused and counted as dropped). A consumer
 * that needs the bytes past its next poll copies them. */
typedef struct
{
   ouro_frame_t frame;
   const uint8_t *payload;
   uint32_t payload_len;
} ouro_event_t;

/* Attach over a connected SOCK_SEQPACKET socket. On OK the client is mapped and
 * ready; on OURO_CLIENT_DENIED the reason is in c->attach_status. */
ouro_client_result_t ouro_client_attach(int sock, ouro_client_t *c);
ouro_client_result_t ouro_client_attach_as(int sock, ouro_client_t *c, uint32_t principal_class,
                                         uint32_t principal_ref);

/* Unmap and forget. Does not close the attach socket (the caller owns it). */
void ouro_client_detach(ouro_client_t *c);

/* Publish a one-way notification of `kind`. Inline payload only (len must fit the
 * inline budget). Returns OURO_CLIENT_WOULD_BLOCK if the outbound ring is full. */
ouro_client_result_t ouro_client_publish(ouro_client_t *c, uint32_t kind, const void *payload,
                                       uint32_t len);

/* Whether this attachment can read arena-placed payloads (the host advertises
 * the D12 lease view). */
int ouro_client_arena_readable(const ouro_client_t *c);

/* Return every owed arena ref the outbound ring has room for now. Poll does
 * this itself; a consumer that stops polling can call it to drain. Returns
 * OURO_CLIENT_WOULD_BLOCK while any remain. */
ouro_client_result_t ouro_client_flush_releases(ouro_client_t *c);

/* Publish a one-way notification whose payload lives in the arena (D3), for a
 * payload too large for the inline budget. The caller must already hold the
 * lease: allocate it on the host arena (ouro_arena_alloc), fill it
 * (ouro_arena_fill_ptr), read its generation (ouro_arena_ref), then call this to
 * emit the reference frame — no bytes are copied here. The host publishes the
 * lease to the kind's observers and forwards the reference; each consumer reads
 * it through ouro_client_poll and returns the ref on its next poll (D12). Once
 * sent, the lease belongs to the host: the producer must not touch it again. */
ouro_client_result_t ouro_client_publish_arena(ouro_client_t *c, uint32_t kind, uint32_t lease_id,
                                             uint32_t generation, uint32_t len);

/* Send a correlated request. The reply arrives later as an inbound event with
 * the same correlation; poll for it. */
ouro_client_result_t ouro_client_request(ouro_client_t *c, uint32_t kind, uint64_t correlation,
                                       const void *payload, uint32_t len);
ouro_client_result_t ouro_client_request_fragment(ouro_client_t *c, uint32_t kind,
                                                uint64_t correlation, const void *payload,
                                                uint32_t len, int more);

/* Reply to a request (a serving module). */
ouro_client_result_t ouro_client_reply(ouro_client_t *c, uint32_t kind, uint64_t correlation,
                                     const void *payload, uint32_t len);
ouro_client_result_t ouro_client_reply_fragment(ouro_client_t *c, uint32_t kind, uint64_t correlation,
                                              const void *payload, uint32_t len, int more);

/* Cancel an outstanding request. */
ouro_client_result_t ouro_client_cancel(ouro_client_t *c, uint32_t kind, uint64_t correlation);

/* Take the next inbound event. Returns OURO_CLIENT_EMPTY if none. The returned
 * payload pointer is valid until the next poll. */
ouro_client_result_t ouro_client_poll(ouro_client_t *c, ouro_event_t *out);

/* Write a heartbeat so the host knows this client is alive. `now` is any value
 * that advances over time. */
void ouro_client_heartbeat(ouro_client_t *c, uint64_t now);

/* 1 if the host has restarted since attach (a bumped epoch) — every handle and
 * mapping is stale and the client must re-attach. */
int ouro_client_epoch_changed(const ouro_client_t *c);

/* 1 if the host set this client's sticky control_lost flag (a control-class
 * notice could not be delivered even from the reserve). */
int ouro_client_control_lost(const ouro_client_t *c);

/* D13. Sleep until the inbound ring has an event, the host has made room in a
 * full outbound ring, ouro_client_interrupt is called, or timeout_ms passes
 * (negative waits indefinitely). Returns 1 when woken or when an event was
 * already waiting, 0 on timeout, and -1 when the host granted no doorbell, in
 * which case the caller keeps its own backoff. One thread at a time, like poll. */
int ouro_client_wait(ouro_client_t *c, int timeout_ms);

/* Wake this client's own ouro_client_wait from another thread: a handler that
 * finished, a caller with something to send, shutdown. Does nothing without a
 * doorbell. */
void ouro_client_interrupt(ouro_client_t *c);

/* The client doorbell, for a caller that multiplexes it with other
 * descriptors. Readable means "poll". -1 without a doorbell. */
int ouro_client_doorbell_fd(const ouro_client_t *c);

const char *ouro_client_result_name(ouro_client_result_t r);

#endif /* OUROBOROS_CLIENT_H */
