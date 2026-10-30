/* ouro_host.h: the ring host — admission, the queue directory, and reaping.
 *
 * The host is the sole admission authority (D1/D2, suite invariant 17). It
 * creates the control and arena regions, and per admitted client a queue-pair
 * region, and hands a client descriptors ONLY for what it may map: the control
 * region read-only, the arena, and its own queue pair. It never hands over
 * another client's queue-pair descriptor, and the queue directory — the map of
 * which slot is whom — is host-private ordinary memory, never a region, so a
 * client cannot enumerate its peers.
 *
 * Admission is gated by an injected decision function (`ouro_admit_fn`). Who is
 * admitted — peer identity, attestation, execution-policy — is owned by
 * module-runtime and is NOT decided here; this slice owns the mechanism (the
 * SOCK_SEQPACKET handshake, slot allocation, SCM_RIGHTS fd grant, heartbeat
 * reaping) and calls the seam. A refused attach is handed a typed reason and no
 * descriptors.
 *
 * This slice does not route events (slice 6) or run flow control (slice 7). It
 * builds the admitted population the router will serve.
 */
#ifndef OUROBOROS_HOST_H
#define OUROBOROS_HOST_H 1

#include <stddef.h>
#include <stdint.h>

#include <ouroboros/attach.h>
#include <ouroboros/arena.h>
#include <ouroboros/region_host.h>
#include <ouroboros/wire.h>

/* A directory slot — host-private. A client never sees this or any other slot. */
#define OURO_HOST_MAX_KINDS   256
#define OURO_HOST_MAX_PENDING 1024
#define OURO_HOST_MAX_GRANTS  256

/* Outbound patterns authorized for an externally admitted slot. Replies and
 * cancellation are authorized from the host's pending-correlation table; only
 * fresh notifications and requests need manifest grants. */
#define OURO_GRANT_NOTIFY  0x01u
#define OURO_GRANT_REQUEST 0x02u

typedef struct
{
   uint32_t kind;
   uint8_t patterns;
} ouro_slot_grant_t;

/* The governance/audit tap: invoked once per event, after seq stamping and
 * before any routing decision (D6). It is the single full-stream observer; a
 * would_block that the host never accepted is not an event and is not tapped. */
typedef void (*ouro_tap_fn)(void *ctx, const ouro_frame_t *frame, const uint8_t *payload,
                           uint32_t payload_len);

/* Rare loss-only observer. Unlike the full-stream tap this is never called for
 * ordinary traffic: only OURO_KIND_OVERFLOW and OURO_KIND_PRODUCER_REAPED reach
 * it. Daemons use it to escape loss evidence into a durable audit sink without
 * putting durability work (or even a capture-enabled branch) on dispatch's hot
 * path. */
typedef void (*ouro_loss_fn)(void *ctx, const ouro_frame_t *frame, const uint8_t *payload,
                            uint32_t payload_len);

typedef struct
{
   int in_use;
   uint32_t principal_class;
   uint32_t principal_ref;
   int qpair_fd;
   ouro_region_t qpair_region; /* host RW mapping of this client's queue pair */
   ouro_qpair_t qpair;         /* host handles into it */
   uint64_t last_heartbeat;   /* last client_heartbeat value the host observed */
   uint64_t heartbeat_at;     /* host clock when it last advanced */
   uint64_t dropped;          /* malformed/undeliverable events, counted not silent */
   int enforce_grants;        /* external slot: fail closed on undeclared output */
   uint32_t grant_count;
   ouro_slot_grant_t grants[OURO_HOST_MAX_GRANTS];

   /* Block-policy backpressure: a destination-full event is left at this
    * producer's outbound ring head (uncommitted) and retried next pump. It is
    * seq-stamped and tapped exactly once, so those are remembered across
    * retries, and the observers already delivered to are tracked so a fan-out is
    * not double-delivered. */
   int blocked;
   uint64_t blocked_seq;
   uint64_t blocked_delivered[OURO_ARENA_SLOT_WORDS];

   /* Arena-notification fan-out: the observer set is snapshotted here when the
    * lease is published (once, at first sight) so retries deliver the reference
    * to exactly the set that was given a consumer ref — a subscriber that arrives
    * mid-retry holds no ref and must not receive an unreadable reference. */
   uint64_t arena_targets[OURO_ARENA_SLOT_WORDS];

   /* D13: this client's doorbell, which the host writes and the client reads;
    * -1 when the client asked for none. */
   int doorbell_fd;
} ouro_slot_t;

/* Per-kind overflow policy (D5). Block is the safe default: a full destination
 * holds the event at the producer's ring head rather than losing it. Shed is
 * opt-in per kind: a full destination is told, via a typed overflow event, that
 * it lost one — never a silent drop. */
typedef enum
{
   OURO_KIND_BLOCK = 0,
   OURO_KIND_SHED
} ouro_kind_policy_t;

/* A registered event kind: who observes its notifications, who serves its
 * requests, and its overflow policy. Observers is a slot bitmap; server is a
 * slot index or NONE. */
typedef struct
{
   int in_use;
   uint32_t kind;
   uint64_t observers[OURO_ARENA_SLOT_WORDS];
   int32_t server; /* serving slot, or -1 */
   ouro_kind_policy_t policy;
} ouro_kind_t;

/* The inline body of an overflow event: which event was shed, and to whom. A
 * consumer can enumerate exactly the seq values it lost from these alone (D6). */
typedef struct
{
   uint64_t shed_seq;
   uint32_t shed_kind;
   uint32_t dst_slot;
} ouro_overflow_t;

_Static_assert(sizeof(ouro_overflow_t) == OURO_OVERFLOW_LEN, "overflow body size");
_Static_assert(offsetof(ouro_overflow_t, shed_kind) == OURO_OVERFLOW_OFF_SHED_KIND,
               "overflow shed_kind offset");
_Static_assert(offsetof(ouro_overflow_t, dst_slot) == OURO_OVERFLOW_OFF_DST_SLOT,
               "overflow dst_slot offset");

/* The block-held event discarded when its producer departs. The old control
 * event had no payload, so it proved that *something* was reaped but did not
 * name the event the comment promised to name. */
typedef struct
{
   uint64_t lost_seq;
   uint32_t lost_kind;
   uint32_t src_slot;
} ouro_producer_reaped_t;

/* An outstanding request, so a reply can be routed back to its requester.
 *
 * Correlation ids are chosen by each client for itself, so they are unique only
 * within one client: two clients calling the same server will eventually pick
 * the same number. The host therefore keeps both -- the requester's id, and a
 * host-assigned id that is unique across the ring and is the only one the server
 * ever sees. Requests are rewritten to server_correlation_id on the way out and
 * replies back to correlation_id on the way in, so each caller sees its own
 * numbering and the server can key its work on something unambiguous. */
typedef struct
{
   int in_use;
   int request_open;
   uint64_t correlation_id;
   uint64_t server_correlation_id;
   uint32_t requester;
   uint32_t server;
} ouro_pending_t;

typedef struct
{
   uint32_t max_slots;
   uint32_t slot_size;
   uint32_t inline_budget;
   uint32_t queue_capacity;
   uint64_t arena_size;
} ouro_host_config_t;

typedef struct ouro_host
{
   ouro_region_t control_region; /* host RW mapping */
   int control_fd;
   ouro_control_t *control;

   ouro_region_t arena_region; /* host RW mapping */
   int arena_fd;
   ouro_arena_t arena;

   ouro_host_config_t cfg;
   ouro_admit_fn admit;
   void *admit_ctx;
   ouro_attach_status_t (*attach_hook)(void *ctx, struct ouro_host *host, uint32_t slot,
                                      const ouro_attach_request_t *request);
   void *attach_hook_ctx;

   ouro_slot_t *slots;
   uint32_t admitted;

   ouro_kind_t kinds[OURO_HOST_MAX_KINDS];
   ouro_pending_t pending[OURO_HOST_MAX_PENDING];
   uint64_t seq;                     /* host-assigned monotonic dispatch order */
   uint64_t next_server_correlation; /* host-unique ids handed to servers */

   ouro_tap_fn tap;
   void *tap_ctx;
   ouro_loss_fn loss;
   void *loss_ctx;

   /* D13: the host doorbell every doorbell client writes; -1 if it could not be
    * created, in which case no client is granted one and the caller polls. */
   int doorbell_fd;
} ouro_host_t;

typedef enum
{
   OURO_HOST_OK = 0,
   OURO_HOST_ERR_ARG,
   OURO_HOST_ERR_OS,     /* an OS call failed (see errno) */
   OURO_HOST_ERR_REGION, /* a region operation failed */
   OURO_HOST_ERR_REFUSED /* the attach was denied (the reason is in the reply) */
} ouro_host_result_t;

/* Create a host: control + arena regions and the arena allocator, an empty
 * directory of cfg.max_slots. The admission callback receives the attach fd so
 * daemon policy can authenticate its OS peer. admit may be NULL for trusted
 * in-process socketpairs and tests; an external listener must provide one. */
ouro_host_result_t ouro_host_create(ouro_host_t *h, const ouro_host_config_t *cfg, ouro_admit_fn admit,
                                  void *admit_ctx);

/* Install or replace admission policy before accepting external clients. The
 * trusted in-process bootstrap attaches before this seam is installed. */
void ouro_host_set_admission(ouro_host_t *h, ouro_admit_fn admit, void *admit_ctx);

/* Tear down: unmap and close every region and slot. */
void ouro_host_destroy(ouro_host_t *h);

/* Process one attach handshake on a connected SOCK_SEQPACKET socket: read the
 * request, consult admission, and on success allocate a slot, create + init its
 * queue-pair region, and send the reply plus the three descriptors; on denial
 * send the typed reply and no descriptors. Returns OURO_HOST_OK when a client was
 * admitted, OURO_HOST_ERR_REFUSED when it was cleanly denied (reply sent), or an
 * error if the handshake itself failed. */
ouro_host_result_t ouro_host_serve_attach(ouro_host_t *h, int conn_fd);
ouro_host_result_t ouro_host_serve_attach_ex(ouro_host_t *h, int conn_fd, uint32_t *slot_out);
/* Private daemon-owned unnamed socketpair only, kernel-verified same PID/UID,
 * principal class0/ref0. External listener admission hooks remain installed.
 * Caller MUST hold the same host lock used by the active external runtime.
 * Platforms lacking same-process peer attestation fail closed. */
ouro_host_result_t ouro_host_serve_attach_owned(ouro_host_t *h, int conn_fd);

/* Release one admitted slot immediately. Runtime owners use this only after an
 * OS-backed process handle proves the client has exited; ordinary liveness
 * remains heartbeat-reaped. The caller must hold the same host lock used for
 * attach, pump, and reap. */
ouro_host_result_t ouro_host_release_slot(ouro_host_t *h, uint32_t slot);

/* Install the daemon-owned capability binder. Admission authenticates the
 * request before allocation; this hook runs after a slot exists but before its
 * descriptors are granted, and may subscribe/serve only the kinds authorized
 * for that module identity. A non-OK result rolls the slot back and grants no
 * descriptors. */
void ouro_host_set_attach_hook(ouro_host_t *h,
                              ouro_attach_status_t (*hook)(void *ctx, ouro_host_t *host,
                                                          uint32_t slot,
                                                          const ouro_attach_request_t *request),
                              void *ctx);

/* Reap slots whose client heartbeat has not advanced within stale_ns. Releases
 * the slot's arena leases (producer and consumer), unmaps and closes its
 * queue-pair region, and frees the slot. `now` and the recorded timestamps share
 * an arbitrary monotonic clock the caller supplies. Returns the number reaped. */
uint32_t ouro_host_reap(ouro_host_t *h, uint64_t now, uint64_t stale_ns);

/* Bump host_epoch, invalidating every handle and mapping at once (a restart). */
void ouro_host_bump_epoch(ouro_host_t *h);

uint32_t ouro_host_admitted(const ouro_host_t *h);

const char *ouro_host_result_name(ouro_host_result_t r);
const char *ouro_attach_status_name(ouro_attach_status_t s);

/* Set the governance/audit tap. Pass NULL to clear. */
void ouro_host_set_tap(ouro_host_t *h, ouro_tap_fn fn, void *ctx);

/* Set the rare loss observer. Pass NULL to clear. */
void ouro_host_set_loss_sink(ouro_host_t *h, ouro_loss_fn fn, void *ctx);

/* Register `slot` as an authorized observer of `event_kind` (its notifications).
 * Subscription is the authorization: a client never receives a kind it did not
 * subscribe to. */
ouro_host_result_t ouro_host_subscribe(ouro_host_t *h, uint32_t slot, uint32_t event_kind);

/* Register `slot` as the single server for `event_kind` (its requests). A second
 * server for the same kind is refused. */
ouro_host_result_t ouro_host_serve_kind(ouro_host_t *h, uint32_t slot, uint32_t event_kind);
int ouro_host_kind_has_server(const ouro_host_t *h, uint32_t event_kind);

/* Mark an admitted slot as manifest-governed, then grant the fresh outbound
 * patterns it declared. A governed slot with no matching grant cannot inject
 * that notification/request kind. Trusted in-process socketpair clients remain
 * unrestricted unless explicitly marked. */
ouro_host_result_t ouro_host_enforce_grants(ouro_host_t *h, uint32_t slot);
ouro_host_result_t ouro_host_grant_outbound(ouro_host_t *h, uint32_t slot, uint32_t event_kind,
                                          uint8_t patterns);

/* Set a kind's overflow policy. Default is OURO_KIND_BLOCK. */
ouro_host_result_t ouro_host_set_kind_policy(ouro_host_t *h, uint32_t event_kind,
                                           ouro_kind_policy_t policy);

/* Drain every admitted client's outbound ring once, routing each event: stamp
 * seq, offer it to the tap, then deliver — notifications to the kind's authorized
 * observers, a request to the kind's server (or a synthesized capability_absent
 * reply), a reply point-to-point to its requester, a cancel to the server.
 * Returns the number of events processed. Inline payloads only in this slice;
 * arena-payload delivery is a separate slice-4/6 integration. */
uint32_t ouro_host_pump(ouro_host_t *h);

/* D13. Sleep until a client publishes, a client consumes inbound room a blocked
 * producer is waiting for, ouro_host_wake is called, or timeout_ms passes
 * (negative waits indefinitely). Returns 1 when woken or when work was already
 * waiting, 0 on timeout, and -1 when the host has no doorbell, in which case the
 * caller keeps its own backoff. The caller must not hold a lock a client's
 * progress depends on: clients run in other processes and never take it, but an
 * in-process client would. */
int ouro_host_wait(ouro_host_t *h, int timeout_ms);

/* The same wait in two halves, for an embedding that guards the host with a
 * lock other threads take (attach, reap), as the runtime listener does.
 * ouro_host_wait_begin re-checks the rings and so must be called under that
 * lock; it returns 1 when work is already waiting, 0 when the caller should
 * sleep, and -1 without a doorbell. The caller then releases the lock, waits
 * for ouro_host_doorbell_fd to be readable, and calls ouro_host_wait_end,
 * which touches no slot and needs no lock. A begin that returns 1 or -1 needs
 * no end. */
int ouro_host_wait_begin(ouro_host_t *h);
void ouro_host_wait_end(ouro_host_t *h);

/* Wake a thread in ouro_host_wait, for work the rings do not show (shutdown,
 * a timer). Safe from any thread. */
void ouro_host_wake(ouro_host_t *h);

/* The host doorbell, for a caller that multiplexes it with other descriptors.
 * Readable means "pump". -1 when the host has none. */
int ouro_host_doorbell_fd(const ouro_host_t *h);

#endif /* OUROBOROS_HOST_H */
