/* ouro_region.h: the fd-backed shared-memory regions of the ring.
 *
 * This is the layer the D1 amendment (spec/decisions.md) turned
 * the ring into: several separately-mapped regions rather than one segment, so a
 * client holds a descriptor only for what it may see.
 *
 *   Control region   — one memfd, mapped PROT_READ by clients. Holds the
 *                      versioned header and the D4 parameters (slot_size,
 *                      inline_budget, arena_size) a client reads at attach, plus
 *                      host_epoch and host_heartbeat. Read-only to clients is an
 *                      MMU fact here, not a convention: a client that tries to
 *                      write it faults.
 *
 *   Queue-pair region — one memfd PER admitted slot, mapped read/write by that
 *                      client and no other. Holds the client's inbound and
 *                      outbound ouro_rings plus its credit counters, control-class
 *                      reserve, client_heartbeat, and control_lost flag.
 *
 *   Arena region     — one memfd, mapped read/write by all admitted clients.
 *                      Slice 3 creates, maps and validates it; the lease
 *                      allocator that lives inside it is slice 4.
 *
 *   Queue directory  — NOT here. It is host-private ordinary memory (slice 5),
 *                      never a mapped region, which is what makes a client
 *                      unable to enumerate other slots.
 *
 * This slice owns creating, mapping and validating the regions. Admission (who
 * gets which descriptor), routing, and the arena allocator are later slices.
 * Nothing here opens a socket or decides policy.
 *
 * Everything a client reads from a region is treated as written by another
 * process: validated against the bytes actually mapped, never trusted.
 */
#ifndef OUROBOROS_REGION_H
#define OUROBOROS_REGION_H 1

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include <ouroboros/ring.h>

#define OURO_CONTROL_MAGIC 0x4c544342u /* "BCTL" */
#define OURO_QPAIR_MAGIC   0x52504251u /* "QPBR" */
#define OURO_ARENA_MAGIC   0x4e524142u /* "BARN" */

#define OURO_SPEC_VERSION   1
#define OURO_LAYOUT_VERSION 1

/* Control-region feature flags (ouro_control_t.flags). A flag advertises an
 * addition made in reserved space, which older readers ignore; layout_version
 * is unchanged (D12). */
#define OURO_CONTROL_F_LEASE_VIEW 0x00000001u
/* D13: the host keeps host_waiting and signals client doorbells. */
#define OURO_CONTROL_F_DOORBELL 0x00000002u

/* Entries in the lease view that follows the control header (D12). Equal to
 * the host's lease-table size; ouro_arena.c asserts it. */
#define OURO_LEASE_VIEW_SLOTS 4096u

/* The reserved control-class credit pool (D6): a client saturated with data
 * events still has room to be told it lost some. Provisional, re-tuned in
 * slice 12 alongside the D4 parameters. */
#define OURO_CONTROL_CREDITS_DEFAULT 4

typedef enum
{
   OURO_REGION_OK = 0,
   OURO_REGION_ERR_ARG,      /* a null or nonsensical argument */
   OURO_REGION_ERR_OS,       /* memfd_create / ftruncate / mmap failed (see errno) */
   OURO_REGION_ERR_SIZE,     /* the mapping is too small for the claimed layout */
   OURO_REGION_ERR_MAGIC,    /* not the region kind expected */
   OURO_REGION_ERR_VERSION,  /* spec/layout version this build cannot map */
   OURO_REGION_ERR_GEOMETRY, /* a parameter is out of range or self-inconsistent */
   OURO_REGION_ERR_EPOCH     /* host_epoch changed since the caller attached */
} ouro_region_result_t;

/* A mapped region. base is the mmap; size is the whole mapping; writable records
 * how it was mapped so a validate can refuse to hand a writer a read-only map. */
typedef struct
{
   int fd;
   void *base;
   size_t size;
   int writable;
} ouro_region_t;

/* The control region header. Read-mostly, and read-only to clients. Fields are
 * atomic so a client's read races cleanly with the host's heartbeat write.
 *
 * slot_size / inline_budget / arena_size are the D4 parameters: a client reads
 * them here at attach and never compiles them in. They are region parameters,
 * not wire fields, and do not participate in layout_version. */
typedef struct
{
   _Atomic uint32_t magic;
   _Atomic uint32_t spec_version;
   _Atomic uint32_t layout_version;
   _Atomic uint32_t flags;
   _Atomic uint32_t slot_size;
   _Atomic uint32_t inline_budget;
   _Atomic uint32_t queue_capacity;   /* ring capacity a queue-pair region uses */
   _Atomic uint32_t lease_view_slots; /* D12; was reserved0. Valid iff F_LEASE_VIEW */
   _Atomic uint64_t arena_size;
   _Atomic uint64_t host_epoch;
   _Atomic uint64_t host_heartbeat;
   _Atomic uint64_t lease_view_off; /* D12; was reserved1[0]. Valid iff F_LEASE_VIEW */
   _Atomic uint32_t host_waiting;   /* D13; host-written. Valid iff F_DOORBELL */
   _Atomic uint32_t reserved2;
   _Atomic uint64_t reserved1[3];
} ouro_control_t;

_Static_assert(sizeof(ouro_control_t) == 96, "control header size is frozen by layout_version");
_Static_assert(offsetof(ouro_control_t, lease_view_slots) == 28, "D12 uses reserved0");
_Static_assert(offsetof(ouro_control_t, lease_view_off) == 56, "D12 uses reserved1[0]");
_Static_assert(offsetof(ouro_control_t, host_waiting) == 64, "D13 uses the old reserved1[0]");

/* One lease-view entry (D12): where a published arena lease's bytes are, so a
 * consumer in another process can read the payload a frame references by lease
 * id. The host writes an entry when it allocates the lease, before any frame
 * can name it, storing generation last with release order; it stores
 * generation 0 when the lease is reclaimed. The view lives in the control
 * region, so it is read-only to every client: a client can read where a lease
 * is, never move it. The lease table that decides lifetimes stays host-private. */
typedef struct
{
   _Atomic uint32_t generation; /* 0 = no live lease in this entry */
   _Atomic uint32_t len;        /* span length (>= any frame's payload_len) */
   _Atomic uint64_t offset;     /* into the arena's usable bytes */
} ouro_lease_view_t;

_Static_assert(sizeof(ouro_lease_view_t) == 16, "lease view entry size is part of D12");

/* The queue-pair region header, followed by the inbound and outbound rings.
 * inbound is host->client, outbound is client->host. The credit and control_lost
 * fields are laid out here by slice 3 and exercised by slice 7 (flow control)
 * and D6 (control-class delivery); they are initialised to defaults now. */
typedef struct
{
   _Atomic uint32_t magic;
   _Atomic uint32_t slot_size;
   _Atomic uint32_t capacity;
   _Atomic uint32_t data_credits;
   _Atomic uint32_t control_credits;
   _Atomic uint32_t control_lost;
   _Atomic uint32_t inbound_off;
   _Atomic uint32_t outbound_off;
   _Atomic uint64_t client_heartbeat;
   _Atomic uint32_t client_waiting; /* D13; client-written. Read iff F_DOORBELL */
   _Atomic uint32_t reserved0;
   _Atomic uint64_t reserved[6];
} ouro_qpair_hdr_t;

_Static_assert(sizeof(ouro_qpair_hdr_t) == 96, "queue-pair header size is frozen by layout_version");

/* D15: the client_heartbeat a detaching client leaves behind. The host releases
 * the slot at its next pump (or reap) instead of waiting for the heartbeat to
 * go stale; no clock reading is ever this value. */
#define OURO_HEARTBEAT_DETACHED UINT64_MAX
_Static_assert(offsetof(ouro_qpair_hdr_t, client_waiting) == 40, "D13 uses the old reserved[0]");

/* Handles into a mapped queue-pair region: the two ring handles plus the header. */
typedef struct
{
   ouro_qpair_hdr_t *hdr;
   ouro_ring_t inbound;
   ouro_ring_t outbound;
} ouro_qpair_t;

/* ---- region lifecycle ---- */

/* Map an existing region fd. writable selects PROT_READ vs PROT_READ|PROT_WRITE:
 * a client maps the control region read-only, its own queue pair and the arena
 * read/write. */
ouro_region_result_t ouro_region_map(int fd, size_t size, int writable, ouro_region_t *out);

/* Unmap and forget. Does not close the fd. */
void ouro_region_unmap(ouro_region_t *r);

/* ---- control region ---- */

/* The control region the host creates: the header followed by the lease view. */
size_t ouro_control_bytes(void);
/* The header alone. A client maps at least this; it maps ouro_control_bytes() when
 * the region is that large (a host that predates D12 creates only the header). */
size_t ouro_control_header_bytes(void);
/* Validate a mapped control region and return a pointer to its header. */
ouro_region_result_t ouro_control_attach(const ouro_region_t *r, ouro_control_t **out);

/* The lease view, if the host advertises it and the mapping covers it.
 * ERR_VERSION if the host does not advertise OURO_CONTROL_F_LEASE_VIEW, ERR_SIZE
 * if the mapping is only the header. */
ouro_region_result_t ouro_control_lease_view(const ouro_region_t *r, const ouro_control_t *c,
                                           const ouro_lease_view_t **out);

/* Resolve an arena frame's (lease_id, generation, payload_len) to an offset into
 * the arena's usable bytes. Every value is a claim and is checked: the entry
 * must be live with the frame's generation, the payload must fit the span, and
 * the span must lie inside the arena. ERR_GEOMETRY otherwise. Valid while the
 * caller holds a consumer ref, which the host gave it by delivering the frame. */
ouro_region_result_t ouro_lease_view_resolve(const ouro_lease_view_t *view, uint32_t lease_id,
                                           uint32_t generation, uint32_t payload_len,
                                           uint64_t arena_size, uint64_t *offset);

/* Liveness. A client caches the epoch it attached at; a change means the host
 * restarted and every handle and mapping is stale. */
uint64_t ouro_control_epoch(const ouro_control_t *c);
int ouro_control_epoch_changed(const ouro_control_t *c, uint64_t attached_epoch);

/* ---- queue-pair region ---- */

size_t ouro_qpair_bytes(uint32_t slot_size, uint32_t capacity);
/* Validate a mapped queue-pair region and resolve both ring handles. */
ouro_region_result_t ouro_qpair_attach(const ouro_region_t *r, ouro_qpair_t *out);

/* ---- arena region ---- */

size_t ouro_arena_region_bytes(uint64_t arena_size);
/* Validate a mapped arena region; returns the usable arena base and size. The
 * allocator over this space is slice 4. */
ouro_region_result_t ouro_arena_region_attach(const ouro_region_t *r, uint8_t **base, uint64_t *size);

const char *ouro_region_result_name(ouro_region_result_t r);

#endif /* OUROBOROS_REGION_H */
