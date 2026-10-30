#ifndef OUROBOROS_ATTACH_H
#define OUROBOROS_ATTACH_H 1

#include <stddef.h>
#include <stdint.h>

#define OURO_ATTACH_REQ_MAGIC   0x51524241u /* "ABRQ" */
#define OURO_ATTACH_REPLY_MAGIC 0x50524241u /* "ABRP" */

/* A successful attach carries exactly these descriptors, in this order, in one
 * SCM_RIGHTS message (D1): control (read-only), arena, the client's queue pair.
 * A doorbell grant (D13) adds the host doorbell and the client's own doorbell,
 * so it carries OURO_ATTACH_FDS_DOORBELL. */
#define OURO_ATTACH_FDS                3
#define OURO_ATTACH_FDS_DOORBELL       5
#define OURO_ATTACH_FDS_MAX            OURO_ATTACH_FDS_DOORBELL
#define OURO_ATTACH_FD_CONTROL         0
#define OURO_ATTACH_FD_ARENA           1
#define OURO_ATTACH_FD_QPAIR           2
#define OURO_ATTACH_FD_HOST_DOORBELL   3
#define OURO_ATTACH_FD_CLIENT_DOORBELL 4

/* Attach capabilities (D13): requested in the request, granted in the reply. */
#define OURO_ATTACH_CAP_DOORBELL 0x00000001u
/* D15: the host holds an OS handle on the client's process and releases its slot
 * when the process exits, so the client need not heartbeat on a timer. */
#define OURO_ATTACH_CAP_PROCESS_LIVENESS 0x00000002u

/* The v0 request was the first 16 bytes of ouro_attach_request_t. A host still
 * accepts it, and an older host reads only those 16 bytes of a longer one. */
#define OURO_ATTACH_REQ_V0_BYTES 16u

typedef enum
{
   OURO_ATTACH_OK = 0,
   OURO_ATTACH_DENIED_POLICY,
   OURO_ATTACH_DENIED_VERSION,
   OURO_ATTACH_DENIED_NOSLOT,
   OURO_ATTACH_PROTOCOL
} ouro_attach_status_t;

typedef struct
{
   uint32_t magic;
   uint16_t wire_version_min;
   uint16_t wire_version_max;
   uint32_t principal_class;
   uint32_t principal_ref;
   uint32_t capabilities; /* D13; absent (zero) in a 16-byte v0 request */
} ouro_attach_request_t;

typedef struct
{
   uint32_t magic;
   uint32_t status;
   uint32_t handle_id;
   uint32_t wire_version;
   uint32_t slot_size;
   uint32_t inline_budget;
   uint32_t queue_capacity;
   uint32_t capabilities; /* D13: the requested bits the host granted; was reserved */
   uint64_t arena_size;
   uint64_t host_epoch;
} ouro_attach_reply_t;

/* The daemon admission callback receives the connected attach socket so it can
 * bind the request's opaque principal to an authenticated OS peer identity
 * (for example SO_PEERCRED) before trusting the claimed values. */
typedef ouro_attach_status_t (*ouro_admit_fn)(void *ctx, int attach_fd,
                                            const ouro_attach_request_t *req);

_Static_assert(sizeof(ouro_attach_request_t) == 20, "attach request size is in layout_vectors.tsv");
_Static_assert(sizeof(ouro_attach_reply_t) == 48, "attach reply size is in layout_vectors.tsv");

/* max_fds may be up to OURO_ATTACH_FDS_MAX. */
int ouro_fd_send(int sock, const void *payload, size_t len, const int *fds, int n);
long ouro_fd_recv(int sock, void *payload, size_t len, int *fds, int max_fds, int *n_out);

#endif
