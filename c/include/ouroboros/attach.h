#ifndef OUROBOROS_ATTACH_H
#define OUROBOROS_ATTACH_H 1

#include <stddef.h>
#include <stdint.h>

#define OURO_ATTACH_REQ_MAGIC   0x51524241u /* "ABRQ" */
#define OURO_ATTACH_REPLY_MAGIC 0x50524241u /* "ABRP" */

/* A successful attach carries exactly these descriptors, in this order, in one
 * SCM_RIGHTS message (D1): control (read-only), arena, the client's queue pair. */
#define OURO_ATTACH_FDS        3
#define OURO_ATTACH_FD_CONTROL 0
#define OURO_ATTACH_FD_ARENA   1
#define OURO_ATTACH_FD_QPAIR   2

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
   uint32_t reserved;
   uint64_t arena_size;
   uint64_t host_epoch;
} ouro_attach_reply_t;

/* The daemon admission callback receives the connected attach socket so it can
 * bind the request's opaque principal to an authenticated OS peer identity
 * (for example SO_PEERCRED) before trusting the claimed values. */
typedef ouro_attach_status_t (*ouro_admit_fn)(void *ctx, int attach_fd,
                                            const ouro_attach_request_t *req);

int ouro_fd_send(int sock, const void *payload, size_t len, const int *fds, int n);
long ouro_fd_recv(int sock, void *payload, size_t len, int *fds, int max_fds, int *n_out);

#endif
