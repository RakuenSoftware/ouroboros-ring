/* Shared core-side client for calls into local process modules.
 *
 * A daemon owns one of these over a dedicated ring client. The core, not
 * feature modules, owns correlation ids, AMOD envelopes, monotonic deadlines,
 * cancellation frames, stale-reply draining, and response validation.
 */
#ifndef OUROBOROS_MODULE_CLIENT_H
#define OUROBOROS_MODULE_CLIENT_H 1

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include <ouroboros/client.h>

typedef enum
{
   OURO_MODULE_CALL_OK = 0,
   OURO_MODULE_CALL_CAPABILITY_ABSENT,
   OURO_MODULE_CALL_CAPABILITY_DENIED,
   OURO_MODULE_CALL_CANCELLED,
   OURO_MODULE_CALL_DEADLINE_EXCEEDED,
   OURO_MODULE_CALL_INVALID_REQUEST,
   OURO_MODULE_CALL_INTERNAL,
   OURO_MODULE_CALL_RESPONSE_TOO_LARGE,
   OURO_MODULE_CALL_TRANSPORT,
   OURO_MODULE_CALL_PROTOCOL,
   OURO_MODULE_CALL_INVALID_ARGUMENT
} ouro_module_call_result_t;

typedef int (*ouro_module_cancelled_fn)(void *context);

typedef struct
{
   ouro_client_t *ring;
   pthread_mutex_t lock;
   uint64_t next_correlation;
   int initialized;
} ouro_module_client_t;

/* The ring client is borrowed and must remain attached until destroy returns. */
int ouro_module_client_init(ouro_module_client_t *client, ouro_client_t *ring);
void ouro_module_client_destroy(ouro_module_client_t *client);

/* Synchronously invoke one module stage. deadline_ns is an absolute
 * CLOCK_MONOTONIC deadline; zero disables the deadline. When cancellation or a
 * deadline fires, core publishes F_CANCEL and returns the typed outcome. A late
 * terminal reply is ignored by the next call through this dedicated client.
 *
 * response_len receives the complete returned body length. If it is greater
 * than response_capacity, no partial response is copied and the result is
 * RESPONSE_TOO_LARGE. */
ouro_module_call_result_t ouro_module_client_call(
    ouro_module_client_t *client, uint32_t event_kind, uint32_t stage_id, uint64_t trace_id,
    uint64_t deadline_ns, const void *request_body, uint32_t request_len, void *response_body,
    uint32_t response_capacity, uint32_t *response_len, ouro_module_cancelled_fn cancelled,
    void *cancel_context);

const char *ouro_module_call_result_name(ouro_module_call_result_t result);

/* A pool of module clients, for a process that makes many calls at once.
 *
 * One ouro_module_client_t serves one call at a time. A host that attaches
 * several of its own clients (ouro_host_attach_local) can pool them: each call
 * checks out a free client, makes the call, and returns it. A caller that finds
 * every client busy waits only within its own deadline, or a bounded 30 seconds
 * when it has none, and is answered DEADLINE_EXCEEDED rather than queued without
 * end. This is a convenience of the C implementation, not part of the contract.
 *
 * The rings are borrowed and must stay attached until destroy returns. */
#define OURO_MODULE_CLIENT_POOL_MAX 64

typedef struct
{
   ouro_module_client_t clients[OURO_MODULE_CLIENT_POOL_MAX];
   int in_use[OURO_MODULE_CLIENT_POOL_MAX];
   size_t count;
   pthread_mutex_t lock;
   pthread_cond_t freed;
   int in_flight;
   int peak_in_flight;
   atomic_int stopping;
   int initialized;
} ouro_module_client_pool_t;

/* rings[0..count) are attached clients this pool may use; 1 <= count <= MAX. */
int ouro_module_client_pool_init(ouro_module_client_pool_t *pool, ouro_client_t *const *rings,
                                 size_t count);

/* ouro_module_client_call over a pooled client, with the same arguments and
 * results. Waiting for a client is bounded as above. After
 * ouro_module_client_pool_stop, new calls are refused with TRANSPORT and calls
 * in flight see themselves cancelled. */
ouro_module_call_result_t ouro_module_client_pool_call(
    ouro_module_client_pool_t *pool, uint32_t event_kind, uint32_t stage_id, uint64_t trace_id,
    uint64_t deadline_ns, const void *request_body, uint32_t request_len, void *response_body,
    uint32_t response_capacity, uint32_t *response_len, ouro_module_cancelled_fn cancelled,
    void *cancel_context);

/* Refuse new calls, cancel those in flight, and wake every waiter. */
void ouro_module_client_pool_stop(ouro_module_client_pool_t *pool);

/* The most calls that have held a client at once: a pool that peaks at its
 * size is serializing callers. */
int ouro_module_client_pool_peak(ouro_module_client_pool_t *pool);

/* Release the pool. Every call must have returned; the rings are not detached. */
void ouro_module_client_pool_destroy(ouro_module_client_pool_t *pool);

#endif /* OUROBOROS_MODULE_CLIENT_H */
