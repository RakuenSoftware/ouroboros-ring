/* Shared core-side client for calls into local process modules.
 *
 * A daemon owns one of these over a dedicated ring client. The core, not
 * feature modules, owns correlation ids, AMOD envelopes, monotonic deadlines,
 * cancellation frames, stale-reply draining, and response validation.
 */
#ifndef OUROBOROS_MODULE_CLIENT_H
#define OUROBOROS_MODULE_CLIENT_H 1

#include <pthread.h>
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

#endif /* OUROBOROS_MODULE_CLIENT_H */
