/* Shared process-module runtime.
 *
 * Modules provide only a stage table and a handler. The core owns attach,
 * envelope validation, deadlines, cancellation, correlation-preserving replies,
 * heartbeats, and shutdown.
 */
#ifndef OUROBOROS_MODULE_RUNTIME_H
#define OUROBOROS_MODULE_RUNTIME_H 1

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <ouroboros/client.h>

#include <ouroboros/module_protocol.h>

typedef struct
{
   uint32_t event_kind;
   uint32_t stage_id;
} ouro_module_stage_t;

typedef struct
{
   uint32_t stage_id;
   uint64_t deadline_ns;
   uint64_t trace_id;
   const void *runtime_state;
   /* Copied from host-stamped bus frame identity, never body JSON. */
   uint32_t source_principal_ref;
   uint32_t source_handle;
} ouro_module_invocation_t;

/* A handler writes at most response_capacity bytes and sets response_len. It
 * should call ouro_module_invocation_cancelled() around bounded units of work.
 * The runtime checks cancellation and deadline again before publishing. */
typedef ouro_module_status_t (*ouro_module_handler_fn)(
    const ouro_module_invocation_t *invocation, const uint8_t *request_body, uint32_t request_len,
    uint8_t *response_body, uint32_t response_capacity, uint32_t *response_len, void *user_data);

typedef struct
{
   const char *socket_path;
   const char *module_name;
   uint32_t principal_class;
   uint32_t principal_ref;
   const ouro_module_stage_t *stages;
   size_t stage_count;
   ouro_module_handler_fn handler;
   void *user_data;
   /* D14. A notification of a kind this slot subscribes to, and an overflow
    * naming a kind it missed. Both run on the polling thread with user_data;
    * the payload is valid only for the call, so copy what you keep, and do not
    * block. NULL discards, as before D14. */
   void (*notify)(uint32_t event_kind, const uint8_t *payload, uint32_t payload_len,
                  void *user_data);
   void (*overflow)(uint32_t shed_kind, void *user_data);
} ouro_module_process_config_t;

/* True after a ring cancel for this correlation or once its absolute monotonic
 * deadline expires. Module code never owns clocks or transport cancellation. */
int ouro_module_invocation_cancelled(const ouro_module_invocation_t *invocation);

/* Borrowed cancellation flag, valid only during this joined handler call. */
const atomic_int *ouro_module_invocation_cancel_flag(const ouro_module_invocation_t *invocation);

/* Embedded host service. Borrows one exclusively owned attached client and an
 * immutable stage table. Installs NO signals and does not detach the client.
 * stop() ends admission, cancels and joins every worker before returning. The
 * loop sleeps on the client's doorbell while idle (D13), so whoever makes stop()
 * true MUST call ouro_client_interrupt(client). Without it, stop() is noticed only
 * at the next wakeup, and a client whose liveness both ends watch (D15) sleeps
 * with no timeout: that wakeup may never come. */
typedef int (*ouro_module_service_stop_fn)(void *context);
int ouro_module_service_run(ouro_client_t *client, const ouro_module_process_config_t *config,
                             ouro_module_service_stop_fn stop, void *stop_context);

/* Run until SIGINT/SIGTERM, host epoch change, or an attach error. */
int ouro_module_process_run(const ouro_module_process_config_t *config);

/* Request an orderly stop without a process signal (also useful in tests). */
void ouro_module_process_stop(void);

#endif /* OUROBOROS_MODULE_RUNTIME_H */
