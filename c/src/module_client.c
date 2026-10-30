#define _POSIX_C_SOURCE 200809L

#include <ouroboros/module_client.h>

#include <ouroboros/module_protocol.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static uint64_t monotonic_now_ns(void)
{
   struct timespec now;
   if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
      return 0;
   return (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
}

static int cancelled_now(ouro_module_cancelled_fn cancelled, void *context)
{
   return cancelled && cancelled(context);
}

static int deadline_now(uint64_t deadline_ns)
{
   uint64_t now = monotonic_now_ns();
   return now != 0 && ouro_module_deadline_expired(deadline_ns, now);
}

/* D13: sleep on the doorbell until the reply or room arrives, bounded by the
 * deadline and, when a cancel callback must be checked, by a short slice. An
 * older host grants no doorbell; then this is the old 1ms poll. */
static void wait_for_ring(ouro_client_t *ring, uint64_t deadline_ns, int cancellable)
{
   int timeout_ms = cancellable ? 50 : 1000;
   uint64_t now = monotonic_now_ns();
   if (deadline_ns != 0 && now != 0)
   {
      uint64_t left_ms = deadline_ns > now ? (deadline_ns - now) / 1000000u + 1u : 0u;
      if (left_ms < (uint64_t)timeout_ms)
         timeout_ms = (int)left_ms;
   }
   if (ouro_client_wait(ring, timeout_ms) < 0)
   {
      const struct timespec idle = {.tv_sec = 0, .tv_nsec = 1000000};
      nanosleep(&idle, NULL);
   }
}

static uint64_t next_correlation(ouro_module_client_t *client)
{
   uint64_t value = client->next_correlation++;
   if (value == 0)
      value = client->next_correlation++;
   if (client->next_correlation == 0)
      client->next_correlation = 1;
   return value;
}

int ouro_module_client_init(ouro_module_client_t *client, ouro_client_t *ring)
{
   if (!client || !ring || !ring->ctl)
      return -1;
   memset(client, 0, sizeof(*client));
   if (pthread_mutex_init(&client->lock, NULL) != 0)
      return -1;
   client->ring = ring;
   client->next_correlation = 1;
   client->initialized = 1;
   return 0;
}

void ouro_module_client_destroy(ouro_module_client_t *client)
{
   if (!client || !client->initialized)
      return;
   pthread_mutex_destroy(&client->lock);
   memset(client, 0, sizeof(*client));
}

static ouro_module_call_result_t status_result(uint16_t status)
{
   switch ((ouro_module_status_t)status)
   {
   case OURO_MODULE_STATUS_OK:
      return OURO_MODULE_CALL_OK;
   case OURO_MODULE_STATUS_CAPABILITY_ABSENT:
      return OURO_MODULE_CALL_CAPABILITY_ABSENT;
   case OURO_MODULE_STATUS_CANCELLED:
      return OURO_MODULE_CALL_CANCELLED;
   case OURO_MODULE_STATUS_DEADLINE_EXCEEDED:
      return OURO_MODULE_CALL_DEADLINE_EXCEEDED;
   case OURO_MODULE_STATUS_INVALID_REQUEST:
      return OURO_MODULE_CALL_INVALID_REQUEST;
   case OURO_MODULE_STATUS_INTERNAL:
      return OURO_MODULE_CALL_INTERNAL;
   default:
      return OURO_MODULE_CALL_PROTOCOL;
   }
}

static void cancel_request(ouro_client_t *ring, uint32_t event_kind, uint64_t correlation)
{
   const struct timespec retry = {.tv_sec = 0, .tv_nsec = 1000000};
   for (unsigned attempt = 0; attempt < 100; ++attempt)
   {
      ouro_client_result_t result = ouro_client_cancel(ring, event_kind, correlation);
      if (result != OURO_CLIENT_WOULD_BLOCK)
         return;
      nanosleep(&retry, NULL);
   }
}

ouro_module_call_result_t
ouro_module_client_call(ouro_module_client_t *client, uint32_t event_kind, uint32_t stage_id,
                         uint64_t trace_id, uint64_t deadline_ns, const void *request_body,
                         uint32_t request_len, void *response_body, uint32_t response_capacity,
                         uint32_t *response_len, ouro_module_cancelled_fn cancelled,
                         void *cancel_context)
{
   if (response_len)
      *response_len = 0;
   if (!client || !client->initialized || !client->ring || event_kind < OURO_KIND_MODULE_BASE ||
       stage_id == 0 || request_len > OURO_MODULE_MESSAGE_MAX_BODY ||
       (request_len > 0 && !request_body) || (response_capacity > 0 && !response_body) ||
       !response_len)
      return OURO_MODULE_CALL_INVALID_ARGUMENT;
   if (cancelled_now(cancelled, cancel_context))
      return OURO_MODULE_CALL_CANCELLED;
   if (deadline_now(deadline_ns))
      return OURO_MODULE_CALL_DEADLINE_EXCEEDED;

   pthread_mutex_lock(&client->lock);
   ouro_module_call_result_t result = OURO_MODULE_CALL_TRANSPORT;
   if (client->ring->reply.inline_budget <= OURO_MODULE_MESSAGE_HEADER_LEN)
   {
      result = OURO_MODULE_CALL_TRANSPORT;
      goto done;
   }
   uint32_t chunk_capacity = client->ring->reply.inline_budget - OURO_MODULE_MESSAGE_HEADER_LEN;
   uint8_t *payload = malloc(client->ring->reply.inline_budget);
   if (!payload)
   {
      result = OURO_MODULE_CALL_INTERNAL;
      goto done;
   }
   const uint64_t correlation = next_correlation(client);
   uint32_t request_offset = 0;
   int first_fragment = 1;
   int request_started = 0;
   while (first_fragment || request_offset < request_len)
   {
      first_fragment = 0;
      if (cancelled_now(cancelled, cancel_context))
      {
         free(payload);
         if (request_started)
            cancel_request(client->ring, event_kind, correlation);
         result = OURO_MODULE_CALL_CANCELLED;
         goto done;
      }
      if (deadline_now(deadline_ns))
      {
         free(payload);
         if (request_started)
            cancel_request(client->ring, event_kind, correlation);
         result = OURO_MODULE_CALL_DEADLINE_EXCEEDED;
         goto done;
      }
      uint32_t remaining = request_len - request_offset;
      uint32_t part = remaining < chunk_capacity ? remaining : chunk_capacity;
      int more = remaining > part;
      size_t payload_len = (size_t)OURO_MODULE_MESSAGE_HEADER_LEN + part;
      ouro_module_message_t request = {.operation = OURO_MODULE_OP_INVOKE,
                                        .stage_id = stage_id,
                                        .body_len = part,
                                        .deadline_ns = deadline_ns,
                                        .trace_id = trace_id};
      if (ouro_module_message_encode(&request, payload, payload_len) == 0)
      {
         free(payload);
         result = OURO_MODULE_CALL_INVALID_ARGUMENT;
         goto done;
      }
      if (part)
         memcpy(payload + OURO_MODULE_MESSAGE_HEADER_LEN,
                (const uint8_t *)request_body + request_offset, part);
      for (;;)
      {
         ouro_client_result_t sent = ouro_client_request_fragment(
             client->ring, event_kind, correlation, payload, (uint32_t)payload_len, more);
         if (sent == OURO_CLIENT_OK)
         {
            request_started = 1;
            break;
         }
         if (sent != OURO_CLIENT_WOULD_BLOCK)
         {
            free(payload);
            if (request_started)
               cancel_request(client->ring, event_kind, correlation);
            result = OURO_MODULE_CALL_TRANSPORT;
            goto done;
         }
         if (cancelled_now(cancelled, cancel_context) || deadline_now(deadline_ns))
            break;
         wait_for_ring(client->ring, deadline_ns, cancelled != NULL);
      }
      if (cancelled_now(cancelled, cancel_context) || deadline_now(deadline_ns))
         continue; /* the top of the loop sends cancellation and returns */
      request_offset += part;
   }
   free(payload);

   uint32_t response_total = 0;
   int response_too_large = 0;
   for (;;)
   {
      if (cancelled_now(cancelled, cancel_context))
      {
         cancel_request(client->ring, event_kind, correlation);
         result = OURO_MODULE_CALL_CANCELLED;
         goto done;
      }
      if (deadline_now(deadline_ns))
      {
         cancel_request(client->ring, event_kind, correlation);
         result = OURO_MODULE_CALL_DEADLINE_EXCEEDED;
         goto done;
      }

      /* A host that restarted or exited (D15) will never answer. */
      if (ouro_client_epoch_changed(client->ring))
      {
         result = OURO_MODULE_CALL_TRANSPORT;
         goto done;
      }
      uint64_t now = monotonic_now_ns();
      if (now)
         ouro_client_heartbeat(client->ring, now);
      ouro_event_t event;
      ouro_client_result_t polled = ouro_client_poll(client->ring, &event);
      if (polled == OURO_CLIENT_EMPTY)
      {
         wait_for_ring(client->ring, deadline_ns, cancelled != NULL);
         continue;
      }
      if (polled != OURO_CLIENT_OK)
      {
         result = OURO_MODULE_CALL_TRANSPORT;
         goto done;
      }
      /* Timed-out calls may finish after their caller has returned. This client
       * is dedicated to module RPC, so an unmatched correlation is stale and can
       * be safely drained here rather than poisoning the next call. */
      if (event.frame.correlation_id != correlation)
         continue;
      if (event.frame.event_kind == OURO_KIND_CAPABILITY_ABSENT)
      {
         result = OURO_MODULE_CALL_CAPABILITY_ABSENT;
         goto done;
      }
      if (event.frame.event_kind == OURO_KIND_CAPABILITY_DENIED)
      {
         result = OURO_MODULE_CALL_CAPABILITY_DENIED;
         goto done;
      }
      if (!(event.frame.hdr_flags & OURO_F_REPLY) || event.frame.event_kind != event_kind)
      {
         result = OURO_MODULE_CALL_PROTOCOL;
         goto done;
      }
      ouro_module_message_t reply;
      if (ouro_module_message_decode(event.payload, event.payload_len, &reply) !=
              OURO_MODULE_MESSAGE_OK ||
          reply.operation != OURO_MODULE_OP_RESULT || reply.stage_id != stage_id ||
          reply.trace_id != trace_id)
      {
         result = OURO_MODULE_CALL_PROTOCOL;
         goto done;
      }
      result = status_result(reply.status);
      if (result != OURO_MODULE_CALL_OK)
      {
         if ((event.frame.hdr_flags & OURO_F_MORE) || reply.body_len != 0 || response_total != 0)
            result = OURO_MODULE_CALL_PROTOCOL;
         goto done;
      }
      if (response_total > OURO_MODULE_MESSAGE_MAX_BODY - reply.body_len)
      {
         result = OURO_MODULE_CALL_PROTOCOL;
         goto done;
      }
      uint32_t next_total = response_total + reply.body_len;
      if (reply.body_len)
      {
         if (next_total <= response_capacity)
            memcpy((uint8_t *)response_body + response_total,
                   event.payload + OURO_MODULE_MESSAGE_HEADER_LEN, reply.body_len);
         else
            response_too_large = 1;
      }
      response_total = next_total;
      *response_len = response_total;
      if (event.frame.hdr_flags & OURO_F_MORE)
         continue;
      result = response_too_large ? OURO_MODULE_CALL_RESPONSE_TOO_LARGE : OURO_MODULE_CALL_OK;
      goto done;
   }

done:
   pthread_mutex_unlock(&client->lock);
   return result;
}

const char *ouro_module_call_result_name(ouro_module_call_result_t result)
{
   static const char *const names[] = {"OK",        "CAPABILITY_ABSENT",  "CAPABILITY_DENIED",
                                       "CANCELLED", "DEADLINE_EXCEEDED",  "INVALID_REQUEST",
                                       "INTERNAL",  "RESPONSE_TOO_LARGE", "TRANSPORT",
                                       "PROTOCOL",  "INVALID_ARGUMENT"};
   return (unsigned)result < sizeof(names) / sizeof(names[0]) ? names[result] : "UNKNOWN";
}

/* ---- pool ---- */

/* Waiting for a client is bounded even without a deadline: an unbounded wait
 * on a busy pool is indistinguishable from a deadlock. */
#define POOL_UNBOUNDED_WAIT_S 30

int ouro_module_client_pool_init(ouro_module_client_pool_t *pool, ouro_client_t *const *rings,
                                 size_t count)
{
   if (!pool || !rings || count == 0 || count > OURO_MODULE_CLIENT_POOL_MAX)
      return -1;
   memset(pool, 0, sizeof(*pool));
   pthread_condattr_t attr;
   if (pthread_condattr_init(&attr) != 0)
      return -1;
   /* The pool waits against monotonic deadlines, so its condition must too. */
   int cond_ok = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) == 0 &&
                 pthread_cond_init(&pool->freed, &attr) == 0;
   pthread_condattr_destroy(&attr);
   if (!cond_ok)
      return -1;
   if (pthread_mutex_init(&pool->lock, NULL) != 0)
   {
      pthread_cond_destroy(&pool->freed);
      return -1;
   }
   for (size_t i = 0; i < count; ++i)
   {
      if (ouro_module_client_init(&pool->clients[i], rings[i]) != 0)
      {
         while (i-- > 0)
            ouro_module_client_destroy(&pool->clients[i]);
         pthread_mutex_destroy(&pool->lock);
         pthread_cond_destroy(&pool->freed);
         return -1;
      }
   }
   pool->count = count;
   atomic_init(&pool->stopping, 0);
   pool->initialized = 1;
   return 0;
}

/* Check out a free client, waiting no later than deadline_ns. -1 when none
 * freed in time or the pool is stopping. */
static int pool_acquire(ouro_module_client_pool_t *pool, uint64_t deadline_ns)
{
   struct timespec until;
   if (deadline_ns)
   {
      until.tv_sec = (time_t)(deadline_ns / 1000000000ULL);
      until.tv_nsec = (long)(deadline_ns % 1000000000ULL);
   }
   else
   {
      if (clock_gettime(CLOCK_MONOTONIC, &until) != 0)
         return -1;
      until.tv_sec += POOL_UNBOUNDED_WAIT_S;
   }
   pthread_mutex_lock(&pool->lock);
   for (;;)
   {
      if (atomic_load_explicit(&pool->stopping, memory_order_acquire))
         break;
      for (size_t i = 0; i < pool->count; ++i)
      {
         if (!pool->in_use[i])
         {
            pool->in_use[i] = 1;
            if (++pool->in_flight > pool->peak_in_flight)
               pool->peak_in_flight = pool->in_flight;
            pthread_mutex_unlock(&pool->lock);
            return (int)i;
         }
      }
      if (pthread_cond_timedwait(&pool->freed, &pool->lock, &until) == ETIMEDOUT)
         break;
   }
   pthread_mutex_unlock(&pool->lock);
   return -1;
}

static void pool_release(ouro_module_client_pool_t *pool, int index)
{
   pthread_mutex_lock(&pool->lock);
   pool->in_use[index] = 0;
   pool->in_flight--;
   pthread_cond_signal(&pool->freed);
   pthread_mutex_unlock(&pool->lock);
}

typedef struct
{
   ouro_module_client_pool_t *pool;
   ouro_module_cancelled_fn cancelled;
   void *context;
} pool_cancel_t;

static int pool_cancelled(void *context)
{
   const pool_cancel_t *state = context;
   return atomic_load_explicit(&state->pool->stopping, memory_order_acquire) ||
          (state->cancelled && state->cancelled(state->context));
}

ouro_module_call_result_t ouro_module_client_pool_call(
    ouro_module_client_pool_t *pool, uint32_t event_kind, uint32_t stage_id, uint64_t trace_id,
    uint64_t deadline_ns, const void *request_body, uint32_t request_len, void *response_body,
    uint32_t response_capacity, uint32_t *response_len, ouro_module_cancelled_fn cancelled,
    void *cancel_context)
{
   if (response_len)
      *response_len = 0;
   if (!pool || !pool->initialized)
      return OURO_MODULE_CALL_INVALID_ARGUMENT;
   if (atomic_load_explicit(&pool->stopping, memory_order_acquire))
      return OURO_MODULE_CALL_TRANSPORT;
   int index = pool_acquire(pool, deadline_ns);
   if (index < 0)
      return atomic_load_explicit(&pool->stopping, memory_order_acquire)
                 ? OURO_MODULE_CALL_TRANSPORT
                 : OURO_MODULE_CALL_DEADLINE_EXCEEDED;
   pool_cancel_t state = {.pool = pool, .cancelled = cancelled, .context = cancel_context};
   ouro_module_call_result_t result = ouro_module_client_call(
       &pool->clients[index], event_kind, stage_id, trace_id, deadline_ns, request_body,
       request_len, response_body, response_capacity, response_len, pool_cancelled, &state);
   pool_release(pool, index);
   return result;
}

void ouro_module_client_pool_stop(ouro_module_client_pool_t *pool)
{
   if (!pool || !pool->initialized)
      return;
   pthread_mutex_lock(&pool->lock);
   atomic_store_explicit(&pool->stopping, 1, memory_order_release);
   pthread_cond_broadcast(&pool->freed);
   pthread_mutex_unlock(&pool->lock);
}

int ouro_module_client_pool_peak(ouro_module_client_pool_t *pool)
{
   if (!pool || !pool->initialized)
      return 0;
   pthread_mutex_lock(&pool->lock);
   int peak = pool->peak_in_flight;
   pthread_mutex_unlock(&pool->lock);
   return peak;
}

void ouro_module_client_pool_destroy(ouro_module_client_pool_t *pool)
{
   if (!pool || !pool->initialized)
      return;
   for (size_t i = 0; i < pool->count; ++i)
      ouro_module_client_destroy(&pool->clients[i]);
   pthread_mutex_destroy(&pool->lock);
   pthread_cond_destroy(&pool->freed);
   memset(pool, 0, sizeof(*pool));
}
