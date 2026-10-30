/* test_module_client_pool.c: concurrent module calls through a pool of clients.
 *
 * One host, one serving module, and a pool of two attached callers. Proves a
 * call goes through; two calls can be in flight at once (the pool's peak);
 * a third, with every client busy, is answered DEADLINE_EXCEEDED within its own
 * deadline instead of waiting behind them; and stop cancels a call in flight
 * and refuses new ones.
 */
#define _POSIX_C_SOURCE 200809L
#include <ouroboros/host.h>
#include <ouroboros/local_attach.h>
#include <ouroboros/module_client.h>
#include <ouroboros/module_runtime.h>
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define KIND 4353u
#define STAGE 1u

static ouro_host_t host;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static atomic_int pump_stop, release_holds, held;
static const struct timespec pause_one = {0, 1000000};

static uint64_t now(void)
{
   struct timespec t;
   assert(!clock_gettime(CLOCK_MONOTONIC, &t));
   return (uint64_t)t.tv_sec * 1000000000ULL + (uint64_t)t.tv_nsec;
}

static void *pump(void *unused)
{
   (void)unused;
   while (!atomic_load(&pump_stop))
   {
      pthread_mutex_lock(&lock);
      ouro_host_pump(&host);
      pthread_mutex_unlock(&lock);
      nanosleep(&pause_one, NULL);
   }
   return NULL;
}

/* "hold" waits until released (or cancelled); anything else is echoed. */
static ouro_module_status_t handle(const ouro_module_invocation_t *inv, const uint8_t *req,
                                   uint32_t len, uint8_t *out, uint32_t cap, uint32_t *outlen,
                                   void *data)
{
   (void)data;
   if (len == 4 && !memcmp(req, "hold", 4))
   {
      atomic_fetch_add(&held, 1);
      while (!atomic_load(&release_holds) && !ouro_module_invocation_cancelled(inv))
         nanosleep(&pause_one, NULL);
      atomic_fetch_sub(&held, 1);
   }
   assert(len <= cap);
   memcpy(out, req, len);
   *outlen = len;
   return OURO_MODULE_STATUS_OK;
}

typedef struct
{
   ouro_client_t bus;
   ouro_module_process_config_t config;
   atomic_int stop;
} service_t;

static int service_stopping(void *data)
{
   return atomic_load(&((service_t *)data)->stop);
}

static void *serve(void *data)
{
   service_t *s = data;
   (void)ouro_module_service_run(&s->bus, &s->config, service_stopping, s);
   return NULL;
}

typedef struct
{
   ouro_module_client_pool_t *pool;
   const char *body;
   uint64_t deadline_ns;
   ouro_module_call_result_t result;
} call_t;

static void *call(void *data)
{
   call_t *c = data;
   char out[16];
   uint32_t n = 0;
   c->result = ouro_module_client_pool_call(c->pool, KIND, STAGE, 7, c->deadline_ns, c->body,
                                            (uint32_t)strlen(c->body), out, sizeof out, &n, NULL,
                                            NULL);
   return NULL;
}

static void wait_for_held(int want)
{
   for (int i = 0; i < 5000 && atomic_load(&held) != want; i++)
      nanosleep(&pause_one, NULL);
   assert(atomic_load(&held) == want);
}

int main(void)
{
   ouro_host_config_t cfg = {.max_slots = 8,
                             .slot_size = 4096,
                             .inline_budget = 512,
                             .queue_capacity = 16,
                             .arena_size = 1024 * 1024};
   assert(ouro_host_create(&host, &cfg, NULL, NULL) == OURO_HOST_OK);

   service_t svc = {0};
   assert(ouro_host_attach_local(&host, &svc.bus, 0) == OURO_CLIENT_OK);
   static const ouro_module_stage_t stages[] = {{KIND, STAGE}};
   svc.config = (ouro_module_process_config_t){
       .stages = stages, .stage_count = 1, .handler = handle};
   assert(ouro_host_serve_kind(&host, svc.bus.reply.handle_id, KIND) == OURO_HOST_OK);

   ouro_client_t a, b;
   assert(ouro_host_attach_local(&host, &a, 0) == OURO_CLIENT_OK);
   assert(ouro_host_attach_local(&host, &b, 0) == OURO_CLIENT_OK);
   ouro_client_t *rings[] = {&a, &b};
   ouro_module_client_pool_t pool;
   assert(ouro_module_client_pool_init(&pool, rings, 0) == -1);
   assert(ouro_module_client_pool_init(&pool, rings, 2) == 0);

   pthread_t pt, st;
   assert(!pthread_create(&pt, NULL, pump, NULL));
   assert(!pthread_create(&st, NULL, serve, &svc));

   /* A call goes through. */
   char out[16];
   uint32_t n = 0;
   assert(ouro_module_client_pool_call(&pool, KIND, STAGE, 1, now() + 5000000000ULL, "echo", 4,
                                       out, sizeof out, &n, NULL, NULL) == OURO_MODULE_CALL_OK);
   assert(n == 4 && !memcmp(out, "echo", 4));

   /* Two at once, then a third that finds both clients busy. */
   call_t one = {&pool, "hold", now() + 10000000000ULL, OURO_MODULE_CALL_INTERNAL};
   call_t two = {&pool, "hold", now() + 10000000000ULL, OURO_MODULE_CALL_INTERNAL};
   pthread_t t1, t2;
   assert(!pthread_create(&t1, NULL, call, &one));
   assert(!pthread_create(&t2, NULL, call, &two));
   wait_for_held(2);
   assert(ouro_module_client_pool_peak(&pool) == 2);
   uint64_t before = now();
   assert(ouro_module_client_pool_call(&pool, KIND, STAGE, 3, before + 50000000ULL, "echo", 4, out,
                                       sizeof out, &n, NULL,
                                       NULL) == OURO_MODULE_CALL_DEADLINE_EXCEEDED);
   assert(now() - before < 2000000000ULL); /* answered within its deadline, not after the holds */
   atomic_store(&release_holds, 1);
   pthread_join(t1, NULL);
   pthread_join(t2, NULL);
   assert(one.result == OURO_MODULE_CALL_OK && two.result == OURO_MODULE_CALL_OK);
   wait_for_held(0);

   /* Stop cancels a call in flight and refuses new ones. */
   atomic_store(&release_holds, 0);
   call_t three = {&pool, "hold", now() + 10000000000ULL, OURO_MODULE_CALL_INTERNAL};
   pthread_t t3;
   assert(!pthread_create(&t3, NULL, call, &three));
   wait_for_held(1);
   ouro_module_client_pool_stop(&pool);
   pthread_join(t3, NULL);
   assert(three.result == OURO_MODULE_CALL_CANCELLED);
   assert(ouro_module_client_pool_call(&pool, KIND, STAGE, 4, now() + 5000000000ULL, "echo", 4,
                                       out, sizeof out, &n, NULL,
                                       NULL) == OURO_MODULE_CALL_TRANSPORT);
   wait_for_held(0);

   atomic_store(&svc.stop, 1);
   ouro_client_interrupt(&svc.bus); /* the loop sleeps on its doorbell (D13) */
   pthread_join(st, NULL);
   atomic_store(&pump_stop, 1);
   pthread_join(pt, NULL);
   ouro_module_client_pool_destroy(&pool);
   ouro_client_detach(&a);
   ouro_client_detach(&b);
   ouro_client_detach(&svc.bus);
   ouro_host_destroy(&host);
   puts("module client pool: call, concurrency and peak, bounded wait, stop PASS");
   return 0;
}
