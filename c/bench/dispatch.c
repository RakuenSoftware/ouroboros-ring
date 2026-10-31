/* dispatch.c: the ring's dispatch-overhead benchmark.
 *
 * It measures the one number the performance budget commits to: per-event
 * dispatch overhead -- a producer writing its outbound ring, the host routing,
 * and a subscriber reading its inbound ring -- excluding any work on the
 * payload. That is the cost the ring adds to move one event.
 *
 * It prints the lines scripts/check_perf_budget.sh parses:
 *   dispatch_overhead_ns=<median per-event nanoseconds>
 *   dispatch_overhead_p99_ns=<p99>
 *
 * With OURO_BENCH_TELEMETRY=1 the host also writes a D16 record per event, and
 * a consumer thread drains them through the reader's own paced wait, as an
 * exporter would, so the same number shows what the hand-off adds. It also
 * prints telemetry_wakes=<doorbell writes>, which a busy consumer keeps near 0.
 *
 * OURO_BENCH_HOST_CPU and OURO_BENCH_DRAIN_CPU pin the two threads. Results
 * compare only on one machine with the same pinning.
 */
#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <ouroboros/local_attach.h>
#include <ouroboros/telemetry.h>

/* Pin the calling thread to the CPU an environment variable names, if any. */
static void pin_from_env(const char *name)
{
   const char *v = getenv(name);
   if (!v || !*v)
      return;
   cpu_set_t set;
   CPU_ZERO(&set);
   CPU_SET(atoi(v), &set);
   if (pthread_setaffinity_np(pthread_self(), sizeof set, &set) != 0)
   {
      fprintf(stderr, "bench: cannot pin to cpu %s\n", v);
      abort();
   }
}

static atomic_int telemetry_stop;
static atomic_ullong telemetry_drained;

static void *telemetry_drain(void *p)
{
   ouro_telemetry_reader_t *r = p;
   pin_from_env("OURO_BENCH_DRAIN_CPU");
   while (!atomic_load_explicit(&telemetry_stop, memory_order_relaxed))
   {
      uint64_t n = ouro_telemetry_available(r);
      if (n == 0)
      {
         (void)ouro_telemetry_wait(r, -1, 10);
         continue;
      }
      ouro_telemetry_release(r, n);
      atomic_fetch_add_explicit(&telemetry_drained, n, memory_order_relaxed);
   }
   return NULL;
}

static uint64_t now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

static void attach_client(ouro_host_t *h, ouro_client_t *c)
{
   if (ouro_host_attach_local(h, c, 0) != OURO_CLIENT_OK)
      abort();
}

static int cmp_u64(const void *a, const void *b)
{
   uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
   return (x > y) - (x < y);
}

#define KIND 900

int main(int argc, char **argv)
{
   uint64_t total = (argc > 1) ? strtoull(argv[1], NULL, 10) : 2000000;
   pin_from_env("OURO_BENCH_HOST_CPU");

   ouro_host_config_t cfg;
   memset(&cfg, 0, sizeof cfg);
   cfg.max_slots = 4;
   cfg.slot_size = 256;
   cfg.inline_budget = 192;
   cfg.queue_capacity = 64;
   cfg.arena_size = 256 * 1024;

   ouro_host_t h;
   if (ouro_host_create(&h, &cfg, NULL, NULL) != OURO_HOST_OK)
      abort();
   ouro_client_t pub, sub;
   attach_client(&h, &pub);
   attach_client(&h, &sub);
   ouro_host_subscribe(&h, sub.reply.handle_id, KIND);

   const char *tenv = getenv("OURO_BENCH_TELEMETRY");
   int telemetry = tenv && strcmp(tenv, "1") == 0;
   ouro_telemetry_reader_t reader;
   pthread_t drainer;
   if (telemetry)
   {
      if (ouro_host_telemetry_enable(&h, 0) != OURO_HOST_OK ||
          ouro_telemetry_reader_open(dup(h.telemetry_region.fd), dup(h.telemetry_doorbell_fd),
                                     &reader) != OURO_TELEMETRY_OK ||
          pthread_create(&drainer, NULL, telemetry_drain, &reader) != 0)
         abort();
   }

   /* One event's dispatch = publish (producer enqueue) + host pump (route) +
    * poll (consumer dequeue). Batched to the ring's data capacity so timer
    * granularity does not dominate; each batch's time is divided by its events.
    * We record per-batch per-event samples and report the median, which is
    * robust to scheduler noise. */
   uint32_t batch = cfg.queue_capacity - 4; /* leave headroom below capacity */
   uint64_t batches = total / batch;
   if (batches < 100)
      batches = 100;

   uint64_t *samples = malloc(batches * sizeof(uint64_t));
   if (!samples)
      abort();

   const char *payload = "benchmark-payload-16b";
   uint32_t plen = 16;

   /* Warm up. */
   for (int w = 0; w < 1000; w++)
   {
      for (uint32_t i = 0; i < batch; i++)
         ouro_client_publish(&pub, KIND, payload, plen);
      ouro_host_pump(&h);
      ouro_event_t ev;
      while (ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK)
         ;
   }

   for (uint64_t b = 0; b < batches; b++)
   {
      uint64_t t0 = now_ns();
      for (uint32_t i = 0; i < batch; i++)
         ouro_client_publish(&pub, KIND, payload, plen);
      ouro_host_pump(&h);
      ouro_event_t ev;
      uint32_t got = 0;
      while (ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK)
         got++;
      uint64_t t1 = now_ns();
      if (got != batch)
      {
         fprintf(stderr, "bench: expected %u events, got %u\n", batch, got);
         return 1;
      }
      samples[b] = (t1 - t0) / batch;
   }

   qsort(samples, batches, sizeof(uint64_t), cmp_u64);
   uint64_t median = samples[batches / 2];
   uint64_t p99 = samples[(batches * 99) / 100];

   printf("dispatch_overhead_ns=%llu\n", (unsigned long long)median);
   printf("dispatch_overhead_p99_ns=%llu\n", (unsigned long long)p99);
   fprintf(stderr, "bench: %llu batches of %u events (median %llu ns/event, p99 %llu ns)\n",
           (unsigned long long)batches, batch, (unsigned long long)median, (unsigned long long)p99);

   if (telemetry)
   {
      atomic_store(&telemetry_stop, 1);
      pthread_join(drainer, NULL);
      printf("telemetry_wakes=%llu\n", (unsigned long long)h.telemetry_wakes);
      fprintf(stderr, "bench: telemetry on, %llu records drained, %llu lost\n",
              (unsigned long long)atomic_load(&telemetry_drained),
              (unsigned long long)ouro_telemetry_dropped(&reader));
      ouro_telemetry_reader_close(&reader);
   }
   free(samples);
   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   return 0;
}
