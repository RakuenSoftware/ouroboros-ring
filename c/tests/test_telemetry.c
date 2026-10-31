/* test_telemetry.c: D16, tap telemetry leaves the host as each event is stamped.
 *
 *   - A record round-trips, carries the stamped frame header, and holds no
 *     payload byte; a record with an unknown flag is refused.
 *   - Only the slot the embedding granted is handed the consumer end, only one
 *     at a time, and only to a client that asked.
 *   - Every stamped event is a record, in seq order, written before the tap
 *     runs.
 *   - A full ring never blocks the host: the loss is counted, and the next
 *     record that fits follows a gap record carrying the count.
 *   - A consumer asleep on the doorbell wakes for a pump; one that is awake is
 *     not signalled.
 *   - A later consumer resumes where the last one stopped.
 *   - Pacing: the region carries the host's drain interval and idle threshold;
 *     a consumer kept busy costs the host one wake, for its first record, and
 *     an idle one costs a wake per record.
 *   - Nothing leaks a descriptor.
 */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <ouroboros/local_attach.h>
#include <ouroboros/telemetry.h>

static void must(int cond, const char *what)
{
   if (!cond)
   {
      fprintf(stderr, "FAIL: %s\n", what);
      abort();
   }
}

static int open_fds(void)
{
   DIR *d = opendir("/proc/self/fd");
   if (!d)
      return -1;
   int n = 0;
   while (readdir(d))
      n++;
   closedir(d);
   return n;
}

#define KIND 0x2a01u

static void host_config(ouro_host_config_t *cfg)
{
   memset(cfg, 0, sizeof *cfg);
   cfg->max_slots = 4;
   cfg->slot_size = 256;
   cfg->inline_budget = 192;
   cfg->queue_capacity = 64;
   cfg->arena_size = 64 * 1024;
}

/* The embedding's attach hook: principal_ref 7 is the telemetry exporter. */
static ouro_attach_status_t grant_exporter(void *ctx, ouro_host_t *host, uint32_t slot,
                                           const ouro_attach_request_t *req)
{
   (void)ctx;
   if (req->principal_ref == 7)
      must(ouro_host_grant_telemetry(host, slot) == OURO_HOST_OK, "grant");
   return OURO_ATTACH_OK;
}

struct serve
{
   ouro_host_t *h;
   int fd;
};

static void *serve_one(void *p)
{
   struct serve *s = p;
   (void)ouro_host_serve_attach(s->h, s->fd);
   return NULL;
}

/* Attach over a real socketpair, asking for telemetry as `principal_ref`. */
static void attach_consumer(ouro_host_t *h, uint32_t principal_ref, ouro_client_t *c,
                            ouro_telemetry_reader_t *r, int *granted)
{
   int sv[2];
   must(socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, sv) == 0, "socketpair");
   struct serve s = {.h = h, .fd = sv[1]};
   pthread_t t;
   must(pthread_create(&t, NULL, serve_one, &s) == 0, "serve thread");
   must(ouro_client_attach_telemetry(sv[0], c, 1, principal_ref, r, granted) == OURO_CLIENT_OK,
        "consumer attached");
   pthread_join(t, NULL);
   close(sv[0]);
   close(sv[1]);
}

static uint64_t tapped_seq[256];
static int tapped;
static ouro_telemetry_reader_t *tap_reader;
static uint64_t tap_saw_records[256];

static void tap(void *ctx, const ouro_frame_t *f, const uint8_t *payload, uint32_t len)
{
   (void)ctx;
   (void)payload;
   (void)len;
   if (tapped < 256)
   {
      tapped_seq[tapped] = f->seq;
      tap_saw_records[tapped] = tap_reader ? ouro_telemetry_available(tap_reader) : 0;
      tapped++;
   }
}

static void test_record_codec(void)
{
   ouro_frame_t f;
   memset(&f, 0, sizeof f);
   f.hdr_flags = OURO_F_NOTIFICATION | OURO_F_INLINE;
   f.wire_version = OURO_WIRE_VERSION;
   f.event_kind = KIND;
   f.principal_ref = 9;
   f.seq = 41;
   f.payload_len = 5;
   f.payload_ref = OURO_WIRE_HDR_LEN;
   f.src_handle = 2;
   uint8_t rec[OURO_TELEMETRY_RECORD_LEN];
   ouro_telemetry_encode(rec, OURO_TELEMETRY_RECORD_EVENT, 1234, 0, &f);
   ouro_telemetry_record_t out;
   must(ouro_telemetry_decode(rec, &out) == OURO_TELEMETRY_OK, "decodes");
   must(out.type == OURO_TELEMETRY_RECORD_EVENT && out.realtime_ns == 1234, "type and time");
   must(out.frame.seq == 41 && out.frame.event_kind == KIND && out.frame.principal_ref == 9 &&
            out.frame.payload_len == 5 && out.frame.src_handle == 2,
        "the stamped header");
   for (int i = 96; i < OURO_TELEMETRY_RECORD_LEN; i++)
      must(rec[i] == 0, "reserved bytes are zero");
   rec[OURO_TELEMETRY_OFF_FLAGS] = 1;
   must(ouro_telemetry_decode(rec, &out) == OURO_TELEMETRY_ERR_LAYOUT, "unknown flag refused");

   ouro_telemetry_encode(rec, OURO_TELEMETRY_RECORD_GAP, 5, 3, NULL);
   must(ouro_telemetry_decode(rec, &out) == OURO_TELEMETRY_OK && out.type == OURO_TELEMETRY_RECORD_GAP &&
            out.lost == 3,
        "gap");
   printf("  codec: a record is the stamped header and nothing of the payload\n");
}

static void test_grant(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_host_set_attach_hook(&h, grant_exporter, NULL);

   /* Before enable, nobody is granted it. */
   ouro_client_t c;
   ouro_telemetry_reader_t r;
   int granted = 1;
   attach_consumer(&h, 7, &c, &r, &granted);
   must(!granted && !(c.reply.capabilities & OURO_ATTACH_CAP_TELEMETRY), "not enabled");
   ouro_client_detach(&c);
   (void)ouro_host_pump(&h);

   must(ouro_host_telemetry_enable(&h, 64) == OURO_HOST_OK, "enable");
   /* A principal the embedding did not grant. */
   attach_consumer(&h, 8, &c, &r, &granted);
   must(!granted, "ungranted principal refused");
   ouro_client_detach(&c);
   (void)ouro_host_pump(&h);

   ouro_client_t a, b;
   ouro_telemetry_reader_t ra, rb;
   attach_consumer(&h, 7, &a, &ra, &granted);
   must(granted && (a.reply.capabilities & OURO_ATTACH_CAP_TELEMETRY), "granted");
   attach_consumer(&h, 7, &b, &rb, &granted);
   must(!granted, "only one consumer at a time");
   ouro_client_detach(&b);
   ouro_telemetry_reader_close(&ra);
   ouro_client_detach(&a);
   (void)ouro_host_pump(&h);
   attach_consumer(&h, 7, &b, &rb, &granted);
   must(granted, "the next consumer is granted once the first is gone");
   ouro_telemetry_reader_close(&rb);
   ouro_client_detach(&b);
   ouro_host_destroy(&h);
   printf("  grant: only the granted slot, one at a time, only when asked\n");
}

static void test_stream(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_host_set_attach_hook(&h, grant_exporter, NULL);
   must(ouro_host_telemetry_enable(&h, 64) == OURO_HOST_OK, "enable");
   ouro_host_set_tap(&h, tap, NULL);

   ouro_client_t cons;
   ouro_telemetry_reader_t r;
   int granted = 0;
   attach_consumer(&h, 7, &cons, &r, &granted);
   must(granted, "granted");
   tap_reader = &r;
   tapped = 0;

   ouro_client_t prod, obs;
   must(ouro_host_attach_local(&h, &prod, 1) == OURO_CLIENT_OK, "producer");
   must(ouro_host_attach_local(&h, &obs, 1) == OURO_CLIENT_OK, "observer");
   must(ouro_host_subscribe(&h, obs.reply.handle_id, KIND) == OURO_HOST_OK, "subscribe");

   const char secret[] = "payload-never-exported";
   for (int i = 0; i < 10; i++)
      must(ouro_client_publish(&prod, KIND, secret, sizeof secret) == OURO_CLIENT_OK, "publish");
   (void)ouro_host_pump(&h);

   must(tapped == 10, "tapped ten");
   must(ouro_telemetry_available(&r) == 10, "ten records");
   for (int i = 0; i < 10; i++)
   {
      must(tap_saw_records[i] == (uint64_t)i + 1, "each record is written before the tap runs");
      const uint8_t *raw = ouro_telemetry_peek(&r, (uint64_t)i);
      ouro_telemetry_record_t rec;
      must(ouro_telemetry_decode(raw, &rec) == OURO_TELEMETRY_OK, "decode");
      must(rec.type == OURO_TELEMETRY_RECORD_EVENT && rec.frame.seq == tapped_seq[i], "seq order");
      must(rec.frame.event_kind == KIND && rec.frame.payload_len == sizeof secret &&
               rec.frame.src_handle == prod.reply.handle_id,
           "kind, length, source");
      must(memmem(raw, OURO_TELEMETRY_RECORD_LEN, "payload", 7) == NULL, "no payload byte");
      must(rec.realtime_ns > 0, "timestamped");
      uint8_t portable[OURO_TELEMETRY_RECORD_LEN];
      ouro_telemetry_encode(portable, rec.type, rec.realtime_ns, rec.lost, &rec.frame);
      must(memcmp(portable, raw, sizeof portable) == 0,
           "the host's fast encoder writes the portable encoding's bytes");
   }
   ouro_telemetry_release(&r, 10);
   must(ouro_telemetry_available(&r) == 0, "released");
   tap_reader = NULL;

   ouro_client_detach(&prod);
   ouro_client_detach(&obs);
   ouro_telemetry_reader_close(&r);
   ouro_client_detach(&cons);
   ouro_host_destroy(&h);
   printf("  stream: every stamped event, in seq order, before the tap\n");
}

static void test_full_ring_gap(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_host_set_attach_hook(&h, grant_exporter, NULL);
   must(ouro_host_telemetry_enable(&h, 4) == OURO_HOST_OK, "enable, four records");
   ouro_client_t cons;
   ouro_telemetry_reader_t r;
   int granted = 0;
   attach_consumer(&h, 7, &cons, &r, &granted);
   must(granted, "granted");

   ouro_client_t prod, obs;
   must(ouro_host_attach_local(&h, &prod, 1) == OURO_CLIENT_OK, "producer");
   must(ouro_host_attach_local(&h, &obs, 1) == OURO_CLIENT_OK, "observer");
   must(ouro_host_subscribe(&h, obs.reply.handle_id, KIND) == OURO_HOST_OK, "subscribe");
   for (int i = 0; i < 10; i++)
      must(ouro_client_publish(&prod, KIND, "x", 1) == OURO_CLIENT_OK, "publish");
   must(ouro_host_pump(&h) == 10, "every event routed: telemetry never blocks the host");
   must(ouro_telemetry_available(&r) == 4, "the ring is full");
   must(ouro_telemetry_dropped(&r) == 6, "six counted lost");

   ouro_telemetry_record_t rec;
   uint64_t last = 0;
   for (int i = 0; i < 4; i++)
   {
      must(ouro_telemetry_decode(ouro_telemetry_peek(&r, (uint64_t)i), &rec) == OURO_TELEMETRY_OK,
           "decode");
      must(rec.type == OURO_TELEMETRY_RECORD_EVENT, "events");
      last = rec.frame.seq;
   }
   ouro_telemetry_release(&r, 4);
   /* Drain what the observer received so the next publish is not blocked. */
   ouro_event_t ev;
   while (ouro_client_poll(&obs, &ev) == OURO_CLIENT_OK)
      ;
   must(ouro_client_publish(&prod, KIND, "y", 1) == OURO_CLIENT_OK, "publish");
   (void)ouro_host_pump(&h);
   must(ouro_telemetry_available(&r) == 2, "a gap and the event");
   must(ouro_telemetry_decode(ouro_telemetry_peek(&r, 0), &rec) == OURO_TELEMETRY_OK &&
            rec.type == OURO_TELEMETRY_RECORD_GAP && rec.lost == 6,
        "the gap carries the count");
   must(ouro_telemetry_decode(ouro_telemetry_peek(&r, 1), &rec) == OURO_TELEMETRY_OK &&
            rec.type == OURO_TELEMETRY_RECORD_EVENT && rec.frame.seq == last + 7,
        "then the event, its seq showing what was lost");
   ouro_telemetry_release(&r, 2);

   ouro_client_detach(&prod);
   ouro_client_detach(&obs);
   ouro_telemetry_reader_close(&r);
   ouro_client_detach(&cons);
   ouro_host_destroy(&h);
   printf("  full: the host never blocks; a gap record says how many were lost\n");
}

struct waiter
{
   ouro_telemetry_reader_t *r;
   int result;
};

static void *wait_one(void *p)
{
   struct waiter *w = p;
   w->result = ouro_telemetry_wait(w->r, -1, 5000);
   return NULL;
}

static void test_doorbell_and_resume(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_host_set_attach_hook(&h, grant_exporter, NULL);
   must(ouro_host_telemetry_enable(&h, 64) == OURO_HOST_OK, "enable");
   ouro_client_t cons;
   ouro_telemetry_reader_t r;
   int granted = 0;
   attach_consumer(&h, 7, &cons, &r, &granted);
   must(granted, "granted");
   ouro_client_t prod, obs;
   must(ouro_host_attach_local(&h, &prod, 1) == OURO_CLIENT_OK, "producer");
   must(ouro_host_attach_local(&h, &obs, 1) == OURO_CLIENT_OK, "observer");
   must(ouro_host_subscribe(&h, obs.reply.handle_id, KIND) == OURO_HOST_OK, "subscribe");

   /* Awake consumer: the host writes records and rings nothing. */
   must(ouro_client_publish(&prod, KIND, "a", 1) == OURO_CLIENT_OK, "publish");
   (void)ouro_host_pump(&h);
   uint64_t count;
   must(read(r.doorbell_fd, &count, sizeof count) < 0 && errno == EAGAIN,
        "an awake consumer is not signalled");
   ouro_telemetry_release(&r, 1);

   /* Asleep: one pump of several events wakes it once. */
   struct waiter w = {.r = &r, .result = -2};
   pthread_t t;
   must(pthread_create(&t, NULL, wait_one, &w) == 0, "waiter");
   while (!atomic_load(&r.hdr->consumer_waiting))
      sched_yield();
   for (int i = 0; i < 3; i++)
      must(ouro_client_publish(&prod, KIND, "b", 1) == OURO_CLIENT_OK, "publish");
   (void)ouro_host_pump(&h);
   pthread_join(t, NULL);
   must(w.result == 1 && ouro_telemetry_available(&r) == 3, "woken for the pump");
   ouro_telemetry_release(&r, 1); /* exported one of three, then went away */

   ouro_telemetry_reader_close(&r);
   ouro_client_detach(&cons);
   (void)ouro_host_pump(&h);
   attach_consumer(&h, 7, &cons, &r, &granted);
   must(granted && ouro_telemetry_available(&r) == 2, "the next consumer resumes at the tail");
   ouro_telemetry_release(&r, 2);

   ouro_client_detach(&prod);
   ouro_client_detach(&obs);
   ouro_telemetry_reader_close(&r);
   ouro_client_detach(&cons);
   ouro_host_destroy(&h);
   printf("  doorbell: asleep wakes once per pump, awake is not signalled; resume at tail\n");
}

struct drainer
{
   ouro_telemetry_reader_t *r;
   int want;
   atomic_int got;
};

static void *drain_records(void *p)
{
   struct drainer *d = p;
   while (atomic_load(&d->got) < d->want)
   {
      if (ouro_telemetry_wait(d->r, -1, 5000) != 1)
         break;
      uint64_t n = ouro_telemetry_available(d->r);
      ouro_telemetry_release(d->r, n);
      atomic_fetch_add(&d->got, (int)n);
   }
   return NULL;
}

static void publish_one(ouro_host_t *h, ouro_client_t *prod, ouro_client_t *obs)
{
   must(ouro_client_publish(prod, KIND, "p", 1) == OURO_CLIENT_OK, "publish");
   (void)ouro_host_pump(h);
   ouro_event_t ev;
   while (ouro_client_poll(obs, &ev) == OURO_CLIENT_OK)
      ;
}

static void test_pacing(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_host_set_attach_hook(&h, grant_exporter, NULL);
   setenv("OURO_TELEMETRY_DRAIN_INTERVAL_US", "2000", 1);
   setenv("OURO_TELEMETRY_IDLE_AFTER_US", "not-a-number", 1);
   must(ouro_host_telemetry_enable(&h, 64) == OURO_HOST_OK, "enable");
   unsetenv("OURO_TELEMETRY_DRAIN_INTERVAL_US");
   unsetenv("OURO_TELEMETRY_IDLE_AFTER_US");
   must(atomic_load(&h.telemetry->drain_interval_ns) == 2000000u,
        "the environment sets the drain interval");
   must(atomic_load(&h.telemetry->idle_after_ns) == OURO_TELEMETRY_IDLE_AFTER_NS_DEFAULT,
        "an unreadable value leaves the default");
   must(ouro_host_telemetry_pacing(&h, 2000000u, 500000000u) == OURO_HOST_OK, "pacing");
   must(atomic_load(&h.telemetry->idle_after_ns) == 500000000u, "the embedding sets it too");

   ouro_client_t cons;
   ouro_telemetry_reader_t r;
   int granted = 0;
   attach_consumer(&h, 7, &cons, &r, &granted);
   must(granted, "granted");
   ouro_client_t prod, obs;
   must(ouro_host_attach_local(&h, &prod, 1) == OURO_CLIENT_OK, "producer");
   must(ouro_host_attach_local(&h, &obs, 1) == OURO_CLIENT_OK, "observer");
   must(ouro_host_subscribe(&h, obs.reply.handle_id, KIND) == OURO_HOST_OK, "subscribe");

   /* Busy: records every 3 ms against a 2 ms drain interval and a 500 ms idle
    * threshold. Only the first record finds the consumer asleep. */
   struct drainer d = {.r = &r, .want = 20};
   pthread_t t;
   uint64_t before = h.telemetry_wakes;
   must(pthread_create(&t, NULL, drain_records, &d) == 0, "drainer");
   while (!atomic_load(&r.hdr->consumer_waiting))
      sched_yield();
   for (int i = 0; i < d.want; i++)
   {
      publish_one(&h, &prod, &obs);
      usleep(3000);
   }
   pthread_join(t, NULL);
   must(atomic_load(&d.got) == d.want, "every record drained");
   must(h.telemetry_wakes - before == 1, "a busy consumer costs the host one wake, its first");

   /* Idle: a 2 ms threshold, and each record lands after the consumer is
    * asleep again. Every one costs a wake. */
   must(ouro_host_telemetry_pacing(&h, 1000000u, 2000000u) == OURO_HOST_OK, "pacing");
   d.want = 5;
   atomic_store(&d.got, 0);
   before = h.telemetry_wakes;
   must(pthread_create(&t, NULL, drain_records, &d) == 0, "drainer");
   for (int i = 0; i < d.want; i++)
   {
      while (!atomic_load(&r.hdr->consumer_waiting))
         sched_yield();
      publish_one(&h, &prod, &obs);
      while (atomic_load(&d.got) < i + 1)
         sched_yield();
   }
   pthread_join(t, NULL);
   must(atomic_load(&d.got) == d.want, "every record drained");
   must(h.telemetry_wakes - before == (uint64_t)d.want, "an idle consumer is woken for each");

   ouro_client_detach(&prod);
   ouro_client_detach(&obs);
   ouro_telemetry_reader_close(&r);
   ouro_client_detach(&cons);
   ouro_host_destroy(&h);
   printf("  pacing: a busy consumer costs one wake; an idle one a wake per record\n");
}

int main(void)
{
   int fds = open_fds();
   test_record_codec();
   test_grant();
   test_stream();
   test_full_ring_gap();
   test_doorbell_and_resume();
   test_pacing();
   must(open_fds() == fds, "no descriptor leaked");
   puts("telemetry: ok");
   return 0;
}
