/* test_doorbell.c: D13, each end of a queue pair sleeps on a doorbell.
 *
 *   - A client that asks is granted a doorbell pair, and the control region
 *     says the host keeps one.
 *   - A 16-byte v0 request is still admitted, and is granted three descriptors
 *     and no capability.
 *   - Against a host with no doorbell a client runs as before, and its wait says
 *     so instead of sleeping.
 *   - A waiting client wakes for an event, for an interrupt, and not otherwise.
 *   - A waiting host wakes for a publish, for a wake call, and for a consumer
 *     making room a blocked producer needs, and not for the blocked producer
 *     itself.
 *   - Nothing leaks a descriptor.
 */
#include <dirent.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <ouroboros/local_attach.h>

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

static long now_ms(void)
{
   struct timespec t;
   clock_gettime(CLOCK_MONOTONIC, &t);
   return t.tv_sec * 1000L + t.tv_nsec / 1000000L;
}

static void sleep_ms(long ms)
{
   struct timespec t = {.tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L};
   nanosleep(&t, NULL);
}

static void host_config(ouro_host_config_t *cfg)
{
   memset(cfg, 0, sizeof *cfg);
   cfg->max_slots = 4;
   cfg->slot_size = 256;
   cfg->inline_budget = 192;
   cfg->queue_capacity = 16;
   cfg->arena_size = 64 * 1024;
}

static void test_grant(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   must(ouro_host_doorbell_fd(&h) >= 0, "the host has a doorbell");
   ouro_client_t c;
   must(ouro_host_attach_local(&h, &c, 0) == OURO_CLIENT_OK, "attached");
   must(c.have_doorbell && c.reply.capabilities == OURO_ATTACH_CAP_DOORBELL, "granted a doorbell");
   must(ouro_client_doorbell_fd(&c) >= 0, "with its own descriptor");
   must((atomic_load(&c.ctl->flags) & OURO_CONTROL_F_DOORBELL) != 0, "the control region says so");
   ouro_client_detach(&c);
   ouro_host_destroy(&h);
   printf("  grant: a client that asks is given a doorbell pair\n");
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

static void test_v0_request(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   int sv[2];
   must(socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, sv) == 0, "socketpair");
   struct serve s = {.h = &h, .fd = sv[1]};
   pthread_t t;
   must(pthread_create(&t, NULL, serve_one, &s) == 0, "serve thread");

   ouro_attach_request_t req;
   memset(&req, 0, sizeof req);
   req.magic = OURO_ATTACH_REQ_MAGIC;
   req.wire_version_min = req.wire_version_max = OURO_WIRE_VERSION;
   must(ouro_fd_send(sv[0], &req, OURO_ATTACH_REQ_V0_BYTES, NULL, 0) == 0, "send 16 bytes");
   ouro_attach_reply_t reply;
   int fds[OURO_ATTACH_FDS_MAX];
   int nfd = 0;
   long n = ouro_fd_recv(sv[0], &reply, sizeof reply, fds, OURO_ATTACH_FDS_MAX, &nfd);
   pthread_join(t, NULL);
   must(n == (long)sizeof reply && reply.status == OURO_ATTACH_OK, "a v0 request is admitted");
   must(reply.capabilities == 0 && nfd == OURO_ATTACH_FDS, "with three descriptors and no doorbell");
   for (int i = 0; i < nfd; i++)
      close(fds[i]);
   close(sv[0]);
   close(sv[1]);
   ouro_host_destroy(&h);
   printf("  v0: a 16-byte request is still admitted, without a doorbell\n");
}

static void test_host_without_doorbell(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   /* An older host: no doorbell, no flag. */
   close(h.doorbell_fd);
   h.doorbell_fd = -1;
   atomic_fetch_and(&h.control->flags, ~OURO_CONTROL_F_DOORBELL);

   ouro_client_t pub, sub;
   must(ouro_host_attach_local(&h, &pub, 0) == OURO_CLIENT_OK, "publisher attached");
   must(ouro_host_attach_local(&h, &sub, 0) == OURO_CLIENT_OK, "subscriber attached");
   must(!sub.have_doorbell && sub.reply.capabilities == 0, "no doorbell is granted");
   must(ouro_client_wait(&sub, 1000) == -1, "the client's wait says so at once");
   must(ouro_host_wait(&h, 1000) == -1, "and so does the host's");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, 900) == OURO_HOST_OK, "subscribe");
   must(ouro_client_publish(&pub, 900, "ping", 4) == OURO_CLIENT_OK, "publish");
   must(ouro_host_pump(&h) == 1, "routed");
   ouro_event_t ev;
   must(ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK, "traffic still flows");
   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   printf("  older host: no doorbell, traffic unchanged, wait reports -1\n");
}

struct delayed
{
   ouro_host_t *h;
   ouro_client_t *c;
   long delay_ms;
   int action; /* 0 publish+pump, 1 interrupt, 2 host wake, 3 host wait, 4 publish only */
   int result;
   long woke_after_ms;
};

static void *delayed_run(void *p)
{
   struct delayed *d = p;
   if (d->action == 3)
   {
      long start = now_ms();
      d->result = ouro_host_wait(d->h, 5000);
      d->woke_after_ms = now_ms() - start;
      return NULL;
   }
   sleep_ms(d->delay_ms);
   if (d->action == 0)
   {
      d->result = ouro_client_publish(d->c, 900, "ping", 4);
      (void)ouro_host_pump(d->h);
   }
   else if (d->action == 1)
      ouro_client_interrupt(d->c);
   else if (d->action == 2)
      ouro_host_wake(d->h);
   else
      d->result = ouro_client_publish(d->c, 900, "ping", 4);
   return NULL;
}

static void test_client_wait(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t pub, sub;
   must(ouro_host_attach_local(&h, &pub, 0) == OURO_CLIENT_OK, "publisher attached");
   must(ouro_host_attach_local(&h, &sub, 0) == OURO_CLIENT_OK, "subscriber attached");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, 900) == OURO_HOST_OK, "subscribe");

   long start = now_ms();
   must(ouro_client_wait(&sub, 40) == 0, "an idle wait times out");
   must(now_ms() - start >= 30, "after sleeping, not spinning");

   struct delayed d = {.h = &h, .c = &pub, .delay_ms = 30, .action = 0};
   pthread_t t;
   must(pthread_create(&t, NULL, delayed_run, &d) == 0, "publisher thread");
   start = now_ms();
   must(ouro_client_wait(&sub, 5000) == 1, "an event wakes the waiter");
   long took = now_ms() - start;
   pthread_join(t, NULL);
   must(d.result == OURO_CLIENT_OK, "the delayed publish was accepted");
   must(took < 2000, "promptly, long before the timeout");
   ouro_event_t ev;
   must(ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK && ev.frame.event_kind == 900,
        "and the event is there");
   must(ouro_client_wait(&sub, 0) == 0, "the event just polled is not counted again");

   d = (struct delayed){.c = &sub, .delay_ms = 30, .action = 1};
   must(pthread_create(&t, NULL, delayed_run, &d) == 0, "interrupt thread");
   start = now_ms();
   must(ouro_client_wait(&sub, 5000) == 1, "an interrupt wakes the waiter");
   pthread_join(t, NULL);
   must(now_ms() - start < 2000, "promptly");

   must(ouro_client_publish(&pub, 900, "ping", 4) == OURO_CLIENT_OK, "publish");
   (void)ouro_host_pump(&h);
   start = now_ms();
   must(ouro_client_wait(&sub, 5000) == 1, "an event already waiting returns at once");
   must(now_ms() - start < 100, "without sleeping");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   printf("  client wait: sleeps when idle, wakes for an event or an interrupt\n");
}

static void test_host_wait(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t pub;
   must(ouro_host_attach_local(&h, &pub, 0) == OURO_CLIENT_OK, "publisher attached");

   long start = now_ms();
   must(ouro_host_wait(&h, 40) == 0, "an idle host wait times out");
   must(now_ms() - start >= 30, "after sleeping, not spinning");

   struct delayed waiter = {.h = &h, .action = 3};
   pthread_t w;
   must(pthread_create(&w, NULL, delayed_run, &waiter) == 0, "host waiter");
   sleep_ms(30);
   must(ouro_client_publish(&pub, 900, "ping", 4) == OURO_CLIENT_OK, "publish");
   pthread_join(w, NULL);
   must(waiter.result == 1 && waiter.woke_after_ms < 2000, "a publish wakes the host");
   (void)ouro_host_pump(&h);

   must(ouro_client_publish(&pub, 900, "ping", 4) == OURO_CLIENT_OK, "publish");
   start = now_ms();
   must(ouro_host_wait(&h, 5000) == 1 && now_ms() - start < 100,
        "work already queued returns at once");
   (void)ouro_host_pump(&h);

   struct delayed waker = {.h = &h, .delay_ms = 30, .action = 2};
   pthread_t t;
   must(pthread_create(&t, NULL, delayed_run, &waker) == 0, "waker");
   start = now_ms();
   must(ouro_host_wait(&h, 5000) == 1, "ouro_host_wake wakes the host");
   pthread_join(t, NULL);
   must(now_ms() - start < 2000, "promptly");

   ouro_client_detach(&pub);
   ouro_host_destroy(&h);
   printf("  host wait: sleeps when idle, wakes for a publish or a wake call\n");
}

static void test_blocked_producer(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t pub, sub;
   must(ouro_host_attach_local(&h, &pub, 0) == OURO_CLIENT_OK, "publisher attached");
   must(ouro_host_attach_local(&h, &sub, 0) == OURO_CLIENT_OK, "subscriber attached");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, 900) == OURO_HOST_OK, "subscribe");

   /* Fill the subscriber's inbound ring until the host holds the publisher. */
   int sent = 0;
   while (sent < 15 && ouro_client_publish(&pub, 900, "x", 1) == OURO_CLIENT_OK)
      sent++;
   (void)ouro_host_pump(&h);
   must(h.slots[pub.reply.handle_id].blocked, "the publisher is held by a full subscriber");

   long start = now_ms();
   must(ouro_host_wait(&h, 40) == 0, "a held producer alone does not wake the host");
   must(now_ms() - start >= 30, "so the host sleeps instead of retrying");

   struct delayed waiter = {.h = &h, .action = 3};
   pthread_t w;
   must(pthread_create(&w, NULL, delayed_run, &waiter) == 0, "host waiter");
   sleep_ms(30);
   ouro_event_t ev;
   must(ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK, "the subscriber reads one");
   must(ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK, "and frees its slot");
   pthread_join(w, NULL);
   must(waiter.result == 1 && waiter.woke_after_ms < 2000, "the freed slot wakes the host");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   printf("  backpressure: the host sleeps while held, wakes when room is made\n");
}

static void test_no_leak(void)
{
   int before = open_fds();
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   for (int i = 0; i < 3; i++)
   {
      ouro_client_t c;
      must(ouro_host_attach_local(&h, &c, 0) == OURO_CLIENT_OK, "attached");
      must(ouro_host_release_slot(&h, c.reply.handle_id) == OURO_HOST_OK, "slot released");
      ouro_client_detach(&c);
   }
   ouro_host_destroy(&h);
   must(open_fds() == before, "attach, release, detach and destroy leave no descriptor");
   printf("  descriptors: none leak across attach, release and destroy\n");
}

int main(void)
{
   printf("test_doorbell:\n");
   test_grant();
   test_v0_request();
   test_host_without_doorbell();
   test_client_wait();
   test_host_wait();
   test_blocked_producer();
   test_no_leak();
   printf("test_doorbell: OK\n");
   return 0;
}
