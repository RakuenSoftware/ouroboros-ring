/* test_client.c: slice 8 of the ring feature tree.
 *
 * The C reference client, driven against a real host in one process. Once
 * attached, host and client share memory, so the test pumps the host's routing
 * by hand between client calls. It exercises every message pattern and the
 * backpressure path through the library rather than the raw rings.
 *
 * The attach handshake is the one place send and recv must cross, so it runs on
 * a helper thread: ouro_client_attach (send-then-recv) on the main thread and
 * ouro_host_serve_attach (recv-then-send) on the helper, both over a
 * SOCK_SEQPACKET socketpair. After that everything is single-threaded.
 */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <ouroboros/client.h>
#include <ouroboros/host.h>
#include <ouroboros/ring.h>

static void must(int cond, const char *what)
{
   if (!cond)
   {
      fprintf(stderr, "FAIL: %s\n", what);
      abort();
   }
}

static void must_rc(ouro_client_result_t got, ouro_client_result_t want, const char *what)
{
   if (got != want)
   {
      fprintf(stderr, "FAIL: %s: expected %s got %s\n", what, ouro_client_result_name(want),
              ouro_client_result_name(got));
      abort();
   }
}

struct serve_arg
{
   ouro_host_t *h;
   int fd;
   ouro_host_result_t result;
};

static void *serve_thread(void *p)
{
   struct serve_arg *a = p;
   a->result = ouro_host_serve_attach(a->h, a->fd);
   return NULL;
}

/* Attach one client, returning the client result. The host end of the socket is
 * served on a helper thread so the single-process handshake does not deadlock. */
static ouro_client_result_t attach_client(ouro_host_t *h, ouro_client_t *c)
{
   int sv[2];
   must(socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sv) == 0, "socketpair");
   struct serve_arg a = {.h = h, .fd = sv[1], .result = OURO_HOST_ERR_ARG};
   pthread_t t;
   must(pthread_create(&t, NULL, serve_thread, &a) == 0, "spawn serve");
   ouro_client_result_t rc = ouro_client_attach(sv[0], c);
   must(pthread_join(t, NULL) == 0, "join serve");
   close(sv[0]);
   close(sv[1]);
   return rc;
}

static ouro_host_config_t cfg(void)
{
   ouro_host_config_t c;
   memset(&c, 0, sizeof c);
   c.max_slots = 4;
   c.slot_size = 256;
   c.inline_budget = 192;
   c.queue_capacity = 8;
   c.arena_size = 128 * 1024;
   return c;
}

#define KIND_A 500

static void test_publish_and_poll(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");

   ouro_client_t pub, sub;
   must_rc(attach_client(&h, &pub), OURO_CLIENT_OK, "publisher attaches");
   must_rc(attach_client(&h, &sub), OURO_CLIENT_OK, "subscriber attaches");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, KIND_A) == OURO_HOST_OK, "subscribe");

   const char *msg = "hello-ring";
   must_rc(ouro_client_publish(&pub, KIND_A, msg, (uint32_t)strlen(msg)), OURO_CLIENT_OK, "publish");

   /* Nothing arrives until the host routes. */
   ouro_event_t ev;
   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_EMPTY, "nothing before pump");
   must(ouro_host_pump(&h) == 1, "host routes the notification");

   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_OK, "subscriber polls the event");
   must(ev.frame.event_kind == KIND_A, "right kind");
   must(ev.payload_len == strlen(msg) && memcmp(ev.payload, msg, ev.payload_len) == 0,
        "zero-copy payload matches");
   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_EMPTY, "only one event");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   printf("  publish/poll: a notification reaches the subscriber zero-copy\n");
}

/* The arena producer/consumer path through the client library: the producer
 * allocates a lease on the (co-located) host arena, fills it, and emits the
 * reference frame with ouro_client_publish_arena — no bytes cross the ring. The
 * host routes it by reference. The subscriber is an ordinary client, as it would
 * be in another process: poll resolves the payload through the read-only lease
 * view (D12) and points at it in place, and the next poll returns the ref with a
 * lease-release event the host consumes. */
static void test_publish_arena(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");

   ouro_client_t pub, sub;
   must_rc(attach_client(&h, &pub), OURO_CLIENT_OK, "publisher attaches");
   must_rc(attach_client(&h, &sub), OURO_CLIENT_OK, "subscriber attaches");
   must(ouro_client_arena_readable(&sub), "a D12 host advertises the lease view");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, KIND_A) == OURO_HOST_OK, "subscribe");

   /* A zero-length arena payload is refused: there is nothing to lease. */
   must_rc(ouro_client_publish_arena(&pub, KIND_A, 0, 0, 0), OURO_CLIENT_ERR_PAYLOAD,
           "empty refused");

   const uint32_t len = cf.inline_budget + 500; /* too big for inline: genuinely arena */
   uint32_t lease = 0;
   must(ouro_arena_alloc(&h.arena, pub.reply.handle_id, len, &lease) == OURO_ARENA_OK, "alloc lease");
   uint8_t *p = NULL;
   must(ouro_arena_fill_ptr(&h.arena, lease, &p) == OURO_ARENA_OK, "fill ptr");
   for (uint32_t i = 0; i < len; i++)
      p[i] = (uint8_t)(i * 7u + 3u);
   ouro_arena_ref_t ref;
   must(ouro_arena_ref(&h.arena, lease, &ref) == OURO_ARENA_OK, "ref");

   must_rc(ouro_client_publish_arena(&pub, KIND_A, lease, ref.generation, len), OURO_CLIENT_OK,
           "publish arena");

   ouro_event_t ev;
   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_EMPTY, "nothing before pump");
   must(ouro_host_pump(&h) == 1, "host routes the arena notification");

   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_OK, "subscriber polls the reference");
   must(ev.frame.event_kind == KIND_A && (ev.frame.hdr_flags & OURO_F_ARENA), "arena frame");
   must((uint32_t)ev.frame.payload_ref == lease && ev.frame.generation == ref.generation &&
            ev.frame.payload_len == len,
        "the lease reference travelled intact");
   must(ev.payload != NULL && ev.payload_len == len, "poll resolves the arena payload (D12)");
   int intact = 1;
   for (uint32_t i = 0; i < len; i++)
      intact &= ev.payload[i] == (uint8_t)(i * 7u + 3u);
   must(intact, "the subscriber reads the producer's bytes in place");
   must(ouro_arena_live_leases(&h.arena, pub.reply.handle_id) == 1, "still held while being read");

   /* The next poll returns the ref; the host consumes the release. */
   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_EMPTY, "only one event");
   must(sub.release_count == 0, "the release left the client");
   ouro_host_pump(&h);
   must(ouro_arena_live_leases(&h.arena, pub.reply.handle_id) == 0, "lease drained after release");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   printf("  publish_arena: a large payload routes by lease, is read in place, and is released\n");
}

/* A release can only return the sender's own ref. Releasing a lease the client
 * does not hold, or with a stale generation, is dropped and the lease lives on;
 * a malformed release is dropped too. None of it is routed. */
static void test_release_is_holder_checked(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t pub, sub, other;
   must_rc(attach_client(&h, &pub), OURO_CLIENT_OK, "publisher attaches");
   must_rc(attach_client(&h, &sub), OURO_CLIENT_OK, "subscriber attaches");
   must_rc(attach_client(&h, &other), OURO_CLIENT_OK, "bystander attaches");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, KIND_A) == OURO_HOST_OK, "subscribe");
   must(ouro_host_subscribe(&h, other.reply.handle_id, OURO_KIND_LEASE_RELEASE) == OURO_HOST_OK,
        "a would-be observer of releases");

   uint32_t lease = 0;
   must(ouro_arena_alloc(&h.arena, pub.reply.handle_id, 600, &lease) == OURO_ARENA_OK, "alloc");
   ouro_arena_ref_t ref;
   must(ouro_arena_ref(&h.arena, lease, &ref) == OURO_ARENA_OK, "ref");
   must_rc(ouro_client_publish_arena(&pub, KIND_A, lease, ref.generation, 600), OURO_CLIENT_OK,
           "publish");
   ouro_host_pump(&h);

   uint8_t forged[OURO_LEASE_RELEASE_LEN];
   for (unsigned i = 0; i < 4; i++)
   {
      forged[i] = (uint8_t)(lease >> (8u * i));
      forged[4 + i] = (uint8_t)(ref.generation >> (8u * i));
   }
   /* The bystander never received the frame, so it holds no ref to return. */
   must_rc(ouro_client_publish(&other, OURO_KIND_LEASE_RELEASE, forged, sizeof forged), OURO_CLIENT_OK,
           "bystander sends a release");
   /* Stale generation, from the real holder. */
   forged[4] ^= 0xff;
   must_rc(ouro_client_publish(&sub, OURO_KIND_LEASE_RELEASE, forged, sizeof forged), OURO_CLIENT_OK,
           "holder sends a stale release");
   /* Malformed: wrong length. */
   must_rc(ouro_client_publish(&sub, OURO_KIND_LEASE_RELEASE, forged, 4), OURO_CLIENT_OK,
           "holder sends a short release");
   /* A lease id past the table. */
   uint8_t past[OURO_LEASE_RELEASE_LEN] = {0x88, 0x13, 0, 0, 1, 0, 0, 0}; /* 5000, gen 1 */
   must_rc(ouro_client_publish(&sub, OURO_KIND_LEASE_RELEASE, past, sizeof past), OURO_CLIENT_OK,
           "holder sends a release for lease 5000");
   uint64_t dropped_before =
       h.slots[other.reply.handle_id].dropped + h.slots[sub.reply.handle_id].dropped;
   ouro_host_pump(&h);
   uint64_t dropped_after =
       h.slots[other.reply.handle_id].dropped + h.slots[sub.reply.handle_id].dropped;
   must(dropped_after == dropped_before + 4, "each bad release is counted as dropped");
   must(ouro_arena_live_leases(&h.arena, pub.reply.handle_id) == 1, "the lease is still live");

   ouro_event_t ev;
   must_rc(ouro_client_poll(&other, &ev), OURO_CLIENT_EMPTY, "releases are never routed");
   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_OK, "the holder reads its frame");
   must(ev.payload != NULL, "and its payload");
   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_EMPTY, "then returns the ref");
   ouro_host_pump(&h);
   must(ouro_arena_live_leases(&h.arena, pub.reply.handle_id) == 0, "the real release drains it");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_client_detach(&other);
   ouro_host_destroy(&h);
   printf("  lease release: holder- and generation-checked, never routed\n");
}

static void test_request_reply(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");

   ouro_client_t req, server;
   must_rc(attach_client(&h, &req), OURO_CLIENT_OK, "requester attaches");
   must_rc(attach_client(&h, &server), OURO_CLIENT_OK, "server attaches");
   must(ouro_host_serve_kind(&h, server.reply.handle_id, KIND_A) == OURO_HOST_OK, "serve kind");

   const uint64_t corr = 0xBEEF;
   must_rc(ouro_client_request(&req, KIND_A, corr, "ping", 4), OURO_CLIENT_OK, "send request");
   must(ouro_host_pump(&h) == 1, "route request");

   ouro_event_t ev;
   must_rc(ouro_client_poll(&server, &ev), OURO_CLIENT_OK, "server gets request");
   must((ev.frame.hdr_flags & OURO_F_REQUEST), "is the request");
   must(ev.payload_len == 4 && memcmp(ev.payload, "ping", 4) == 0, "request payload");
   /* The server answers with the correlation the host handed it, which is
    * ring-unique rather than the requester's own; the host maps it back. */
   const uint64_t served = ev.frame.correlation_id;

   must_rc(ouro_client_reply(&server, KIND_A, served, "pong", 4), OURO_CLIENT_OK, "server replies");
   must(ouro_host_pump(&h) == 1, "route reply");

   must_rc(ouro_client_poll(&req, &ev), OURO_CLIENT_OK, "requester gets reply");
   must(ev.frame.correlation_id == corr && (ev.frame.hdr_flags & OURO_F_REPLY), "is the reply");
   must(ev.payload_len == 4 && memcmp(ev.payload, "pong", 4) == 0, "reply payload");

   ouro_client_detach(&req);
   ouro_client_detach(&server);
   ouro_host_destroy(&h);
   printf("  request/reply: round-trips through the client library\n");
}

static void test_capability_absent(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t req;
   must_rc(attach_client(&h, &req), OURO_CLIENT_OK, "attach");

   must_rc(ouro_client_request(&req, KIND_A, 0x1234, NULL, 0), OURO_CLIENT_OK, "request no server");
   must(ouro_host_pump(&h) == 1, "route");
   ouro_event_t ev;
   must_rc(ouro_client_poll(&req, &ev), OURO_CLIENT_OK, "poll");
   must(ev.frame.event_kind == OURO_KIND_CAPABILITY_ABSENT && ev.frame.correlation_id == 0x1234,
        "got capability_absent for the missing server");

   ouro_client_detach(&req);
   ouro_host_destroy(&h);
   printf("  capability_absent: surfaced to the requester\n");
}

static void test_would_block(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t pub;
   must_rc(attach_client(&h, &pub), OURO_CLIENT_OK, "attach");

   /* Fill the outbound ring without pumping the host: capacity publishes
    * succeed, then the next returns would_block rather than overwriting or
    * crashing. */
   uint32_t cap = pub.reply.queue_capacity;
   uint32_t ok = 0;
   for (uint32_t i = 0; i < cap + 4; i++)
   {
      ouro_client_result_t r = ouro_client_publish(&pub, KIND_A, "x", 1);
      if (r == OURO_CLIENT_OK)
         ok++;
      else
      {
         must_rc(r, OURO_CLIENT_WOULD_BLOCK, "full outbound returns would_block");
         break;
      }
   }
   must(ok == cap, "exactly capacity publishes succeeded before would_block");

   /* Draining via the host frees the ring, and publishing resumes. */
   ouro_host_pump(&h);
   must_rc(ouro_client_publish(&pub, KIND_A, "x", 1), OURO_CLIENT_OK, "publish resumes after drain");

   /* An oversize payload is refused up front, not truncated. */
   char big[512];
   memset(big, 'z', sizeof big);
   must_rc(ouro_client_publish(&pub, KIND_A, big, cf.inline_budget + 1), OURO_CLIENT_ERR_PAYLOAD,
           "oversize inline payload refused");

   ouro_client_detach(&pub);
   ouro_host_destroy(&h);
   printf("  would_block: a full outbound ring is reported, not overrun\n");
}

static void test_heartbeat_and_epoch(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t c;
   must_rc(attach_client(&h, &c), OURO_CLIENT_OK, "attach");

   /* Heartbeat keeps the client from being reaped. */
   ouro_client_heartbeat(&c, 1);
   must(ouro_host_reap(&h, 100, 50) == 0, "baseline");
   ouro_client_heartbeat(&c, 2);
   must(ouro_host_reap(&h, 130, 50) == 0, "beating client not reaped");
   must(ouro_host_admitted(&h) == 1, "still admitted");

   /* A host restart is observed. */
   must(!ouro_client_epoch_changed(&c), "no restart yet");
   ouro_host_bump_epoch(&h);
   must(ouro_client_epoch_changed(&c), "client observes the host restart");
   /* And a publish after a restart refuses rather than writing a stale mapping. */
   must_rc(ouro_client_publish(&c, KIND_A, "x", 1), OURO_CLIENT_EPOCH,
           "publish refused after restart");
   ouro_event_t ev;
   must_rc(ouro_client_poll(&c, &ev), OURO_CLIENT_EPOCH, "poll refused after restart");

   ouro_client_detach(&c);
   ouro_host_destroy(&h);
   printf("  heartbeat/epoch: kept alive; a restart is observed and fails closed\n");
}

/* Write a raw frame into a client's outbound ring, bypassing the client's own
 * checks, as a hostile or broken client could. */
static void inject(ouro_client_t *c, const ouro_frame_t *f, uint32_t slot_size)
{
   uint8_t *slot = ouro_ring_produce_begin(&c->qp.outbound);
   must(slot != NULL, "outbound has room");
   must(ouro_wire_encode(f, slot, slot_size) == OURO_WIRE_HDR_LEN, "frame encodes");
   ouro_ring_produce_commit(&c->qp.outbound);
}

static int g_tapped;
static void count_tap(void *ctx, const ouro_frame_t *f, const uint8_t *p, uint32_t n)
{
   (void)ctx;
   (void)f;
   (void)p;
   (void)n;
   g_tapped++;
}

/* An inline payload_ref is a client's claim. One near 2^64 wraps
 * payload_ref + payload_len past the slot bound, which once let any admitted
 * client make the host read outside the slot (a release, a tap) or write
 * outside a destination slot (a routed notification), or crash it. Ingress now
 * decodes with the placement check, so each is dropped before anything reads it. */
static void test_wrapped_payload_ref_is_dropped(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_host_set_tap(&h, count_tap, NULL);
   ouro_client_t pub, sub;
   must_rc(attach_client(&h, &pub), OURO_CLIENT_OK, "publisher attaches");
   must_rc(attach_client(&h, &sub), OURO_CLIENT_OK, "subscriber attaches");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, KIND_A) == OURO_HOST_OK, "subscribe");

   const struct
   {
      uint32_t kind;
      uint64_t back;
      uint32_t len;
   } cases[] = {
       {KIND_A, 64, 72},                 /* would write 72 bytes before the destination slot */
       {KIND_A, 200000, 200008},         /* would crash the pump */
       {OURO_KIND_LEASE_RELEASE, 8, 8},   /* would read 8 bytes before the ring slot */
       {KIND_A, 0, 8},                   /* payload_ref inside the header */
       {KIND_A, (uint64_t)0 - 200, 100}, /* payload_ref past the slot */
   };
   for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++)
   {
      ouro_frame_t f;
      memset(&f, 0, sizeof f);
      f.hdr_flags = OURO_F_NOTIFICATION | OURO_F_INLINE;
      f.wire_version = OURO_WIRE_VERSION;
      f.event_kind = cases[i].kind;
      f.payload_ref = (uint64_t)0 - cases[i].back;
      f.payload_len = cases[i].len;
      inject(&pub, &f, cf.slot_size);
      uint64_t dropped = h.slots[pub.reply.handle_id].dropped;
      g_tapped = 0;
      ouro_host_pump(&h);
      must(h.slots[pub.reply.handle_id].dropped == dropped + 1, "the frame is dropped");
      must(g_tapped == 0, "and never reaches the tap");
      ouro_event_t ev;
      must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_EMPTY, "nothing is routed");
   }
   /* The host is intact: an ordinary notification still routes. */
   must_rc(ouro_client_publish(&pub, KIND_A, "ok", 2), OURO_CLIENT_OK, "publish");
   must(ouro_host_pump(&h) == 1, "routes");
   ouro_event_t ev;
   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_OK, "delivered");
   must(ev.payload_len == 2 && memcmp(ev.payload, "ok", 2) == 0, "intact");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   printf("  wrapped payload_ref: dropped at ingress, never read, written or tapped\n");
}

static uint32_t publish_arena_payload(ouro_host_t *h, ouro_client_t *pub, uint32_t kind, uint32_t len,
                                      uint8_t fill, uint32_t *generation)
{
   uint32_t lease = 0;
   must(ouro_arena_alloc(&h->arena, pub->reply.handle_id, len, &lease) == OURO_ARENA_OK, "alloc");
   uint8_t *p = NULL;
   must(ouro_arena_fill_ptr(&h->arena, lease, &p) == OURO_ARENA_OK, "fill");
   memset(p, fill, len);
   ouro_arena_ref_t ref;
   must(ouro_arena_ref(&h->arena, lease, &ref) == OURO_ARENA_OK, "ref");
   must_rc(ouro_client_publish_arena(pub, kind, lease, ref.generation, len), OURO_CLIENT_OK,
           "publish arena");
   if (generation)
      *generation = ref.generation;
   return lease;
}

/* A blocked arena delivery is retried from the target set captured at first
 * sight. If that slot is reaped and a new client admitted into it, the
 * newcomer holds no reference: it must not be handed the frame, even though
 * the lease is still live for another consumer and would resolve. */
static void test_retry_never_delivers_to_a_reused_slot(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t filler, prod, sub, keeper, newcomer;
   must_rc(attach_client(&h, &filler), OURO_CLIENT_OK, "filler attaches");
   must_rc(attach_client(&h, &prod), OURO_CLIENT_OK, "producer attaches");
   must_rc(attach_client(&h, &sub), OURO_CLIENT_OK, "subscriber attaches");
   must_rc(attach_client(&h, &keeper), OURO_CLIENT_OK, "keeper attaches");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, KIND_A) == OURO_HOST_OK, "subscribe sub");
   must(ouro_host_subscribe(&h, keeper.reply.handle_id, KIND_A) == OURO_HOST_OK, "subscribe keeper");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, KIND_A + 1) == OURO_HOST_OK,
        "sub alone takes the filler kind");

   /* Fill sub's inbound with a kind only it observes. */
   for (uint32_t i = 0; i < cf.queue_capacity * 2; i++)
   {
      if (ouro_client_publish(&filler, KIND_A + 1, "f", 1) != OURO_CLIENT_OK)
         break;
      ouro_host_pump(&h);
   }
   uint32_t gen = 0;
   uint32_t lease = publish_arena_payload(&h, &prod, KIND_A, 700, 0x5a, &gen);
   ouro_host_pump(&h);
   must(ouro_arena_holds(&h.arena, lease, gen, keeper.reply.handle_id), "keeper holds a ref");
   must(ouro_arena_holds(&h.arena, lease, gen, sub.reply.handle_id), "so does the blocked sub");

   uint32_t sub_slot = sub.reply.handle_id;
   must(ouro_host_release_slot(&h, sub_slot) == OURO_HOST_OK, "sub's slot is released");
   ouro_client_detach(&sub);
   must(!ouro_arena_holds(&h.arena, lease, gen, sub_slot), "its reference went with it");
   must_rc(attach_client(&h, &newcomer), OURO_CLIENT_OK, "a newcomer attaches");
   must(newcomer.reply.handle_id == sub_slot, "into the same slot");

   for (int i = 0; i < 4; i++)
      ouro_host_pump(&h);
   ouro_event_t ev;
   must_rc(ouro_client_poll(&newcomer, &ev), OURO_CLIENT_EMPTY,
           "the newcomer is never handed a frame it holds no reference for");

   must_rc(ouro_client_poll(&keeper, &ev), OURO_CLIENT_OK, "the keeper reads it");
   must(ev.payload && ev.payload_len == 700 && ev.payload[699] == 0x5a, "intact");
   must_rc(ouro_client_poll(&keeper, &ev), OURO_CLIENT_EMPTY, "and returns it");
   ouro_host_pump(&h);
   must(ouro_arena_bytes_in_use(&h.arena) == 0, "the lease drained");

   ouro_client_detach(&filler);
   ouro_client_detach(&prod);
   ouro_client_detach(&keeper);
   ouro_client_detach(&newcomer);
   ouro_host_destroy(&h);
   printf("  arena retry: delivery needs the reference, not just an occupied slot\n");
}

/* A malformed release placed in the arena still names a lease its sender
 * produced; it is reclaimed, by its producer only, rather than stranded. */
static void test_arena_placed_release_reclaims_only_its_own(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t prod, other;
   must_rc(attach_client(&h, &prod), OURO_CLIENT_OK, "producer attaches");
   must_rc(attach_client(&h, &other), OURO_CLIENT_OK, "bystander attaches");
   uint32_t lease = 0;
   must(ouro_arena_alloc(&h.arena, prod.reply.handle_id, 300, &lease) == OURO_ARENA_OK, "alloc");
   ouro_arena_ref_t ref;
   must(ouro_arena_ref(&h.arena, lease, &ref) == OURO_ARENA_OK, "ref");

   ouro_frame_t f;
   memset(&f, 0, sizeof f);
   f.hdr_flags = OURO_F_NOTIFICATION | OURO_F_ARENA;
   f.wire_version = OURO_WIRE_VERSION;
   f.event_kind = OURO_KIND_LEASE_RELEASE;
   f.payload_ref = lease;
   f.payload_len = 300;
   f.generation = ref.generation;
   inject(&other, &f, cf.slot_size);
   ouro_host_pump(&h);
   must(ouro_arena_live_leases(&h.arena, prod.reply.handle_id) == 1,
        "a bystander cannot reclaim another producer's lease");
   must(h.slots[other.reply.handle_id].dropped == 1, "its frame is dropped");

   inject(&prod, &f, cf.slot_size);
   ouro_host_pump(&h);
   must(ouro_arena_live_leases(&h.arena, prod.reply.handle_id) == 0, "its producer's is reclaimed");
   must(h.slots[prod.reply.handle_id].dropped == 1, "and still counted as malformed");

   ouro_client_detach(&prod);
   ouro_client_detach(&other);
   ouro_host_destroy(&h);
   printf("  arena-placed release: reclaimed by its producer only, never stranded\n");
}

/* A governed module process has grants on what it may inject. Returning a
 * reference is not an injection, so a slot with no outbound grants at all must
 * still release what it was given. */
static void test_release_needs_no_grant(void)
{
   ouro_host_config_t cf = cfg();
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t pub, sub;
   must_rc(attach_client(&h, &pub), OURO_CLIENT_OK, "publisher attaches");
   must_rc(attach_client(&h, &sub), OURO_CLIENT_OK, "subscriber attaches");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, KIND_A) == OURO_HOST_OK, "subscribe");
   must(ouro_host_enforce_grants(&h, sub.reply.handle_id) == OURO_HOST_OK, "sub is governed");

   must_rc(ouro_client_publish(&sub, KIND_A, "x", 1), OURO_CLIENT_OK, "an ungranted publish");
   ouro_host_pump(&h);
   must(h.slots[sub.reply.handle_id].dropped == 1, "is refused (grants are enforced)");

   publish_arena_payload(&h, &pub, KIND_A, 400, 0x11, NULL);
   ouro_host_pump(&h);
   ouro_event_t ev;
   while (ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK && !(ev.frame.hdr_flags & OURO_F_ARENA))
      ; /* skip the capability_denied control frame */
   must(ev.frame.hdr_flags & OURO_F_ARENA, "the governed slot receives the arena frame");
   must(ev.payload && ev.payload[0] == 0x11, "and reads it");
   must_rc(ouro_client_poll(&sub, &ev), OURO_CLIENT_EMPTY, "then returns it");
   ouro_host_pump(&h);
   must(h.slots[sub.reply.handle_id].dropped == 1, "the release is not refused");
   must(ouro_arena_bytes_in_use(&h.arena) == 0, "the lease drained");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   printf("  release: accepted from a governed slot with no grants\n");
}

/* With its outbound ring full, a client cannot return refs; it queues up to
 * OURO_CLIENT_MAX_PENDING_RELEASES and then stops taking arena frames, so
 * backpressure reaches the host. When the ring drains, everything resumes and
 * every reference goes back. */
static void test_release_backlog(void)
{
   ouro_host_config_t cf = cfg();
   cf.queue_capacity = 128; /* room for the backlog's arena frames, and the producer's cap */
   ouro_host_t h;
   must(ouro_host_create(&h, &cf, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t pub, sub;
   must_rc(attach_client(&h, &pub), OURO_CLIENT_OK, "publisher attaches");
   must_rc(attach_client(&h, &sub), OURO_CLIENT_OK, "subscriber attaches");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, KIND_A) == OURO_HOST_OK, "subscribe");

   const uint32_t n = OURO_CLIENT_MAX_PENDING_RELEASES + 10;
   for (uint32_t i = 0; i < n; i++)
   {
      publish_arena_payload(&h, &pub, KIND_A, 300, (uint8_t)i, NULL);
      ouro_host_pump(&h);
   }
   must(ouro_ring_count(&sub.qp.inbound) == n, "every arena frame is queued for sub");

   /* Fill sub's outbound so no release can leave. */
   uint32_t filled = 0;
   while (ouro_client_publish(&sub, KIND_A + 7, "z", 1) == OURO_CLIENT_OK)
      filled++;
   must(filled == cf.queue_capacity, "outbound full");

   uint32_t got = 0;
   ouro_event_t ev;
   while (ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK)
   {
      must(ev.payload && ev.payload[0] == (uint8_t)got, "in order, readable");
      got++;
   }
   must(sub.release_count == OURO_CLIENT_MAX_PENDING_RELEASES, "the backlog is full");
   must(got == OURO_CLIENT_MAX_PENDING_RELEASES, "one frame read per queued ref, then none");
   must(ouro_ring_count(&sub.qp.inbound) == n - got, "the rest wait unread");
   must_rc(ouro_client_flush_releases(&sub), OURO_CLIENT_WOULD_BLOCK, "flush cannot send yet");

   ouro_host_pump(&h); /* drains sub's outbound */
   while (ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK)
   {
      must(ev.payload && ev.payload[0] == (uint8_t)got, "resumes in order");
      got++;
   }
   must(got == n, "every frame was read");
   must_rc(ouro_client_flush_releases(&sub), OURO_CLIENT_OK, "nothing left owed");
   ouro_host_pump(&h);
   must(ouro_arena_bytes_in_use(&h.arena) == 0, "every reference went back");
   must(h.slots[sub.reply.handle_id].dropped == 0, "none was refused");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   printf("  release backlog: bounded, applies backpressure, drains completely\n");
}

int main(void)
{
   printf("test_client:\n");
   test_wrapped_payload_ref_is_dropped();
   test_retry_never_delivers_to_a_reused_slot();
   test_arena_placed_release_reclaims_only_its_own();
   test_release_needs_no_grant();
   test_release_backlog();
   test_publish_arena();
   test_release_is_holder_checked();
   test_publish_and_poll();
   test_request_reply();
   test_capability_absent();
   test_would_block();
   test_heartbeat_and_epoch();
   printf("test_client: OK\n");
   return 0;
}
