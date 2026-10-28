/* test_bus_capture.c: slice 11 of the ring feature tree.
 *
 * The capture stream and observational replay (D10):
 *
 *   - end to end: the tap records a host's event stream; reading it back yields
 *     the same events, in contiguous seq order, with materialized payloads, and
 *     observational replay reproduces them exactly;
 *   - the terminal-states parser is total and decides open / complete /
 *     truncated / corrupt from the bytes alone — a clean tail is open, an
 *     epoch_change tail is complete, a torn final record is truncated, and a
 *     seq gap, a mid-stream CRC failure, a record after epoch_change, or an
 *     unknown format_version are all corrupt.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <ouroboros/capture.h>
#include <ouroboros/client.h>
#include <ouroboros/host.h>
#include <ouroboros/local_attach.h>
#include <ouroboros/wire.h>

#include "support/tmpdir.h"

static void must(int cond, const char *what)
{
   if (!cond)
   {
      fprintf(stderr, "FAIL: %s\n", what);
      abort();
   }
}

/* ---- replay collector ---- */

#define MAXREC 256
static uint64_t g_seq[MAXREC];
static uint32_t g_kind[MAXREC];
static uint32_t g_n;
static void collect(void *ctx, const ouro_capture_event_t *ev)
{
   (void)ctx;
   must(g_n < MAXREC, "replay buffer");
   g_seq[g_n] = ev->frame.seq;
   g_kind[g_n] = ev->frame.event_kind;
   g_n++;
}

/* ---- a directly-driven tap, for precise stream construction ---- */

static void tap_frame(ouro_capture_t *c, uint32_t kind, uint64_t seq, const char *payload)
{
   ouro_frame_t f;
   memset(&f, 0, sizeof f);
   f.wire_version = OURO_WIRE_VERSION;
   f.event_kind = kind;
   f.hdr_flags = OURO_F_NOTIFICATION;
   f.seq = seq;
   uint32_t plen = payload ? (uint32_t)strlen(payload) : 0;
   if (plen)
   {
      f.hdr_flags |= OURO_F_INLINE;
      f.payload_len = plen;
      f.payload_ref = OURO_WIRE_HDR_LEN;
   }
   ouro_capture_tap(c, &f, (const uint8_t *)payload, plen);
}

/* ------------------------------------------------------------------ */

static void test_roundtrip_and_replay(void)
{
   ouro_capture_t cap;
   ouro_capture_init(&cap, 1, 1, 7);

   const char *msgs[] = {"alpha", "bravo", "charlie", "delta"};
   for (int i = 0; i < 4; i++)
      tap_frame(&cap, 300 + i, (uint64_t)(100 + i), msgs[i]);

   g_n = 0;
   ouro_capture_report_t rep = ouro_capture_read(cap.buf, cap.len, collect, NULL);
   must(rep.status == OURO_CAPTURE_OPEN, "a clean, non-terminated stream is open");
   must(rep.records == 4, "all four records parsed");
   must(g_n == 4, "replay produced four events");
   for (int i = 0; i < 4; i++)
   {
      must(g_seq[i] == (uint64_t)(100 + i), "seq preserved in order");
      must(g_kind[i] == (uint32_t)(300 + i), "kind preserved");
   }
   /* Materialized payloads survive: re-read and check the bytes of one. */
   must(rep.last_good_seq == 103, "last good seq");

   ouro_capture_free(&cap);
   printf("  roundtrip: capture -> read -> replay reproduces the stream in order\n");
}

/* Verify the materialized payload bytes come back exactly. */
static const char *g_want_payload;
static int g_payload_ok;
static void check_payload(void *ctx, const ouro_capture_event_t *ev)
{
   (void)ctx;
   if (ev->frame.seq == 500)
   {
      g_payload_ok = ev->payload_len == (uint32_t)strlen(g_want_payload) &&
                     memcmp(ev->payload, g_want_payload, ev->payload_len) == 0;
   }
}

static void test_materialized_payload(void)
{
   ouro_capture_t cap;
   ouro_capture_init(&cap, 1, 1, 1);
   g_want_payload = "the-payload-bytes";
   tap_frame(&cap, 400, 500, g_want_payload);
   g_payload_ok = 0;
   ouro_capture_read(cap.buf, cap.len, check_payload, NULL);
   must(g_payload_ok, "materialized payload bytes come back exactly");
   ouro_capture_free(&cap);
   printf("  materialized payload: recorded and read back byte-exact\n");
}

static void attach_client(ouro_host_t *h, ouro_client_t *c)
{
   must(ouro_host_attach_local(h, c, 0) == OURO_CLIENT_OK, "attach");
}

/* Route real events through the host with the capture tap installed, and confirm
 * the captured stream is exactly what was routed. */
static uint32_t g_e2e_kinds[8];
static uint32_t g_e2e_n;
static void e2e_collect(void *ctx, const ouro_capture_event_t *ev)
{
   (void)ctx;
   if (g_e2e_n < 8)
      g_e2e_kinds[g_e2e_n++] = ev->frame.event_kind;
}

static void test_end_to_end_host(void)
{
   ouro_host_config_t cfg;
   memset(&cfg, 0, sizeof cfg);
   cfg.max_slots = 4;
   cfg.slot_size = 256;
   cfg.inline_budget = 192;
   cfg.queue_capacity = 16;
   cfg.arena_size = 128 * 1024;

   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_capture_t cap;
   ouro_capture_init(&cap, 1, 1, ouro_control_epoch(h.control));
   ouro_host_set_tap(&h, ouro_capture_tap, &cap);

   ouro_client_t pub, sub;
   attach_client(&h, &pub);
   attach_client(&h, &sub);
   must(ouro_host_subscribe(&h, sub.reply.handle_id, 700) == OURO_HOST_OK, "subscribe");

   /* Two real publishes routed through the host; the tap records each once. */
   must(ouro_client_publish(&pub, 700, "one", 3) == OURO_CLIENT_OK, "publish 1");
   must(ouro_client_publish(&pub, 700, "two", 3) == OURO_CLIENT_OK, "publish 2");
   must(ouro_host_pump(&h) == 2, "host routes both");

   /* The subscriber actually received them (delivered stream), and the capture
    * recorded them (intended stream) — the two derivable from one another (D6). */
   ouro_event_t ev;
   must(ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK && ev.frame.event_kind == 700, "delivered 1");
   must(ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK && ev.frame.event_kind == 700, "delivered 2");

   g_e2e_n = 0;
   ouro_capture_report_t rep = ouro_capture_read(cap.buf, cap.len, e2e_collect, NULL);
   must(rep.status == OURO_CAPTURE_OPEN && rep.records == 2, "capture recorded both routed events");
   must(g_e2e_n == 2 && g_e2e_kinds[0] == 700 && g_e2e_kinds[1] == 700,
        "replay reproduces the routed kinds in order");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_capture_free(&cap);
   ouro_host_destroy(&h);
   printf("  end-to-end: real routed events are captured and replayed in order\n");
}

static void test_terminal_states(void)
{
   /* complete: a stream ending in an epoch_change record. */
   {
      ouro_capture_t c;
      ouro_capture_init(&c, 1, 1, 1);
      tap_frame(&c, 800, 10, "x");
      tap_frame(&c, OURO_KIND_EPOCH_CHANGE, 11, NULL);
      ouro_capture_report_t r = ouro_capture_read(c.buf, c.len, NULL, NULL);
      must(r.status == OURO_CAPTURE_COMPLETE, "epoch_change tail -> complete");
      ouro_capture_free(&c);
   }

   /* corrupt: a record after epoch_change. */
   {
      ouro_capture_t c;
      ouro_capture_init(&c, 1, 1, 1);
      tap_frame(&c, OURO_KIND_EPOCH_CHANGE, 20, NULL);
      tap_frame(&c, 800, 21, "x");
      ouro_capture_report_t r = ouro_capture_read(c.buf, c.len, NULL, NULL);
      must(r.status == OURO_CAPTURE_CORRUPT, "record after epoch_change -> corrupt");
      ouro_capture_free(&c);
   }

   /* corrupt: a seq gap. */
   {
      ouro_capture_t c;
      ouro_capture_init(&c, 1, 1, 1);
      tap_frame(&c, 800, 30, "a");
      tap_frame(&c, 801, 32, "b"); /* 31 skipped */
      ouro_capture_report_t r = ouro_capture_read(c.buf, c.len, NULL, NULL);
      must(r.status == OURO_CAPTURE_CORRUPT && r.rule == 6, "seq gap -> corrupt");
      ouro_capture_free(&c);
   }

   /* truncated: chop the last record mid-body. */
   {
      ouro_capture_t c;
      ouro_capture_init(&c, 1, 1, 1);
      tap_frame(&c, 800, 40, "hello");
      tap_frame(&c, 801, 41, "world");
      size_t chopped = c.len - 3; /* lose the last 3 bytes of the final record */
      ouro_capture_report_t r = ouro_capture_read(c.buf, chopped, NULL, NULL);
      must(r.status == OURO_CAPTURE_TRUNCATED, "torn final record -> truncated");
      must(r.records == 1 && r.last_good_seq == 40, "the first record survived");
      ouro_capture_free(&c);
   }

   /* corrupt: a mid-stream CRC failure (bytes follow it). */
   {
      ouro_capture_t c;
      ouro_capture_init(&c, 1, 1, 1);
      tap_frame(&c, 800, 50, "aaaa");
      tap_frame(&c, 801, 51, "bbbb");
      /* flip a byte inside the first record's payload; its CRC now fails and it
       * is not the last record. */
      c.buf[OURO_CAPTURE_HEADER_LEN + 8 + OURO_WIRE_HDR_LEN] ^= 0xff;
      ouro_capture_report_t r = ouro_capture_read(c.buf, c.len, NULL, NULL);
      must(r.status == OURO_CAPTURE_CORRUPT && r.rule == 5, "mid-stream CRC failure -> corrupt");
      ouro_capture_free(&c);
   }

   /* corrupt: an unknown format_version is refused before any record. */
   {
      ouro_capture_t c;
      ouro_capture_init(&c, 1, 1, 1);
      tap_frame(&c, 800, 60, "x");
      c.buf[8] = 0x7f; /* format_version */
      ouro_capture_report_t r = ouro_capture_read(c.buf, c.len, NULL, NULL);
      must(r.status == OURO_CAPTURE_CORRUPT && r.rule == 0, "unknown format_version -> corrupt");
      ouro_capture_free(&c);
   }

   printf("  terminal states: open/complete/truncated/corrupt decided from bytes\n");
}

/* An arena event is captured too. The host resolves the producer-held span
 * pre-routing and hands the bytes to the tap, so the record materializes them and
 * replay reproduces them from the record blob — never the long-gone lease. */
static uint8_t g_arena_seen[4096];
static uint32_t g_arena_seen_len;
static int g_arena_materialized;
static void arena_collect(void *ctx, const ouro_capture_event_t *ev)
{
   (void)ctx;
   if (!(ev->frame.hdr_flags & OURO_F_ARENA))
      return;
   g_arena_materialized = ev->payload_len > 0 && ev->payload != NULL;
   g_arena_seen_len = ev->payload_len;
   if (ev->payload && ev->payload_len <= sizeof g_arena_seen)
      memcpy(g_arena_seen, ev->payload, ev->payload_len);
}

static void test_arena_captured_and_replayed(void)
{
   ouro_host_config_t cfg;
   memset(&cfg, 0, sizeof cfg);
   cfg.max_slots = 4;
   cfg.slot_size = 256;
   cfg.inline_budget = 192;
   cfg.queue_capacity = 16;
   cfg.arena_size = 128 * 1024;

   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_capture_t cap;
   ouro_capture_init(&cap, 1, 1, ouro_control_epoch(h.control));
   ouro_host_set_tap(&h, ouro_capture_tap, &cap);

   ouro_client_t pub, sub;
   attach_client(&h, &pub);
   attach_client(&h, &sub);
   must(ouro_host_subscribe(&h, sub.reply.handle_id, 700) == OURO_HOST_OK, "subscribe");

   /* An arena-sized payload (> inline budget) with a known per-byte pattern. */
   const uint32_t len = 1000;
   uint32_t lease = 0;
   must(ouro_arena_alloc(&h.arena, pub.reply.handle_id, len, &lease) == OURO_ARENA_OK, "alloc");
   uint8_t *fp = NULL;
   must(ouro_arena_fill_ptr(&h.arena, lease, &fp) == OURO_ARENA_OK, "fill");
   for (uint32_t i = 0; i < len; i++)
      fp[i] = (uint8_t)(i * 7 + 1);
   ouro_arena_ref_t ref;
   must(ouro_arena_ref(&h.arena, lease, &ref) == OURO_ARENA_OK, "ref");
   must(ouro_client_publish_arena(&pub, 700, lease, ref.generation, len) == OURO_CLIENT_OK,
        "publish");
   must(ouro_host_pump(&h) == 1, "host routes the arena event");

   /* Routing still works alongside capture: the subscriber reads the payload in
    * place and its next poll returns the ref (D12), which the tap records too. */
   ouro_event_t ev;
   must(ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK && (ev.frame.hdr_flags & OURO_F_ARENA),
        "arena event delivered");
   must(ev.payload && ev.payload_len == len && ev.payload[len - 1] == (uint8_t)((len - 1) * 7 + 1),
        "consumer reads in place");
   must(ouro_client_poll(&sub, &ev) == OURO_CLIENT_EMPTY, "next poll returns the ref");
   must(ouro_host_pump(&h) == 1, "host consumes the release");
   must(ouro_arena_bytes_in_use(&h.arena) == 0, "the lease drained");
   must(h.slots[sub.reply.handle_id].dropped == 0, "the release was accepted");

   /* The capture materialized the arena bytes; replay returns them byte-exact. */
   g_arena_materialized = 0;
   g_arena_seen_len = 0;
   ouro_capture_report_t rep = ouro_capture_read(cap.buf, cap.len, arena_collect, NULL);
   must(rep.status == OURO_CAPTURE_OPEN && rep.records == 2, "arena event and its release captured");
   must(g_arena_materialized && g_arena_seen_len == len,
        "arena payload materialized in the record");
   int ok = 1;
   for (uint32_t i = 0; i < len; i++)
      if (g_arena_seen[i] != (uint8_t)(i * 7 + 1))
         ok = 0;
   must(ok, "replayed arena bytes match what the producer filled");

   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_capture_free(&cap);
   ouro_host_destroy(&h);
   printf("  arena capture: an arena payload is materialized and replayed byte-exact\n");
}

/* ---- writing to a file ---- */

static size_t read_file(const char *path, uint8_t *buf, size_t cap)
{
   FILE *f = fopen(path, "rb");
   must(f != NULL, "open capture file");
   size_t n = fread(buf, 1, cap, f);
   fclose(f);
   return n;
}

/* Flushing many times builds one stream: the header once, then every record in
 * seq order, so the file reads back exactly like the sink would have. */
static void test_flush_to_file(void)
{
   char path[4096];
   snprintf(path, sizeof path, "%s/ouro-capture-XXXXXX", test_tmpdir());
   int fd = mkstemp(path);
   must(fd >= 0, "temp file");

   ouro_capture_t c;
   ouro_capture_init(&c, 1, 1, 7);
   uint64_t written = 0;
   must(ouro_capture_flush_fd(&c, fd, &written, 0) == OURO_CAPTURE_FLUSHED && written == 0,
        "an empty sink writes nothing");
   tap_frame(&c, 700, 1, "a");
   tap_frame(&c, 700, 2, "bb");
   must(ouro_capture_flush_fd(&c, fd, &written, 0) == OURO_CAPTURE_FLUSHED, "first flush");
   must(c.len == 0 && c.header_written, "sink emptied, header remembered");
   uint64_t after_first = written;
   tap_frame(&c, 701, 3, "ccc");
   must(ouro_capture_flush_fd(&c, fd, &written, 0) == OURO_CAPTURE_FLUSHED, "second flush");
   must(written > after_first, "running count advanced");
   close(fd);

   static uint8_t buf[8192];
   size_t n = read_file(path, buf, sizeof buf);
   must(n == written, "file holds exactly what was counted");
   g_n = 0;
   ouro_capture_report_t rep = ouro_capture_read(buf, n, collect, NULL);
   must(rep.status == OURO_CAPTURE_OPEN && rep.records == 3, "one open stream of three records");
   must(g_seq[0] == 1 && g_seq[1] == 2 && g_seq[2] == 3 && g_kind[2] == 701, "in seq order");
   unlink(path);
   ouro_capture_free(&c);
   printf("  file: repeated flushes write one header and one contiguous stream\n");
}

/* The limit is checked before writing, so the file never passes it, and the sink
 * breaks so the tap stops growing a stream nothing will persist. */
static void test_flush_limit_and_failure(void)
{
   char path[4096];
   snprintf(path, sizeof path, "%s/ouro-capture-XXXXXX", test_tmpdir());
   int fd = mkstemp(path);
   must(fd >= 0, "temp file");

   ouro_capture_t c;
   ouro_capture_init(&c, 1, 1, 7);
   uint64_t written = 0;
   tap_frame(&c, 700, 1, "a");
   uint64_t limit = (uint64_t)c.len + 8;
   must(ouro_capture_flush_fd(&c, fd, &written, limit) == OURO_CAPTURE_FLUSHED, "fits");
   tap_frame(&c, 700, 2, "a record that does not fit in eight bytes");
   must(ouro_capture_flush_fd(&c, fd, &written, limit) == OURO_CAPTURE_FLUSH_LIMIT, "limit");
   must(written <= limit && c.broken && c.len == 0, "nothing past the limit, sink broken");
   must(ouro_capture_flush_fd(&c, fd, &written, limit) == OURO_CAPTURE_FLUSH_BROKEN,
        "a broken sink stays broken");
   close(fd);
   static uint8_t buf[4096];
   size_t n = read_file(path, buf, sizeof buf);
   must(ouro_capture_read(buf, n, NULL, NULL).status == OURO_CAPTURE_OPEN,
        "what was written is still a valid stream");
   unlink(path);
   ouro_capture_free(&c);

   ouro_capture_init(&c, 1, 1, 7);
   written = 0;
   tap_frame(&c, 700, 1, "a");
   must(ouro_capture_flush_fd(&c, -1, &written, 0) == OURO_CAPTURE_FLUSH_WRITE_FAILED,
        "a failed write is reported");
   must(c.broken && c.len == 0 && written == 0, "and breaks the sink");
   must(strcmp(ouro_capture_flush_name(OURO_CAPTURE_FLUSH_LIMIT), "limit") == 0, "names");
   ouro_capture_free(&c);
   printf("  file: the byte limit holds and a failed write breaks the sink\n");
}

int main(void)
{
   printf("test_bus_capture:\n");
   test_roundtrip_and_replay();
   test_materialized_payload();
   test_end_to_end_host();
   test_arena_captured_and_replayed();
   test_terminal_states();
   test_flush_to_file();
   test_flush_limit_and_failure();
   printf("test_bus_capture: OK\n");
   return 0;
}
