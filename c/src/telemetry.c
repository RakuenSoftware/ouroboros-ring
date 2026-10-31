/* telemetry.c: the D16 record encoding and the telemetry consumer end. */
#define _GNU_SOURCE /* ppoll */
#include <ouroboros/telemetry.h>

#include <errno.h>
#include <poll.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static void put32(uint8_t *p, uint32_t v)
{
   for (int i = 0; i < 4; i++)
      p[i] = (uint8_t)(v >> (8 * i));
}
static void put64(uint8_t *p, uint64_t v)
{
   for (int i = 0; i < 8; i++)
      p[i] = (uint8_t)(v >> (8 * i));
}
static uint32_t get32(const uint8_t *p)
{
   return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint64_t get64(const uint8_t *p)
{
   return (uint64_t)get32(p) | ((uint64_t)get32(p + 4) << 32);
}

size_t ouro_telemetry_region_bytes(uint32_t capacity)
{
   size_t ring = ouro_ring_bytes(OURO_TELEMETRY_RECORD_LEN, capacity);
   return ring ? OURO_TELEMETRY_REGION_OFF_RING + ring : 0;
}

void ouro_telemetry_encode(uint8_t *out, uint32_t type, uint64_t realtime_ns, uint64_t lost,
                           const ouro_frame_t *frame)
{
   memset(out, 0, OURO_TELEMETRY_RECORD_LEN);
   put32(out + OURO_TELEMETRY_OFF_TYPE, type);
   put64(out + OURO_TELEMETRY_OFF_REALTIME_NS, realtime_ns);
   put64(out + OURO_TELEMETRY_OFF_LOST, lost);
   if (frame)
      (void)ouro_wire_encode(frame, out + OURO_TELEMETRY_OFF_FRAME, OURO_WIRE_HDR_LEN);
}

ouro_telemetry_result_t ouro_telemetry_decode(const uint8_t *in, ouro_telemetry_record_t *out)
{
   if (!in || !out)
      return OURO_TELEMETRY_ERR_ARG;
   memset(out, 0, sizeof *out);
   out->type = get32(in + OURO_TELEMETRY_OFF_TYPE);
   out->realtime_ns = get64(in + OURO_TELEMETRY_OFF_REALTIME_NS);
   out->lost = get64(in + OURO_TELEMETRY_OFF_LOST);
   if (get32(in + OURO_TELEMETRY_OFF_FLAGS) != 0)
      return OURO_TELEMETRY_ERR_LAYOUT;
   if (out->type == OURO_TELEMETRY_RECORD_GAP)
      return out->lost ? OURO_TELEMETRY_OK : OURO_TELEMETRY_ERR_LAYOUT;
   if (out->type != OURO_TELEMETRY_RECORD_EVENT || out->lost != 0)
      return OURO_TELEMETRY_ERR_LAYOUT;
   return ouro_wire_decode(in + OURO_TELEMETRY_OFF_FRAME, OURO_WIRE_HDR_LEN, &out->frame) ==
                  OURO_WIRE_OK
              ? OURO_TELEMETRY_OK
              : OURO_TELEMETRY_ERR_LAYOUT;
}

ouro_telemetry_result_t ouro_telemetry_reader_open(int region_fd, int doorbell_fd,
                                                   ouro_telemetry_reader_t *r)
{
   if (!r)
      return OURO_TELEMETRY_ERR_ARG;
   memset(r, 0, sizeof *r);
   r->region.fd = -1;
   r->doorbell_fd = doorbell_fd;
   struct stat st;
   if (region_fd < 0 || doorbell_fd < 0 || fstat(region_fd, &st) != 0 ||
       st.st_size <= OURO_TELEMETRY_REGION_OFF_RING)
      goto fail;
   if (ouro_region_map(region_fd, (size_t)st.st_size, 1, &r->region) != OURO_REGION_OK)
      goto fail;
   close(region_fd); /* the mapping holds its own reference */
   region_fd = r->region.fd = -1;
   r->hdr = r->region.base;
   /* Validate once, then use only the local copy (as every ring does). */
   if (atomic_load_explicit(&r->hdr->magic, memory_order_acquire) != OURO_TELEMETRY_MAGIC ||
       atomic_load_explicit(&r->hdr->version, memory_order_relaxed) != OURO_TELEMETRY_VERSION ||
       atomic_load_explicit(&r->hdr->record_size, memory_order_relaxed) !=
           OURO_TELEMETRY_RECORD_LEN ||
       ouro_ring_attach((uint8_t *)r->region.base + OURO_TELEMETRY_REGION_OFF_RING,
                        r->region.size - OURO_TELEMETRY_REGION_OFF_RING,
                        &r->ring) != OURO_RING_OK ||
       r->ring.slot_size != OURO_TELEMETRY_RECORD_LEN)
   {
      ouro_telemetry_reader_close(r);
      return OURO_TELEMETRY_ERR_LAYOUT;
   }
   return OURO_TELEMETRY_OK;
fail:
   if (region_fd >= 0)
      close(region_fd);
   ouro_telemetry_reader_close(r);
   return OURO_TELEMETRY_ERR_OS;
}

void ouro_telemetry_reader_close(ouro_telemetry_reader_t *r)
{
   if (!r)
      return;
   ouro_region_unmap(&r->region);
   if (r->doorbell_fd >= 0)
      close(r->doorbell_fd);
   memset(r, 0, sizeof *r);
   r->region.fd = -1;
   r->doorbell_fd = -1;
}

uint64_t ouro_telemetry_available(const ouro_telemetry_reader_t *r)
{
   if (!r || !r->ring.shared)
      return 0;
   uint64_t n = ouro_ring_count(&r->ring);
   /* A head the host never wrote (a scribbled tail) is not a ring state. */
   return n > r->ring.capacity ? 0 : n;
}

const uint8_t *ouro_telemetry_peek(const ouro_telemetry_reader_t *r, uint64_t i)
{
   if (!r || !r->ring.shared || i >= ouro_telemetry_available(r))
      return NULL;
   uint64_t tail = atomic_load_explicit(&r->ring.shared->tail, memory_order_relaxed);
   return r->ring.slots + (size_t)((tail + i) & r->ring.mask) * r->ring.slot_size;
}

void ouro_telemetry_release(ouro_telemetry_reader_t *r, uint64_t n)
{
   if (!r || !r->ring.shared || n == 0)
      return;
   uint64_t avail = ouro_telemetry_available(r);
   if (n > avail)
      n = avail;
   uint64_t tail = atomic_load_explicit(&r->ring.shared->tail, memory_order_relaxed);
   /* Release: the reads of these slots happen-before the host reuses them. */
   atomic_store_explicit(&r->ring.shared->tail, tail + n, memory_order_release);
}

uint64_t ouro_telemetry_dropped(const ouro_telemetry_reader_t *r)
{
   return r && r->hdr ? atomic_load_explicit(&r->hdr->dropped, memory_order_relaxed) : 0;
}

static uint64_t monotonic_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

int ouro_telemetry_wait(ouro_telemetry_reader_t *r, int extra_fd, int timeout_ms)
{
   if (!r || !r->hdr)
      return -1;
   uint64_t start = monotonic_ns();
   uint64_t deadline =
       timeout_ms < 0 ? UINT64_MAX : start + (uint64_t)timeout_ms * 1000000ull;
   uint64_t interval = atomic_load_explicit(&r->hdr->drain_interval_ns, memory_order_relaxed);
   uint64_t idle = atomic_load_explicit(&r->hdr->idle_after_ns, memory_order_relaxed);
   struct pollfd p[2] = {{.fd = r->doorbell_fd, .events = POLLIN},
                         {.fd = extra_fd, .events = POLLIN}};
   nfds_t n = extra_fd >= 0 ? 2 : 1;
   /* Busy (D16 pacing): look again every drain interval while records keep
    * arriving, with the waiting flag down so the host never rings for it. The
    * doorbell is still polled, so an explicit wake ends the sleep early. */
   for (;;)
   {
      if (ouro_telemetry_available(r) > 0)
      {
         r->active_ns = monotonic_ns();
         return 1;
      }
      uint64_t now = monotonic_ns();
      if (now >= deadline)
         return 0;
      if (!interval || !r->active_ns || now - r->active_ns >= idle)
         break;
      uint64_t step = deadline - now < interval ? deadline - now : interval;
      struct timespec ts = {.tv_sec = (time_t)(step / 1000000000ull),
                            .tv_nsec = (long)(step % 1000000000ull)};
      int rc = ppoll(p, n, &ts, NULL);
      if (rc < 0 && errno != EINTR)
         return -1;
      if (rc > 0 && ((n > 1 && p[1].revents) || (p[0].revents & POLLIN)))
      {
         /* extra_fd, or an explicit wake: return now, as an unpaced wait would. */
         uint64_t count;
         if (p[0].revents & POLLIN)
            while (read(r->doorbell_fd, &count, sizeof count) < 0 && errno == EINTR)
               ;
         return ouro_telemetry_available(r) > 0 ? 1 : 0;
      }
   }
   /* Idle: D13 flag, fence, re-check, sleep. Either the host's fenced load sees
    * the flag and rings, or this re-check sees its record. */
   atomic_store_explicit(&r->hdr->consumer_waiting, 1, memory_order_seq_cst);
   atomic_thread_fence(memory_order_seq_cst);
   if (ouro_telemetry_available(r) > 0)
   {
      atomic_store_explicit(&r->hdr->consumer_waiting, 0, memory_order_relaxed);
      r->active_ns = monotonic_ns();
      return 1;
   }
   int wait_ms = -1;
   if (deadline != UINT64_MAX)
   {
      uint64_t now = monotonic_ns();
      wait_ms = now >= deadline ? 0 : (int)((deadline - now + 999999ull) / 1000000ull);
   }
   int rc = poll(p, n, wait_ms);
   if (rc > 0 && (p[0].revents & POLLIN))
   {
      uint64_t count;
      while (read(r->doorbell_fd, &count, sizeof count) < 0 && errno == EINTR)
         ;
   }
   atomic_store_explicit(&r->hdr->consumer_waiting, 0, memory_order_relaxed);
   if (rc < 0 && errno != EINTR)
      return -1;
   if (ouro_telemetry_available(r) > 0)
   {
      r->active_ns = monotonic_ns();
      return 1;
   }
   return 0;
}
