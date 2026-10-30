/* telemetry_host.c: the host writes one D16 record per seq-stamped event. */
#include "telemetry_internal.h"

#include <errno.h>
#include <stddef.h>
#include <string.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

#include <ouroboros/region_host.h>

ouro_host_result_t ouro_host_telemetry_enable(ouro_host_t *h, uint32_t capacity)
{
   if (!h || !h->control)
      return OURO_HOST_ERR_ARG;
   if (h->telemetry)
      return OURO_HOST_OK;
   if (capacity == 0)
      capacity = OURO_TELEMETRY_CAPACITY_DEFAULT;
   size_t bytes = ouro_telemetry_region_bytes(capacity);
   if (bytes == 0)
      return OURO_HOST_ERR_ARG;
   h->telemetry_doorbell_fd = -1;
   h->telemetry_slot = UINT32_MAX;
   if (ouro_region_create("ring-telemetry", bytes, &h->telemetry_region) != OURO_REGION_OK)
      return OURO_HOST_ERR_REGION;
   int fd = h->telemetry_region.fd;
   if (ouro_region_map(fd, bytes, 1, &h->telemetry_region) != OURO_REGION_OK)
   {
      close(fd);
      memset(&h->telemetry_region, 0, sizeof h->telemetry_region);
      return OURO_HOST_ERR_REGION;
   }
   ouro_telemetry_region_t *t = h->telemetry_region.base;
   uint8_t *ring_mem = (uint8_t *)h->telemetry_region.base + OURO_TELEMETRY_REGION_OFF_RING;
   if (ouro_ring_init(ring_mem, bytes - OURO_TELEMETRY_REGION_OFF_RING, OURO_TELEMETRY_RECORD_LEN,
                      capacity, &h->telemetry_ring) != OURO_RING_OK)
      goto fail;
   h->telemetry_doorbell_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
   if (h->telemetry_doorbell_fd < 0)
      goto fail;
   atomic_store_explicit(&t->version, OURO_TELEMETRY_VERSION, memory_order_relaxed);
   atomic_store_explicit(&t->record_size, OURO_TELEMETRY_RECORD_LEN, memory_order_relaxed);
   atomic_store_explicit(&t->host_epoch, ouro_control_epoch(h->control), memory_order_relaxed);
   atomic_store_explicit(&t->magic, OURO_TELEMETRY_MAGIC, memory_order_release);
   h->telemetry_head = h->telemetry_tail_cache = h->telemetry_lost = 0;
   h->telemetry = t;
   return OURO_HOST_OK;
fail:
   ouro_telemetry_host_destroy(h);
   return OURO_HOST_ERR_OS;
}

ouro_host_result_t ouro_host_grant_telemetry(ouro_host_t *h, uint32_t slot)
{
   if (!h || !h->slots || slot >= h->cfg.max_slots || !h->slots[slot].in_use)
      return OURO_HOST_ERR_ARG;
   h->slots[slot].telemetry_granted = 1;
   return OURO_HOST_OK;
}

void ouro_telemetry_host_destroy(ouro_host_t *h)
{
   h->telemetry = NULL;
   if (h->telemetry_region.size)
   {
      int fd = h->telemetry_region.fd;
      ouro_region_unmap(&h->telemetry_region);
      if (fd >= 0)
         close(fd);
   }
   memset(&h->telemetry_region, 0, sizeof h->telemetry_region);
   memset(&h->telemetry_ring, 0, sizeof h->telemetry_ring);
   if (h->telemetry_doorbell_fd >= 0)
      close(h->telemetry_doorbell_fd);
   h->telemetry_doorbell_fd = -1;
   h->telemetry_slot = UINT32_MAX;
}

static uint64_t realtime_ns(void)
{
   struct timespec ts;
   if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
      return 0;
   return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* The host's encoder. A slot's reserved bytes are zero from creation and the
 * host never writes them, so only owned fields are stored. On a little-endian
 * host the wire header after its magic is the frame's own bytes in two runs;
 * the asserts pin that. The frame was validated at ingress (or built by the
 * host), so it is not validated again. */
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
_Static_assert(offsetof(ouro_frame_t, hdr_flags) == 0 && offsetof(ouro_frame_t, principal_ref) == 8,
               "wire bytes 4..16 are frame bytes 0..12");
_Static_assert(offsetof(ouro_frame_t, correlation_id) == 16 &&
                   offsetof(ouro_frame_t, generation) == 60 && sizeof(ouro_frame_t) == 64,
               "wire bytes 16..64 are frame bytes 16..64");
static void put_record(uint8_t *slot, uint32_t type, uint64_t now, uint64_t lost,
                       const ouro_frame_t *f)
{
   memcpy(slot + OURO_TELEMETRY_OFF_TYPE, &type, 4);
   memcpy(slot + OURO_TELEMETRY_OFF_REALTIME_NS, &now, 8);
   memcpy(slot + OURO_TELEMETRY_OFF_LOST, &lost, 8);
   uint8_t *hdr = slot + OURO_TELEMETRY_OFF_FRAME;
   if (!f)
   {
      memset(hdr, 0, OURO_WIRE_HDR_LEN);
      return;
   }
   uint32_t magic = OURO_WIRE_MAGIC;
   memcpy(hdr, &magic, 4);
   memcpy(hdr + 4, f, 12);
   memcpy(hdr + 16, (const uint8_t *)f + 16, 48);
}
#else
static void put_record(uint8_t *slot, uint32_t type, uint64_t now, uint64_t lost,
                       const ouro_frame_t *f)
{
   ouro_telemetry_encode(slot, type, now, lost, f);
}
#endif

/* The producer end, with the consumer's index cached: it is read across cores
 * only when the cached view says the ring is full. */
static uint8_t *claim(ouro_host_t *h, uint64_t need)
{
   uint64_t head = h->telemetry_head;
   if (head - h->telemetry_tail_cache + need > h->telemetry_ring.capacity)
   {
      h->telemetry_tail_cache =
          atomic_load_explicit(&h->telemetry_ring.shared->tail, memory_order_acquire);
      /* A tail past head is a scribble, not a state: treat it as full. */
      if (h->telemetry_tail_cache > head ||
          head - h->telemetry_tail_cache + need > h->telemetry_ring.capacity)
         return NULL;
   }
   return h->telemetry_ring.slots + (size_t)(head & h->telemetry_ring.mask) * OURO_TELEMETRY_RECORD_LEN;
}

static void publish(ouro_host_t *h)
{
   /* Release: the record's bytes happen-before the consumer's acquire of head. */
   atomic_store_explicit(&h->telemetry_ring.shared->head, ++h->telemetry_head,
                         memory_order_release);
}

void ouro_telemetry_host_write(ouro_host_t *h, const ouro_frame_t *f)
{
   /* One clock read per pump: records in a pump share its time, and seq orders
    * them exactly. An event stamped outside a pump reads its own. */
   uint64_t now = h->telemetry_now;
   if (!now || !h->in_pump)
      now = h->telemetry_now = realtime_ns();
   if (h->telemetry_lost)
   {
      /* The gap record goes first, and only when the event fits after it, so the
       * stream never says "n lost" and then loses the next one silently. */
      uint8_t *gap = claim(h, 2);
      if (!gap)
         goto lost;
      put_record(gap, OURO_TELEMETRY_RECORD_GAP, now, h->telemetry_lost, NULL);
      publish(h);
      h->telemetry_lost = 0;
   }
   uint8_t *slot = claim(h, 1);
   if (!slot)
      goto lost;
   put_record(slot, OURO_TELEMETRY_RECORD_EVENT, now, 0, f);
   publish(h);
   h->telemetry_unsignalled = 1;
   if (!h->in_pump)
      ouro_telemetry_host_signal(h);
   return;
lost:
   h->telemetry_lost++;
   atomic_fetch_add_explicit(&h->telemetry->dropped, 1, memory_order_relaxed);
}

void ouro_telemetry_host_signal(ouro_host_t *h)
{
   h->telemetry_now = 0; /* the next pump reads the clock again */
   if (!h->telemetry || !h->telemetry_unsignalled)
      return;
   h->telemetry_unsignalled = 0;
   /* D13: pairs with the fence in ouro_telemetry_wait. */
   atomic_thread_fence(memory_order_seq_cst);
   if (atomic_load_explicit(&h->telemetry->consumer_waiting, memory_order_relaxed) != 0)
   {
      uint64_t one = 1;
      while (write(h->telemetry_doorbell_fd, &one, sizeof one) < 0 && errno == EINTR)
         ;
   }
}
