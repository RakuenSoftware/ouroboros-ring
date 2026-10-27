/* Host-only creation and initialisation for ring shared-memory regions. */
#define _GNU_SOURCE
#include <errno.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <ouroboros/region_host.h>

#include "region_internal.h"

static size_t page_round_up(size_t n)
{
   long ps = sysconf(_SC_PAGESIZE);
   size_t page = (ps > 0) ? (size_t)ps : 4096;
   size_t rounded = (n + page - 1) & ~(page - 1);
   if (rounded < n)
      return 0;
   return rounded;
}

ouro_region_result_t ouro_region_create(const char *name, size_t size, ouro_region_t *out)
{
   if (!out || size == 0)
      return OURO_REGION_ERR_ARG;

   size_t rounded = page_round_up(size);
   if (rounded == 0)
      return OURO_REGION_ERR_SIZE;

   int fd = memfd_create(name ? name : "ouroboros-ring", MFD_CLOEXEC);
   if (fd < 0)
      return OURO_REGION_ERR_OS;
   if (ftruncate(fd, (off_t)rounded) != 0)
   {
      int saved = errno;
      close(fd);
      errno = saved;
      return OURO_REGION_ERR_OS;
   }

   out->fd = fd;
   out->base = NULL;
   out->size = rounded;
   out->writable = 0;
   return OURO_REGION_OK;
}

ouro_region_result_t ouro_control_init(ouro_region_t *r, uint32_t slot_size, uint32_t inline_budget,
                                     uint32_t queue_capacity, uint64_t arena_size)
{
   if (!r || !r->base || !r->writable)
      return OURO_REGION_ERR_ARG;
   if (r->size < sizeof(ouro_control_t))
      return OURO_REGION_ERR_SIZE;
   if (slot_size == 0 || inline_budget > slot_size || arena_size == 0 ||
       arena_size > OURO_ARENA_MAX_SIZE || ouro_qpair_bytes(slot_size, queue_capacity) == 0)
      return OURO_REGION_ERR_GEOMETRY;

   ouro_control_t *c = (ouro_control_t *)r->base;
   /* Zero the whole region: the header and, when present, every lease-view
    * entry (generation 0 = no live lease). */
   memset(r->base, 0, r->size);
   int with_view = r->size >= ouro_control_bytes();
   atomic_store_explicit(&c->spec_version, OURO_SPEC_VERSION, memory_order_relaxed);
   atomic_store_explicit(&c->layout_version, OURO_LAYOUT_VERSION, memory_order_relaxed);
   atomic_store_explicit(&c->flags, with_view ? OURO_CONTROL_F_LEASE_VIEW : 0, memory_order_relaxed);
   atomic_store_explicit(&c->lease_view_slots, with_view ? OURO_LEASE_VIEW_SLOTS : 0,
                         memory_order_relaxed);
   atomic_store_explicit(&c->lease_view_off, with_view ? sizeof(ouro_control_t) : 0,
                         memory_order_relaxed);
   atomic_store_explicit(&c->slot_size, slot_size, memory_order_relaxed);
   atomic_store_explicit(&c->inline_budget, inline_budget, memory_order_relaxed);
   atomic_store_explicit(&c->queue_capacity, queue_capacity, memory_order_relaxed);
   atomic_store_explicit(&c->arena_size, arena_size, memory_order_relaxed);
   atomic_store_explicit(&c->host_epoch, 1, memory_order_relaxed);
   atomic_store_explicit(&c->host_heartbeat, 0, memory_order_relaxed);
   atomic_store_explicit(&c->magic, OURO_CONTROL_MAGIC, memory_order_release);
   return OURO_REGION_OK;
}

ouro_lease_view_t *ouro_control_lease_view_host(ouro_region_t *r)
{
   if (!r || !r->base || !r->writable || r->size < ouro_control_bytes())
      return NULL;
   return (ouro_lease_view_t *)((uint8_t *)r->base + sizeof(ouro_control_t));
}

void ouro_control_bump_epoch(ouro_control_t *c)
{
   if (c)
      atomic_fetch_add_explicit(&c->host_epoch, 1, memory_order_acq_rel);
}

void ouro_control_heartbeat(ouro_control_t *c, uint64_t now)
{
   if (c)
      atomic_store_explicit(&c->host_heartbeat, now, memory_order_release);
}

ouro_region_result_t ouro_qpair_init(ouro_region_t *r, uint32_t slot_size, uint32_t capacity)
{
   if (!r || !r->base || !r->writable)
      return OURO_REGION_ERR_ARG;
   size_t need = ouro_qpair_bytes(slot_size, capacity);
   if (need == 0)
      return OURO_REGION_ERR_GEOMETRY;
   if (r->size < need)
      return OURO_REGION_ERR_SIZE;

   uint8_t *b = (uint8_t *)r->base;
   ouro_qpair_hdr_t *h = (ouro_qpair_hdr_t *)b;
   memset(h, 0, sizeof *h);
   atomic_store_explicit(&h->slot_size, slot_size, memory_order_relaxed);
   atomic_store_explicit(&h->capacity, capacity, memory_order_relaxed);
   atomic_store_explicit(&h->data_credits, capacity, memory_order_relaxed);
   atomic_store_explicit(&h->control_credits, OURO_CONTROL_CREDITS_DEFAULT, memory_order_relaxed);
   atomic_store_explicit(&h->control_lost, 0, memory_order_relaxed);
   atomic_store_explicit(&h->inbound_off, (uint32_t)ouro_qpair_inbound_offset(),
                         memory_order_relaxed);
   atomic_store_explicit(&h->outbound_off, (uint32_t)ouro_qpair_outbound_offset(slot_size, capacity),
                         memory_order_relaxed);
   atomic_store_explicit(&h->client_heartbeat, 0, memory_order_relaxed);

   ouro_ring_t tmp;
   ouro_ring_result_t rr =
       ouro_ring_init(b + ouro_qpair_inbound_offset(), r->size - ouro_qpair_inbound_offset(),
                     slot_size, capacity, &tmp);
   if (rr != OURO_RING_OK)
      return OURO_REGION_ERR_GEOMETRY;
   size_t out_off = ouro_qpair_outbound_offset(slot_size, capacity);
   rr = ouro_ring_init(b + out_off, r->size - out_off, slot_size, capacity, &tmp);
   if (rr != OURO_RING_OK)
      return OURO_REGION_ERR_GEOMETRY;

   atomic_store_explicit(&h->magic, OURO_QPAIR_MAGIC, memory_order_release);
   return OURO_REGION_OK;
}

ouro_region_result_t ouro_arena_region_init(ouro_region_t *r, uint64_t arena_size)
{
   if (!r || !r->base || !r->writable)
      return OURO_REGION_ERR_ARG;
   size_t need = ouro_arena_region_bytes(arena_size);
   if (need == 0)
      return OURO_REGION_ERR_GEOMETRY;
   if (r->size < need)
      return OURO_REGION_ERR_SIZE;

   ouro_arena_hdr_t *h = (ouro_arena_hdr_t *)r->base;
   memset(h, 0, sizeof *h);
   atomic_store_explicit(&h->size, arena_size, memory_order_relaxed);
   atomic_store_explicit(&h->magic, OURO_ARENA_MAGIC, memory_order_release);
   return OURO_REGION_OK;
}
