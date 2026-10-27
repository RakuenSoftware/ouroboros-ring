/* ouro_region.c: client-safe mapping and validation. See ouro_region.h. */
#include <sys/mman.h>
#include <sys/stat.h>

#include <ouroboros/region.h>

#include "region_internal.h"

ouro_region_result_t ouro_region_map(int fd, size_t size, int writable, ouro_region_t *out)
{
   if (!out || fd < 0 || size == 0)
      return OURO_REGION_ERR_ARG;

   /* mmap will happily map past the end of the backing memfd; touching those
    * pages then raises SIGBUS, and every later bounds check that trusts r->size
    * would have approved a header whose backing object is shorter. So the
    * mapping may not exceed the fd's actual length — verified before mmap, not
    * discovered by a fault later. */
   struct stat st;
   if (fstat(fd, &st) != 0)
      return OURO_REGION_ERR_OS;
   if (st.st_size < 0 || size > (size_t)st.st_size)
      return OURO_REGION_ERR_SIZE;

   int prot = writable ? (PROT_READ | PROT_WRITE) : PROT_READ;
   void *base = mmap(NULL, size, prot, MAP_SHARED, fd, 0);
   if (base == MAP_FAILED)
      return OURO_REGION_ERR_OS;

   out->fd = fd;
   out->base = base;
   out->size = size;
   out->writable = writable ? 1 : 0;
   return OURO_REGION_OK;
}

void ouro_region_unmap(ouro_region_t *r)
{
   if (r && r->base)
   {
      munmap(r->base, r->size);
      r->base = NULL;
   }
}

/* ------------------------------------------------------------------ */
/* control region                                                      */

size_t ouro_control_bytes(void)
{
   return sizeof(ouro_control_t) + (size_t)OURO_LEASE_VIEW_SLOTS * sizeof(ouro_lease_view_t);
}

size_t ouro_control_header_bytes(void)
{
   return sizeof(ouro_control_t);
}

ouro_region_result_t ouro_control_attach(const ouro_region_t *r, ouro_control_t **out)
{
   if (!r || !r->base || !out)
      return OURO_REGION_ERR_ARG;
   if (r->size < sizeof(ouro_control_t))
      return OURO_REGION_ERR_SIZE;

   ouro_control_t *c = (ouro_control_t *)r->base;
   if (atomic_load_explicit(&c->magic, memory_order_acquire) != OURO_CONTROL_MAGIC)
      return OURO_REGION_ERR_MAGIC;
   if (atomic_load_explicit(&c->spec_version, memory_order_acquire) != OURO_SPEC_VERSION ||
       atomic_load_explicit(&c->layout_version, memory_order_acquire) != OURO_LAYOUT_VERSION)
      return OURO_REGION_ERR_VERSION;

   /* The D4 parameters are a claim; a client will size mappings against them, so
    * a header describing nonsense must be refused rather than propagated. Every
    * value a mapping or a ring walk is later sized on is validated here,
    * including queue_capacity (which sizes the queue-pair rings) — an omission
    * the first review caught. */
   uint32_t slot = atomic_load_explicit(&c->slot_size, memory_order_acquire);
   uint32_t inl = atomic_load_explicit(&c->inline_budget, memory_order_acquire);
   uint32_t qcap = atomic_load_explicit(&c->queue_capacity, memory_order_acquire);
   uint64_t arena = atomic_load_explicit(&c->arena_size, memory_order_acquire);
   if (slot == 0 || inl > slot || arena == 0 || arena > OURO_ARENA_MAX_SIZE)
      return OURO_REGION_ERR_GEOMETRY;
   if (ouro_qpair_bytes(slot, qcap) == 0)
      return OURO_REGION_ERR_GEOMETRY;
   /* An advertised lease view must be the one this build reads: right after the
    * header, one entry per lease-table slot. */
   if ((atomic_load_explicit(&c->flags, memory_order_acquire) & OURO_CONTROL_F_LEASE_VIEW) &&
       (atomic_load_explicit(&c->lease_view_slots, memory_order_acquire) != OURO_LEASE_VIEW_SLOTS ||
        atomic_load_explicit(&c->lease_view_off, memory_order_acquire) != sizeof(ouro_control_t)))
      return OURO_REGION_ERR_GEOMETRY;

   *out = c;
   return OURO_REGION_OK;
}

ouro_region_result_t ouro_control_lease_view(const ouro_region_t *r, const ouro_control_t *c,
                                           const ouro_lease_view_t **out)
{
   if (!r || !r->base || !c || !out)
      return OURO_REGION_ERR_ARG;
   if (!(atomic_load_explicit(&c->flags, memory_order_acquire) & OURO_CONTROL_F_LEASE_VIEW))
      return OURO_REGION_ERR_VERSION;
   if (r->size < ouro_control_bytes())
      return OURO_REGION_ERR_SIZE;
   *out = (const ouro_lease_view_t *)((const uint8_t *)r->base + sizeof(ouro_control_t));
   return OURO_REGION_OK;
}

ouro_region_result_t ouro_lease_view_resolve(const ouro_lease_view_t *view, uint32_t lease_id,
                                           uint32_t generation, uint32_t payload_len,
                                           uint64_t arena_size, uint64_t *offset)
{
   if (!view || !offset || generation == 0 || payload_len == 0)
      return OURO_REGION_ERR_ARG;
   if (lease_id >= OURO_LEASE_VIEW_SLOTS)
      return OURO_REGION_ERR_GEOMETRY;
   const ouro_lease_view_t *e = &view[lease_id];
   /* Acquire pairs with the host's release store of generation, which it makes
    * after offset and len; the frame naming this lease was published after that. */
   if (atomic_load_explicit(&e->generation, memory_order_acquire) != generation)
      return OURO_REGION_ERR_GEOMETRY;
   /* Seqlock read (writer: view_publish). Acquire on the span pairs with its
    * release stores, so a span written for a later lease makes the re-read
    * below see that lease's generation, or 0, and fail. */
   uint64_t off = atomic_load_explicit(&e->offset, memory_order_acquire);
   uint32_t len = atomic_load_explicit(&e->len, memory_order_acquire);
   if (atomic_load_explicit(&e->generation, memory_order_relaxed) != generation)
      return OURO_REGION_ERR_GEOMETRY;
   if (payload_len > len || off > arena_size || arena_size - off < payload_len)
      return OURO_REGION_ERR_GEOMETRY;
   *offset = off;
   return OURO_REGION_OK;
}

uint64_t ouro_control_epoch(const ouro_control_t *c)
{
   return c ? atomic_load_explicit(&c->host_epoch, memory_order_acquire) : 0;
}

int ouro_control_epoch_changed(const ouro_control_t *c, uint64_t attached_epoch)
{
   return c && atomic_load_explicit(&c->host_epoch, memory_order_acquire) != attached_epoch;
}

/* ------------------------------------------------------------------ */
/* queue-pair region                                                   */

size_t ouro_qpair_bytes(uint32_t slot_size, uint32_t capacity)
{
   size_t ring = ouro_ring_bytes(slot_size, capacity);
   if (ring == 0)
      return 0;
   size_t out_off = ouro_qpair_outbound_offset(slot_size, capacity);
   if (out_off == 0)
      return 0;
   return out_off + ring;
}

ouro_region_result_t ouro_qpair_attach(const ouro_region_t *r, ouro_qpair_t *out)
{
   if (!r || !r->base || !out)
      return OURO_REGION_ERR_ARG;
   if (r->size < sizeof(ouro_qpair_hdr_t))
      return OURO_REGION_ERR_SIZE;

   ouro_qpair_hdr_t *h = (ouro_qpair_hdr_t *)r->base;
   if (atomic_load_explicit(&h->magic, memory_order_acquire) != OURO_QPAIR_MAGIC)
      return OURO_REGION_ERR_MAGIC;

   uint32_t slot = atomic_load_explicit(&h->slot_size, memory_order_acquire);
   uint32_t cap = atomic_load_explicit(&h->capacity, memory_order_acquire);
   uint32_t in_off = atomic_load_explicit(&h->inbound_off, memory_order_acquire);
   uint32_t out_off = atomic_load_explicit(&h->outbound_off, memory_order_acquire);

   /* Everything is a claim. The offsets and geometry must place two whole rings
    * inside the mapping, or a later hot-path access walks off the end. */
   size_t need = ouro_qpair_bytes(slot, cap);
   if (need == 0 || r->size < need)
      return OURO_REGION_ERR_GEOMETRY;
   if (in_off != ouro_qpair_inbound_offset() || out_off != ouro_qpair_outbound_offset(slot, cap))
      return OURO_REGION_ERR_GEOMETRY;

   uint8_t *b = (uint8_t *)r->base;
   ouro_ring_result_t rr;
   rr = ouro_ring_attach(b + in_off, r->size - in_off, &out->inbound);
   if (rr != OURO_RING_OK)
      return OURO_REGION_ERR_GEOMETRY;
   rr = ouro_ring_attach(b + out_off, r->size - out_off, &out->outbound);
   if (rr != OURO_RING_OK)
      return OURO_REGION_ERR_GEOMETRY;

   out->hdr = h;
   return OURO_REGION_OK;
}

/* ------------------------------------------------------------------ */
/* arena region                                                        */

size_t ouro_arena_region_bytes(uint64_t arena_size)
{
   if (arena_size == 0 || arena_size > OURO_ARENA_MAX_SIZE)
      return 0;
   return sizeof(ouro_arena_hdr_t) + (size_t)arena_size;
}

ouro_region_result_t ouro_arena_region_attach(const ouro_region_t *r, uint8_t **base, uint64_t *size)
{
   if (!r || !r->base || !base || !size)
      return OURO_REGION_ERR_ARG;
   if (r->size < sizeof(ouro_arena_hdr_t))
      return OURO_REGION_ERR_SIZE;

   ouro_arena_hdr_t *h = (ouro_arena_hdr_t *)r->base;
   if (atomic_load_explicit(&h->magic, memory_order_acquire) != OURO_ARENA_MAGIC)
      return OURO_REGION_ERR_MAGIC;

   uint64_t arena = atomic_load_explicit(&h->size, memory_order_acquire);
   if (arena == 0 || arena > OURO_ARENA_MAX_SIZE)
      return OURO_REGION_ERR_GEOMETRY;
   if (r->size < sizeof(ouro_arena_hdr_t) + arena)
      return OURO_REGION_ERR_SIZE;

   *base = (uint8_t *)r->base + sizeof(ouro_arena_hdr_t);
   *size = arena;
   return OURO_REGION_OK;
}

const char *ouro_region_result_name(ouro_region_result_t r)
{
   switch (r)
   {
   case OURO_REGION_OK:
      return "OK";
   case OURO_REGION_ERR_ARG:
      return "ERR_ARG";
   case OURO_REGION_ERR_OS:
      return "ERR_OS";
   case OURO_REGION_ERR_SIZE:
      return "ERR_SIZE";
   case OURO_REGION_ERR_MAGIC:
      return "ERR_MAGIC";
   case OURO_REGION_ERR_VERSION:
      return "ERR_VERSION";
   case OURO_REGION_ERR_GEOMETRY:
      return "ERR_GEOMETRY";
   case OURO_REGION_ERR_EPOCH:
      return "ERR_EPOCH";
   default:
      return "ERR_UNKNOWN";
   }
}
