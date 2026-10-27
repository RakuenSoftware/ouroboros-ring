#ifndef OUROBOROS_REGION_INTERNAL_H
#define OUROBOROS_REGION_INTERNAL_H 1

#include <stdatomic.h>

#include <ouroboros/region.h>

#define OURO_ARENA_MAX_SIZE (1ull << 32) /* 4 GiB */

typedef struct
{
   _Atomic uint32_t magic;
   _Atomic uint32_t reserved;
   _Atomic uint64_t size;
} ouro_arena_hdr_t;

_Static_assert(sizeof(ouro_arena_hdr_t) == 16, "arena header size is frozen");

static inline size_t ouro_region_align64(size_t n)
{
   return (n + 63) & ~((size_t)63);
}

static inline size_t ouro_qpair_inbound_offset(void)
{
   return ouro_region_align64(sizeof(ouro_qpair_hdr_t));
}

static inline size_t ouro_qpair_outbound_offset(uint32_t slot_size, uint32_t capacity)
{
   size_t ring = ouro_ring_bytes(slot_size, capacity);
   if (ring == 0)
      return 0;
   return ouro_region_align64(ouro_qpair_inbound_offset() + ring);
}

#endif
