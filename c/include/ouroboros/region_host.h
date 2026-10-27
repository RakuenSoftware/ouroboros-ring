#ifndef OUROBOROS_REGION_HOST_H
#define OUROBOROS_REGION_HOST_H 1

#include <ouroboros/region.h>

/* Host-only region lifecycle. External modules link the client archive, which
 * deliberately does not provide these symbols. */
ouro_region_result_t ouro_region_create(const char *name, size_t size, ouro_region_t *out);
ouro_region_result_t ouro_control_init(ouro_region_t *r, uint32_t slot_size, uint32_t inline_budget,
                                     uint32_t queue_capacity, uint64_t arena_size);
void ouro_control_bump_epoch(ouro_control_t *c);
void ouro_control_heartbeat(ouro_control_t *c, uint64_t now);
ouro_region_result_t ouro_qpair_init(ouro_region_t *r, uint32_t slot_size, uint32_t capacity);
ouro_region_result_t ouro_arena_region_init(ouro_region_t *r, uint64_t arena_size);
/* The host's writable view of the lease table in its own control mapping, or
 * NULL if the region was not created with ouro_control_bytes(). */
ouro_lease_view_t *ouro_control_lease_view_host(ouro_region_t *r);

#endif
