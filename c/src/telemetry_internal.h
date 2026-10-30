/* telemetry_internal.h: the host's D16 producer, called by route.c and host.c. */
#ifndef OUROBOROS_TELEMETRY_INTERNAL_H
#define OUROBOROS_TELEMETRY_INTERNAL_H 1

#include <ouroboros/host.h>

/* Write the record for a just-stamped event. Never blocks; outside a pump it
 * also signals the consumer. Inline so a host without telemetry pays a branch. */
void ouro_telemetry_host_write(ouro_host_t *h, const ouro_frame_t *f);
static inline void ouro_telemetry_stamped(ouro_host_t *h, const ouro_frame_t *f)
{
   if (h->telemetry)
      ouro_telemetry_host_write(h, f);
}

/* Wake the consumer if it sleeps and something was written since it was last
 * signalled. The pump calls it once on the way out. */
void ouro_telemetry_host_signal(ouro_host_t *h);

void ouro_telemetry_host_destroy(ouro_host_t *h);

#endif
