/* ouro_telemetry.h: tap telemetry handed off the host as each event is stamped (D16).
 *
 * When the host stamps an event's seq it writes one 128-byte, content-free record
 * into a single-producer, single-consumer ring in its own region, before the tap
 * callback and before routing. One telemetry process, granted by the embedding,
 * holds the consumer end and exports the records off the host. The host never
 * blocks on it: a full ring counts the record lost and the next record that fits
 * is preceded by a gap record carrying the count.
 *
 * This header is the record and region contract plus the consumer API. The host
 * side is in host.h (ouro_host_telemetry_enable, ouro_host_grant_telemetry).
 */
#ifndef OUROBOROS_TELEMETRY_H
#define OUROBOROS_TELEMETRY_H 1

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include <ouroboros/client.h>
#include <ouroboros/region.h>
#include <ouroboros/ring.h>
#include <ouroboros/wire.h>

#define OURO_TELEMETRY_MAGIC   0x594d4c54u /* "TLMY" */
#define OURO_TELEMETRY_VERSION 1

/* The record (D16). All integers little-endian. */
#define OURO_TELEMETRY_RECORD_LEN       128
#define OURO_TELEMETRY_OFF_TYPE         0
#define OURO_TELEMETRY_OFF_FLAGS        4
#define OURO_TELEMETRY_OFF_REALTIME_NS  8
#define OURO_TELEMETRY_OFF_LOST         16
#define OURO_TELEMETRY_OFF_RESERVED     24
#define OURO_TELEMETRY_OFF_FRAME        32 /* OURO_WIRE_HDR_LEN bytes, wire encoding */
#define OURO_TELEMETRY_OFF_RESERVED_TAIL 96
#define OURO_TELEMETRY_RECORD_EVENT     0u
#define OURO_TELEMETRY_RECORD_GAP       1u

/* Ring capacity a host uses when the embedding passes 0: 8 MiB of records. */
#define OURO_TELEMETRY_CAPACITY_DEFAULT 65536u

/* Region offsets. The ring starts on its own cache line after the header, and
 * consumer_waiting sits alone on the line between, so neither end's writes
 * invalidate the line the other end writes. */
#define OURO_TELEMETRY_REGION_OFF_WAITING 64
#define OURO_TELEMETRY_REGION_OFF_RING    128

typedef struct
{
   _Atomic uint32_t magic; /* release-stored last; publishes the header */
   _Atomic uint32_t version;
   _Atomic uint32_t record_size;
   _Atomic uint32_t flags;
   _Atomic uint64_t host_epoch;
   _Atomic uint64_t dropped; /* host-written: every record ever lost */
   _Atomic uint64_t reserved[4];
   _Alignas(64) _Atomic uint32_t consumer_waiting; /* consumer-written (D13) */
} ouro_telemetry_region_t;

_Static_assert(offsetof(ouro_telemetry_region_t, dropped) == 24, "dropped offset is in D16");
_Static_assert(offsetof(ouro_telemetry_region_t, consumer_waiting) ==
                   OURO_TELEMETRY_REGION_OFF_WAITING,
               "consumer_waiting offset is in D16");
_Static_assert(sizeof(ouro_telemetry_region_t) <= OURO_TELEMETRY_REGION_OFF_RING,
               "the ring follows the header");

/* Bytes of a telemetry region holding `capacity` records; 0 if invalid. */
size_t ouro_telemetry_region_bytes(uint32_t capacity);

/* A decoded record. For a gap, frame is all zero and lost is the count. */
typedef struct
{
   uint32_t type;
   uint64_t realtime_ns;
   uint64_t lost;
   ouro_frame_t frame;
} ouro_telemetry_record_t;

typedef enum
{
   OURO_TELEMETRY_OK = 0,
   OURO_TELEMETRY_ERR_ARG,
   OURO_TELEMETRY_ERR_LAYOUT, /* region or record disagrees with D16 */
   OURO_TELEMETRY_ERR_OS
} ouro_telemetry_result_t;

/* Encode a record into exactly OURO_TELEMETRY_RECORD_LEN bytes. */
void ouro_telemetry_encode(uint8_t *out, uint32_t type, uint64_t realtime_ns, uint64_t lost,
                           const ouro_frame_t *frame);
ouro_telemetry_result_t ouro_telemetry_decode(const uint8_t *in, ouro_telemetry_record_t *out);

/* The consumer end, held by the one telemetry process. */
typedef struct
{
   ouro_region_t region;
   ouro_telemetry_region_t *hdr;
   ouro_ring_t ring;
   int doorbell_fd;
} ouro_telemetry_reader_t;

/* Attach as a client that also asks for the telemetry grant (D16). On success
 * the client is attached as usual, and *granted says whether `reader` holds the
 * consumer end; a host that did not grant it leaves the client attached and
 * reader unmapped. */
ouro_client_result_t ouro_client_attach_telemetry(int sock, ouro_client_t *c,
                                                  uint32_t principal_class,
                                                  uint32_t principal_ref,
                                                  ouro_telemetry_reader_t *reader, int *granted);

/* Adopt a region from its descriptors (both are taken over). */
ouro_telemetry_result_t ouro_telemetry_reader_open(int region_fd, int doorbell_fd,
                                                   ouro_telemetry_reader_t *r);
void ouro_telemetry_reader_close(ouro_telemetry_reader_t *r);

/* Records written and not yet released. */
uint64_t ouro_telemetry_available(const ouro_telemetry_reader_t *r);
/* The i-th unreleased record, i < ouro_telemetry_available. */
const uint8_t *ouro_telemetry_peek(const ouro_telemetry_reader_t *r, uint64_t i);
/* Release the oldest n records once they have been exported. */
void ouro_telemetry_release(ouro_telemetry_reader_t *r, uint64_t n);
/* Total records the host has lost since the region was created. */
uint64_t ouro_telemetry_dropped(const ouro_telemetry_reader_t *r);

/* Sleep until a record is available, `extra_fd` (may be -1, e.g. the client's
 * host pidfd) is readable, or timeout_ms passes (negative: no timeout). Returns
 * 1 when records are available, 0 on timeout or extra_fd, -1 on error. */
int ouro_telemetry_wait(ouro_telemetry_reader_t *r, int extra_fd, int timeout_ms);

#endif /* OUROBOROS_TELEMETRY_H */
