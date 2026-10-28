/* ouro_capture.h: the ring capture stream and observational replay (D10).
 *
 * The capture stream is exactly the host's seq order: it is fed by the
 * governance/audit tap (D6/D9), so every seq-stamped event — including the
 * host-generated overflow and producer_reaped notices — is recorded once, in
 * order. From that one ordered stream the delivered stream is derivable (D6),
 * which is the property the later governance-and-capture tree depends on.
 *
 * The wire format is normative (D10), frozen by this slice's tests: a fixed file
 * header, then length-prefixed CRC-checked records carrying the verbatim frame
 * header and the materialized payload bytes. Payloads are materialized because
 * an arena region does not outlive the host; a reader uses the inline bytes and
 * must never dereference a captured payload_ref.
 *
 * Observational replay re-presents the ordered stream for inspection. Nothing is
 * re-executed, so it is exact by construction. Module replay (re-driving a module
 * against recorded inbound events) is a later tree, not this one.
 */
#ifndef OUROBOROS_CAPTURE_H
#define OUROBOROS_CAPTURE_H 1

#include <stddef.h>
#include <stdint.h>

#include <ouroboros/host.h> /* ouro_tap_fn, ouro_frame_t */

/* File header, 64 bytes. All integers little-endian. magic is the 8 ASCII bytes
 * "AIMEECAP": a frozen value of capture format 1, kept from before the ring had
 * its own name so that every capture written since remains readable. */
#define OURO_CAPTURE_MAGIC          "AIMEECAP"
#define OURO_CAPTURE_FORMAT_VERSION 1
#define OURO_CAPTURE_HEADER_LEN     64

/* Record types. */
typedef enum
{
   OURO_CAP_EVENT = 0,
   OURO_CAP_OVERFLOW = 1,
   OURO_CAP_PRODUCER_REAPED = 2,
   OURO_CAP_EPOCH_CHANGE = 3
} ouro_capture_record_type_t;

/* record_flags bit 0: the payload bytes were materialized inline. */
#define OURO_CAP_F_MATERIALIZED 0x0001u

/* A growable in-memory capture sink. The format on disk and in memory is
 * identical; ouro_capture_flush_fd moves it to a file. */
typedef struct
{
   uint8_t *buf;
   size_t len;
   size_t cap;
   uint64_t first_seq;
   uint64_t last_seq;
   int have_first;
   int header_written;
   uint32_t spec_version;
   uint32_t layout_version;
   uint64_t host_epoch;
   int broken; /* an allocation failed; the stream is abandoned */
} ouro_capture_t;

/* Terminal classification of a stream, decided from the bytes alone (D10). */
typedef enum
{
   OURO_CAPTURE_COMPLETE = 0, /* ended cleanly with an epoch_change */
   OURO_CAPTURE_OPEN,         /* valid capture of a still-running host */
   OURO_CAPTURE_TRUNCATED,    /* cut mid-write; last good seq/offset reported */
   OURO_CAPTURE_CORRUPT       /* a structural contradiction before the end */
} ouro_capture_status_t;

/* Result of reading a stream. */
typedef struct
{
   ouro_capture_status_t status;
   uint64_t records;      /* records successfully parsed */
   uint64_t last_good_seq;
   size_t offending_off;  /* byte offset of the record that failed, if any */
   int rule;              /* which classification rule fired (for diagnostics) */
} ouro_capture_report_t;

/* One replayed record handed to a callback. payload points into the stream and
 * is valid for the duration of the callback. */
typedef struct
{
   ouro_capture_record_type_t type;
   ouro_frame_t frame;
   const uint8_t *payload;
   uint32_t payload_len;
} ouro_capture_event_t;

typedef void (*ouro_capture_cb)(void *ctx, const ouro_capture_event_t *ev);

/* ---- writing ---- */

/* Initialise a sink. The spec/layout/epoch go in the file header. */
void ouro_capture_init(ouro_capture_t *c, uint32_t spec_version, uint32_t layout_version,
                      uint64_t host_epoch);

/* The tap function: register with ouro_host_set_tap(h, ouro_capture_tap, c). It
 * classifies overflow/producer_reaped/epoch_change by event_kind and records
 * everything else as an event. */
void ouro_capture_tap(void *ctx, const ouro_frame_t *frame, const uint8_t *payload,
                     uint32_t payload_len);

/* Free the sink's buffer. */
void ouro_capture_free(ouro_capture_t *c);

/* ---- writing to a file ---- */

/* What ouro_capture_flush_fd did with the sink's pending bytes. */
typedef enum
{
   OURO_CAPTURE_FLUSHED = 0,     /* every pending byte reached fd (or none was pending) */
   OURO_CAPTURE_FLUSH_BROKEN,    /* the sink was already broken; nothing was written */
   OURO_CAPTURE_FLUSH_LIMIT,     /* writing would pass the byte limit; nothing was written */
   OURO_CAPTURE_FLUSH_WRITE_FAILED /* a write failed or wrote nothing; the file is cut short */
} ouro_capture_flush_t;

/* Append the sink's pending bytes to fd, then empty the sink without forgetting
 * that the header was written, so one file stays a single seq-contiguous stream
 * across any number of flushes. *written is the file's running byte count: it is
 * advanced by what was written and checked against limit (0 = no limit) before
 * anything is. On LIMIT or WRITE_FAILED the sink is marked broken and emptied, so
 * the tap stops appending to a stream that can no longer be persisted; the file
 * then reads as truncated, never corrupt. The caller owns fd and decides what a
 * lost stream means. */
ouro_capture_flush_t ouro_capture_flush_fd(ouro_capture_t *c, int fd, uint64_t *written,
                                          uint64_t limit);

const char *ouro_capture_flush_name(ouro_capture_flush_t r);

/* ---- reading / replay ---- */

/* Classify a byte stream and, if not corrupt/truncated before it, replay each
 * record to cb in order. cb may be NULL to classify without replaying. */
ouro_capture_report_t ouro_capture_read(const uint8_t *buf, size_t len, ouro_capture_cb cb,
                                      void *ctx);

const char *ouro_capture_status_name(ouro_capture_status_t s);

#endif /* OUROBOROS_CAPTURE_H */
