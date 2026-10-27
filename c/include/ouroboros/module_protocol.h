/* Versioned payload envelope for core <-> process-module calls.
 *
 * The ring frame carries routing, correlation, and cancellation.  This
 * envelope describes the operation inside each inline payload. Bodies larger
 * than the negotiated inline budget use ordered OURO_F_MORE fragments under one
 * correlation. It deliberately
 * contains no pointers, native structs, or host-endian fields, so C, Go, and
 * future module SDKs can implement the same contract independently.
 */
#ifndef OUROBOROS_MODULE_PROTOCOL_H
#define OUROBOROS_MODULE_PROTOCOL_H 1

#include <stddef.h>
#include <stdint.h>

#define OURO_MODULE_MESSAGE_MAGIC 0x444f4d41u /* "AMOD", little-endian */
#define OURO_MODULE_MESSAGE_VERSION 1u
#define OURO_MODULE_MESSAGE_HEADER_LEN 40u
#define OURO_MODULE_MESSAGE_MAX_BODY (16u * 1024u * 1024u)

typedef enum
{
   OURO_MODULE_OP_INVOKE = 1,
   OURO_MODULE_OP_RESULT = 2
} ouro_module_operation_t;

typedef enum
{
   OURO_MODULE_STATUS_OK = 0,
   OURO_MODULE_STATUS_CAPABILITY_ABSENT = 1,
   OURO_MODULE_STATUS_CANCELLED = 2,
   OURO_MODULE_STATUS_DEADLINE_EXCEEDED = 3,
   OURO_MODULE_STATUS_INVALID_REQUEST = 4,
   OURO_MODULE_STATUS_INTERNAL = 5
} ouro_module_status_t;

typedef enum
{
   OURO_MODULE_MESSAGE_OK = 0,
   OURO_MODULE_MESSAGE_ERR_SHORT,
   OURO_MODULE_MESSAGE_ERR_MAGIC,
   OURO_MODULE_MESSAGE_ERR_VERSION,
   OURO_MODULE_MESSAGE_ERR_HEADER,
   OURO_MODULE_MESSAGE_ERR_OPERATION,
   OURO_MODULE_MESSAGE_ERR_STATUS,
   OURO_MODULE_MESSAGE_ERR_STAGE,
   OURO_MODULE_MESSAGE_ERR_FLAGS,
   OURO_MODULE_MESSAGE_ERR_BODY
} ouro_module_message_result_t;

typedef struct
{
   uint16_t operation;
   uint16_t status;
   uint32_t stage_id;
   uint32_t flags;
   uint32_t body_len;
   uint64_t deadline_ns; /* absolute CLOCK_MONOTONIC time; zero means none */
   uint64_t trace_id;
} ouro_module_message_t;

/* Encode/decode the fixed header. The body immediately follows the returned
 * header bytes and is owned by the caller/event frame. Decode validates that
 * the complete declared body is present in input_len. */
size_t ouro_module_message_encode(const ouro_module_message_t *message, uint8_t *output,
                                   size_t output_len);
ouro_module_message_result_t ouro_module_message_decode(const uint8_t *input,
                                                          size_t input_len,
                                                          ouro_module_message_t *message);

/* Deadlines and cancellation are core transport concerns. A runtime checks an
 * invoke before dispatch and uses the ring frame's F_CANCEL for cancellation. */
int ouro_module_deadline_expired(uint64_t deadline_ns, uint64_t now_ns);
const char *ouro_module_message_result_name(ouro_module_message_result_t result);

#endif
