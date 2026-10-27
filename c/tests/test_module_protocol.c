#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ouroboros/module_protocol.h>

static void must(int condition, const char *message)
{
   if (!condition)
   {
      fprintf(stderr, "FAIL: %s\n", message);
      abort();
   }
}

int main(void)
{
   ouro_module_message_t input = {.operation = OURO_MODULE_OP_INVOKE,
                                   .stage_id = 0x1701,
                                   .body_len = 3,
                                   .deadline_ns = 900,
                                   .trace_id = 42};
   uint8_t bytes[OURO_MODULE_MESSAGE_HEADER_LEN + 3];
   memset(bytes, 0, sizeof bytes);
   must(ouro_module_message_encode(&input, bytes, sizeof bytes) == OURO_MODULE_MESSAGE_HEADER_LEN,
        "encode invoke");
   memcpy(bytes + OURO_MODULE_MESSAGE_HEADER_LEN, "abc", 3);

   ouro_module_message_t output;
   memset(&output, 0xa5, sizeof output);
   must(ouro_module_message_decode(bytes, sizeof bytes, &output) == OURO_MODULE_MESSAGE_OK,
        "decode invoke");
   must(output.operation == input.operation && output.stage_id == input.stage_id &&
            output.body_len == input.body_len && output.deadline_ns == input.deadline_ns &&
            output.trace_id == input.trace_id,
        "round trip fields");
   must(!ouro_module_deadline_expired(900, 899), "deadline before boundary");
   must(ouro_module_deadline_expired(900, 900), "deadline at boundary");
   must(!ouro_module_deadline_expired(0, UINT64_MAX), "zero deadline disabled");

   ouro_module_message_t untouched;
   memset(&untouched, 0x5a, sizeof untouched);
   ouro_module_message_t snapshot = untouched;
   bytes[20] = 4;
   must(ouro_module_message_decode(bytes, sizeof bytes, &untouched) ==
            OURO_MODULE_MESSAGE_ERR_BODY,
        "reject truncated body");
   must(memcmp(&untouched, &snapshot, sizeof untouched) == 0,
        "failed decode leaves output untouched");

   input.operation = OURO_MODULE_OP_RESULT;
   input.status = OURO_MODULE_STATUS_CAPABILITY_ABSENT;
   input.body_len = 0;
   must(ouro_module_message_encode(&input, bytes, sizeof bytes) == OURO_MODULE_MESSAGE_HEADER_LEN,
        "encode typed capability absence");
   puts("test_module_protocol: ok");
   return 0;
}
