/* test_local_attach.c: attaching a client to a host in the same process.
 *
 *   - A local attach is an ordinary attach: the client is admitted, gets its
 *     rings, and carries traffic both ways through the host.
 *   - The unowned mode goes through admission, so a host that refuses gets a
 *     denied client and no admitted slot.
 *   - The owned mode is the daemon's same-process seam and bypasses the
 *     admission callback, so the same refusing host admits it.
 *   - Neither mode leaves a descriptor behind.
 */
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ouroboros/local_attach.h>

static void must(int cond, const char *what)
{
   if (!cond)
   {
      fprintf(stderr, "FAIL: %s\n", what);
      abort();
   }
}

static int open_fds(void)
{
   DIR *d = opendir("/proc/self/fd");
   if (!d)
      return -1;
   int n = 0;
   while (readdir(d))
      n++;
   closedir(d);
   return n;
}

static ouro_attach_status_t refuse_all(void *ctx, int attach_fd, const ouro_attach_request_t *req)
{
   (void)ctx;
   (void)attach_fd;
   (void)req;
   return OURO_ATTACH_DENIED_POLICY;
}

static void host_config(ouro_host_config_t *cfg)
{
   memset(cfg, 0, sizeof *cfg);
   cfg->max_slots = 4;
   cfg->slot_size = 256;
   cfg->inline_budget = 192;
   cfg->queue_capacity = 16;
   cfg->arena_size = 64 * 1024;
}

static void test_traffic(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, NULL, NULL) == OURO_HOST_OK, "host");
   ouro_client_t pub, sub;
   must(ouro_host_attach_local(&h, &pub, 0) == OURO_CLIENT_OK, "publisher attached");
   must(ouro_host_attach_local(&h, &sub, 0) == OURO_CLIENT_OK, "subscriber attached");
   must(ouro_host_admitted(&h) == 2, "both admitted");
   must(ouro_host_subscribe(&h, sub.reply.handle_id, 900) == OURO_HOST_OK, "subscribe");
   must(ouro_client_publish(&pub, 900, "ping", 4) == OURO_CLIENT_OK, "publish");
   must(ouro_host_pump(&h) == 1, "routed");
   ouro_event_t ev;
   must(ouro_client_poll(&sub, &ev) == OURO_CLIENT_OK, "delivered");
   must(ev.frame.event_kind == 900 && ev.frame.payload_len == 4, "the event that was sent");
   ouro_client_detach(&pub);
   ouro_client_detach(&sub);
   ouro_host_destroy(&h);
   printf("  traffic: two local clients exchange an event through the host\n");
}

static void test_admission(void)
{
   ouro_host_config_t cfg;
   host_config(&cfg);
   ouro_host_t h;
   must(ouro_host_create(&h, &cfg, refuse_all, NULL) == OURO_HOST_OK, "host");
   int before = open_fds();

   ouro_client_t c;
   must(ouro_host_attach_local(&h, &c, 0) == OURO_CLIENT_DENIED, "unowned attach is refused");
   must(c.attach_status == OURO_ATTACH_DENIED_POLICY, "with the policy reason");
   must(ouro_host_admitted(&h) == 0, "and admits nothing");
   must(open_fds() == before, "a refused attach leaves no descriptor");

   must(ouro_host_attach_local(&h, &c, 1) == OURO_CLIENT_OK, "owned attach is admitted");
   must(ouro_host_admitted(&h) == 1, "one slot");
   ouro_client_detach(&c);
   ouro_host_destroy(&h);
   printf("  admission: unowned is refused by policy, owned uses the same-process seam\n");
}

static void test_bad_arguments(void)
{
   ouro_client_t c;
   must(ouro_host_attach_local(NULL, &c, 0) == OURO_CLIENT_ERR_OS, "no host");
   printf("  arguments: a missing host is an error, not a crash\n");
}

int main(void)
{
   printf("test_local_attach:\n");
   test_traffic();
   test_admission();
   test_bad_arguments();
   printf("test_local_attach: OK\n");
   return 0;
}
