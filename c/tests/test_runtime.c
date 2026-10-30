#define _GNU_SOURCE
#include <ouroboros/client.h>
#include <ouroboros/endpoint.h>
#include <ouroboros/host.h>
#include <ouroboros/runtime.h>
#include <ouroboros/telemetry.h>

#include <assert.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "support/tmpdir.h"

#define TEST_KIND          4100U
#define TEST_SERVER_KIND   4101U
#define TEST_SERVER_KIND_2 4102U

static ouro_client_result_t attach(const char *path, uint32_t principal_ref, ouro_client_t *client)
{
   int fd = -1;
   assert(ouro_endpoint_connect(path, &fd) == 0);
   ouro_client_result_t result = ouro_client_attach_as(fd, client, 1, principal_ref);
   assert(ouro_endpoint_close(&fd) == 0);
   return result;
}

/* D16: a grant file says which principal is the telemetry exporter. Only that
 * principal is handed the consumer end, and only it while it lives. */
static void test_telemetry_grant(const char *directory, const char *executable)
{
   char policy[PATH_MAX], grant_path[PATH_MAX], bad_path[PATH_MAX], sock[PATH_MAX];
   assert(snprintf(policy, sizeof policy, "%s/tpolicy", directory) > 0);
   assert(snprintf(grant_path, sizeof grant_path, "%s/exporter.grant", policy) > 0);
   assert(snprintf(sock, sizeof sock, "%s/telemetry.sock", directory) > 0);
   assert(mkdir(policy, 0700) == 0);
   FILE *f = fopen(grant_path, "w");
   assert(f != NULL);
   assert(fprintf(f, "version=1\nprincipal_class=1\nprincipal_ref=11\nuid=self\nexecutable=%s\n"
                     "telemetry=yes\n",
                  executable) > 0);
   assert(fclose(f) == 0);
   ouro_runtime_policy_t *parsed = NULL;
   assert(ouro_runtime_policy_load_dir(policy, &parsed) == 0);
   size_t count = 0;
   const ouro_runtime_grant_t *loaded = ouro_runtime_policy_grants(parsed, &count);
   assert(count == 1 && loaded[0].telemetry == 1);
   ouro_runtime_policy_free(&parsed);

   /* Any other value is a malformed policy, not a quiet "no". */
   assert(snprintf(bad_path, sizeof bad_path, "%s/bad.grant", policy) > 0);
   f = fopen(bad_path, "w");
   assert(f != NULL);
   assert(fprintf(f, "version=1\nprincipal_class=1\nprincipal_ref=12\nuid=self\nexecutable=%s\n"
                     "telemetry=maybe\n",
                  executable) > 0);
   assert(fclose(f) == 0);
   assert(ouro_runtime_policy_load_dir(policy, &parsed) != 0);
   assert(unlink(bad_path) == 0);

   ouro_runtime_grant_t grants[] = {
       {.principal_class = 1, .principal_ref = 11, .uid = OURO_RUNTIME_SELF_UID,
        .executable = executable, .telemetry = 1},
       {.principal_class = 1, .principal_ref = 12, .uid = OURO_RUNTIME_SELF_UID,
        .executable = executable}};
   ouro_host_config_t host_config = {.max_slots = 8, .slot_size = 512, .inline_budget = 400,
                                    .queue_capacity = 8, .arena_size = 16384};
   ouro_host_t host;
   assert(ouro_host_create(&host, &host_config, NULL, NULL) == OURO_HOST_OK);
   assert(ouro_host_telemetry_enable(&host, 64) == OURO_HOST_OK);
   pthread_mutex_t host_lock = PTHREAD_MUTEX_INITIALIZER;
   ouro_runtime_config_t config = {.socket_path = sock, .socket_mode = 0600, .backlog = 8,
                                   .stale_after_ns = 60000000000ULL, .grants = grants,
                                   .grant_count = 2};
   ouro_runtime_t *runtime = ouro_runtime_start(&host, &host_lock, &config);
   assert(runtime != NULL);

   ouro_client_t other, exporter;
   ouro_telemetry_reader_t reader;
   int granted = 1, fd = -1;
   assert(ouro_endpoint_connect(sock, &fd) == 0);
   assert(ouro_client_attach_telemetry(fd, &other, 1, 12, &reader, &granted) == OURO_CLIENT_OK);
   assert(ouro_endpoint_close(&fd) == 0);
   assert(!granted);
   assert(ouro_endpoint_connect(sock, &fd) == 0);
   assert(ouro_client_attach_telemetry(fd, &exporter, 1, 11, &reader, &granted) == OURO_CLIENT_OK);
   assert(ouro_endpoint_close(&fd) == 0);
   assert(granted && reader.hdr != NULL);

   ouro_telemetry_reader_close(&reader);
   ouro_client_detach(&exporter);
   ouro_client_detach(&other);
   ouro_runtime_stop(&runtime);
   ouro_host_destroy(&host);
   pthread_mutex_destroy(&host_lock);
   assert(unlink(grant_path) == 0);
   assert(rmdir(policy) == 0);
}

static int crash_client(const char *socket_path)
{
   ouro_client_t client;
   if (attach(socket_path, 9, &client) != OURO_CLIENT_OK || write(STDOUT_FILENO, "R", 1) != 1)
      return 2;
   for (;;)
      pause();
}

#define HOSTED_REF 10U

/* D15: a host in its own process, for a client to watch exit. */
static int run_host(const char *socket_path)
{
   char executable[PATH_MAX];
   if (!realpath("/proc/self/exe", executable))
      return 2;
   ouro_runtime_grant_t grant = {.principal_class = 1,
                                 .principal_ref = HOSTED_REF,
                                 .uid = OURO_RUNTIME_SELF_UID,
                                 .executable = executable};
   ouro_host_config_t config = {.max_slots = 4,
                                .slot_size = 512,
                                .inline_budget = 400,
                                .queue_capacity = 8,
                                .arena_size = 16384};
   static ouro_host_t host;
   static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
   if (ouro_host_create(&host, &config, NULL, NULL) != OURO_HOST_OK)
      return 2;
   ouro_runtime_config_t runtime_config = {.socket_path = socket_path,
                                           .socket_mode = 0600,
                                           .backlog = 4,
                                           .stale_after_ns = 60000000000ULL,
                                           .grants = &grant,
                                           .grant_count = 1};
   if (!ouro_runtime_start(&host, &lock, &runtime_config) || write(STDOUT_FILENO, "R", 1) != 1)
      return 2;
   for (;;)
      pause();
}

/* D15: a client asleep with no timeout wakes when its host's process exits,
 * and sees it as an epoch change, so it never waits on a host that is gone. */
static void test_host_exit_wakes_client(const char *directory, const char *executable)
{
   char socket_path[PATH_MAX];
   assert(snprintf(socket_path, sizeof socket_path, "%s/hosted.sock", directory) > 0);
   int ready[2];
   assert(pipe(ready) == 0);
   pid_t hosting = fork();
   assert(hosting >= 0);
   if (hosting == 0)
   {
      close(ready[0]);
      assert(dup2(ready[1], STDOUT_FILENO) == STDOUT_FILENO);
      close(ready[1]);
      execl(executable, executable, "--host", socket_path, (char *)NULL);
      _exit(127);
   }
   close(ready[1]);
   char marker = 0;
   assert(read(ready[0], &marker, 1) == 1 && marker == 'R');
   close(ready[0]);

   ouro_client_t client;
   assert(attach(socket_path, HOSTED_REF, &client) == OURO_CLIENT_OK);
   assert(ouro_client_liveness_watched(&client));
   assert(!ouro_client_epoch_changed(&client));
   pid_t killer = fork();
   assert(killer >= 0);
   if (killer == 0)
   {
      nanosleep(&(struct timespec){.tv_nsec = 200000000}, NULL);
      kill(hosting, SIGKILL);
      _exit(0);
   }
   assert(ouro_client_wait(&client, -1) == 1);
   assert(ouro_client_epoch_changed(&client));
   int status = 0;
   assert(waitpid(killer, &status, 0) == killer);
   assert(waitpid(hosting, &status, 0) == hosting && WIFSIGNALED(status));
   ouro_client_detach(&client);
   unlink(socket_path);
}

/* Refused role refs are stricter than the grant file. Both role grants
 * deliberately exist here so placement generation cannot accidentally make this
 * test pass: each instance refuses the ref that is not its own, and an instance
 * with no role refuses both. */
#define ROLE_A_REF 34U
#define ROLE_B_REF 35U

static void test_role_admission(const char *path, const char *executable)
{
   ouro_runtime_grant_t grants[2] = {{.principal_class = OURO_RUNTIME_ROLE_CLASS,
                                     .principal_ref = ROLE_A_REF,
                                     .uid = OURO_RUNTIME_SELF_UID,
                                     .executable = executable},
                                    {.principal_class = OURO_RUNTIME_ROLE_CLASS,
                                     .principal_ref = ROLE_B_REF,
                                     .uid = OURO_RUNTIME_SELF_UID,
                                     .executable = executable}};
   static const uint32_t refuse_none_owned[] = {ROLE_A_REF, ROLE_B_REF};
   static const uint32_t refuse_b[] = {ROLE_B_REF};
   static const uint32_t refuse_a[] = {ROLE_A_REF};
   const struct
   {
      const uint32_t *refused;
      size_t count;
      uint32_t own; /* 0: this instance owns no role */
   } cases[] = {{refuse_none_owned, 2, 0}, {refuse_b, 1, ROLE_A_REF}, {refuse_a, 1, ROLE_B_REF}};
   for (size_t c = 0; c < sizeof(cases) / sizeof(cases[0]); c++)
   {
      ouro_host_config_t hc = {.max_slots = 8,
                              .slot_size = 512,
                              .inline_budget = 400,
                              .queue_capacity = 8,
                              .arena_size = 16384};
      ouro_host_t host;
      assert(ouro_host_create(&host, &hc, NULL, NULL) == OURO_HOST_OK);
      pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
      ouro_runtime_config_t cfg = {.socket_path = path,
                                  .socket_mode = 0600,
                                  .backlog = 8,
                                  .grants = grants,
                                  .grant_count = 2,
                                  .refused_role_refs = cases[c].refused,
                                  .refused_role_ref_count = cases[c].count};
      ouro_runtime_t *runtime = ouro_runtime_start(&host, &lock, &cfg);
      assert(runtime);
      ouro_client_t allowed, denied;
      if (!cases[c].own)
      {
         assert(attach(path, ROLE_A_REF, &denied) == OURO_CLIENT_DENIED);
         assert(attach(path, ROLE_B_REF, &denied) == OURO_CLIENT_DENIED);
      }
      else
      {
         uint32_t ref = cases[c].own;
         uint32_t other = ref == ROLE_A_REF ? ROLE_B_REF : ROLE_A_REF;
         assert(attach(path, ref, &allowed) == OURO_CLIENT_OK);
         assert(attach(path, other, &denied) == OURO_CLIENT_DENIED);
         assert(attach(path, ref, &denied) == OURO_CLIENT_DENIED);
         ouro_client_detach(&allowed);
      }
      ouro_runtime_stop(&runtime);
      ouro_host_destroy(&host);
      pthread_mutex_destroy(&lock);
   }
   /* A refused-ref count without the list is a configuration error. */
   ouro_host_config_t hc = {.max_slots = 8,
                           .slot_size = 512,
                           .inline_budget = 400,
                           .queue_capacity = 8,
                           .arena_size = 16384};
   ouro_host_t host;
   assert(ouro_host_create(&host, &hc, NULL, NULL) == OURO_HOST_OK);
   pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
   ouro_runtime_config_t bad = {
       .socket_path = path, .socket_mode = 0600, .backlog = 8, .refused_role_ref_count = 1};
   assert(ouro_runtime_start(&host, &lock, &bad) == NULL);
   ouro_host_destroy(&host);
   pthread_mutex_destroy(&lock);
}

int main(int argc, char **argv)
{
   if (argc == 3 && strcmp(argv[1], "--crash-client") == 0)
      return crash_client(argv[2]);
   if (argc == 3 && strcmp(argv[1], "--host") == 0)
      return run_host(argv[2]);

   char directory[256];
   snprintf(directory, sizeof directory, "%s/ouroboros-ring-runtime-XXXXXX", test_tmpdir());
   assert(mkdtemp(directory) != NULL);
   char socket_path[PATH_MAX], policy_path[PATH_MAX], grant_path[PATH_MAX], executable[PATH_MAX];
   assert(snprintf(socket_path, sizeof(socket_path), "%s/module.sock", directory) > 0);
   assert(snprintf(policy_path, sizeof(policy_path), "%s/policy", directory) > 0);
   assert(snprintf(grant_path, sizeof(grant_path), "%s/test.grant", policy_path) > 0);
   assert(realpath("/proc/self/exe", executable) != NULL);
   assert(mkdir(policy_path, 0700) == 0);

   FILE *manifest = fopen(grant_path, "w");
   assert(manifest != NULL);
   assert(fprintf(manifest,
                  "version=1\nprincipal_class=1\nprincipal_ref=7\nuid=self\n"
                  "executable=%s\npublish=%u\nsubscribe=\nrequest=\nserve=\n",
                  executable, TEST_KIND) > 0);
   assert(fclose(manifest) == 0);
   ouro_runtime_policy_t *parsed = NULL;
   assert(ouro_runtime_policy_load_dir(policy_path, &parsed) == 0);
   size_t parsed_count = 0;
   const ouro_runtime_grant_t *parsed_grants = ouro_runtime_policy_grants(parsed, &parsed_count);
   assert(parsed_grants != NULL && parsed_count == 1);
   assert(parsed_grants[0].principal_ref == 7 && parsed_grants[0].publish_count == 1);
   ouro_runtime_policy_free(&parsed);

   uint32_t publisher_kinds[] = {TEST_KIND};
   uint32_t subscriber_kinds[] = {TEST_KIND};
   uint32_t server_kinds[] = {TEST_SERVER_KIND, TEST_SERVER_KIND_2};
   ouro_runtime_grant_t grants[] = {{.principal_class = 1,
                                    .principal_ref = 7,
                                    .uid = OURO_RUNTIME_SELF_UID,
                                    .executable = executable,
                                    .publish = publisher_kinds,
                                    .publish_count = 1},
                                   {.principal_class = 1,
                                    .principal_ref = 8,
                                    .uid = OURO_RUNTIME_SELF_UID,
                                    .executable = executable,
                                    .subscribe = subscriber_kinds,
                                    .subscribe_count = 1},
                                   {.principal_class = 1,
                                    .principal_ref = 9,
                                    .uid = OURO_RUNTIME_SELF_UID,
                                    .executable = executable,
                                    .serve = server_kinds,
                                    .serve_count = 2}};

   ouro_host_config_t host_config = {.max_slots = 8,
                                    .slot_size = 512,
                                    .inline_budget = 400,
                                    .queue_capacity = 8,
                                    .arena_size = 16384};
   ouro_host_t host;
   assert(ouro_host_create(&host, &host_config, NULL, NULL) == OURO_HOST_OK);
   pthread_mutex_t host_lock = PTHREAD_MUTEX_INITIALIZER;
   ouro_runtime_config_t runtime_config = {.socket_path = socket_path,
                                          .socket_mode = 0600,
                                          .backlog = 8,
                                          .stale_after_ns = 60000000000ULL,
                                          .grants = grants,
                                          .grant_count = 3};
   ouro_runtime_t *runtime = ouro_runtime_start(&host, &host_lock, &runtime_config);
   assert(runtime != NULL);

   ouro_client_t publisher, subscriber, denied;
   assert(attach(socket_path, 7, &publisher) == OURO_CLIENT_OK);
   assert(attach(socket_path, 8, &subscriber) == OURO_CLIENT_OK);
   assert(attach(socket_path, 99, &denied) == OURO_CLIENT_DENIED);
   /* D15: the runtime holds a process handle for each client it admits. */
   assert(ouro_client_process_liveness(&publisher) && ouro_client_process_liveness(&subscriber));

   static const char payload[] = "authorized";
   assert(ouro_client_publish(&publisher, TEST_KIND, payload, sizeof(payload)) == OURO_CLIENT_OK);
   pthread_mutex_lock(&host_lock);
   assert(ouro_host_pump(&host) == 1);
   pthread_mutex_unlock(&host_lock);
   ouro_event_t event;
   assert(ouro_client_poll(&subscriber, &event) == OURO_CLIENT_OK);
   assert(event.frame.event_kind == TEST_KIND && event.payload_len == sizeof(payload));
   assert(event.frame.principal_ref == 7 && event.frame.src_handle == publisher.reply.handle_id);
   assert(memcmp(event.payload, payload, sizeof(payload)) == 0);

   assert(ouro_client_publish(&publisher, TEST_KIND + 1U, NULL, 0) == OURO_CLIENT_OK);
   pthread_mutex_lock(&host_lock);
   assert(ouro_host_pump(&host) == 0);
   pthread_mutex_unlock(&host_lock);
   assert(ouro_client_poll(&publisher, &event) == OURO_CLIENT_OK);
   assert(event.frame.event_kind == OURO_KIND_CAPABILITY_DENIED);

   /* A live duplicate of a process principal is denied. Once that exact
    * process exits, pidfd-backed runtime admission releases its slot immediately
    * so a supervisor replacement need not wait for the 60-second heartbeat
    * timeout configured above. */
   int ready[2];
   assert(pipe(ready) == 0);
   pid_t crashed = fork();
   assert(crashed >= 0);
   if (crashed == 0)
   {
      close(ready[0]);
      assert(dup2(ready[1], STDOUT_FILENO) == STDOUT_FILENO);
      close(ready[1]);
      execl(executable, executable, "--crash-client", socket_path, (char *)NULL);
      _exit(127);
   }
   close(ready[1]);
   char marker = 0;
   assert(read(ready[0], &marker, 1) == 1 && marker == 'R');
   close(ready[0]);
   pthread_mutex_lock(&host_lock);
   assert(ouro_host_kind_has_server(&host, TEST_SERVER_KIND));
   assert(ouro_host_kind_has_server(&host, TEST_SERVER_KIND_2));
   pthread_mutex_unlock(&host_lock);
   ouro_client_t duplicate;
   assert(attach(socket_path, 9, &duplicate) == OURO_CLIENT_DENIED);
   assert(kill(crashed, SIGKILL) == 0);
   int status = 0;
   assert(waitpid(crashed, &status, 0) == crashed);
   assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);
   /* D15: the exit alone releases the slot -- no heartbeat timeout, and no
    * replacement attach needed to notice. */
   int released = 0;
   for (int i = 0; i < 200 && !released; ++i)
   {
      pthread_mutex_lock(&host_lock);
      released = ouro_host_admitted(&host) == 2 && !ouro_host_kind_has_server(&host, TEST_SERVER_KIND);
      pthread_mutex_unlock(&host_lock);
      if (!released)
         nanosleep(&(struct timespec){.tv_nsec = 10000000}, NULL);
   }
   assert(released);
   ouro_client_t replacement;
   assert(attach(socket_path, 9, &replacement) == OURO_CLIENT_OK);
   pthread_mutex_lock(&host_lock);
   assert(ouro_host_admitted(&host) == 3);
   assert(ouro_host_kind_has_server(&host, TEST_SERVER_KIND));
   assert(ouro_host_kind_has_server(&host, TEST_SERVER_KIND_2));
   pthread_mutex_unlock(&host_lock);
   ouro_client_detach(&replacement);
   /* D15: a detach is announced, so the next pump releases the slot even though
    * its process (this one) is still alive. */
   pthread_mutex_lock(&host_lock);
   (void)ouro_host_pump(&host);
   assert(ouro_host_admitted(&host) == 2);
   pthread_mutex_unlock(&host_lock);

   uint64_t now = ouro_runtime_monotonic_ns();
   pthread_mutex_lock(&host_lock);
   /* D15: every admitted client is process-bound, so there is nothing for a
    * reap timer to do, and clients that never heartbeat are not reaped even
    * long past the stale window. */
   assert(!ouro_runtime_maintenance_due(runtime));
   assert(ouro_runtime_maintain(runtime, now) == 0);
   assert(ouro_runtime_maintain(runtime, now + 2 * runtime_config.stale_after_ns) == 0);
   assert(ouro_host_admitted(&host) == 2);
   pthread_mutex_unlock(&host_lock);

   ouro_client_detach(&publisher);
   ouro_client_detach(&subscriber);
   ouro_runtime_stop(&runtime);
   ouro_host_destroy(&host);
   pthread_mutex_destroy(&host_lock);
   test_role_admission(socket_path, executable);
   test_host_exit_wakes_client(directory, executable);
   test_telemetry_grant(directory, executable);
   assert(unlink(grant_path) == 0);
   assert(rmdir(policy_path) == 0);
   assert(rmdir(directory) == 0);
   puts("ring runtime: authenticated attach, least privilege, and crash reattach passed");
   return 0;
}
