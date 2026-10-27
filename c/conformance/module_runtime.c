/* module_runtime.c: the module process runtime, end to end over a real host,
 * a real attach socket and real shared memory.
 *
 *   module-runtime-harness                 C caller -> in-process C module
 *   module-runtime-harness MODULE_BINARY   C caller -> an external module process
 *                                          (go/ring/testdata/module_helper,
 *                                          rust ouroboros-ring-module-helper)
 *   OURO_TEST_MODULE_CALLER=CALLER_BINARY  additionally, an external caller
 *                                          (rust ouroboros-ring-module-caller-check)
 *                                          must reach the same outcomes
 *
 * The module serves TEST_KIND stage TEST_STAGE as principal 1/MODULE_REF: it
 * echoes the request, except "cancel", which waits until the call is
 * cancelled. Every implementation's module helper and caller check are written
 * to these numbers, so a passing run means two independent implementations
 * agree on dispatch, fragmentation, deadlines, cancellation, typed absence and
 * denial. */
#define _GNU_SOURCE
#include <ouroboros/client.h>
#include <ouroboros/endpoint.h>
#include <ouroboros/host.h>
#include <ouroboros/runtime.h>
#include <ouroboros/module_client.h>
#include <ouroboros/module_runtime.h>

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "support/tmpdir.h"

#define TEST_KIND           5889U
#define EMPTY_KIND          5890U
#define DENIED_KIND         5891U
#define TEST_STAGE          1U
#define MODULE_REF          7U
#define CALLER_REF          90U
#define EXTERNAL_CALLER_REF 91U
#define LARGE_BODY          (128U * 1024U + 37U)

typedef struct
{
   ouro_module_process_config_t config;
   int result;
} process_thread_t;

typedef struct
{
   ouro_host_t *host;
   pthread_mutex_t *lock;
   atomic_int stop;
} pump_thread_t;

static ouro_module_status_t handle(const ouro_module_invocation_t *invocation,
                                    const uint8_t *request, uint32_t request_len, uint8_t *response,
                                    uint32_t response_capacity, uint32_t *response_len,
                                    void *user_data)
{
   (void)user_data;
   if (request_len == 6 && memcmp(request, "cancel", 6) == 0)
   {
      const struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
      while (!ouro_module_invocation_cancelled(invocation))
         nanosleep(&pause, NULL);
      return OURO_MODULE_STATUS_OK; /* the runtime converts this to CANCELLED */
   }
   if (request_len > response_capacity)
      return OURO_MODULE_STATUS_INTERNAL;
   memcpy(response, request, request_len);
   *response_len = request_len;
   return OURO_MODULE_STATUS_OK;
}

static void *run_process(void *argument)
{
   process_thread_t *thread = argument;
   thread->result = ouro_module_process_run(&thread->config);
   return NULL;
}

static void pump(ouro_host_t *host, pthread_mutex_t *lock)
{
   pthread_mutex_lock(lock);
   (void)ouro_host_pump(host);
   pthread_mutex_unlock(lock);
}

static void *run_pump(void *argument)
{
   pump_thread_t *pump_state = argument;
   const struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
   while (!atomic_load_explicit(&pump_state->stop, memory_order_acquire))
   {
      pump(pump_state->host, pump_state->lock);
      nanosleep(&pause, NULL);
   }
   return NULL;
}

static int cancellation_flag(void *context)
{
   return atomic_load_explicit((atomic_int *)context, memory_order_acquire);
}

static void *cancel_soon(void *context)
{
   const struct timespec pause = {.tv_sec = 0, .tv_nsec = 10000000};
   nanosleep(&pause, NULL);
   atomic_store_explicit((atomic_int *)context, 1, memory_order_release);
   return NULL;
}

static void wait_for_clients(ouro_host_t *host, pthread_mutex_t *lock, uint32_t count, pid_t child,
                             uint32_t event_kind)
{
   const struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
   for (int i = 0; i < 10000; ++i)
   {
      pthread_mutex_lock(lock);
      uint32_t admitted = host->admitted;
      int serving = ouro_host_kind_has_server(host, event_kind);
      pthread_mutex_unlock(lock);
      if (admitted >= count && serving)
         return;
      int status = 0;
      if (child > 0 && waitpid(child, &status, WNOHANG) == child)
      {
         fprintf(stderr, "module exited before admission (status %d)\n", status);
         assert(!"module exited before admission");
      }
      nanosleep(&pause, NULL);
   }
   assert(!"timed out waiting for module clients");
}

static pid_t spawn_child(const char *executable, const char *socket_path)
{
   pid_t parent = getpid();
   pid_t child = fork();
   assert(child >= 0);
   if (child == 0)
   {
      prctl(PR_SET_PDEATHSIG, SIGKILL);
      if (getppid() != parent)
         _exit(0);
      execl(executable, executable, socket_path, (char *)NULL);
      _exit(127);
   }
   return child;
}

int main(int argc, char **argv)
{
   assert(argc == 1 || argc == 2);
   char directory[256];
   snprintf(directory, sizeof directory, "%s/ouroboros-module-runtime-XXXXXX", test_tmpdir());
   assert(mkdtemp(directory) != NULL);
   char socket_path[PATH_MAX], executable[PATH_MAX], module_executable[PATH_MAX];
   assert(snprintf(socket_path, sizeof socket_path, "%s/module.sock", directory) > 0);
   assert(realpath("/proc/self/exe", executable) != NULL);
   if (argc == 2)
      assert(realpath(argv[1], module_executable) != NULL);
   else
      assert(snprintf(module_executable, sizeof module_executable, "%s", executable) > 0);

   const char *external_caller_env = getenv("OURO_TEST_MODULE_CALLER");
   char external_caller[PATH_MAX] = "";
   if (external_caller_env && *external_caller_env)
      assert(realpath(external_caller_env, external_caller) != NULL);

   const uint32_t served[] = {TEST_KIND};
   const uint32_t requested[] = {TEST_KIND, EMPTY_KIND};
   ouro_runtime_grant_t grants[] = {{.principal_class = 1,
                                    .principal_ref = MODULE_REF,
                                    .uid = OURO_RUNTIME_SELF_UID,
                                    .executable = module_executable,
                                    .serve = served,
                                    .serve_count = 1},
                                   {.principal_class = 1,
                                    .principal_ref = CALLER_REF,
                                    .uid = OURO_RUNTIME_SELF_UID,
                                    .executable = executable,
                                    .request = requested,
                                    .request_count = 2},
                                   /* An external caller joins under its own grant:
                                    * the served kind and EMPTY_KIND, nothing else. */
                                   {.principal_class = 1,
                                    .principal_ref = EXTERNAL_CALLER_REF,
                                    .uid = OURO_RUNTIME_SELF_UID,
                                    .executable = external_caller,
                                    .request = requested,
                                    .request_count = 2}};
   ouro_host_config_t host_config = {.max_slots = 8,
                                    .slot_size = 512,
                                    .inline_budget = 400,
                                    .queue_capacity = 16,
                                    .arena_size = 16384};
   ouro_host_t host;
   assert(ouro_host_create(&host, &host_config, NULL, NULL) == OURO_HOST_OK);
   pthread_mutex_t host_lock = PTHREAD_MUTEX_INITIALIZER;
   ouro_runtime_config_t runtime_config = {.socket_path = socket_path,
                                          .socket_mode = 0600,
                                          .backlog = 8,
                                          .stale_after_ns = 5000000000ULL,
                                          .grants = grants,
                                          .grant_count = *external_caller ? 3 : 2};
   ouro_runtime_t *runtime = ouro_runtime_start(&host, &host_lock, &runtime_config);
   assert(runtime != NULL);

   static const ouro_module_stage_t stages[] = {{TEST_KIND, TEST_STAGE}};
   process_thread_t process = {.config = {.socket_path = socket_path,
                                          .module_name = "test-module",
                                          .principal_class = 1,
                                          .principal_ref = MODULE_REF,
                                          .stages = stages,
                                          .stage_count = 1,
                                          .handler = handle}};
   pump_thread_t pump_state = {.host = &host, .lock = &host_lock};
   atomic_init(&pump_state.stop, 0);
   pthread_t pump_thread;
   assert(pthread_create(&pump_thread, NULL, run_pump, &pump_state) == 0);

   pthread_t module_thread;
   pid_t module_pid = -1;
   if (argc == 2)
      module_pid = spawn_child(module_executable, socket_path);
   else
      assert(pthread_create(&module_thread, NULL, run_process, &process) == 0);

   int caller_fd = -1;
   ouro_client_t caller;
   assert(ouro_endpoint_connect(socket_path, &caller_fd) == 0);
   assert(ouro_client_attach_as(caller_fd, &caller, 1, CALLER_REF) == OURO_CLIENT_OK);
   assert(ouro_endpoint_close(&caller_fd) == 0);
   wait_for_clients(&host, &host_lock, 2, module_pid, TEST_KIND);

   ouro_module_client_t module_client;
   assert(ouro_module_client_init(&module_client, &caller) == 0);

   char body[64];
   uint32_t body_len = 0;
   assert(ouro_module_client_call(&module_client, TEST_KIND, TEST_STAGE, 1001, 0, "real-result",
                                   11, body, sizeof body, &body_len, NULL,
                                   NULL) == OURO_MODULE_CALL_OK);
   assert(body_len == 11 && memcmp(body, "real-result", 11) == 0);

   /* Larger than any inline budget: request and reply both fragment. */
   uint8_t *large_request = malloc(LARGE_BODY);
   uint8_t *large_response = malloc(LARGE_BODY);
   assert(large_request != NULL && large_response != NULL);
   for (uint32_t i = 0; i < LARGE_BODY; ++i)
      large_request[i] = (uint8_t)((i * 131U + 17U) & 0xffU);
   assert(ouro_module_client_call(&module_client, TEST_KIND, TEST_STAGE, 1007, 0, large_request,
                                   LARGE_BODY, large_response, LARGE_BODY, &body_len, NULL,
                                   NULL) == OURO_MODULE_CALL_OK);
   assert(body_len == LARGE_BODY && memcmp(large_request, large_response, LARGE_BODY) == 0);

   /* A too-small destination still drains every response fragment and reports
    * the complete response length, leaving the next correlation usable. */
   assert(ouro_module_client_call(&module_client, TEST_KIND, TEST_STAGE, 1008, 0, large_request,
                                   LARGE_BODY, body, sizeof body, &body_len, NULL,
                                   NULL) == OURO_MODULE_CALL_RESPONSE_TOO_LARGE);
   assert(body_len == LARGE_BODY);
   free(large_response);
   free(large_request);

   assert(ouro_module_client_call(&module_client, TEST_KIND, TEST_STAGE, 1002, 1, "late", 4, body,
                                   sizeof body, &body_len, NULL,
                                   NULL) == OURO_MODULE_CALL_DEADLINE_EXCEEDED);
   assert(body_len == 0);

   atomic_int cancel;
   atomic_init(&cancel, 0);
   pthread_t cancel_thread;
   assert(pthread_create(&cancel_thread, NULL, cancel_soon, &cancel) == 0);
   assert(ouro_module_client_call(&module_client, TEST_KIND, TEST_STAGE, 1003, 0, "cancel", 6,
                                   body, sizeof body, &body_len, cancellation_flag,
                                   &cancel) == OURO_MODULE_CALL_CANCELLED);
   assert(pthread_join(cancel_thread, NULL) == 0 && body_len == 0);

   /* The cancelled handler's terminal reply may arrive after call() returned.
    * A subsequent call must drain that stale correlation and still complete. */
   assert(ouro_module_client_call(&module_client, TEST_KIND, TEST_STAGE, 1004, 0, "after", 5, body,
                                   sizeof body, &body_len, NULL, NULL) == OURO_MODULE_CALL_OK);
   assert(body_len == 5 && memcmp(body, "after", 5) == 0);

   assert(ouro_module_client_call(&module_client, EMPTY_KIND, 2, 1005, 0, NULL, 0, body,
                                   sizeof body, &body_len, NULL,
                                   NULL) == OURO_MODULE_CALL_CAPABILITY_ABSENT);

   assert(ouro_module_client_call(&module_client, TEST_KIND, TEST_STAGE, 1006, 0, "toolarge", 8,
                                   body, 3, &body_len, NULL,
                                   NULL) == OURO_MODULE_CALL_RESPONSE_TOO_LARGE);
   assert(body_len == 8);

   /* A kind outside the caller's grant is refused by the host at once, as
    * CAPABILITY_DENIED, not waited out to a deadline. */
   struct timespec denied_now;
   assert(clock_gettime(CLOCK_MONOTONIC, &denied_now) == 0);
   uint64_t denied_deadline =
       (uint64_t)denied_now.tv_sec * 1000000000ULL + (uint64_t)denied_now.tv_nsec + 20000000000ULL;
   assert(ouro_module_client_call(&module_client, DENIED_KIND, 3, 1009, denied_deadline, "x", 1,
                                   body, sizeof body, &body_len, NULL,
                                   NULL) == OURO_MODULE_CALL_CAPABILITY_DENIED);
   assert(body_len == 0);

   if (*external_caller)
   {
      /* The same outcomes, from an independent caller in another process. Run
       * after the C caller so its stale correlations are already behind it. */
      pid_t caller_pid = spawn_child(external_caller, socket_path);
      int caller_status = 0;
      while (waitpid(caller_pid, &caller_status, 0) < 0)
         assert(errno == EINTR);
      if (!WIFEXITED(caller_status) || WEXITSTATUS(caller_status) != 0)
         fprintf(stderr, "external module caller failed (status %d)\n", caller_status);
      assert(WIFEXITED(caller_status) && WEXITSTATUS(caller_status) == 0);
      puts("module runtime: external caller matched the C caller's outcomes");
   }

   ouro_module_client_destroy(&module_client);
   if (module_pid > 0)
   {
      assert(kill(module_pid, SIGTERM) == 0);
      int status = 0;
      while (waitpid(module_pid, &status, 0) < 0)
         assert(errno == EINTR);
      assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
   }
   else
   {
      ouro_module_process_stop();
      assert(pthread_join(module_thread, NULL) == 0 && process.result == 0);
   }
   atomic_store_explicit(&pump_state.stop, 1, memory_order_release);
   assert(pthread_join(pump_thread, NULL) == 0);
   ouro_client_detach(&caller);
   ouro_runtime_stop(&runtime);
   ouro_host_destroy(&host);
   pthread_mutex_destroy(&host_lock);
   assert(rmdir(directory) == 0);
   printf("module runtime (%s): dispatch, fragmented payloads, deadline, and cancellation passed\n",
          argc == 2 ? "external process" : "C process");
   return 0;
}
