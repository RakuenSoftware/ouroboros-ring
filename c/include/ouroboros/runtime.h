#ifndef OUROBOROS_RUNTIME_H
#define OUROBOROS_RUNTIME_H 1

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#include <ouroboros/host.h>

#define OURO_RUNTIME_SELF_UID UINT32_MAX

typedef struct
{
   uint32_t principal_class;
   uint32_t principal_ref;
   uint32_t uid;           /* OURO_RUNTIME_SELF_UID means the daemon's effective uid */
   const char *executable; /* canonical absolute /proc/<pid>/exe target */
   const uint32_t *publish;
   size_t publish_count;
   const uint32_t *subscribe;
   size_t subscribe_count;
   const uint32_t *request;
   size_t request_count;
   const uint32_t *serve;
   size_t serve_count;
} ouro_runtime_grant_t;

typedef struct ouro_runtime ouro_runtime_t;
typedef struct ouro_runtime_policy ouro_runtime_policy_t;

/* Principal class of reserved role identities. A host that composes several
 * instances names, per instance, the role refs it must refuse; that keeps a role
 * from attaching to an instance it does not belong to. Which refs exist and
 * which instance takes which is the host's business, not the ring's. */
#define OURO_RUNTIME_ROLE_CLASS 1U

typedef struct
{
   const char *socket_path;
   unsigned socket_mode;
   int backlog;
   uint64_t stale_after_ns;
   const ouro_runtime_grant_t *grants;
   size_t grant_count;
   const uint32_t *refused_role_refs; /* class OURO_RUNTIME_ROLE_CLASS refs denied here */
   size_t refused_role_ref_count;
} ouro_runtime_config_t;

/* Start the authenticated local module endpoint. host_lock must guard every
 * other access to host while this runtime is active; the accept thread uses the
 * same lock for admission, capability binding, and descriptor grant. */
ouro_runtime_t *ouro_runtime_start(ouro_host_t *host, pthread_mutex_t *host_lock,
                                 const ouro_runtime_config_t *config);
void ouro_runtime_stop(ouro_runtime_t **runtime);

/* Call periodically while holding host_lock. Updates the host heartbeat and
 * reaps clients whose own heartbeat stopped advancing. */
uint32_t ouro_runtime_maintain(ouro_runtime_t *runtime, uint64_t now_ns);
/* D15: whether ouro_runtime_maintain has anything to watch. 0 when every
 * admitted client is bound to a process handle the runtime releases on exit, so
 * the owner may sleep until woken; a later attach wakes the host doorbell.
 * Caller holds the host lock. */
int ouro_runtime_maintenance_due(const ouro_runtime_t *runtime);
uint64_t ouro_runtime_monotonic_ns(void);

/* Strict deployment policy: one *.grant file per installed module. A missing
 * directory is an empty policy (the listener still runs and denies all). */
int ouro_runtime_policy_load_dir(const char *directory, ouro_runtime_policy_t **out);
void ouro_runtime_policy_free(ouro_runtime_policy_t **policy);
const ouro_runtime_grant_t *ouro_runtime_policy_grants(const ouro_runtime_policy_t *policy,
                                                     size_t *count_out);

#endif
