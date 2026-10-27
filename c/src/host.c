/* ouro_host.c: ring host admission, directory, and reaping. See ouro_host.h.
 *
 * The wire version this host speaks. A client declares a [min,max] range at
 * attach and the host picks the highest common value, or denies.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <stddef.h>
#include <unistd.h>

#include <ouroboros/host.h>
#include <ouroboros/ring.h>

/* Attach negotiation and frame validation must advertise the same version. */
#define OURO_HOST_WIRE_VERSION OURO_WIRE_VERSION

/* ------------------------------------------------------------------ */
/* lifecycle                                                           */

/* Defined in ouro_route.c: scrub a departing slot from the observer/server
 * registry and drop any pending request it was party to. */
void ouro_route_forget_slot(ouro_host_t *h, uint32_t slot);

static void slot_release(ouro_host_t *h, uint32_t idx)
{
   ouro_slot_t *s = &h->slots[idx];
   if (!s->in_use)
      return;
   ouro_route_forget_slot(h, idx);
   /* A departing client's arena leases must not strand the arena: drop the refs
    * it held as a producer and as a consumer. */
   ouro_arena_reap_producer(&h->arena, idx);
   ouro_arena_reap_consumer(&h->arena, idx);
   ouro_region_unmap(&s->qpair_region);
   if (s->qpair_fd >= 0)
      close(s->qpair_fd);
   memset(s, 0, sizeof *s);
   s->qpair_fd = -1;
   if (h->admitted > 0)
      h->admitted--;
}

ouro_host_result_t ouro_host_release_slot(ouro_host_t *h, uint32_t slot)
{
   if (!h || !h->slots || slot >= h->cfg.max_slots || !h->slots[slot].in_use)
      return OURO_HOST_ERR_ARG;
   slot_release(h, slot);
   return OURO_HOST_OK;
}

ouro_host_result_t ouro_host_create(ouro_host_t *h, const ouro_host_config_t *cfg, ouro_admit_fn admit,
                                  void *admit_ctx)
{
   if (!h || !cfg || cfg->max_slots == 0 || cfg->max_slots > OURO_ARENA_MAX_SLOTS)
      return OURO_HOST_ERR_ARG;

   memset(h, 0, sizeof *h);
   h->cfg = *cfg;
   h->admit = admit;
   h->admit_ctx = admit_ctx;
   h->control_fd = -1;
   h->arena_fd = -1;

   /* Control region. */
   if (ouro_region_create("ring-control", ouro_control_bytes(), &h->control_region) != OURO_REGION_OK)
      goto fail;
   h->control_fd = h->control_region.fd;
   if (ouro_region_map(h->control_fd, h->control_region.size, 1, &h->control_region) !=
       OURO_REGION_OK)
      goto fail;
   if (ouro_control_init(&h->control_region, cfg->slot_size, cfg->inline_budget, cfg->queue_capacity,
                        cfg->arena_size) != OURO_REGION_OK)
      goto fail;
   if (ouro_control_attach(&h->control_region, &h->control) != OURO_REGION_OK)
      goto fail;

   /* Arena region + allocator. */
   size_t arena_bytes = ouro_arena_region_bytes(cfg->arena_size);
   if (arena_bytes == 0)
      goto fail;
   if (ouro_region_create("ring-arena", arena_bytes, &h->arena_region) != OURO_REGION_OK)
      goto fail;
   h->arena_fd = h->arena_region.fd;
   if (ouro_region_map(h->arena_fd, h->arena_region.size, 1, &h->arena_region) != OURO_REGION_OK)
      goto fail;
   if (ouro_arena_region_init(&h->arena_region, cfg->arena_size) != OURO_REGION_OK)
      goto fail;
   uint8_t *abase = NULL;
   uint64_t asize = 0;
   if (ouro_arena_region_attach(&h->arena_region, &abase, &asize) != OURO_REGION_OK)
      goto fail;
   /* Per-client lease cap: capacity's worth per client is a sane provisional
    * bound, re-tuned in slice 12 with the other parameters. */
   if (ouro_arena_init(&h->arena, abase, asize, cfg->max_slots, cfg->queue_capacity) != OURO_ARENA_OK)
      goto fail;
   /* D12: publish lease placement to clients through the read-only control
    * region, so a consumer in another process can read what it is sent. */
   ouro_lease_view_t *view = ouro_control_lease_view_host(&h->control_region);
   if (!view)
      goto fail;
   ouro_arena_set_view(&h->arena, view);

   h->slots = calloc(cfg->max_slots, sizeof(ouro_slot_t));
   if (!h->slots)
      goto fail;
   for (uint32_t i = 0; i < cfg->max_slots; i++)
      h->slots[i].qpair_fd = -1;

   return OURO_HOST_OK;

fail:
   ouro_host_destroy(h);
   return OURO_HOST_ERR_REGION;
}

void ouro_host_set_admission(ouro_host_t *h, ouro_admit_fn admit, void *admit_ctx)
{
   if (!h)
      return;
   h->admit = admit;
   h->admit_ctx = admit ? admit_ctx : NULL;
}

void ouro_host_destroy(ouro_host_t *h)
{
   if (!h)
      return;
   if (h->slots)
   {
      for (uint32_t i = 0; i < h->cfg.max_slots; i++)
         slot_release(h, i);
      free(h->slots);
      h->slots = NULL;
   }
   ouro_arena_fini(&h->arena);
   ouro_region_unmap(&h->arena_region);
   if (h->arena_fd >= 0)
      close(h->arena_fd);
   ouro_region_unmap(&h->control_region);
   if (h->control_fd >= 0)
      close(h->control_fd);
   h->arena_fd = h->control_fd = -1;
}

/* ------------------------------------------------------------------ */
/* admission                                                           */

static uint32_t find_free_slot(const ouro_host_t *h)
{
   for (uint32_t i = 0; i < h->cfg.max_slots; i++)
      if (!h->slots[i].in_use)
         return i;
   return h->cfg.max_slots;
}

/* Send a denial reply with no descriptors. */
static ouro_host_result_t deny(int conn_fd, ouro_attach_status_t status)
{
   ouro_attach_reply_t reply;
   memset(&reply, 0, sizeof reply);
   reply.magic = OURO_ATTACH_REPLY_MAGIC;
   reply.status = (uint32_t)status;
   ouro_fd_send(conn_fd, &reply, sizeof reply, NULL, 0);
   return OURO_HOST_ERR_REFUSED;
}

/* A held unnamed socketpair is a host capability, not a listener request.
 * Verify kernel peer identity before admitting the daemon's private class0
 * client; no external grant callback is disabled or replaced. */
static int host_owned_socketpair(int fd)
{
#if defined(__linux__) && defined(SO_PEERCRED)
   struct ucred peer;
   socklen_t peer_size = sizeof peer;
   struct sockaddr_un local, remote;
   socklen_t local_size = sizeof local, remote_size = sizeof remote;
   int type = 0;
   socklen_t type_size = sizeof type;
   return getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &type_size) == 0 && type == SOCK_SEQPACKET &&
          getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &peer, &peer_size) == 0 &&
          peer_size == sizeof peer && peer.pid == getpid() && peer.uid == geteuid() &&
          getsockname(fd, (struct sockaddr *)&local, &local_size) == 0 &&
          getpeername(fd, (struct sockaddr *)&remote, &remote_size) == 0 &&
          local.sun_family == AF_UNIX && remote.sun_family == AF_UNIX &&
          local_size == offsetof(struct sockaddr_un, sun_path) &&
          remote_size == offsetof(struct sockaddr_un, sun_path);
#else
   (void)fd;
   return 0; /* No guessed identity on platforms without same-process attestation. */
#endif
}
static ouro_host_result_t serve_attach(ouro_host_t *h, int conn_fd, uint32_t *slot_out, int owned)
{
   if (slot_out)
      *slot_out = UINT32_MAX;
   if (!h || conn_fd < 0)
      return OURO_HOST_ERR_ARG;

   ouro_attach_request_t req;
   int rfds[3];
   int nfd = 0;
   long n = ouro_fd_recv(conn_fd, &req, sizeof req, rfds, 3, &nfd);
   /* A client never sends descriptors; if it tried, drop them. */
   for (int i = 0; i < nfd; i++)
      close(rfds[i]);
   if (n != (long)sizeof req || req.magic != OURO_ATTACH_REQ_MAGIC)
      return deny(conn_fd, OURO_ATTACH_PROTOCOL);

   /* Version negotiation: highest common, or deny. */
   if (req.wire_version_min > OURO_HOST_WIRE_VERSION || req.wire_version_max < OURO_HOST_WIRE_VERSION)
      return deny(conn_fd, OURO_ATTACH_DENIED_VERSION);

   if (owned &&
       (req.principal_class != 0 || req.principal_ref != 0 || !host_owned_socketpair(conn_fd)))
      return deny(conn_fd, OURO_ATTACH_DENIED_POLICY);

   /* External admission and capability binding are unchanged. A private owned
    * socketpair is verified above and never enters the external grant policy. */
   if (!owned && h->admit)
   {
      ouro_attach_status_t d = h->admit(h->admit_ctx, conn_fd, &req);
      if (d != OURO_ATTACH_OK)
         return deny(conn_fd, d);
   }

   uint32_t idx = find_free_slot(h);
   if (idx == h->cfg.max_slots)
      return deny(conn_fd, OURO_ATTACH_DENIED_NOSLOT);

   /* Build this client's queue-pair region. Everything below can fail without
    * leaving a half-admitted slot: on any failure the slot is released. */
   ouro_slot_t *s = &h->slots[idx];
   memset(s, 0, sizeof *s);
   s->qpair_fd = -1;

   size_t qbytes = ouro_qpair_bytes(h->cfg.slot_size, h->cfg.queue_capacity);
   if (qbytes == 0)
      return deny(conn_fd, OURO_ATTACH_PROTOCOL);
   if (ouro_region_create("ring-qpair", qbytes, &s->qpair_region) != OURO_REGION_OK)
   {
      slot_release(h, idx);
      return deny(conn_fd, OURO_ATTACH_DENIED_NOSLOT);
   }
   s->qpair_fd = s->qpair_region.fd;
   if (ouro_region_map(s->qpair_fd, s->qpair_region.size, 1, &s->qpair_region) != OURO_REGION_OK ||
       ouro_qpair_init(&s->qpair_region, h->cfg.slot_size, h->cfg.queue_capacity) != OURO_REGION_OK ||
       ouro_qpair_attach(&s->qpair_region, &s->qpair) != OURO_REGION_OK)
   {
      slot_release(h, idx);
      return deny(conn_fd, OURO_ATTACH_DENIED_NOSLOT);
   }

   s->in_use = 1;
   s->principal_class = req.principal_class;
   s->principal_ref = req.principal_ref;
   s->last_heartbeat = 0;
   s->heartbeat_at = 0;
   h->admitted++;

   if (!owned && h->attach_hook)
   {
      ouro_attach_status_t hook_status = h->attach_hook(h->attach_hook_ctx, h, idx, &req);
      if (hook_status != OURO_ATTACH_OK)
      {
         slot_release(h, idx);
         return deny(conn_fd, hook_status);
      }
   }

   /* Grant: the reply plus exactly three descriptors — control (the client will
    * map it read-only), arena, and this client's own queue pair. No other
    * client's descriptor is ever in this set. */
   ouro_attach_reply_t reply;
   memset(&reply, 0, sizeof reply);
   reply.magic = OURO_ATTACH_REPLY_MAGIC;
   reply.status = OURO_ATTACH_OK;
   reply.handle_id = idx;
   reply.wire_version = OURO_HOST_WIRE_VERSION;
   reply.slot_size = h->cfg.slot_size;
   reply.inline_budget = h->cfg.inline_budget;
   reply.queue_capacity = h->cfg.queue_capacity;
   reply.arena_size = h->cfg.arena_size;
   reply.host_epoch = ouro_control_epoch(h->control);

   /* "The control region is read-only to clients" (D1/D2) has to be enforced on
    * the DESCRIPTOR, not merely on the client's mapping: a client handed the
    * host's read-write control fd could map it PROT_WRITE and corrupt the region
    * for everyone. So the client receives a re-opened O_RDONLY view of the same
    * memfd — mmap PROT_WRITE on it fails — while the host keeps its own
    * read-write fd to write heartbeats and bump the epoch. The arena and the
    * client's own queue pair are legitimately read-write. */
   char proc[64];
   snprintf(proc, sizeof proc, "/proc/self/fd/%d", h->control_fd);
   int control_ro = open(proc, O_RDONLY | O_CLOEXEC);
   if (control_ro < 0)
   {
      slot_release(h, idx);
      return deny(conn_fd, OURO_ATTACH_DENIED_NOSLOT);
   }

   int gfds[OURO_ATTACH_FDS];
   gfds[OURO_ATTACH_FD_CONTROL] = control_ro;
   gfds[OURO_ATTACH_FD_ARENA] = h->arena_fd;
   gfds[OURO_ATTACH_FD_QPAIR] = s->qpair_fd;
   int rc = ouro_fd_send(conn_fd, &reply, sizeof reply, gfds, OURO_ATTACH_FDS);
   close(control_ro); /* the client holds its own dup now */
   if (rc != 0)
   {
      /* The client never received its grant; undo the admission. */
      slot_release(h, idx);
      return OURO_HOST_ERR_OS;
   }
   if (slot_out)
      *slot_out = idx;
   return OURO_HOST_OK;
}

ouro_host_result_t ouro_host_serve_attach(ouro_host_t *h, int fd)
{
   return serve_attach(h, fd, NULL, 0);
}
ouro_host_result_t ouro_host_serve_attach_ex(ouro_host_t *h, int fd, uint32_t *slot)
{
   return serve_attach(h, fd, slot, 0);
}
ouro_host_result_t ouro_host_serve_attach_owned(ouro_host_t *h, int fd)
{
   return serve_attach(h, fd, NULL, 1);
}

/* ------------------------------------------------------------------ */
/* reaping                                                             */

uint32_t ouro_host_reap(ouro_host_t *h, uint64_t now, uint64_t stale_ns)
{
   if (!h || !h->slots)
      return 0;
   uint32_t reaped = 0;
   for (uint32_t i = 0; i < h->cfg.max_slots; i++)
   {
      ouro_slot_t *s = &h->slots[i];
      if (!s->in_use)
         continue;

      uint64_t hb = atomic_load_explicit(&s->qpair.hdr->client_heartbeat, memory_order_acquire);
      if (hb != s->last_heartbeat)
      {
         /* The client is alive; note the advance. */
         s->last_heartbeat = hb;
         s->heartbeat_at = now;
         continue;
      }
      /* No advance since we last looked. A slot never yet seen advancing uses
       * its admission time as the baseline (heartbeat_at was set to 0 at
       * admission, so treat 0 as "start the clock now" the first time). */
      if (s->heartbeat_at == 0)
      {
         s->heartbeat_at = now;
         continue;
      }
      if (now - s->heartbeat_at >= stale_ns)
      {
         slot_release(h, i);
         reaped++;
      }
   }
   return reaped;
}

void ouro_host_bump_epoch(ouro_host_t *h)
{
   if (h && h->control)
      ouro_control_bump_epoch(h->control);
}

uint32_t ouro_host_admitted(const ouro_host_t *h)
{
   return h ? h->admitted : 0;
}

void ouro_host_set_attach_hook(ouro_host_t *h,
                              ouro_attach_status_t (*hook)(void *ctx, ouro_host_t *host,
                                                          uint32_t slot,
                                                          const ouro_attach_request_t *request),
                              void *ctx)
{
   if (!h)
      return;
   h->attach_hook = hook;
   h->attach_hook_ctx = hook ? ctx : NULL;
}

const char *ouro_host_result_name(ouro_host_result_t r)
{
   switch (r)
   {
   case OURO_HOST_OK:
      return "OK";
   case OURO_HOST_ERR_ARG:
      return "ERR_ARG";
   case OURO_HOST_ERR_OS:
      return "ERR_OS";
   case OURO_HOST_ERR_REGION:
      return "ERR_REGION";
   case OURO_HOST_ERR_REFUSED:
      return "ERR_REFUSED";
   default:
      return "ERR_UNKNOWN";
   }
}

const char *ouro_attach_status_name(ouro_attach_status_t s)
{
   switch (s)
   {
   case OURO_ATTACH_OK:
      return "OK";
   case OURO_ATTACH_DENIED_POLICY:
      return "DENIED_POLICY";
   case OURO_ATTACH_DENIED_VERSION:
      return "DENIED_VERSION";
   case OURO_ATTACH_DENIED_NOSLOT:
      return "DENIED_NOSLOT";
   case OURO_ATTACH_PROTOCOL:
      return "PROTOCOL";
   default:
      return "UNKNOWN";
   }
}
