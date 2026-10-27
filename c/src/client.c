/* ouro_client.c: the C reference ring client. See ouro_client.h. */
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <ouroboros/client.h>
#include <ouroboros/ring.h>

ouro_client_result_t ouro_client_attach(int sock, ouro_client_t *c)
{
   return ouro_client_attach_as(sock, c, 0, 0);
}

ouro_client_result_t ouro_client_attach_as(int sock, ouro_client_t *c, uint32_t principal_class,
                                         uint32_t principal_ref)
{
   if (!c || sock < 0)
      return OURO_CLIENT_ERR_ARG;
   memset(c, 0, sizeof *c);

   ouro_attach_request_t req;
   memset(&req, 0, sizeof req);
   req.magic = OURO_ATTACH_REQ_MAGIC;
   req.wire_version_min = OURO_WIRE_VERSION;
   req.wire_version_max = OURO_WIRE_VERSION;
   req.principal_class = principal_class;
   req.principal_ref = principal_ref;
   if (ouro_fd_send(sock, &req, sizeof req, NULL, 0) != 0)
      return OURO_CLIENT_ERR_OS;

   int fds[OURO_ATTACH_FDS];
   int nfd = 0;
   long n = ouro_fd_recv(sock, &c->reply, sizeof c->reply, fds, OURO_ATTACH_FDS, &nfd);
   if (n != (long)sizeof c->reply || c->reply.magic != OURO_ATTACH_REPLY_MAGIC)
   {
      for (int i = 0; i < nfd; i++)
         close(fds[i]);
      return OURO_CLIENT_ERR_PROTOCOL;
   }

   c->attach_status = (ouro_attach_status_t)c->reply.status;
   if (c->reply.status != OURO_ATTACH_OK)
   {
      for (int i = 0; i < nfd; i++)
         close(fds[i]);
      return OURO_CLIENT_DENIED;
   }
   if (nfd != OURO_ATTACH_FDS)
   {
      for (int i = 0; i < nfd; i++)
         close(fds[i]);
      return OURO_CLIENT_ERR_PROTOCOL;
   }

   /* control (read-only), arena (rw), own queue pair (rw). */
   ouro_client_result_t rc = OURO_CLIENT_ERR_OS;
   /* A D12 host's control region carries the lease view after the header; an
    * older host's is the header alone. Map what is there. */
   struct stat cst;
   if (fstat(fds[OURO_ATTACH_FD_CONTROL], &cst) != 0)
      goto done;
   size_t ctl_bytes = (cst.st_size >= 0 && (size_t)cst.st_size >= ouro_control_bytes())
                          ? ouro_control_bytes()
                          : ouro_control_header_bytes();
   if (ouro_region_map(fds[OURO_ATTACH_FD_CONTROL], ctl_bytes, 0, &c->control) != OURO_REGION_OK)
      goto done;
   if (ouro_region_map(fds[OURO_ATTACH_FD_ARENA], ouro_arena_region_bytes(c->reply.arena_size), 1,
                      &c->arena) != OURO_REGION_OK)
      goto done;
   if (ouro_region_map(fds[OURO_ATTACH_FD_QPAIR],
                      ouro_qpair_bytes(c->reply.slot_size, c->reply.queue_capacity), 1,
                      &c->qpair) != OURO_REGION_OK)
      goto done;
   if (ouro_control_attach(&c->control, &c->ctl) != OURO_REGION_OK)
      goto done;
   if (ouro_qpair_attach(&c->qpair, &c->qp) != OURO_REGION_OK)
      goto done;
   if (ouro_arena_region_attach(&c->arena, &c->arena_base, &c->arena_size) != OURO_REGION_OK)
      goto done;
   if (ouro_control_lease_view(&c->control, c->ctl, &c->lease_view) != OURO_REGION_OK)
      c->lease_view = NULL; /* an older host: arena payloads stay unreadable */

   c->attached_epoch = c->reply.host_epoch;
   rc = OURO_CLIENT_OK;

done:
   /* The mappings hold their own references; the descriptors can be closed. */
   for (int i = 0; i < OURO_ATTACH_FDS; i++)
      close(fds[i]);
   if (rc != OURO_CLIENT_OK)
      ouro_client_detach(c);
   return rc;
}

void ouro_client_detach(ouro_client_t *c)
{
   if (!c)
      return;
   ouro_region_unmap(&c->control);
   ouro_region_unmap(&c->arena);
   ouro_region_unmap(&c->qpair);
   memset(c, 0, sizeof *c);
}

/* Encode a frame (and inline payload) into the outbound ring. */
static ouro_client_result_t emit(ouro_client_t *c, uint16_t flags, uint32_t kind, uint64_t corr,
                                const void *payload, uint32_t len)
{
   if (!c || c->ctl == NULL)
      return OURO_CLIENT_ERR_ARG;
   /* This is the inline path (a large payload goes via ouro_client_publish_arena).
    * An inline payload sits after the 64-byte header, so it must fit the inline
    * budget AND leave the header room in the slot — a config with inline_budget ==
    * slot_size would otherwise let this overrun the ring slot. */
   if (len > 0 &&
       (len > c->reply.inline_budget || (uint64_t)OURO_WIRE_HDR_LEN + len > c->reply.slot_size))
      return OURO_CLIENT_ERR_PAYLOAD;
   if (len > 0 && !payload)
      return OURO_CLIENT_ERR_ARG;

   /* Fail fast if the host has gone; a stale mapping must not be written as if
    * it were live. */
   if (ouro_client_epoch_changed(c))
      return OURO_CLIENT_EPOCH;

   uint8_t *slot = ouro_ring_produce_begin(&c->qp.outbound);
   if (!slot)
      return OURO_CLIENT_WOULD_BLOCK;

   ouro_frame_t f;
   memset(&f, 0, sizeof f);
   f.hdr_flags = flags | (len ? OURO_F_INLINE : 0);
   f.wire_version = OURO_WIRE_VERSION;
   f.event_kind = kind;
   f.correlation_id = corr;
   f.src_handle = c->reply.handle_id;
   if (len)
   {
      f.payload_len = len;
      f.payload_ref = OURO_WIRE_HDR_LEN;
   }
   if (ouro_wire_encode(&f, slot, c->reply.slot_size) != OURO_WIRE_HDR_LEN)
      return OURO_CLIENT_ERR_ARG;
   if (len)
      memcpy(slot + OURO_WIRE_HDR_LEN, payload, len);
   ouro_ring_produce_commit(&c->qp.outbound);
   return OURO_CLIENT_OK;
}

ouro_client_result_t ouro_client_publish(ouro_client_t *c, uint32_t kind, const void *payload,
                                       uint32_t len)
{
   return emit(c, OURO_F_NOTIFICATION, kind, 0, payload, len);
}

ouro_client_result_t ouro_client_publish_arena(ouro_client_t *c, uint32_t kind, uint32_t lease_id,
                                             uint32_t generation, uint32_t len)
{
   if (!c || c->ctl == NULL)
      return OURO_CLIENT_ERR_ARG;
   /* A zero-length arena payload is a contradiction — there is nothing to lease.
    * len is bounded by the arena, not the inline budget; the lease table already
    * refused an over-large allocation, so no size check is duplicated here. */
   if (len == 0)
      return OURO_CLIENT_ERR_PAYLOAD;
   if (ouro_client_epoch_changed(c))
      return OURO_CLIENT_EPOCH;

   uint8_t *slot = ouro_ring_produce_begin(&c->qp.outbound);
   if (!slot)
      return OURO_CLIENT_WOULD_BLOCK;

   ouro_frame_t f;
   memset(&f, 0, sizeof f);
   f.hdr_flags = OURO_F_NOTIFICATION | OURO_F_ARENA;
   f.wire_version = OURO_WIRE_VERSION;
   f.event_kind = kind;
   f.src_handle = c->reply.handle_id;
   f.payload_ref = lease_id; /* ARENA (v2): payload_ref carries the lease id */
   f.generation = generation;
   f.payload_len = len;
   if (ouro_wire_encode(&f, slot, c->reply.slot_size) != OURO_WIRE_HDR_LEN)
      return OURO_CLIENT_ERR_ARG;
   ouro_ring_produce_commit(&c->qp.outbound);
   return OURO_CLIENT_OK;
}

ouro_client_result_t ouro_client_request(ouro_client_t *c, uint32_t kind, uint64_t correlation,
                                       const void *payload, uint32_t len)
{
   return ouro_client_request_fragment(c, kind, correlation, payload, len, 0);
}

ouro_client_result_t ouro_client_request_fragment(ouro_client_t *c, uint32_t kind,
                                                uint64_t correlation, const void *payload,
                                                uint32_t len, int more)
{
   if (correlation == 0)
      return OURO_CLIENT_ERR_ARG; /* a correlated pattern needs a nonzero id */
   if (more && len == 0)
      return OURO_CLIENT_ERR_ARG;
   return emit(c, (uint16_t)(OURO_F_REQUEST | (more ? OURO_F_MORE : 0)), kind, correlation, payload,
               len);
}

ouro_client_result_t ouro_client_reply(ouro_client_t *c, uint32_t kind, uint64_t correlation,
                                     const void *payload, uint32_t len)
{
   return ouro_client_reply_fragment(c, kind, correlation, payload, len, 0);
}

ouro_client_result_t ouro_client_reply_fragment(ouro_client_t *c, uint32_t kind, uint64_t correlation,
                                              const void *payload, uint32_t len, int more)
{
   if (correlation == 0)
      return OURO_CLIENT_ERR_ARG;
   if (more && len == 0)
      return OURO_CLIENT_ERR_ARG;
   return emit(c, (uint16_t)(OURO_F_REPLY | (more ? OURO_F_MORE : 0)), kind, correlation, payload,
               len);
}

ouro_client_result_t ouro_client_cancel(ouro_client_t *c, uint32_t kind, uint64_t correlation)
{
   if (correlation == 0)
      return OURO_CLIENT_ERR_ARG;
   return emit(c, OURO_F_CANCEL, kind, correlation, NULL, 0);
}

int ouro_client_arena_readable(const ouro_client_t *c)
{
   return c && c->lease_view != NULL;
}

static ouro_client_result_t send_release(ouro_client_t *c, ouro_client_lease_t l)
{
   uint8_t p[OURO_LEASE_RELEASE_LEN];
   for (unsigned i = 0; i < 4; i++)
   {
      p[OURO_LEASE_RELEASE_OFF_LEASE + i] = (uint8_t)(l.lease_id >> (8u * i));
      p[OURO_LEASE_RELEASE_OFF_GENERATION + i] = (uint8_t)(l.generation >> (8u * i));
   }
   return emit(c, OURO_F_NOTIFICATION | OURO_F_CONTROL, OURO_KIND_LEASE_RELEASE, 0, p, sizeof p);
}

ouro_client_result_t ouro_client_flush_releases(ouro_client_t *c)
{
   if (!c || c->ctl == NULL)
      return OURO_CLIENT_ERR_ARG;
   uint32_t sent = 0;
   while (sent < c->release_count)
   {
      ouro_client_result_t r = send_release(c, c->releases[sent]);
      if (r == OURO_CLIENT_WOULD_BLOCK)
         break;
      if (r == OURO_CLIENT_EPOCH)
         return r; /* the host restarted: every ref died with it */
      sent++;      /* sent, or unsendable: either way not retried */
   }
   memmove(c->releases, c->releases + sent, (c->release_count - sent) * sizeof c->releases[0]);
   c->release_count -= sent;
   return c->release_count ? OURO_CLIENT_WOULD_BLOCK : OURO_CLIENT_OK;
}

ouro_client_result_t ouro_client_poll(ouro_client_t *c, ouro_event_t *out)
{
   if (!c || !out || c->ctl == NULL)
      return OURO_CLIENT_ERR_ARG;

   /* After a host restart every mapping is stale — the inbound ring belongs to a
    * defunct region and will never carry a new event. Fail closed rather than
    * hand back a stale frame; the caller must re-attach. */
   if (ouro_client_epoch_changed(c))
      return OURO_CLIENT_EPOCH;

   /* Release the slot handed out by the previous poll — its payload pointer is
    * now invalid, which is the documented contract. */
   if (c->have_pending_read)
   {
      ouro_ring_consume_commit(&c->qp.inbound);
      c->have_pending_read = 0;
   }
   /* The arena span handed out by the previous poll is no longer the caller's
    * to read: return its ref (D12). */
   if (c->have_pending_lease)
   {
      c->releases[c->release_count++] = c->pending_lease;
      c->have_pending_lease = 0;
   }
   if (c->release_count && ouro_client_flush_releases(c) == OURO_CLIENT_EPOCH)
      return OURO_CLIENT_EPOCH;

   const uint8_t *slot = ouro_ring_consume_begin(&c->qp.inbound);
   if (!slot)
      return OURO_CLIENT_EMPTY;

   memset(out, 0, sizeof *out);
   if (ouro_wire_decode_checked(slot, c->reply.slot_size, c->reply.slot_size, c->arena_size,
                               &out->frame) != OURO_WIRE_OK)
   {
      /* A malformed inbound frame should never happen — the host encodes it —
       * but if it does, drop it rather than hand back garbage. */
      ouro_ring_consume_commit(&c->qp.inbound);
      return OURO_CLIENT_EMPTY;
   }
   if ((out->frame.hdr_flags & OURO_F_INLINE) && out->frame.payload_len > 0)
   {
      out->payload = slot + out->frame.payload_ref;
      out->payload_len = out->frame.payload_len;
   }
   else if ((out->frame.hdr_flags & OURO_F_ARENA) && c->lease_view)
   {
      /* Delivering an arena frame gave this client a consumer ref, which it owes
       * back whether or not the payload resolves. With the release backlog
       * full, leave the frame unread until the outbound ring drains. */
      if (c->release_count >= OURO_CLIENT_MAX_PENDING_RELEASES)
         return OURO_CLIENT_EMPTY;
      uint64_t off = 0;
      if (ouro_lease_view_resolve(c->lease_view, (uint32_t)out->frame.payload_ref,
                                 out->frame.generation, out->frame.payload_len, c->arena_size,
                                 &off) == OURO_REGION_OK)
      {
         out->payload = c->arena_base + off;
         out->payload_len = out->frame.payload_len;
      }
      c->pending_lease.lease_id = (uint32_t)out->frame.payload_ref;
      c->pending_lease.generation = out->frame.generation;
      c->have_pending_lease = 1;
   }
   c->have_pending_read = 1;
   return OURO_CLIENT_OK;
}

void ouro_client_heartbeat(ouro_client_t *c, uint64_t now)
{
   if (c && c->qp.hdr)
      atomic_store_explicit(&c->qp.hdr->client_heartbeat, now, memory_order_release);
}

int ouro_client_epoch_changed(const ouro_client_t *c)
{
   return c && c->ctl && ouro_control_epoch_changed(c->ctl, c->attached_epoch);
}

int ouro_client_control_lost(const ouro_client_t *c)
{
   return c && c->qp.hdr &&
          atomic_load_explicit(&c->qp.hdr->control_lost, memory_order_acquire) != 0;
}

const char *ouro_client_result_name(ouro_client_result_t r)
{
   switch (r)
   {
   case OURO_CLIENT_OK:
      return "OK";
   case OURO_CLIENT_WOULD_BLOCK:
      return "WOULD_BLOCK";
   case OURO_CLIENT_EMPTY:
      return "EMPTY";
   case OURO_CLIENT_DENIED:
      return "DENIED";
   case OURO_CLIENT_EPOCH:
      return "EPOCH";
   case OURO_CLIENT_ERR_ARG:
      return "ERR_ARG";
   case OURO_CLIENT_ERR_PAYLOAD:
      return "ERR_PAYLOAD";
   case OURO_CLIENT_ERR_OS:
      return "ERR_OS";
   case OURO_CLIENT_ERR_PROTOCOL:
      return "ERR_PROTOCOL";
   default:
      return "ERR_UNKNOWN";
   }
}
