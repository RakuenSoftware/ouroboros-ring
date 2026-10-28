/* ouroboros/local_attach.h: attach a client to a host in the same process.
 *
 * A host that also runs some of its own clients (an observability consumer, an
 * in-process module) attaches them exactly as an external process would: the
 * same handshake, over a private SOCK_SEQPACKET socketpair whose host end is
 * served on a short-lived thread. Only the transport is local; admission, the
 * reply and the three descriptors are the ordinary attach (D1). Once the call
 * returns, the rings live in shared memory and both sockets are closed.
 *
 * This is a convenience of the C reference implementation, not part of the
 * contract: nothing on the wire tells a local attach from any other.
 */
#ifndef OUROBOROS_LOCAL_ATTACH_H
#define OUROBOROS_LOCAL_ATTACH_H 1

#include <ouroboros/client.h>
#include <ouroboros/host.h>

/* Attach c to h. With owned == 0 the host end goes through
 * ouro_host_serve_attach, so the host's admission callback decides; with
 * owned != 0 it goes through ouro_host_serve_attach_owned, the daemon-owned
 * same-process seam, and the caller MUST hold the lock it uses for every other
 * host mutation. Either way this function takes no lock of its own.
 *
 * Returns ouro_client_attach's result; OURO_CLIENT_ERR_OS if the socketpair or
 * the serving thread could not be created. Both sockets are close-on-exec, so a
 * child forked mid-attach inherits neither. */
ouro_client_result_t ouro_host_attach_local(ouro_host_t *h, ouro_client_t *c, int owned);

#endif /* OUROBOROS_LOCAL_ATTACH_H */
