/* local_attach.c: attach a client to a host in the same process. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

#include <ouroboros/local_attach.h>

struct serve_arg
{
   ouro_host_t *h;
   int fd;
   int owned;
};

static void *serve_thread(void *p)
{
   struct serve_arg *a = p;
   /* D15: this client is in the host's process, so it owes no heartbeat. */
   (void)ouro_host_serve_attach_in_process(a->h, a->fd, a->owned);
   return NULL;
}

static int private_pair(int sv[2])
{
#ifdef SOCK_CLOEXEC
   if (socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, sv) == 0)
      return 0;
   if (errno != EINVAL && errno != EPROTONOSUPPORT)
      return -1;
#endif
   if (socketpair(AF_UNIX, SOCK_SEQPACKET, 0, sv) != 0)
      return -1;
   /* Without atomic socket flags, set close-on-exec before anyone can fork. */
   if (fcntl(sv[0], F_SETFD, FD_CLOEXEC) < 0 || fcntl(sv[1], F_SETFD, FD_CLOEXEC) < 0)
   {
      close(sv[0]);
      close(sv[1]);
      return -1;
   }
   return 0;
}

ouro_client_result_t ouro_host_attach_local(ouro_host_t *h, ouro_client_t *c, int owned)
{
   int sv[2];
   if (!h || !c || private_pair(sv) != 0)
      return OURO_CLIENT_ERR_OS;
   struct serve_arg a = {.h = h, .fd = sv[1], .owned = owned};
   pthread_t t;
   if (pthread_create(&t, NULL, serve_thread, &a) != 0)
   {
      close(sv[0]);
      close(sv[1]);
      return OURO_CLIENT_ERR_OS;
   }
   ouro_client_result_t rc = ouro_client_attach(sv[0], c);
   pthread_join(t, NULL);
   close(sv[0]);
   close(sv[1]);
   return rc;
}
