/* test_region.c: slice 3 of the ring feature tree.
 *
 * The regions are the D1 isolation boundary, so the tests check the boundary as
 * a fact rather than a convention:
 *
 *   - A client maps the control region PROT_READ. Writing it must FAULT, not be
 *     tolerated. That is verified by forking a child that writes the mapping and
 *     asserting it dies on SIGSEGV/SIGBUS — the enforced half of D2, proven
 *     against the MMU rather than asserted in prose.
 *
 *   - Every header a client reads is treated as written by another process:
 *     corrupt magic, version, or geometry is refused, not propagated into a
 *     mapping size or a ring walk.
 *
 *   - The D4 parameters are read back from the control region, so a value that
 *     was never compiled in survives create -> map -> attach.
 *
 *   - A stale host_epoch is detected.
 *
 *   - A queue-pair region round-trips real traffic through both of its rings
 *     while mapped, which is the proof that the region layout actually places
 *     two working rings where the header claims.
 */
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include <ouroboros/region.h>
#include <ouroboros/region_host.h>

static void must(int cond, const char *what)
{
   if (!cond)
   {
      fprintf(stderr, "FAIL: %s\n", what);
      abort();
   }
}

static void must_result(ouro_region_result_t got, ouro_region_result_t want, const char *what)
{
   if (got != want)
   {
      fprintf(stderr, "FAIL: %s: expected %s, got %s\n", what, ouro_region_result_name(want),
              ouro_region_result_name(got));
      abort();
   }
}

/* ------------------------------------------------------------------ */
/* control region                                                      */

static void test_control_roundtrip(void)
{
   /* Deliberately non-default parameters, so "read back from the region" is a
    * real claim and not "matches a compiled constant". */
   const uint32_t slot = 320, inl = 200, cap = 32;
   const uint64_t arena = 512 * 1024;

   ouro_region_t create;
   must_result(ouro_region_create("ctl", ouro_control_bytes(), &create), OURO_REGION_OK, "create");

   ouro_region_t host;
   must_result(ouro_region_map(create.fd, create.size, 1, &host), OURO_REGION_OK, "host map rw");
   must_result(ouro_control_init(&host, slot, inl, cap, arena), OURO_REGION_OK, "control init");

   /* A client maps the same fd read-only and reads the parameters back. */
   ouro_region_t client;
   must_result(ouro_region_map(create.fd, create.size, 0, &client), OURO_REGION_OK, "client map ro");
   ouro_control_t *c = NULL;
   must_result(ouro_control_attach(&client, &c), OURO_REGION_OK, "control attach");
   must(atomic_load_explicit(&c->slot_size, memory_order_relaxed) == slot, "slot_size read back");
   must(atomic_load_explicit(&c->inline_budget, memory_order_relaxed) == inl,
        "inline_budget read back");
   must(atomic_load_explicit(&c->queue_capacity, memory_order_relaxed) == cap,
        "capacity read back");
   must(atomic_load_explicit(&c->arena_size, memory_order_relaxed) == arena, "arena read back");

   /* Epoch: cache it, bump it on the host mapping, detect the change. */
   uint64_t attached = ouro_control_epoch(c);
   must(attached == 1, "fresh epoch is 1");
   must(!ouro_control_epoch_changed(c, attached), "no change yet");
   ouro_control_bump_epoch((ouro_control_t *)host.base);
   must(ouro_control_epoch_changed(c, attached), "client sees the epoch bump");

   ouro_region_unmap(&client);
   ouro_region_unmap(&host);
   close(create.fd);
   printf("  control: params read back from the region, epoch bump detected\n");
}

#define RO_FAULT_SENTINEL 42

static void ro_fault_handler(int sig)
{
   (void)sig;
   _exit(RO_FAULT_SENTINEL);
}

/* Fork a child that writes the read-only control mapping. The write must fault.
 * Doing it in a child keeps the fault from taking the test down, and turns "is
 * the mapping really read-only" into a checked outcome.
 *
 * How the fault surfaces depends on the build: on a plain build the child's own
 * SIGSEGV/SIGBUS handler runs and exits with a sentinel; under ASAN, ASAN's
 * deadly-signal handler runs first, reports the SEGV, and aborts the child with
 * SIGABRT. Both prove the write faulted. The one outcome that fails the test is
 * the child exiting 0 — that would mean the write to a PROT_READ page was
 * tolerated. */
static void test_control_readonly_faults(void)
{
   ouro_region_t create;
   must_result(ouro_region_create("ctl-ro", ouro_control_bytes(), &create), OURO_REGION_OK, "create");
   ouro_region_t host;
   must_result(ouro_region_map(create.fd, create.size, 1, &host), OURO_REGION_OK, "host map");
   must_result(ouro_control_init(&host, 256, 192, 16, 4096), OURO_REGION_OK, "init");

   ouro_region_t ro;
   must_result(ouro_region_map(create.fd, create.size, 0, &ro), OURO_REGION_OK, "map ro");

   fflush(stdout);
   pid_t pid = fork();
   must(pid >= 0, "fork");
   if (pid == 0)
   {
      signal(SIGSEGV, ro_fault_handler);
      signal(SIGBUS, ro_fault_handler);
      /* Child: this store is to a PROT_READ page. It must fault. */
      volatile uint32_t *p = (volatile uint32_t *)ro.base;
      *p = 0xdeadbeef;
      _exit(0); /* reached only if the write did NOT fault */
   }

   int status = 0;
   must(waitpid(pid, &status, 0) == pid, "waitpid");

   int faulted = 0;
   const char *how = "?";
   if (WIFEXITED(status) && WEXITSTATUS(status) == RO_FAULT_SENTINEL)
   {
      faulted = 1;
      how = "handler";
   }
   else if (WIFSIGNALED(status))
   {
      int sig = WTERMSIG(status);
      faulted = (sig == SIGSEGV || sig == SIGBUS || sig == SIGABRT);
      how = (sig == SIGABRT) ? "sanitizer-abort" : "signal";
   }
   must(faulted, "write to the read-only mapping faulted (child did not exit 0)");

   ouro_region_unmap(&ro);
   ouro_region_unmap(&host);
   close(create.fd);
   printf("  control read-only: a client write faults (%s)\n", how);
}

static void test_control_corruption_refused(void)
{
   ouro_region_t create;
   must_result(ouro_region_create("ctl-bad", ouro_control_bytes(), &create), OURO_REGION_OK, "create");
   ouro_region_t host;
   must_result(ouro_region_map(create.fd, create.size, 1, &host), OURO_REGION_OK, "map");
   must_result(ouro_control_init(&host, 256, 192, 16, 4096), OURO_REGION_OK, "init");
   ouro_control_t *c = (ouro_control_t *)host.base;
   ouro_control_t *dummy;

   uint32_t u32;
   uint64_t u64;

   u32 = atomic_load_explicit(&c->magic, memory_order_relaxed);
   atomic_store_explicit(&c->magic, 0, memory_order_relaxed);
   must_result(ouro_control_attach(&host, &dummy), OURO_REGION_ERR_MAGIC, "bad magic refused");
   atomic_store_explicit(&c->magic, u32, memory_order_relaxed);

   u32 = atomic_load_explicit(&c->layout_version, memory_order_relaxed);
   atomic_store_explicit(&c->layout_version, u32 + 1, memory_order_relaxed);
   must_result(ouro_control_attach(&host, &dummy), OURO_REGION_ERR_VERSION, "bad version refused");
   atomic_store_explicit(&c->layout_version, u32, memory_order_relaxed);

   u32 = atomic_load_explicit(&c->slot_size, memory_order_relaxed);
   atomic_store_explicit(&c->slot_size, 0, memory_order_relaxed);
   must_result(ouro_control_attach(&host, &dummy), OURO_REGION_ERR_GEOMETRY,
               "zero slot_size refused");
   atomic_store_explicit(&c->slot_size, u32, memory_order_relaxed);

   /* inline_budget larger than the slot is nonsense a client must not size on. */
   atomic_store_explicit(&c->inline_budget, u32 + 1, memory_order_relaxed);
   must_result(ouro_control_attach(&host, &dummy), OURO_REGION_ERR_GEOMETRY,
               "inline_budget past slot refused");
   atomic_store_explicit(&c->inline_budget, 192, memory_order_relaxed);

   u64 = atomic_load_explicit(&c->arena_size, memory_order_relaxed);
   atomic_store_explicit(&c->arena_size, 0, memory_order_relaxed);
   must_result(ouro_control_attach(&host, &dummy), OURO_REGION_ERR_GEOMETRY,
               "zero arena_size refused");
   atomic_store_explicit(&c->arena_size, u64, memory_order_relaxed);

   u32 = atomic_load_explicit(&c->spec_version, memory_order_relaxed);
   atomic_store_explicit(&c->spec_version, u32 + 1, memory_order_relaxed);
   must_result(ouro_control_attach(&host, &dummy), OURO_REGION_ERR_VERSION,
               "bad spec_version refused");
   atomic_store_explicit(&c->spec_version, u32, memory_order_relaxed);

   /* queue_capacity sizes the queue-pair rings; a non-power-of-two must be
    * refused here rather than sizing a ring on it later. */
   u32 = atomic_load_explicit(&c->queue_capacity, memory_order_relaxed);
   atomic_store_explicit(&c->queue_capacity, 15, memory_order_relaxed);
   must_result(ouro_control_attach(&host, &dummy), OURO_REGION_ERR_GEOMETRY,
               "bad queue_capacity refused");
   atomic_store_explicit(&c->queue_capacity, 0, memory_order_relaxed);
   must_result(ouro_control_attach(&host, &dummy), OURO_REGION_ERR_GEOMETRY,
               "zero queue_capacity refused");
   atomic_store_explicit(&c->queue_capacity, u32, memory_order_relaxed);

   /* And a valid header still attaches after all that poking. */
   must_result(ouro_control_attach(&host, &dummy), OURO_REGION_OK, "restored header attaches");

   ouro_region_unmap(&host);
   close(create.fd);
   printf("  control corruption: magic, version, and geometry all refused\n");
}

/* A short backing object must be refused before it can fault, not merely a
 * short size claim. Two real cases:
 *   1. Mapping MORE than the memfd holds — mmap would succeed and later touches
 *      would SIGBUS. ouro_region_map must refuse it up front.
 *   2. A memfd large enough to map but too small for the claimed layout — attach
 *      must refuse rather than walk past the backing. */
static void test_short_backing(void)
{
   /* Case 1: a genuinely short memfd, asked to map beyond its length. */
   ouro_region_t small;
   must_result(ouro_region_create("short-fd", 64, &small), OURO_REGION_OK, "create small");
   /* small.size is one page (rounded). Ask to map two pages of a one-page fd. */
   ouro_region_t over;
   must_result(ouro_region_map(small.fd, small.size + small.size, 0, &over), OURO_REGION_ERR_SIZE,
               "map beyond the backing fd is refused before it can SIGBUS");
   close(small.fd);

   /* Case 2: a one-page region is fine to map, but a queue-pair layout that
    * needs more than a page must be refused at init/attach, not walked past. */
   ouro_region_t page;
   must_result(ouro_region_create("one-page", 64, &page), OURO_REGION_OK, "create page");
   ouro_region_t mapped;
   must_result(ouro_region_map(page.fd, page.size, 1, &mapped), OURO_REGION_OK, "map one page");
   /* 4096-byte slots x 64 needs far more than a page. */
   must_result(ouro_qpair_init(&mapped, 4096, 64), OURO_REGION_ERR_SIZE,
               "qpair layout larger than the mapping is refused");
   ouro_region_unmap(&mapped);
   close(page.fd);
   printf("  short backing: over-length map and over-large layout both refused\n");
}

/* ------------------------------------------------------------------ */
/* queue-pair region                                                   */

static void test_qpair_roundtrip(void)
{
   const uint32_t slot = 128, cap = 16;
   size_t need = ouro_qpair_bytes(slot, cap);
   must(need > 0, "qpair geometry valid");

   ouro_region_t create;
   must_result(ouro_region_create("qp", need, &create), OURO_REGION_OK, "create");
   ouro_region_t host;
   must_result(ouro_region_map(create.fd, create.size, 1, &host), OURO_REGION_OK, "map");
   must_result(ouro_qpair_init(&host, slot, cap), OURO_REGION_OK, "qpair init");

   /* Attach as the host would, and drive both rings — inbound host->client,
    * outbound client->host — through the mapped region. */
   ouro_qpair_t qp;
   must_result(ouro_qpair_attach(&host, &qp), OURO_REGION_OK, "qpair attach");
   must(ouro_ring_capacity(&qp.inbound) == cap, "inbound capacity");
   must(ouro_ring_capacity(&qp.outbound) == cap, "outbound capacity");
   must(atomic_load_explicit(&qp.hdr->control_credits, memory_order_relaxed) ==
            OURO_CONTROL_CREDITS_DEFAULT,
        "control-class reserve initialised");

   /* Both rings, driven independently with distinct payloads, so a defect in
    * the inbound ring's placement or init cannot hide behind the outbound one.
    * inbound is host->client (0x2000+), outbound is client->host (0x1000+). */
   ouro_ring_t *rings[2] = {&qp.inbound, &qp.outbound};
   uint32_t bases[2] = {0x2000, 0x1000};
   for (int r = 0; r < 2; r++)
   {
      for (uint32_t i = 0; i < cap; i++)
      {
         void *s = ouro_ring_produce_begin(rings[r]);
         must(s != NULL, "produce into ring");
         *(uint32_t *)s = bases[r] + i;
         ouro_ring_produce_commit(rings[r]);
      }
      must(ouro_ring_produce_begin(rings[r]) == NULL, "ring full at capacity");
      for (uint32_t i = 0; i < cap; i++)
      {
         const void *s = ouro_ring_consume_begin(rings[r]);
         must(s != NULL, "consume ring");
         must(*(const uint32_t *)s == bases[r] + i, "ring FIFO through the region");
         ouro_ring_consume_commit(rings[r]);
      }
      must(ouro_ring_count(rings[r]) == 0, "ring drained");
   }

   /* Corruption of the qpair header surface: magic, geometry, and offsets. */
   ouro_qpair_t dummy;
   struct
   {
      const char *what;
      _Atomic uint32_t *field;
      uint32_t bad;
      ouro_region_result_t want;
   } lies[] = {
       {"bad magic", &qp.hdr->magic, 0, OURO_REGION_ERR_MAGIC},
       {"bad slot_size", &qp.hdr->slot_size, 0, OURO_REGION_ERR_GEOMETRY},
       {"bad capacity", &qp.hdr->capacity, 15, OURO_REGION_ERR_GEOMETRY},
       {"moved inbound offset", &qp.hdr->inbound_off, 8, OURO_REGION_ERR_GEOMETRY},
       {"moved outbound offset", &qp.hdr->outbound_off,
        atomic_load_explicit(&qp.hdr->outbound_off, memory_order_relaxed) + 64,
        OURO_REGION_ERR_GEOMETRY},
   };
   for (size_t i = 0; i < sizeof lies / sizeof lies[0]; i++)
   {
      uint32_t saved = atomic_load_explicit(lies[i].field, memory_order_relaxed);
      atomic_store_explicit(lies[i].field, lies[i].bad, memory_order_relaxed);
      must_result(ouro_qpair_attach(&host, &dummy), lies[i].want, lies[i].what);
      atomic_store_explicit(lies[i].field, saved, memory_order_relaxed);
   }
   must_result(ouro_qpair_attach(&host, &dummy), OURO_REGION_OK, "restored qpair attaches");

   ouro_region_unmap(&host);
   close(create.fd);
   printf("  queue-pair: both rings round-trip independently, header lies refused\n");
}

/* ------------------------------------------------------------------ */
/* arena region                                                        */

/* Corrupt the arena magic in place and confirm attach refuses it. Separated
 * from the body below only for readability. */
static void arena_corruption_refused(ouro_region_t *host)
{
   uint8_t *base;
   uint64_t size;
   uint32_t *magic = (uint32_t *)host->base; /* first 4 bytes are the arena magic */
   uint32_t saved = *magic;
   *magic = 0;
   must_result(ouro_arena_region_attach(host, &base, &size), OURO_REGION_ERR_MAGIC,
               "bad arena magic refused");
   *magic = saved;

   /* A size the mapping cannot back must be refused, not walked past. The size
    * field is the u64 at offset 8 of the arena header. */
   uint64_t *szp = (uint64_t *)((uint8_t *)host->base + 8);
   uint64_t saved_sz = *szp;
   *szp = host->size + (1u << 20); /* claim far more than is mapped */
   must_result(ouro_arena_region_attach(host, &base, &size), OURO_REGION_ERR_SIZE,
               "arena size larger than the mapping refused");
   *szp = 0;
   must_result(ouro_arena_region_attach(host, &base, &size), OURO_REGION_ERR_GEOMETRY,
               "zero arena size refused");
   *szp = saved_sz;
   must_result(ouro_arena_region_attach(host, &base, &size), OURO_REGION_OK,
               "restored arena attaches");
}

static void test_arena_region(void)
{
   const uint64_t arena = 256 * 1024;
   size_t need = ouro_arena_region_bytes(arena);
   must(need > 0, "arena geometry valid");

   ouro_region_t create;
   must_result(ouro_region_create("arena", need, &create), OURO_REGION_OK, "create");
   ouro_region_t host;
   must_result(ouro_region_map(create.fd, create.size, 1, &host), OURO_REGION_OK, "map");
   must_result(ouro_arena_region_init(&host, arena), OURO_REGION_OK, "arena init");

   uint8_t *base = NULL;
   uint64_t size = 0;
   must_result(ouro_arena_region_attach(&host, &base, &size), OURO_REGION_OK, "arena attach");
   must(size == arena, "arena size read back");
   must(base > (uint8_t *)host.base, "arena base is past the header");
   /* The usable arena is inside the mapping. */
   must(base + size <= (uint8_t *)host.base + host.size, "arena fits the mapping");

   arena_corruption_refused(&host);

   ouro_region_unmap(&host);
   close(create.fd);
   printf("  arena: base and size validated, header corruption refused\n");
}

/* D12: the lease view after the control header. A host region advertises it;
 * a header-only mapping (an older host, or a client that mapped too little)
 * has none; a corrupt advertisement is refused; resolve checks every claim. */
static void test_lease_view(void)
{
   ouro_region_t create;
   must(ouro_control_bytes() == ouro_control_header_bytes() + OURO_LEASE_VIEW_SLOTS * 16u,
        "control region = header + one 16-byte entry per lease");
   must_result(ouro_region_create("ctl-view", ouro_control_bytes(), &create), OURO_REGION_OK,
               "create");
   ouro_region_t host;
   must_result(ouro_region_map(create.fd, create.size, 1, &host), OURO_REGION_OK, "map");
   must_result(ouro_control_init(&host, 256, 192, 16, 4096), OURO_REGION_OK, "init");
   ouro_control_t *c = NULL;
   must_result(ouro_control_attach(&host, &c), OURO_REGION_OK, "attach");
   must(atomic_load(&c->flags) & OURO_CONTROL_F_LEASE_VIEW, "the view is advertised");
   must(atomic_load(&c->lease_view_slots) == OURO_LEASE_VIEW_SLOTS, "slot count");
   must(atomic_load(&c->lease_view_off) == ouro_control_header_bytes(), "after the header");

   ouro_lease_view_t *w = ouro_control_lease_view_host(&host);
   must(w != NULL, "the host has a writable view");
   const ouro_lease_view_t *v = NULL;
   must_result(ouro_control_lease_view(&host, c, &v), OURO_REGION_OK, "a client view");
   must(v == w, "one table");

   /* A header-only mapping sees no view. */
   ouro_region_t hdr;
   must_result(ouro_region_map(create.fd, ouro_control_header_bytes(), 0, &hdr), OURO_REGION_OK,
               "header-only map");
   ouro_control_t *hc = NULL;
   must_result(ouro_control_attach(&hdr, &hc), OURO_REGION_OK, "header-only attach still works");
   must_result(ouro_control_lease_view(&hdr, hc, &v), OURO_REGION_ERR_SIZE, "but has no view");
   ouro_region_unmap(&hdr);

   /* Resolve. Entry 5 holds generation 3 at offset 128, len 256, in a 4096 arena. */
   uint64_t off = 0;
   atomic_store(&w[5].offset, 128);
   atomic_store(&w[5].len, 256);
   atomic_store(&w[5].generation, 3);
   must_result(ouro_lease_view_resolve(w, 5, 3, 200, 4096, &off), OURO_REGION_OK, "live lease");
   must(off == 128, "offset");
   must_result(ouro_lease_view_resolve(w, 5, 2, 200, 4096, &off), OURO_REGION_ERR_GEOMETRY,
               "stale generation refused");
   must_result(ouro_lease_view_resolve(w, 5, 3, 257, 4096, &off), OURO_REGION_ERR_GEOMETRY,
               "payload longer than the span refused");
   must_result(ouro_lease_view_resolve(w, 5, 3, 200, 300, &off), OURO_REGION_ERR_GEOMETRY,
               "span past the arena refused");
   must_result(ouro_lease_view_resolve(w, OURO_LEASE_VIEW_SLOTS, 3, 1, 4096, &off),
               OURO_REGION_ERR_GEOMETRY, "lease id past the table refused");
   must_result(ouro_lease_view_resolve(w, 5, 0, 1, 4096, &off), OURO_REGION_ERR_ARG,
               "generation 0 (no lease) refused");
   atomic_store(&w[5].generation, 0);
   must_result(ouro_lease_view_resolve(w, 5, 3, 200, 4096, &off), OURO_REGION_ERR_GEOMETRY,
               "a retired entry refused");

   /* A corrupt advertisement is refused at attach. */
   atomic_store(&c->lease_view_slots, OURO_LEASE_VIEW_SLOTS + 1);
   must_result(ouro_control_attach(&host, &hc), OURO_REGION_ERR_GEOMETRY, "wrong slot count");
   atomic_store(&c->lease_view_slots, OURO_LEASE_VIEW_SLOTS);
   atomic_store(&c->lease_view_off, 64);
   must_result(ouro_control_attach(&host, &hc), OURO_REGION_ERR_GEOMETRY, "wrong offset");
   atomic_store(&c->lease_view_off, ouro_control_header_bytes());

   ouro_region_unmap(&host);
   close(create.fd);
   printf("  lease view: advertised, mapped, resolved, and validated (D12)\n");
}

int main(void)
{
   printf("test_region:\n");
   test_control_roundtrip();
   test_control_readonly_faults();
   test_control_corruption_refused();
   test_short_backing();
   test_qpair_roundtrip();
   test_arena_region();
   test_lease_view();
   printf("test_region: OK\n");
   return 0;
}
