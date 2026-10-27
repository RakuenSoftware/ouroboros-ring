/* tmpdir.h: where ring tests make their scratch directories.
 *
 * TMPDIR when it names an absolute path, else /tmp, so a run under a private or
 * quota-limited temp directory never leaks into the shared one. */
#ifndef OUROBOROS_TEST_TMPDIR_H
#define OUROBOROS_TEST_TMPDIR_H

#include <stdlib.h>

static inline const char *test_tmpdir(void)
{
   const char *dir = getenv("TMPDIR");
   return dir && dir[0] == '/' ? dir : "/tmp";
}

#endif
