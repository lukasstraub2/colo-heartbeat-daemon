#ifndef SOMOKETEST_H
#define SOMOKETEST_H

#include "daemon.h"

typedef struct SmokeTestcase SmokeTestcase;

typedef struct SmokeColodContext {
    ColodContext cctx;
    GIOChannel *qmp_ch, *qmp_yank_ch;
    GIOChannel *client_ch;
} SmokeColodContext;

typedef struct SmokeContext {
    gchar *base_dir;
    gboolean do_trace;

    SmokeColodContext sctx;
    SmokeTestcase *testcase;
} SmokeContext;

SmokeTestcase *testcase_new(SmokeContext *ctx);
void testcase_free(SmokeTestcase *this);

#endif // SOMOKETEST_H
