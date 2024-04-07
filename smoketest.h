/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

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

    SmokeColodContext *sctx;
    SmokeTestcase *testcase;
} SmokeContext;

SmokeColodContext *smoke_context_new(SmokeContext *ctx, GError **errp);
void smoke_context_free(SmokeColodContext *sctx);

SmokeTestcase *testcase_new(SmokeContext *ctx);
void testcase_free(SmokeTestcase *this);

#endif // SOMOKETEST_H
