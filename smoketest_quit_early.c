/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <glib-2.0/glib.h>

#include "base_types.h"
#include "smoketest.h"
#include "coroutine_stack.h"
#include "smoke_util.h"

struct SmokeTestcase {
    Coroutine coroutine;
    SmokeColodContext *sctx;
    gboolean autoquit, qemu_quit;
    gboolean do_quit, quit;
};

static gboolean _testcase_co(Coroutine *coroutine, SmokeTestcase *this) {
    SmokeColodContext *sctx = this->sctx;
    gchar *line;
    gsize len;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    if (this->qemu_quit) {
        colod_shutdown_channel(sctx->qmp_ch);
        colod_shutdown_channel(sctx->qmp_yank_ch);
    }

    if (this->autoquit) {
        co_recurse(ch_write_co(coroutine, sctx->client_ch,
                               "{'exec-colod': 'autoquit'}\n", 1000));
    } else {
        co_recurse(ch_write_co(coroutine, sctx->client_ch,
                               "{'exec-colod': 'quit'}\n", 1000));
    }

    co_recurse(ch_readln_co(coroutine, sctx->client_ch, &line, &len, 1000));
    g_free(line);

    if (this->autoquit && !this->qemu_quit) {
        g_timeout_add(500, coroutine->cb.plain, this);
        co_yield_int(G_SOURCE_REMOVE);

        colod_shutdown_channel(sctx->qmp_ch);
        colod_shutdown_channel(sctx->qmp_yank_ch);
    }

    assert(!this->do_quit);
    while (!this->do_quit) {
        progress_source_add(coroutine->cb.plain, this);
        co_yield_int(G_SOURCE_REMOVE);
    }
    this->quit = TRUE;
    co_end;

    return G_SOURCE_REMOVE;
}

static gboolean testcase_co(gpointer data) {
    SmokeTestcase *this = data;
    Coroutine *coroutine = data;
    gboolean ret;

    co_enter(coroutine, ret = _testcase_co(coroutine, this));
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    g_source_remove_by_user_data(coroutine);
    assert(!g_source_remove_by_user_data(coroutine));
    return ret;
}

static gboolean testcase_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return testcase_co(data);
}

static SmokeTestcase *testcase_new(SmokeColodContext *sctx,
                                   gboolean autoquit,
                                   gboolean qemu_quit) {
    SmokeTestcase *this;
    Coroutine *coroutine;

    this = g_new0(SmokeTestcase, 1);
    coroutine = &this->coroutine;
    coroutine->cb.plain = testcase_co;
    coroutine->cb.iofunc = testcase_co_wrap;
    this->sctx = sctx;
    this->autoquit = autoquit;
    this->qemu_quit = qemu_quit;

    sctx->cctx.qmp_timeout_low = 10;

    g_idle_add(testcase_co, this);
    return this;
}

static void testcase_free(SmokeTestcase *this) {
    this->do_quit = TRUE;

    while (!this->quit) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }

    g_free(this);
}

static int _test_run(SmokeContext *ctx, gboolean autoquit,
                     gboolean qemu_quit, GError **errp) {
    SmokeColodContext *sctx;
    SmokeTestcase *testcase;

    sctx = smoke_context_new(ctx, errp);
    if (!sctx) {
        return -1;
    }
    testcase = testcase_new(sctx, autoquit, qemu_quit);

    daemon_mainloop(&sctx->cctx);

    testcase_free(testcase);
    smoke_context_free(sctx);

    return 0;
}

int test_run(SmokeContext *ctx, GError **errp) {
    int ret;

    ret = _test_run(ctx, FALSE, FALSE, errp);
    if (ret < 0) {
        return -1;
    }

    ret = _test_run(ctx, TRUE, FALSE, errp);
    if (ret < 0) {
        return -1;
    }

    ret = _test_run(ctx, FALSE, TRUE, errp);
    if (ret < 0) {
        return -1;
    }

    ret = _test_run(ctx, TRUE, TRUE, errp);
    if (ret < 0) {
        return -1;
    }

    return 0;
}
