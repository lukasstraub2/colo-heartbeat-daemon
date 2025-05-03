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

typedef struct QuitEarlyConfig {
    gboolean qemu_quit;
} QuitEarlyConfig;

struct SmokeTestcase {
    Coroutine coroutine;
    SmokeColodContext *sctx;
    const QuitEarlyConfig *config;
    gboolean do_quit, quit;
};

static gboolean _testcase_co(Coroutine *coroutine, SmokeTestcase *this) {
    SmokeColodContext *sctx = this->sctx;
    gchar *line;
    gsize len;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    co_recurse(ch_write_co(coroutine, sctx->client_ch,
                           "{'exec-colod': 'start'}\n", 1000));
    co_recurse(ch_readln_co(coroutine, sctx->client_ch, &line, &len, 1000));
    g_free(line);

    if (this->config->qemu_quit) {
        colod_shutdown_channel(sctx->qmp_ch);
        colod_shutdown_channel(sctx->qmp_yank_ch);
    }

    co_recurse(ch_write_co(coroutine, sctx->client_ch,
                           "{'exec-colod': 'quit'}\n", 1000));

    co_recurse(ch_readln_co(coroutine, sctx->client_ch, &line, &len, 1000));
    g_free(line);

    assert(!this->do_quit);
    while (!this->do_quit) {
        progress_source_add(coroutine->cb, this);
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

    colod_assert_remove_one_source(coroutine);
    return ret;
}

static SmokeTestcase *testcase_new(SmokeColodContext *sctx,
                                   const QuitEarlyConfig *config) {
    SmokeTestcase *this;
    Coroutine *coroutine;

    this = g_new0(SmokeTestcase, 1);
    coroutine = &this->coroutine;
    coroutine->cb = testcase_co;
    this->sctx = sctx;
    this->config = config;

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

static void test_run(gconstpointer opaque) {
    GError *errp = NULL;
    const QuitEarlyConfig *config = opaque;
    SmokeColodContext *sctx;
    SmokeTestcase *testcase;

    sctx = smoke_context_new(&errp);
    g_assert_true(sctx);

    testcase = testcase_new(sctx, config);

    daemon_mainloop(&sctx->cctx);

    testcase_free(testcase);
    smoke_context_free(sctx);
}

int main(int argc, char **argv) {
    smoke_init();

    g_test_init(&argc, &argv, NULL);

    g_test_add_data_func("/quit_early/normal",
                         &(QuitEarlyConfig) {
                             .qemu_quit = FALSE
                         }, test_run);
    g_test_add_data_func("/quit_early/qemu_quit",
                         &(QuitEarlyConfig) {
                             .qemu_quit = TRUE
                         }, test_run);

    return g_test_run();
}
