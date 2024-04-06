/*
 * COLO background daemon smoketest
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <syslog.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>
#include <assert.h>

#include <glib-2.0/glib.h>
#include <glib-2.0/glib-unix.h>

#include "base_types.h"
#include "smoketest.h"
#include "daemon.h"
#include "main_coroutine.h"
#include "coroutine.h"
#include "coroutine_stack.h"
#include "util.h"
#include "client.h"
#include "qmp.h"
#include "cpg.h"
#include "watchdog.h"

static FILE *trace = NULL;

void colod_trace(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    if (trace) {
        vfprintf(trace, fmt, args);
        fflush(trace);
    }

    va_end(args);
}

void colod_syslog(G_GNUC_UNUSED int pri, const char *fmt, ...) {
    va_list args;

    if (trace) {
        va_start(args, fmt);
        vfprintf(trace, fmt, args);
        fwrite("\n", 1, 1, trace);
        fflush(trace);
        va_end(args);
    }

    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    fwrite("\n", 1, 1, stderr);
    va_end(args);
}

static void colod_mainloop(ColodContext *ctx) {
    GError *local_errp = NULL;

    // g_main_context_default creates the global context on demand
    GMainContext *main_context = g_main_context_default();
    ctx->mainloop = g_main_loop_new(main_context, FALSE);

    ctx->qmp = qmp_new(ctx->qmp1_fd, ctx->qmp2_fd, ctx->qmp_timeout_low,
                       &local_errp);
    if (!ctx->qmp) {
        colod_syslog(LOG_ERR, "Failed to initialize qmp: %s",
                     local_errp->message);
        g_error_free(local_errp);
        exit(EXIT_FAILURE);
    }

    colod_main_coroutine(ctx);

    ctx->listener = client_listener_new(ctx->mngmt_listen_fd, ctx);
    ctx->watchdog = colod_watchdog_new(ctx);
    ctx->cpg = cpg_new(ctx->cpg, &local_errp);
    if (!ctx->cpg) {
        colod_syslog(LOG_ERR, "Failed to initialize cpg: %s",
                     local_errp->message);
        g_error_free(local_errp);
        exit(EXIT_FAILURE);
    }

    g_main_loop_run(ctx->mainloop);
    g_main_loop_unref(ctx->mainloop);
    ctx->mainloop = NULL;

    cpg_free(ctx->cpg);
    colod_raise_timeout_coroutine_free(ctx);
    colod_main_free(ctx->main_coroutine);
    colo_watchdog_free(ctx->watchdog);
    client_listener_free(ctx->listener);
    qmp_free(ctx->qmp);
}

static int smoke_open_mngmt(SmokeColodContext *sctx, SmokeContext *ctx,
                            GError **errp) {
    int sockfd, clientfd, ret;
    struct sockaddr_un address = { 0 };
    g_autofree char *path = NULL;

    path = g_strconcat(ctx->base_dir, "/colod.sock", NULL);
    if (strlen(path) >= sizeof(address.sun_path)) {
        colod_error_set(errp, "Management unix path too long");
        return -1;
    }
    strcpy(address.sun_path, path);
    address.sun_family = AF_UNIX;

    ret = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ret < 0) {
        colod_error_set(errp, "Failed to create management socket: %s",
                        g_strerror(errno));
        return -1;
    }
    sockfd = ret;

    unlink(path);
    ret = bind(sockfd, (const struct sockaddr *) &address, sizeof(address));
    if (ret < 0) {
        colod_error_set(errp, "Failed to bind management socket: %s",
                        g_strerror(errno));
        goto err;
    }

    ret = listen(sockfd, 2);
    if (ret < 0) {
        colod_error_set(errp, "Failed to listen management socket: %s",
                        g_strerror(errno));
        goto err;
    }

    ret = colod_fd_set_blocking(sockfd, FALSE, errp);
    if (ret < 0) {
        goto err;
    }

    sctx->cctx.mngmt_listen_fd = sockfd;

    ret = colod_unix_connect(path, errp);
    if (ret < 0) {
        colod_error_set(errp, "Failed to connect to management socket: %s",
                        g_strerror(errno));
        goto err;
    }
    clientfd = ret;

    sctx->client_ch = colod_create_channel(ret, errp);
    if (!sctx->client_ch) {
        close(clientfd);
        goto err;
    }

    return 0;

err:
    close(sockfd);
    return -1;
}

static int socketpair_channel(GIOChannel **channel, GError **errp) {
    int ret;
    int fd[2];

    ret = socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
    if (ret < 0) {
        colod_error_set(errp, "Failed to open qmp socketpair: %s",
                        g_strerror(errno));
        return -1;
    }

    *channel = colod_create_channel(fd[0], errp);
    if (!*channel) {
        close(fd[0]);
        close(fd[1]);
        return -1;
    }

    return fd[1];
}

static int smoke_open_qmp(SmokeColodContext *sctx, GError **errp) {
    int ret;

    ret = socketpair_channel(&sctx->qmp_ch, errp);
    if (ret < 0) {
        return -1;
    }
    sctx->cctx.qmp1_fd = ret;

    ret = socketpair_channel(&sctx->qmp_yank_ch, errp);
    if (ret < 0) {
        return -1;
    }
    sctx->cctx.qmp2_fd = ret;

    return 0;
}

static int smoke_parse_options(SmokeContext *ctx, int *argc, char ***argv,
                               GError **errp) {
    gboolean ret;
    GOptionContext *context;
    GOptionEntry entries[] =
    {
        {"base_directory", 'b', 0, G_OPTION_ARG_FILENAME, &ctx->base_dir, "The base directory to store logs and sockets", NULL},
        {"trace", 0, 0, G_OPTION_ARG_NONE, &ctx->do_trace, "Enable tracing", NULL},
        {0}
    };

    context = g_option_context_new("- qemu colo daemon smoketest");
    g_option_context_set_help_enabled(context, TRUE);
    g_option_context_add_main_entries(context, entries, 0);

    ret = g_option_context_parse(context, argc, argv, errp);
    g_option_context_free(context);
    if (!ret) {
        return -1;
    }

    if (!ctx->base_dir) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_FATAL,
                    "--base_directory needs to be given.");
        return -1;
    }

    return 0;
}

void smoke_fill_ctx(ColodContext *cctx, SmokeContext *ctx) {
    cctx->node_name = "teleclu-01";
    cctx->instance_name = "colo_test";
    cctx->base_dir = "";
    cctx->qmp_path = "";
    cctx->qmp_yank_path = "";
    cctx->daemonize = FALSE;
    cctx->qmp_timeout_low = 600;
    cctx->qmp_timeout_high = 10000;
    cctx->watchdog_interval = 0;
    cctx->do_trace = ctx->do_trace;
    cctx->primary_startup = FALSE;
}

int main(int argc, char **argv) {
    GError *errp = NULL;
    SmokeContext ctx_struct = { 0 };
    SmokeContext *ctx = &ctx_struct;
    int ret;

    ret = smoke_parse_options(ctx, &argc, &argv, &errp);
    if (ret < 0) {
        fprintf(stderr, "%s\n", errp->message);
        g_error_free(errp);
        exit(EXIT_FAILURE);
    }

    prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
    prctl(PR_SET_DUMPABLE, 1);

    signal(SIGPIPE, SIG_IGN); // TODO: Handle this properly

    if (ctx->do_trace) {
        gchar *path = g_strconcat(ctx->base_dir, "/trace.log", NULL);
        trace = fopen(path, "a");
        g_free(path);
    }

    smoke_fill_ctx(&ctx->sctx.cctx, ctx);
    ret = smoke_open_qmp(&ctx->sctx, &errp);
    if (ret < 0) {
        goto err;
    }

    ret = smoke_open_mngmt(&ctx->sctx, ctx, &errp);
    if (ret < 0) {
        goto err;
    }

    ctx->sctx.cctx.cpg = colod_open_cpg(&ctx->sctx.cctx, &errp);
    if (!ctx->sctx.cctx.cpg) {
        goto err;
    }

    ctx->testcase = testcase_new(ctx);
    colod_mainloop(&ctx->sctx.cctx);
    testcase_free(ctx->testcase);
    g_main_context_unref(g_main_context_default());
    g_io_channel_unref(ctx->sctx.client_ch);
    g_io_channel_unref(ctx->sctx.qmp_ch);
    g_io_channel_unref(ctx->sctx.qmp_yank_ch);

    g_free(ctx->base_dir);

    // cleanup pidfile, cpg, qmp and mgmt connection

    return EXIT_SUCCESS;

err:
    if (errp) {
        colod_syslog(LOG_ERR, "Fatal: %s", errp->message);
        g_error_free(errp);
    }
    exit(EXIT_FAILURE);
}
