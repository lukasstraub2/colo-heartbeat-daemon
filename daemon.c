/*
 * COLO background daemon
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

#include <corosync/cpg.h>
#include <corosync/corotypes.h>

#include "base_types.h"
#include "daemon.h"
#include "main_coroutine.h"
#include "coroutine.h"
#include "coroutine_stack.h"
#include "json_util.h"
#include "util.h"
#include "client.h"
#include "qmp.h"
#include "watchdog.h"

static FILE *trace = NULL;
static gboolean do_syslog = FALSE;

void colod_trace(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    if (trace) {
        vfprintf(trace, fmt, args);
        fflush(trace);
    }

    va_end(args);
}

void colod_syslog(int pri, const char *fmt, ...) {
    va_list args;

    if (trace) {
        va_start(args, fmt);
        vfprintf(trace, fmt, args);
        fwrite("\n", 1, 1, trace);
        fflush(trace);
        va_end(args);
    }

    va_start(args, fmt);
    if (do_syslog) {
        vsyslog(pri, fmt, args);
    } else {
        vfprintf(stderr, fmt, args);
        fwrite("\n", 1, 1, stderr);
    }
    va_end(args);
}

static void colod_cpg_deliver(cpg_handle_t handle,
                              G_GNUC_UNUSED const struct cpg_name *group_name,
                              uint32_t nodeid,
                              G_GNUC_UNUSED uint32_t pid,
                              void *msg,
                              size_t msg_len) {
    ColodContext *ctx;
    uint32_t conv;
    uint32_t myid;

    cpg_context_get(handle, (void**) &ctx);
    cpg_local_get(handle, &myid);

    if (msg_len != sizeof(conv)) {
        log_error_fmt("Got message of invalid length %zu", msg_len);
        return;
    }
    conv = ntohl((*(uint32_t*)msg));

    switch (conv) {
        case MESSAGE_FAILOVER:
            if (nodeid == myid) {
                colod_event_queue(ctx, EVENT_FAILOVER_WIN, "");
            } else {
                colod_event_queue(ctx, EVENT_PEER_FAILOVER, "");
            }
        break;

        case MESSAGE_FAILED:
            if (nodeid != myid) {
                log_error("Peer failed");
                ctx->peer_failed = TRUE;
                colod_event_queue(ctx, EVENT_PEER_FAILED, "got MESSAGE_FAILED");
            }
        break;
    }
}

static void colod_cpg_confchg(cpg_handle_t handle,
    G_GNUC_UNUSED const struct cpg_name *group_name,
    G_GNUC_UNUSED const struct cpg_address *member_list,
    G_GNUC_UNUSED size_t member_list_entries,
    G_GNUC_UNUSED const struct cpg_address *left_list,
    size_t left_list_entries,
    G_GNUC_UNUSED const struct cpg_address *joined_list,
    G_GNUC_UNUSED size_t joined_list_entries) {
    ColodContext *ctx;

    cpg_context_get(handle, (void**) &ctx);

    if (left_list_entries) {
        log_error("Peer failed");
        ctx->peer_failed = TRUE;
        colod_event_queue(ctx, EVENT_PEER_FAILED, "peer left cpg group");
    }
}

static void colod_cpg_totem_confchg(G_GNUC_UNUSED cpg_handle_t handle,
                                    G_GNUC_UNUSED struct cpg_ring_id ring_id,
                                    G_GNUC_UNUSED uint32_t member_list_entries,
                                    G_GNUC_UNUSED const uint32_t *member_list) {

}

static gboolean colod_cpg_readable(G_GNUC_UNUSED gint fd,
                                   G_GNUC_UNUSED GIOCondition events,
                                   gpointer data) {
    ColodContext *ctx = data;
    cpg_dispatch(ctx->cpg_handle, CS_DISPATCH_ALL);
    return G_SOURCE_CONTINUE;
}

void colod_cpg_send(ColodContext *ctx, uint32_t message) {
    struct iovec vec;
    uint32_t conv = htonl(message);

    if (ctx->disable_cpg) {
        if (message == MESSAGE_FAILOVER) {
            colod_event_queue(ctx, EVENT_FAILOVER_WIN,
                              "running without corosync");
        }
        return;
    }

    vec.iov_len = sizeof(conv);
    vec.iov_base = &conv;
    cpg_mcast_joined(ctx->cpg_handle, CPG_TYPE_AGREED, &vec, 1);
}

static gboolean colod_hup_cb(G_GNUC_UNUSED GIOChannel *channel,
                             G_GNUC_UNUSED GIOCondition revents,
                             gpointer data) {
    ColodContext *ctx = data;

    log_error("qemu quit");
    ctx->qemu_quit = TRUE;
    colod_event_queue(ctx, EVENT_QEMU_QUIT, "qmp hup");
    return G_SOURCE_REMOVE;
}

static void colod_qmp_event_cb(gpointer data, ColodQmpResult *result) {
    ColodContext *ctx = data;
    const gchar *event;

    event = get_member_str(result->json_root, "event");

    if (!strcmp(event, "QUORUM_REPORT_BAD")) {
        const gchar *node, *type;
        node = get_member_member_str(result->json_root, "data", "node-name");
        type = get_member_member_str(result->json_root, "data", "type");

        if (!strcmp(node, "nbd0")) {
            if (!!strcmp(type, "read")) {
                colod_event_queue(ctx, EVENT_FAILOVER_SYNC,
                                  "nbd write/flush error");
            }
        } else {
            if (!!strcmp(type, "read")) {
                colod_event_queue(ctx, EVENT_YELLOW,
                                  "local disk write/flush error");
            }
        }
    } else if (!strcmp(event, "COLO_EXIT")) {
        const gchar *reason;
        reason = get_member_member_str(result->json_root, "data", "reason");

        if (!strcmp(reason, "error")) {
            colod_event_queue(ctx, EVENT_FAILOVER_SYNC, "COLO_EXIT");
        }
    } else if (!strcmp(event, "RESET")) {
        colod_raise_timeout_coroutine(ctx);
    }
}

static void colod_mainloop(ColodContext *ctx) {
    GError *local_errp = NULL;

    // g_main_context_default creates the global context on demand
    ctx->mainctx = g_main_context_default();
    ctx->mainloop = g_main_loop_new(ctx->mainctx, FALSE);

    ctx->qmp = qmp_new(ctx->qmp1_fd, ctx->qmp2_fd, ctx->qmp_timeout_low,
                       &local_errp);
    if (local_errp) {
        colod_syslog(LOG_ERR, "Failed to initialize qmp: %s",
                     local_errp->message);
        g_error_free(local_errp);
        exit(EXIT_FAILURE);
    }

    ctx->listener = client_listener_new(ctx->mngmt_listen_fd, ctx);
    ctx->watchdog = colod_watchdog_new(ctx);
    colod_main_coroutine(ctx);
    qmp_add_notify_event(ctx->qmp, colod_qmp_event_cb, ctx);
    qmp_hup_source(ctx->qmp, colod_hup_cb, ctx);
    if (!ctx->disable_cpg) {
        ctx->cpg_source_id = g_unix_fd_add(ctx->cpg_fd, G_IO_IN | G_IO_HUP,
                                           colod_cpg_readable, ctx);
    }

    g_main_loop_run(ctx->mainloop);
    g_main_loop_unref(ctx->mainloop);
    ctx->mainloop = NULL;

    if (ctx->cpg_source_id) {
        g_source_remove(ctx->cpg_source_id);
    }
    qmp_del_notify_event(ctx->qmp, colod_qmp_event_cb, ctx);
    colod_raise_timeout_coroutine_free(ctx);
    colod_main_free(ctx);
    colo_watchdog_free(ctx->watchdog);
    client_listener_free(ctx->listener);
    qmp_free(ctx->qmp);

    g_main_context_unref(ctx->mainctx);
}

cpg_model_v1_data_t cpg_data = {
    CPG_MODEL_V1,
    colod_cpg_deliver,
    colod_cpg_confchg,
    colod_cpg_totem_confchg,
    0
};

static int colod_open_cpg(ColodContext *ctx, GError **errp) {
    cs_error_t ret;
    int fd;
    struct cpg_name name;

    if (strlen(ctx->instance_name) >= sizeof(name.value)) {
        colod_error_set(errp, "Instance name too long");
        return -1;
    }
    strcpy(name.value, ctx->instance_name);
    name.length = strlen(name.value);

    ret = cpg_model_initialize(&ctx->cpg_handle, CPG_MODEL_V1,
                               (cpg_model_data_t*) &cpg_data, ctx);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to initialize cpg: %s", cs_strerror(ret));
        return -1;
    }

    ret = cpg_join(ctx->cpg_handle, &name);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to join cpg group: %s", cs_strerror(ret));
        goto err;
    }

    ret = cpg_fd_get(ctx->cpg_handle, &fd);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to get cpg file descriptor: %s",
                        cs_strerror(ret));
        goto err_joined;
    }

    ctx->cpg_fd = fd;
    return 0;

err_joined:
    cpg_leave(ctx->cpg_handle, &name);
err:
    cpg_finalize(ctx->cpg_handle);
    return -1;
}

static int colod_open_mngmt(ColodContext *ctx, GError **errp) {
    int sockfd, ret;
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

    ctx->mngmt_listen_fd = sockfd;
    return 0;

err:
    close(sockfd);
    return -1;
}

static int colod_open_qmp(ColodContext *ctx, GError **errp) {
    int ret;

    ret = colod_unix_connect(ctx->qmp_path, errp);
    if (ret < 0) {
        return -1;
    }
    ctx->qmp1_fd = ret;

    ret = colod_unix_connect(ctx->qmp_yank_path, errp);
    if (ret < 0) {
        close(ctx->qmp1_fd);
        ctx->qmp1_fd = 0;
        return -1;
    }
    ctx->qmp2_fd = ret;

    return 0;
}

static int colod_daemonize(ColodContext *ctx) {
    GError *local_errp = NULL;
    gchar *path;
    int logfd, pipefd, ret;

    pipefd = os_daemonize();

    path = g_strconcat(ctx->base_dir, "/colod.log", NULL);
    logfd = open(path, O_WRONLY | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR);
    g_free(path);

    if (logfd < 0) {
        openlog("colod", LOG_PID, LOG_DAEMON);
        syslog(LOG_ERR, "Fatal: Unable to open log file");
        exit(EXIT_FAILURE);
    }

    assert(logfd == 0);
    assert(dup(0) == 1);
    assert(dup(0) == 2);

    openlog("colod", LOG_PID, LOG_DAEMON);

    if (ctx->do_trace) {
        path = g_strconcat(ctx->base_dir, "/trace.log", NULL);
        trace = fopen(path, "a");
        g_free(path);
    }

    path = g_strconcat(ctx->base_dir, "/colod.pid", NULL);
    ret = colod_write_pidfile(path, &local_errp);
    g_free(path);
    if (!ret) {
        syslog(LOG_ERR, "Fatal: %s", local_errp->message);
        g_error_free(local_errp);
        exit(EXIT_FAILURE);
    }

    return pipefd;
}

static int colod_parse_options(ColodContext *ctx, int *argc, char ***argv,
                               GError **errp) {
    gboolean ret;
    GOptionContext *context;
    GOptionEntry entries[] =
    {
        {"daemonize", 'd', 0, G_OPTION_ARG_NONE, &ctx->daemonize, "Daemonize", NULL},
        {"syslog", 's', 0, G_OPTION_ARG_NONE, &do_syslog, "Log to syslog", NULL},
        {"disable_cpg", 0, 0, G_OPTION_ARG_NONE, &ctx->disable_cpg, "Disable corosync communication", NULL},
        {"instance_name", 'i', 0, G_OPTION_ARG_STRING, &ctx->instance_name, "The CPG group name for corosync communication", NULL},
        {"node_name", 'n', 0, G_OPTION_ARG_STRING, &ctx->node_name, "The node hostname", NULL},
        {"base_directory", 'b', 0, G_OPTION_ARG_FILENAME, &ctx->base_dir, "The base directory to store logs and sockets", NULL},
        {"qmp_path", 'q', 0, G_OPTION_ARG_FILENAME, &ctx->qmp_path, "The path to the qmp socket", NULL},
        {"qmp_yank_path", 'y', 0, G_OPTION_ARG_FILENAME, &ctx->qmp_yank_path, "The path to the qmp socket used for yank", NULL},
        {"timeout_low", 'l', 0, G_OPTION_ARG_INT, &ctx->qmp_timeout_low, "Low qmp timeout", NULL},
        {"timeout_high", 't', 0, G_OPTION_ARG_INT, &ctx->qmp_timeout_high, "High qmp timeout", NULL},
        {"watchdog_interval", 'a', 0, G_OPTION_ARG_INT, &ctx->watchdog_interval, "Watchdog interval (0 to disable)", NULL},
        {"primary", 'p', 0, G_OPTION_ARG_NONE, &ctx->primary, "Startup in primary mode", NULL},
        {"trace", 0, 0, G_OPTION_ARG_NONE, &ctx->do_trace, "Enable tracing", NULL},
        {0}
    };

    ctx->qmp_timeout_low = 600;
    ctx->qmp_timeout_high = 10000;

    context = g_option_context_new("- qemu colo heartbeat daemon");
    g_option_context_set_help_enabled(context, TRUE);
    g_option_context_add_main_entries(context, entries, 0);

    ret = g_option_context_parse(context, argc, argv, errp);
    g_option_context_free(context);
    if (!ret) {
        return -1;
    }

    if (!ctx->node_name || !ctx->instance_name || !ctx->base_dir ||
            !ctx->qmp_path) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_FATAL,
                    "--instance_name, --node_name, --base_directory and --qmp_path need to be given.");
        return -1;
    }

    return 0;
}

int main(int argc, char **argv) {
    GError *errp = NULL;
    ColodContext ctx_struct = { 0 };
    ColodContext *ctx = &ctx_struct;
    int ret;
    int pipefd = 0;

    ret = colod_parse_options(ctx, &argc, &argv, &errp);
    if (ret < 0) {
        fprintf(stderr, "%s\n", errp->message);
        g_error_free(errp);
        exit(EXIT_FAILURE);
    }

    if (ctx->daemonize) {
        pipefd = colod_daemonize(ctx);
    }
    prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
    prctl(PR_SET_DUMPABLE, 1);

    signal(SIGPIPE, SIG_IGN); // TODO: Handle this properly

    ret = colod_open_qmp(ctx, &errp);
    if (ret < 0) {
        goto err;
    }

    ret = colod_open_mngmt(ctx, &errp);
    if (ret < 0) {
        goto err;
    }

    if (!ctx->disable_cpg) {
        ret = colod_open_cpg(ctx, &errp);
        if (ret < 0) {
            goto err;
        }
    }

    if (ctx->daemonize) {
        ret = os_daemonize_post_init(pipefd, &errp);
        if (ret < 0) {
            goto err;
        }
    }

    colod_mainloop(ctx);

    // cleanup pidfile, cpg, qmp and mgmt connection

    return EXIT_SUCCESS;

err:
    if (errp) {
        colod_syslog(LOG_ERR, "Fatal: %s", errp->message);
        g_error_free(errp);
    }
    exit(EXIT_FAILURE);
}
