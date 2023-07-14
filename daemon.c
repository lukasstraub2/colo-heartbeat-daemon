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
#include "coroutine.h"
#include "coutil.h"
#include "json_util.h"
#include "util.h"
#include "client.h"
#include "qmp.h"
#include "coroutine_stack.h"

void colod_syslog(ColodContext *ctx, int pri, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    if (ctx->daemonize && ctx->syslog) {
        vsyslog(pri, fmt, args);
    } else {
        vfprintf(stderr, fmt, args);
        fwrite("\n", 1, 1, stderr);
    }

    va_end(args);
}

static void colod_cpg_deliver(cpg_handle_t handle,
                              const struct cpg_name *group_name,
                              uint32_t nodeid,
                              uint32_t pid,
                              void *msg,
                              size_t msg_len) {

}

static void colod_cpg_confchg(cpg_handle_t handle,
    const struct cpg_name *group_name,
    const struct cpg_address *member_list, size_t member_list_entries,
    const struct cpg_address *left_list, size_t left_list_entries,
    const struct cpg_address *joined_list, size_t joined_list_entries) {

}

static void colod_cpg_totem_confchg(cpg_handle_t handle,
                                    struct cpg_ring_id ring_id,
                                    uint32_t member_list_entries,
                                    const uint32_t *member_list) {

}

static gboolean colod_cpg_readable(gpointer data) {
    ColodContext *ctx = data;
    cpg_dispatch(ctx->cpg_handle, CS_DISPATCH_ALL);
    return G_SOURCE_CONTINUE;
}

static gboolean qemu_runnng(const gchar *status) {
    return !strcmp(status, "running")
            || !strcmp(status, "finish-migrate")
            || !strcmp(status, "colo");
}

#define qemu_query_status_co(result, qmp, role, replication, errp) \
    co_call_co((result), _qemu_query_status_co, (qmp), (role), \
               (replication), (errp))

static int _qemu_query_status_co(Coroutine *coroutine, ColodQmpState *qmp,
                                 ColodRole *role, gboolean *replication,
                                 GError **errp) {
    ColodClientCo *co = co_stack(clientco);

    co_begin(int, -1);

    qmp_execute_co(CO qemu_status, qmp, errp,
                   "{'execute': 'query-status'}\n");
    if (!CO qemu_status) {
        return -1;
    }

    qmp_execute_co(CO colo_status, qmp, errp,
                   "{'execute': 'query-colo-status'}\n");
    if (!CO colo_status) {
        qmp_result_free(CO qemu_status);
        return -1;
    }

    co_end;

    const gchar *status, *colo_mode, *colo_reason;
    status = get_member_member_str(CO qemu_status->json_root,
                                   "return", "status");
    colo_mode = get_member_member_str(CO colo_status->json_root,
                                      "return", "mode");
    colo_reason = get_member_member_str(CO colo_status->json_root,
                                        "return", "reason");
    if (!status || !colo_mode || !colo_reason) {
        colod_error_set(errp, "Failed to parse query-status "
                        "and query-colo-status output");
        qmp_result_free(CO qemu_status);
        qmp_result_free(CO colo_status);
        return -1;
    }

    if (!strcmp(status, "inmigrate") || !strcmp(status, "shutdown")) {
        *role = ROLE_SECONDARY;
        *replication = FALSE;
    } else if (qemu_runnng(status) && !strcmp(colo_mode, "none")
               && (!strcmp(colo_reason, "none")
                   || !strcmp(colo_reason, "request"))) {
        *role = ROLE_PRIMARY;
        *replication = FALSE;
    } else if (qemu_runnng(status) &&!strcmp(colo_mode, "primary")) {
        *role = ROLE_PRIMARY;
        *replication = TRUE;
    } else if (qemu_runnng(status) && !strcmp(colo_mode, "secondary")) {
        *role = ROLE_SECONDARY;
        *replication = TRUE;
    } else {
        colod_error_set(errp, "Unknown qemu status: %s, %s",
                        CO qemu_status->line, CO colo_status->line);
        qmp_result_free(CO qemu_status);
        qmp_result_free(CO colo_status);
        return -1;
    }

    qmp_result_free(CO qemu_status);
    qmp_result_free(CO colo_status);
    return 0;
}

int _colod_check_health_co(Coroutine *coroutine, ColodContext *ctx,
                           GError **errp) {
    ColodRole role;
    gboolean replication;
    int ret;

    ret = _qemu_query_status_co(coroutine, ctx->qmpstate, &role, &replication,
                                errp);
    if (coroutine->yield) {
        return -1;
    }
    if (ret < 0) {
        return -1;
    }

    ret = qmp_get_error(ctx->qmpstate, errp);
    if (ret < 0) {
        return -1;
    }

    if (ctx->role != role || ctx->replication != replication) {
        colod_error_set(errp, "qemu status mismatch");
        return -1;
    }

    return 0;
}

void colod_quit(ColodContext *ctx) {
    g_main_loop_quit(ctx->mainloop);
}

static void colod_mainloop(ColodContext *ctx) {
    GError *local_errp = NULL;

    // g_main_context_default creates the global context on demand
    ctx->mainctx = g_main_context_default();
    ctx->mainloop = g_main_loop_new(ctx->mainctx, FALSE);

    ctx->qmpstate = qmp_new(ctx->qmp_fd, &local_errp);
    if (local_errp) {
        colod_syslog(ctx, LOG_ERR, "Failed to initialize qmp: %s",
                     local_errp->message);
        g_error_free(local_errp);
        exit(EXIT_FAILURE);
    }

    ctx->listener = client_listener_new(ctx->mngmt_listen_fd, ctx);
    if (!ctx->listener) {
        colod_syslog(ctx, LOG_ERR, "Failed to initialize client listener: %s",
                     local_errp->message);
        g_error_free(local_errp);
        exit(EXIT_FAILURE);
    }

    //ctx->cpg_source_id = colod_create_source(ctx->mainctx, ctx->cpg_fd,
    //                                         G_IO_IN | G_IO_HUP,
    //                                         colod_cpg_readable, ctx);

    g_main_loop_run(ctx->mainloop);
    g_main_loop_unref(ctx->mainloop);

    client_listener_free(ctx->listener);
    qmp_free(ctx->qmpstate);

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
    int sockfd, ret;
    struct sockaddr_un address = { 0 };

    if (strlen(ctx->qmp_path) >= sizeof(address.sun_path)) {
        colod_error_set(errp, "Qmp unix path too long");
        return -1;
    }
    strcpy(address.sun_path, ctx->qmp_path);
    address.sun_family = AF_UNIX;

    ret = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ret < 0) {
        colod_error_set(errp, "Failed to create qmp socket: %s",
                        g_strerror(errno));
        return -1;
    }
    sockfd = ret;

    ret = connect(sockfd, (const struct sockaddr *) &address,
                  sizeof(address));
    if (ret < 0) {
        colod_error_set(errp, "Failed to connect qmp socket: %s",
                        g_strerror(errno));
        goto err;
    }

    ctx->qmp_fd = sockfd;
    return 0;

err:
    close(sockfd);
    return -1;
}

static int colod_daemonize(ColodContext *ctx) {
    GError *local_errp = NULL;
    char *path;
    int logfd, pipefd, ret;

    if (!ctx->daemonize) {
        return 0;
    }

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

int main(int argc, char **argv) {
    GError *errp = NULL;
    char *node_name, *instance_name, *base_dir, *qmp_path;
    ColodContext ctx_struct = { 0 };
    ColodContext *ctx = &ctx_struct;
    int ret;

    if (argc != 5) {
        fprintf(stderr, "Usage: %s <node name> <instance name> "
                        "<base directory> <qmp unix socket>\n",
                        argv[0]);
        exit(EXIT_FAILURE);
    }

    node_name = argv[1];
    instance_name = argv[2];
    base_dir = argv[3];
    qmp_path = argv[4];

    ctx->daemonize = TRUE;
    ctx->syslog = FALSE;
    ctx->node_name = node_name;
    ctx->instance_name = instance_name;
    ctx->base_dir = base_dir;
    ctx->qmp_path = qmp_path;

    int pipefd = colod_daemonize(ctx);
    prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);

    signal(SIGPIPE, SIG_IGN); // TODO: Handle this properly

    ret = colod_open_qmp(ctx, &errp);
    if (ret < 0) {
        goto err;
    }

    ret = colod_open_mngmt(ctx, &errp);
    if (ret < 0) {
        goto err;
    }

    //ret = colod_open_cpg(ctx, &errp);
    if (ret < 0) {
        goto err;
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
        colod_syslog(ctx, LOG_ERR, "Fatal: %s", errp->message);
        g_error_free(errp);
    }
    exit(EXIT_FAILURE);
}
