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
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>
#include <assert.h>

#include <glib-2.0/glib.h>
#include <glib-2.0/glib-unix.h>

#include <corosync/cpg.h>
#include <corosync/corotypes.h>

#include "daemon.h"
#include "coroutine.h"
#include "coutil.h"
#include "util.h"
#include "coroutine_stack.h"

static void colod_syslog(ColodContext *ctx, int pri,
                         const char *fmt, ...)
     __attribute__ ((__format__ (__printf__, 3, 4)));

static void colod_syslog(ColodContext *ctx, int pri,
                         const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    if (ctx->daemonize) {
        vsyslog(pri, fmt, args);
    } else {
        vfprintf(stderr, fmt, args);
        fwrite("\n", 1, 1, stderr);
    }

    va_end(args);
}

static guint colod_create_source(GMainContext *context, int fd,
                                 G_GNUC_UNUSED GIOCondition events,
                                 GSourceFunc func, gpointer data) {
    GSource *source = fd_source_create(fd, G_IO_IN);
    g_source_set_callback(source, func, data, NULL);
    return g_source_attach(source, context);
}

static GIOChannel *colod_create_channel(int fd, GError **errp) {
    GError *local_errp = NULL;
    GIOChannel *channel;

    channel = g_io_channel_unix_new(fd);
    g_io_channel_set_encoding(channel, NULL, &local_errp);
    if (local_errp) {
        colod_error_set(errp, "Failed to set channel encoding: %s",
                        local_errp->message);
        g_error_free(local_errp);
        g_io_channel_unref(channel);
        return NULL;
    }

    g_io_channel_set_flags(channel, G_IO_FLAG_NONBLOCK, &local_errp);
    if (local_errp) {
        colod_error_set(errp, "Failed to set channel nonblocking: %s",
                        local_errp->message);
        g_error_free(local_errp);
        g_io_channel_unref(channel);
        return NULL;
    }
    g_io_channel_set_close_on_unref(channel, TRUE);

    return channel;
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

static gboolean _colod_client_co(Coroutine *coroutine);
static gboolean colod_client_co(gpointer data) {
    Coroutine *coroutine = (Coroutine *) data;
    ColodClientCo *co = co_stack(clientco);
    gboolean ret;

    co_enter(ret, coroutine, _colod_client_co);
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    g_io_channel_unref(CO client_channel);
    g_free(coroutine);
    return ret;
}
static gboolean colod_client_co_wrap(G_GNUC_UNUSED GIOChannel *channel,
                                     G_GNUC_UNUSED GIOCondition revents,
                                     gpointer data) {
    return colod_client_co(data);
}

static gboolean _colod_client_co(Coroutine *coroutine) {
    ColodClientCo *co = co_stack(clientco);
    GError *errp = NULL;
    GIOStatus ret;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    while (TRUE) {
        colod_channel_read_line_co(ret, CO client_channel, &CO line,
                                   &CO read_len, &errp);
        if (ret != G_IO_STATUS_NORMAL) {
            if (ret == G_IO_STATUS_ERROR) {
                colod_syslog(CO ctx, LOG_WARNING,
                             "Client connection broke: %s",
                             errp->message);
                g_error_free(errp);
            }
            return G_SOURCE_REMOVE;
        }

        // lock qmp for writing
        if (CO ctx->qmp_lock) {
            progress_source_add(colod_client_co, coroutine);
            co_yield(GINT_TO_POINTER(G_SOURCE_REMOVE));
        }
        while (CO ctx->qmp_lock) {
            co_yield(GINT_TO_POINTER(G_SOURCE_CONTINUE));
        }
        CO ctx->qmp_lock = TRUE;

        // switch source to qmp
        g_io_add_watch(CO ctx->qmp_channel, G_IO_OUT,
                       colod_client_co_wrap, coroutine);
        co_yield(GINT_TO_POINTER(G_SOURCE_REMOVE));

        colod_channel_write_co(ret, CO ctx->qmp_channel, CO line, CO read_len,
                               &errp);
        if (ret != G_IO_STATUS_NORMAL) {
            if (ret == G_IO_STATUS_ERROR) {
                colod_syslog(CO ctx, LOG_WARNING, "QMP connection broke: %s",
                             errp->message);
                g_error_free(errp);
            }
            // qemu is gone: set global error, broadcast, ...
            exit(EXIT_FAILURE);
        }

        g_free(CO line);
        CO ctx->qmp_lock = FALSE;

        g_io_add_watch(CO ctx->qmp_channel, G_IO_IN,
                       colod_client_co_wrap, coroutine);
        co_yield(GINT_TO_POINTER(G_SOURCE_REMOVE));

        colod_channel_read_line_co(ret, CO ctx->qmp_channel, &CO line,
                                   &CO read_len, &errp);
        if (ret != G_IO_STATUS_NORMAL) {
            if (ret == G_IO_STATUS_ERROR) {
                colod_syslog(CO ctx, LOG_WARNING, "QMP connection broke: %s",
                             errp->message);
                g_error_free(errp);
            }
            // qemu is gone
            exit(EXIT_FAILURE);
        }

        // switch source to client
        g_io_add_watch(CO client_channel, G_IO_OUT,
                       colod_client_co_wrap, coroutine);
        co_yield(GINT_TO_POINTER(G_SOURCE_REMOVE));

        colod_channel_write_co(ret, CO client_channel, CO line, CO read_len,
                               &errp);
        if (ret != G_IO_STATUS_NORMAL) {
            if (ret == G_IO_STATUS_ERROR) {
                colod_syslog(CO ctx, LOG_WARNING,
                             "Client connection broke: %s",
                             errp->message);
                g_error_free(errp);
            }
            g_io_channel_unref(CO client_channel);
            return G_SOURCE_REMOVE;
        }

        g_free(CO line);
        g_io_add_watch(CO client_channel, G_IO_IN,
                       colod_client_co_wrap, coroutine);
        co_yield(GINT_TO_POINTER(G_SOURCE_REMOVE));
    }

    co_end;

    return G_SOURCE_REMOVE;
}

static int colod_client_new(ColodContext *ctx, int fd, GError **errp) {
    GIOChannel *channel;
    Coroutine *coroutine;
    ColodClientCo *co;

    assert(errp);

    channel = colod_create_channel(fd, errp);
    if (*errp) {
        return -1;
    }

    coroutine = g_new0(Coroutine, 1);
    co = co_stack(clientco);
    co->ctx = ctx;
    co->client_channel = channel;

    g_io_add_watch(channel, G_IO_IN, colod_client_co_wrap, coroutine);
    return 0;
}

static gboolean colod_mngmt_new_client(G_GNUC_UNUSED int fd,
                                       G_GNUC_UNUSED GIOCondition condition,
                                       gpointer data) {
    ColodContext *ctx = (ColodContext *) data;
    GError *errp = NULL;

    while (TRUE) {
        int clientfd = accept(ctx->mngmt_listen_fd, NULL, NULL);
        if (clientfd < 0) {
            if (errno != EWOULDBLOCK) {
                colod_syslog(ctx, LOG_ERR,
                             "Fatal: Failed to accept() new client: %s",
                             g_strerror(errno));
                exit(EXIT_FAILURE);
            }

            break;
        }

        if (colod_client_new(ctx, clientfd, &errp) < 0) {
            colod_syslog(ctx, LOG_WARNING, "Failed to create new client: %s",
                         errp->message);
            g_error_free(errp);
            continue;
        }
    }

    return G_SOURCE_CONTINUE;
}

static gboolean colod_qmp_readable(gpointer data);

static void colod_mainloop(ColodContext *ctx) {
    GError *errp = NULL;

    // g_main_context_default creates the global context on demand
    ctx->mainctx = g_main_context_default();
    ctx->mainloop = g_main_loop_new(ctx->mainctx, FALSE);

    ctx->qmp_channel = colod_create_channel(ctx->qmp_fd, &errp);
    if (errp) {
        colod_syslog(ctx, LOG_ERR, "Failed to create qmp GIOChannel: %s",
                     errp->message);
        g_error_free(errp);
        exit(EXIT_FAILURE);
    }

    ctx->mngmt_listen_source_id = g_unix_fd_add(ctx->mngmt_listen_fd, G_IO_IN,
                                                colod_mngmt_new_client, ctx);

    //ctx->cpg_source_id = colod_create_source(ctx->mainctx, ctx->cpg_fd,
    //                                         G_IO_IN, colod_cpg_readable,
    //                                         ctx);

    g_main_loop_run(ctx->mainloop);

    g_main_loop_unref(ctx->mainloop);
    g_main_context_unref(ctx->mainctx);
}

cpg_model_v1_data_t cpg_data = {
    CPG_MODEL_V1,
    colod_cpg_deliver,
    colod_cpg_confchg,
    colod_cpg_totem_confchg,
    0
};

static void colod_open_cpg(ColodContext *ctx, GError **errp) {
    cs_error_t ret;
    int fd;
    struct cpg_name name;

    assert(errp);

    if (strlen(ctx->instance_name) >= sizeof(name.value)) {
        colod_error_set(errp, "Instance name too long");
        return;
    }
    strcpy(name.value, ctx->instance_name);
    name.length = strlen(name.value);

    ret = cpg_model_initialize(&ctx->cpg_handle, CPG_MODEL_V1,
                               (cpg_model_data_t*) &cpg_data, ctx);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to initialize cpg: %s", cs_strerror(ret));
        return;
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
    return;

err_joined:
    cpg_leave(ctx->cpg_handle, &name);
err:
    cpg_finalize(ctx->cpg_handle);
}

static void colod_open_mngmt(ColodContext *ctx, GError **errp) {
    int sockfd, ret;
    struct sockaddr_un address = { 0 };
    g_autofree char *path = NULL;

    assert(errp);

    path = g_strconcat(ctx->base_dir, "/colod.sock", NULL);
    if (strlen(path) >= sizeof(address.sun_path)) {
        colod_error_set(errp, "Management unix path too long");
        return;
    }
    strcpy(address.sun_path, path);
    address.sun_family = AF_UNIX;

    ret = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ret < 0) {
        colod_error_set(errp, "Failed to create management socket: %s",
                        g_strerror(errno));
        return;
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
        assert(*errp);
        goto err;
    }

    ctx->mngmt_listen_fd = sockfd;
    return;

err:
    close(sockfd);
}

static void colod_open_qmp(ColodContext *ctx, GError **errp) {
    int sockfd, ret;
    struct sockaddr_un address = { 0 };

    assert(errp);

    if (strlen(ctx->qmp_path) >= sizeof(address.sun_path)) {
        colod_error_set(errp, "Qmp unix path too long");
        return;
    }
    strcpy(address.sun_path, ctx->qmp_path);
    address.sun_family = AF_UNIX;

    ret = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ret < 0) {
        colod_error_set(errp, "Failed to create qmp socket: %s",
                        g_strerror(errno));
        return;
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
    return;

err:
    close(sockfd);
}

static int colod_daemonize(ColodContext *ctx) {
    GError *errp = NULL;
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
    ret = colod_write_pidfile(path, &errp);
    g_free(path);
    if (!ret) {
        syslog(LOG_ERR, "Fatal: %s", errp->message);
        g_error_free(errp);
        exit(EXIT_FAILURE);
    }

    return pipefd;
}

int main(int argc, char **argv) {
    GError *errp = NULL;
    char *node_name, *instance_name, *base_dir, *qmp_path;
    ColodContext ctx_struct = { 0 };
    ColodContext *ctx = &ctx_struct;

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

    ctx->daemonize = FALSE;
    ctx->node_name = node_name;
    ctx->instance_name = instance_name;
    ctx->base_dir = base_dir;
    ctx->qmp_path = qmp_path;

    int pipefd = colod_daemonize(ctx);

    signal(SIGPIPE, SIG_IGN); // TODO: Handle this properly

    colod_open_qmp(ctx, &errp);
    if (errp) {
        goto err;
    }

    colod_open_mngmt(ctx, &errp);
    if (errp) {
        goto err;
    }

    //colod_open_cpg(ctx, &errp);
    if (errp) {
        goto err;
    }

    if (ctx->daemonize) {
        os_daemonize_post_init(pipefd, &errp);
        if (errp) {
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
