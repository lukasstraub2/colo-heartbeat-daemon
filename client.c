/*
 * COLO background daemon client handling
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

#include <glib-2.0/glib.h>
#include <glib-2.0/glib-unix.h>

#include "client.h"
#include "util.h"
#include "daemon.h"
#include "qmp.h"
#include "json_util.h"
#include "coroutine_stack.h"


typedef struct ColodClient {
    struct Coroutine coroutine;
    QLIST_ENTRY(ColodClient) next;
    ColodContext *ctx;
    GIOChannel *channel;
    gboolean quit;
    gboolean busy;
} ColodClient;

QLIST_HEAD(ColodClientHead, ColodClient);
struct ColodClientListener {
    int socket;
    ColodContext *ctx;
    guint listen_source_id;
    struct ColodClientHead head;
};

static ColodQmpResult *create_error_reply(const gchar *message) {
    ColodQmpResult *result;

    gchar *reply = g_strdup_printf("{'error': '%s'}\n", message);
    result = qmp_parse_result(reply, strlen(reply), NULL);
    assert(result);

    return result;
}

static const gchar *role_to_string(ColodRole role) {
    if (role == ROLE_PRIMARY) {
        return "primary";
    } else {
        return "secondary";
    }
}

#define handle_query_status_co(result, ctx) \
    co_call_co((result), _handle_query_status_co, (ctx))

static ColodQmpResult *_handle_query_status_co(Coroutine *coroutine,
                                               ColodContext *ctx) {
    int ret;
    ColodQmpResult *result;
    GError *local_errp;

    ret = _colod_check_health(coroutine, ctx, &local_errp);
    if (coroutine->yield) {
        return NULL;
    }
    if (ret < 0) {
        // TODO should return "state": "error" instead
        result = create_error_reply(local_errp->message);
        g_error_free(local_errp);
        return result;
    }

    gchar *reply;
    reply = g_strdup_printf("{'return': {'role': '%s', 'replication': %s}}\n",
                            role_to_string(ctx->role),
                            bool_to_json(ctx->replication));

    result = qmp_parse_result(reply, strlen(reply), NULL);
    assert(result);
    return result;
}

static ColodQmpResult *handle_quit(ColodContext *ctx) {
    ColodQmpResult *result;
    gchar *reply;

    colod_quit(ctx);

    reply = g_strdup("{'return': {}}\n");
    result = qmp_parse_result(reply, strlen(reply), NULL);
    assert(result);
    return result;
}

static void client_free(ColodClient *client) {
    QLIST_REMOVE(client, next);
    g_io_channel_unref(client->channel);
    g_free(client);
}

static gboolean _colod_client_co(Coroutine *coroutine);
static gboolean colod_client_co(gpointer data) {
    ColodClient *client = (ColodClient *) data;
    Coroutine *coroutine = &client->coroutine;
    gboolean ret;

    co_enter(ret, coroutine, _colod_client_co);
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    client_free(client);
    return ret;
}
static gboolean colod_client_co_wrap(G_GNUC_UNUSED GIOChannel *channel,
                                     G_GNUC_UNUSED GIOCondition revents,
                                     gpointer data) {
    return colod_client_co(data);
}

static gboolean _colod_client_co(Coroutine *coroutine) {
    ColodClient *client = (ColodClient *) coroutine;
    ColodClientCo *co = co_stack(clientco);
    GIOStatus ret;
    GError *errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    while (!client->quit) {
        CO line = NULL;
        client->busy = FALSE;
        colod_channel_read_line_co(ret, client->channel, &CO line,
                                   &CO len, &errp);
        if (client->quit) {
            g_free(CO line);
            break;
        }
        if (ret != G_IO_STATUS_NORMAL) {
            goto error_client;
        }

        client->busy = TRUE;

        CO request = qmp_parse_result(CO line, CO len, &errp);
        if (!CO request) {
            goto error_client;
        }

        if (has_member(CO request->json_root, "exec-colod")) {
            const gchar *command = get_member_str(CO request->json_root,
                                                  "exec-colod");
            if (!command) {
                CO result = create_error_reply("Could not get exec-colod "
                                               "member");
            } else if (!strcmp(command, "query-status")) {
                handle_query_status_co(CO result, client->ctx);
            } else if (!strcmp(command, "quit")) {
                CO result = handle_quit(client->ctx);
            } else {
                CO result = create_error_reply("Unknown command");
            }
        } else {
            qmp_execute_co(CO result, client->ctx->qmpstate, &errp,
                           CO request->line);
            if (!CO result) {
                CO result = create_error_reply(errp->message);
                g_error_free(errp);
                errp = NULL;
            }
        }

        qmp_result_free(CO request);

        colod_channel_write_timeout_co(ret, client->channel, CO result->line,
                                       CO result->len, 1000, &errp);
        if (ret != G_IO_STATUS_NORMAL) {
            qmp_result_free(CO result);
            goto error_client;
        }

        qmp_result_free(CO result);
    }

    co_end;

    return G_SOURCE_REMOVE;

error_client:
    if (ret == G_IO_STATUS_ERROR) {
        colod_syslog(client->ctx, LOG_WARNING, "Client connection broke: %s",
                     errp->message);
        g_error_free(errp);
    }
    return G_SOURCE_REMOVE;
}

static int client_new(ColodClientListener *listener, int fd, GError **errp) {
    GIOChannel *channel;
    ColodClient *client;
    Coroutine *coroutine;

    channel = colod_create_channel(fd, errp);
    if (!channel) {
        return -1;
    }

    client = g_new0(ColodClient, 1);
    coroutine = &client->coroutine;
    coroutine->cb.plain = colod_client_co;
    coroutine->cb.iofunc = colod_client_co_wrap;
    client->ctx = listener->ctx;
    client->channel = channel;
    QLIST_INSERT_HEAD(&listener->head, client, next);

    g_io_add_watch(channel, G_IO_IN | G_IO_HUP, colod_client_co_wrap, client);
    return 0;
}

static gboolean client_listener_new_client(G_GNUC_UNUSED int fd,
                                           G_GNUC_UNUSED GIOCondition condition,
                                           gpointer data) {
    ColodClientListener *listener = (ColodClientListener *) data;
    ColodContext *ctx = listener->ctx;
    GError *errp = NULL;

    while (TRUE) {
        int clientfd = accept(listener->socket, NULL, NULL);
        if (clientfd < 0) {
            if (errno != EWOULDBLOCK) {
                colod_syslog(ctx, LOG_ERR,
                             "Failed to accept() new client: %s",
                             g_strerror(errno));
                listener->listen_source_id = 0;
                close(listener->socket);
                return G_SOURCE_REMOVE;
            }

            break;
        }

        if (client_new(listener, clientfd, &errp) < 0) {
            colod_syslog(ctx, LOG_WARNING, "Failed to create new client: %s",
                         errp->message);
            g_error_free(errp);
            errp = NULL;
            continue;
        }
    }

    return G_SOURCE_CONTINUE;
}

void client_listener_free(ColodClientListener *listener) {
    ColodClient *entry;

    if (listener->listen_source_id) {
        g_source_remove(listener->listen_source_id);
        close(listener->socket);
    }

    QLIST_FOREACH(entry, &listener->head, next) {
        entry->quit = TRUE;
        if (!entry->busy) {
            int fd = g_io_channel_unix_get_fd(entry->channel);
            shutdown(fd, SHUT_RDWR);
        }
    }

    while (!QLIST_EMPTY(&listener->head)) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }

    g_free(listener);
}

ColodClientListener *client_listener_new(int socket, ColodContext *ctx,
                                         GError **errp) {
    ColodClientListener *listener;

    listener = g_new0(ColodClientListener, 1);
    listener->socket = socket;
    listener->ctx = ctx;
    listener->listen_source_id = g_unix_fd_add(socket, G_IO_IN,
                                               client_listener_new_client,
                                               listener);

    return listener;
}
