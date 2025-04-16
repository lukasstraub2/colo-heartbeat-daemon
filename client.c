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
#include <sys/socket.h>

#include <glib-2.0/glib.h>
#include <glib-2.0/glib-unix.h>

#include "client.h"
#include "util.h"
#include "daemon.h"
#include "json_util.h"
#include "qmp.h"
#include "main_coroutine.h"
#include "coroutine_stack.h"


typedef struct ColodClient {
    Coroutine coroutine;
    QLIST_ENTRY(ColodClient) next;
    const ColodContext *ctx;
    ColodClientListener *parent;
    GIOChannel *channel;
    gboolean stopped_qemu;
    gboolean quit;
    gboolean busy;
} ColodClient;

QLIST_HEAD(ColodClientHead, ColodClient);
struct ColodClientListener {
    int socket;
    const ColodContext *ctx;
    guint listen_source_id;
    struct ColodClientHead head;
    JsonNode *store;

    const ClientCallbacks *cb;
    gpointer cb_data;
};

void client_register(ColodClientListener *this, const ClientCallbacks *cb, gpointer data) {
    assert(!this->cb && !this->cb_data);
    this->cb = cb;
    this->cb_data = data;
}

void client_unregister(ColodClientListener *this, const ClientCallbacks *cb, gpointer data) {
    assert(this->cb == cb && this->cb_data == data);
    this->cb = NULL;
    this->cb_data = NULL;
}

#define check_health_co(...) \
    co_wrap(_check_health_co(__VA_ARGS__))
static int _check_health_co(Coroutine *coroutine, ColodClientListener *this, GError **errp) {
    struct {
        const ClientCallbacks *cb;
        gpointer data;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO cb = this->cb;
    CO data = this->cb_data;
    co_recurse(((void)__use_co_recurse, ret = CO cb->_check_health_co(coroutine, CO data, errp)));

    co_end;

    return ret;
}

static void query_status(ColodClientListener *this, ColodState *ret) {
    this->cb->query_status(this->cb_data, ret);
}

static void set_peer(ColodClientListener *this, const gchar *peer) {
    this->cb->set_peer(this->cb_data, peer);
}

static const gchar *get_peer(ColodClientListener *this) {
    return this->cb->get_peer(this->cb_data);
}

static void clear_peer(ColodClientListener *this) {
    this->cb->clear_peer(this->cb_data);
}

static int start_migration(ColodClientListener *this) {
    return this->cb->start_migration(this->cb_data);
}

static void autoquit(ColodClientListener *this) {
    this->cb->autoquit(this->cb_data);
}

static void quit(ColodClientListener *this) {
    this->cb->quit(this->cb_data);
}

static void client_cont_failed(ColodClientListener *this){
    this->cb->client_cont_failed(this->cb_data);
}

#define yank(...) \
    co_wrap(_yank_co(__VA_ARGS__))
static int _yank_co(Coroutine *coroutine, ColodClientListener *this, GError **errp){
    struct {
        const ClientCallbacks *cb;
        gpointer data;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO cb = this->cb;
    CO data = this->cb_data;
    co_recurse(((void)__use_co_recurse, ret = CO cb->_yank_co(coroutine, CO data, errp)));

    co_end;

    return ret;
}

#define execute_nocheck_co(...) \
    co_wrap(_execute_nocheck_co(__VA_ARGS__))
static ColodQmpResult *_execute_nocheck_co(Coroutine *coroutine, ColodClientListener *this,
                                           GError **errp, const gchar *command){
    struct {
        const ClientCallbacks *cb;
        gpointer data;
    } *co;
    ColodQmpResult *ret;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpResult *, NULL);

    CO cb = this->cb;
    CO data = this->cb_data;
    co_recurse(((void)__use_co_recurse, ret = CO cb->_execute_nocheck_co(coroutine, CO data, errp, command)));

    co_end;

    return ret;
}

#define execute_co(...) \
    co_wrap(_execute_co(__VA_ARGS__))
static ColodQmpResult *_execute_co(Coroutine *coroutine, ColodClientListener *this,
                                   GError **errp, const gchar *command){
    struct {
        const ClientCallbacks *cb;
        gpointer data;
    } *co;
    ColodQmpResult *ret;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpResult *, NULL);

    CO cb = this->cb;
    CO data = this->cb_data;
    co_recurse(((void)__use_co_recurse, ret = CO cb->_execute_co(coroutine, CO data, errp, command)));

    co_end;

    return ret;
}

static ColodQmpResult *create_reply(const gchar *member) {
    ColodQmpResult *result;

    gchar *reply = g_strdup_printf("{\"return\": %s}\n", member);
    result = qmp_parse_result(reply, strlen(reply), NULL);
    assert(result);

    return result;
}

static ColodQmpResult *create_error_reply(const gchar *message) {
    ColodQmpResult *result;

    gchar *reply = g_strdup_printf("{\"error\": \"%s\"}\n", message);
    result = qmp_parse_result(reply, strlen(reply), NULL);
    assert(result);

    return result;
}

#define handle_query_status_co(...) \
    co_wrap(_handle_query_status_co(__VA_ARGS__))
static ColodQmpResult *_handle_query_status_co(Coroutine *coroutine,
                                               ColodClientListener *this) {
    int ret;
    ColodQmpResult *result;
    ColodState state;
    gboolean failed = FALSE;
    GError *local_errp = NULL;

    co_begin(ColodQmpResult*, NULL);

    co_recurse(ret = check_health_co(coroutine, this, &local_errp));
    if (coroutine->yield) {
        return NULL;
    }
    if (ret < 0) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        local_errp = NULL;
        failed = TRUE;
    }

    co_end;


    query_status(this, &state);
    gchar *reply;
    reply = g_strdup_printf("{\"return\": "
                            "{\"primary\": %s, \"replication\": %s,"
                            " \"failed\": %s, \"peer-failover\": %s,"
                            " \"peer-failed\": %s}}\n",
                            bool_to_json(state.primary),
                            bool_to_json(state.replication),
                            bool_to_json(failed || state.failed),
                            bool_to_json(state.peer_failover),
                            bool_to_json(state.peer_failed));

    result = qmp_parse_result(reply, strlen(reply), NULL);
    assert(result);
    return result;
}

static ColodQmpResult *handle_query_store(ColodClient *client) {
    ColodQmpResult *result;
    gchar *store_str;
    JsonNode *store = client->parent->store;

    if (store) {
        store_str = json_to_string(store, FALSE);
    } else {
        store_str = g_strdup("{}");
    }

    result = create_reply(store_str);
    g_free(store_str);
    return result;
}

static ColodQmpResult *handle_set_store(ColodQmpResult *request,
                                        ColodClient *client) {
    JsonNode *store;

    if (!has_member(request->json_root, "store")) {
        return create_error_reply("Member 'store' missing");
    }

    store = get_member_node(request->json_root, "store");

    if (client->parent->store) {
        json_node_unref(client->parent->store);
    }
    client->parent->store = json_node_ref(store);

    return create_reply("{}");
}

static ColodQmpResult *handle_quit(ColodClientListener *this) {
    quit(this);

    return create_reply("{}");
}

static ColodQmpResult *handle_autoquit(ColodClientListener *this) {
    autoquit(this);

    return create_reply("{}");
}

static ColodQmpResult *handle_start_migration(ColodClientListener *this) {
    int ret;

    ret = start_migration(this);
    if (ret < 0) {
        return create_error_reply("Pending actions");
    }

    return create_reply("{}");
}

static JsonNode *get_commands(ColodQmpResult *request, GError **errp) {
    JsonNode *commands;

    if (!has_member(request->json_root, "commands")) {
        colod_error_set(errp, "Member 'commands' missing");
        return NULL;
    }

    commands = get_member_node(request->json_root, "commands");
    if (!JSON_NODE_HOLDS_ARRAY(commands)) {
        colod_error_set(errp, "Member 'commands' must be an array");
        return NULL;
    }

    assert(commands);
    return commands;
}

static ColodQmpResult *handle_set(int (*func)(QmpCommands *, JsonNode *, GError **),
                                  ColodQmpResult *request,
                                  const ColodContext *ctx) {
    GError *local_errp = NULL;
    ColodQmpResult *reply;

    JsonNode *commands = get_commands(request, &local_errp);
    if (!commands) {
        reply = create_error_reply(local_errp->message);
        g_error_free(local_errp);
        return reply;
    }

    int ret = func(ctx->commands, commands, &local_errp);
    if (ret < 0) {
        reply = create_error_reply(local_errp->message);
        g_error_free(local_errp);
        return reply;
    }

    return create_reply("{}");
}

static ColodQmpResult *handle_set_prepare_secondary(ColodQmpResult *request,
                                                  const ColodContext *ctx) {
    return handle_set(qmp_commands_set_prepare_secondary, request, ctx);
}

static ColodQmpResult *handle_set_migration_start(ColodQmpResult *request,
                                                  const ColodContext *ctx) {
    return handle_set(qmp_commands_set_migration_start, request, ctx);
}

static ColodQmpResult *handle_set_migration_switchover(ColodQmpResult *request,
                                                       const ColodContext *ctx) {
    return handle_set(qmp_commands_set_migration_switchover, request, ctx);
}

static ColodQmpResult *handle_set_primary_failover(ColodQmpResult *request,
                                                   const ColodContext *ctx) {
    return handle_set(qmp_commands_set_failover_primary, request, ctx);
}

static ColodQmpResult *handle_set_secondary_failover(ColodQmpResult *request,
                                                     const ColodContext *ctx) {
    return handle_set(qmp_commands_set_failover_secondary, request, ctx);
}

static ColodQmpResult *handle_set_yank(ColodQmpResult *request,
                                       const ColodContext *ctx) {
    JsonNode *instances;

    if (!has_member(request->json_root, "instances")) {
        return create_error_reply("Member 'instances' missing");
    }

    instances = get_member_node(request->json_root, "instances");
    if (!JSON_NODE_HOLDS_ARRAY(instances)) {
        return create_error_reply("Member 'instances' must be an array");
    }

    qmp_set_yank_instances(ctx->qmp, instances);

    return create_reply("{}");
}

#define handle_yank_co(...) \
    co_wrap(_handle_yank_co(__VA_ARGS__))
static ColodQmpResult *_handle_yank_co(Coroutine *coroutine,
                                       ColodClientListener *this) {
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    ret = _yank_co(coroutine, this, &local_errp);
    if (coroutine->yield) {
        return NULL;
    }
    if (ret < 0) {
        result = create_error_reply(local_errp->message);
        g_error_free(local_errp);
        return result;
    }

    return create_reply("{}");
}

#define handle_stop_co(...) \
    co_wrap(_handle_stop_co(__VA_ARGS__))
static ColodQmpResult *_handle_stop_co(Coroutine *coroutine,
                                       ColodClient *client) {
    ColodQmpResult *result;
    GError *local_errp = NULL;

    result = _execute_co(coroutine, client->parent, &local_errp,
                               "{'execute': 'stop'}\n");
    if (coroutine->yield) {
        return NULL;
    }
    if (!result) {
        result = create_error_reply(local_errp->message);
        g_error_free(local_errp);
        return result;
    }

    client->stopped_qemu = TRUE;
    return result;
}

#define handle_cont_co(...) \
    co_wrap(_handle_cont_co(__VA_ARGS__))
static ColodQmpResult *_handle_cont_co(Coroutine *coroutine,
                                       ColodClient *client) {
    ColodQmpResult *result;
    GError *local_errp = NULL;

    result = _execute_co(coroutine, client->parent, &local_errp,
                               "{'execute': 'cont'}\n");
    if (coroutine->yield) {
        return NULL;
    }
    if (!result) {
        result = create_error_reply(local_errp->message);
        g_error_free(local_errp);
        return result;
    }

    client->stopped_qemu = FALSE;
    return result;
}

static ColodQmpResult *handle_set_peer(ColodQmpResult *request,
                                       ColodClientListener *this) {
    const gchar *peer;

    if (!has_member(request->json_root, "peer")) {
        return create_error_reply("Member 'peer' missing");
    }

    peer = get_member_str(request->json_root, "peer");
    set_peer(this, peer);

    return create_reply("{}");
}

static ColodQmpResult *handle_query_peer(ColodClientListener *this) {
    ColodQmpResult *result;
    gchar *reply;
    reply = g_strdup_printf("{\"return\": "
                            "{\"peer\": \"%s\"}}\n",
                            get_peer(this));

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
    ColodClient *client = data;
    Coroutine *coroutine = &client->coroutine;
    gboolean ret;

    assert(client->parent->cb && client->parent->cb_data);

    co_enter(coroutine, ret = _colod_client_co(coroutine));
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    colod_assert_remove_one_source(coroutine);
    client_free(client);
    return ret;
}

static gboolean _colod_client_co(Coroutine *coroutine) {
    ColodClient *client = (ColodClient *) coroutine;
    struct {
        gchar *line;
        gsize len;
        ColodQmpResult *request, *result;
    } *co;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(gboolean, G_SOURCE_CONTINUE);

    while (!client->quit) {
        CO line = NULL;
        client->busy = FALSE;
        co_recurse(ret = colod_channel_read_line_co(coroutine, client->channel,
                                                    &CO line, &CO len, &local_errp));
        if (client->quit) {
            if (local_errp) {
                g_error_free(local_errp);
            }
            g_free(CO line);
            break;
        }
        if (ret < 0) {
            goto error_client;
        }

        client->busy = TRUE;

        CO request = qmp_parse_result(CO line, CO len, &local_errp);
        if (!CO request) {
            goto error_client;
        }

        colod_trace("client: %s", CO request->line);
        if (has_member(CO request->json_root, "exec-colod")) {
            const gchar *command = get_member_str(CO request->json_root,
                                                  "exec-colod");
            if (!command) {
                CO result = create_error_reply("Could not get exec-colod "
                                               "member");
            } else if (!strcmp(command, "query-status")) {
                co_recurse(CO result = handle_query_status_co(coroutine, client->parent));
            } else if (!strcmp(command, "query-store")) {
                CO result = handle_query_store(client);
            } else if (!strcmp(command, "set-store")) {
                CO result = handle_set_store(CO request, client);
            } else if (!strcmp(command, "quit")) {
                CO result = handle_quit(client->parent);
            } else if (!strcmp(command, "autoquit")) {
                CO result = handle_autoquit(client->parent);
            } else if (!strcmp(command, "set-prepare-secondary")) {
                CO result = handle_set_prepare_secondary(CO request,
                                                         client->ctx);
            } else if (!strcmp(command, "set-migration-start")) {
                CO result = handle_set_migration_start(CO request,
                                                       client->ctx);
            } else if (!strcmp(command, "set-migration-switchover")) {
                CO result = handle_set_migration_switchover(CO request,
                                                            client->ctx);
            } else if (!strcmp(command, "start-migration")) {
                CO result = handle_start_migration(client->parent);
            } else if (!strcmp(command, "set-primary-failover")) {
                CO result = handle_set_primary_failover(CO request,
                                                        client->ctx);
            } else if (!strcmp(command, "set-secondary-failover")) {
                CO result = handle_set_secondary_failover(CO request,
                                                          client->ctx);
            } else if (!strcmp(command, "set-yank")) {
                CO result = handle_set_yank(CO request, client->ctx);
            } else if (!strcmp(command, "yank")) {
                co_recurse(CO result = handle_yank_co(coroutine, client->parent));
            } else if (!strcmp(command, "stop")) {
                co_recurse(CO result = handle_stop_co(coroutine, client));
            } else if (!strcmp(command, "cont")) {
                co_recurse(CO result = handle_cont_co(coroutine, client));
            } else if (!strcmp(command, "set-peer")) {
                CO result = handle_set_peer(CO request, client->parent);
            } else if (!strcmp(command, "query-peer")) {
                CO result = handle_query_peer(client->parent);
            } else if (!strcmp(command, "clear-peer")) {
                clear_peer(client->parent);
                CO result = create_reply("{}");
            } else {
                CO result = create_error_reply("Unknown command");
            }
        } else {
            co_recurse(CO result = execute_nocheck_co(coroutine,
                                                      client->parent,
                                                      &local_errp,
                                                      CO request->line));
            if (!CO result) {
                CO result = create_error_reply(local_errp->message);
                g_error_free(local_errp);
                local_errp = NULL;
            }
        }

        qmp_result_free(CO request);

        colod_trace("client: %s", CO result->line);
        co_recurse(ret = colod_channel_write_timeout_co(coroutine, client->channel,
                                                        CO result->line,
                                                        CO result->len, 1000,
                                                        &local_errp));
        if (ret < 0) {
            qmp_result_free(CO result);
            goto error_client;
        }

        qmp_result_free(CO result);
    }

    return G_SOURCE_REMOVE;

error_client:
    if (!g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_EOF)) {
        colod_syslog(LOG_WARNING, "Client connection broke: %s",
                     local_errp->message);
    }
    g_error_free(local_errp);
    local_errp = NULL;

    if (client->stopped_qemu) {
        co_recurse(CO result = execute_co(coroutine, client->parent,
                                          &local_errp, "{'execute': 'cont'}\n"));
        if (!CO result) {
            log_error(local_errp->message);
            g_error_free(local_errp);
            client_cont_failed(client->parent);
        }
        qmp_result_free(CO result);
    }

    co_end;

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
    coroutine->cb = colod_client_co;
    client->ctx = listener->ctx;
    client->parent = listener;
    client->channel = channel;
    QLIST_INSERT_HEAD(&listener->head, client, next);

    g_io_add_watch(channel, G_IO_IN | G_IO_HUP, coroutine_giofunc_cb, client);
    return 0;
}

static gboolean client_listener_new_client(G_GNUC_UNUSED int fd,
                                           G_GNUC_UNUSED GIOCondition condition,
                                           gpointer data) {
    ColodClientListener *listener = (ColodClientListener *) data;
    GError *errp = NULL;

    while (TRUE) {
        int clientfd = accept(listener->socket, NULL, NULL);
        if (clientfd < 0) {
            if (errno != EWOULDBLOCK) {
                colod_syslog(LOG_ERR, "Failed to accept() new client: %s",
                             g_strerror(errno));
                listener->listen_source_id = 0;
                close(listener->socket);
                return G_SOURCE_REMOVE;
            }

            break;
        }

        if (client_new(listener, clientfd, &errp) < 0) {
            colod_syslog(LOG_WARNING, "Failed to create new client: %s",
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
            colod_shutdown_channel(entry->channel);
        }
    }

    while (!QLIST_EMPTY(&listener->head)) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }

    g_free(listener);
}

ColodClientListener *client_listener_new(int socket, const ColodContext *ctx) {
    ColodClientListener *listener;

    listener = g_new0(ColodClientListener, 1);
    listener->socket = socket;
    listener->ctx = ctx;
    listener->listen_source_id = g_unix_fd_add(socket, G_IO_IN,
                                               client_listener_new_client,
                                               listener);

    return listener;
}
