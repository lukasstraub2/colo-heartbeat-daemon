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
#include "coroutine_stack.h"
#include "peer_manager.h"


typedef struct ColodClient {
    Coroutine coroutine;
    QLIST_ENTRY(ColodClient) next;
    QmpCommands *commands;
    ColodClientListener *parent;
    GIOChannel *channel;
    gboolean stopped_qemu;
    gboolean quit;
    gboolean busy;
} ColodClient;

QLIST_HEAD(ColodClientHead, ColodClient);
struct ColodClientListener {
    int socket;
    QmpCommands *commands;
    PeerManager *peer;
    guint listen_source_id;
    struct ColodClientHead head;
    JsonNode *store;

    const ClientCallbacks *cb;
    gpointer cb_data;
    CoroutineLock lock;
};

struct MyTimeout {
    GTimer *timer;
    guint timeout_ms;
};

guint my_timeout_remaining_ms(MyTimeout *this) {
    guint elapsed = g_timer_elapsed(this->timer, NULL)/1000;

    if (elapsed >= this->timeout_ms) {
        return 0;
    }

    return this->timeout_ms - elapsed;
}

guint my_timeout_remaining_minus_ms(MyTimeout *this, guint minus) {
    guint remaining = my_timeout_remaining_ms(this);

    if (minus >= remaining) {
        return 0;
    }

    return remaining - minus;
}

MyTimeout *my_timeout_new(guint timeout_ms) {
    MyTimeout *this = g_rc_box_new0(MyTimeout);
    this->timer = g_timer_new();
    this->timeout_ms = MAX(0, timeout_ms - 1000);
    return this;
}

void my_timeout_free(gpointer data) {
    MyTimeout *this = data;
    g_timer_destroy(this->timer);
}

MyTimeout *my_timeout_ref(MyTimeout *this) {
    return g_rc_box_acquire(this);
}

void my_timeout_unref(MyTimeout *this) {
    g_rc_box_release_full(this, my_timeout_free);
}

void client_register(ColodClientListener *this, const ClientCallbacks *cb, gpointer data) {
    assert(!this->cb && !this->cb_data);
    assert(cb->query_status);
    this->cb = cb;
    this->cb_data = data;
}

void client_unregister(ColodClientListener *this, const ClientCallbacks *cb, gpointer data) {
    assert(this->cb == cb && this->cb_data == data);
    this->cb = NULL;
    this->cb_data = NULL;
}

static void query_status(ColodClientListener *this, ColodState *ret) {
    this->cb->query_status(this->cb_data, ret);
}

#define wait_while(cond) \
    while (cond) { \
        progress_source_add(coroutine->cb, coroutine); \
        co_yield_int(G_SOURCE_REMOVE); \
    }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"

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

    wait_while(!this->cb || !this->cb->_check_health_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;
    co_recurse(ret = CO cb->_check_health_co(coroutine, CO data, errp));

    colod_unlock_co(this->lock);
    co_end;

    return ret;
}

#define set_peer(...) co_wrap(_set_peer(__VA_ARGS__))
static void _set_peer(Coroutine *coroutine, ColodClientListener *this, const char *peer) {
    (void) coroutine;
    peer_manager_set_peer(this->peer, peer);
}

#define clear_peer(...) co_wrap(_clear_peer(__VA_ARGS__))
static void _clear_peer(Coroutine *coroutine, ColodClientListener *this) {
    (void) coroutine;
    peer_manager_clear_peer(this->peer);
}

#define start(...) co_wrap(_start(__VA_ARGS__))
static int _start(Coroutine *coroutine, ColodClientListener *this) {
    struct {
        const ClientCallbacks *cb;
        gpointer data;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    wait_while(!this->cb || !this->cb->_start_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;

    co_recurse(ret = CO cb->_start_co(coroutine, CO data));

    colod_unlock_co(this->lock);
    co_end;

    return ret;
}

#define promote(...) co_wrap(_promote(__VA_ARGS__))
static int _promote(Coroutine *coroutine, ColodClientListener *this) {
    struct {
        const ClientCallbacks *cb;
        gpointer data;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    wait_while(!this->cb || !this->cb->_promote_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;

    co_recurse(ret = CO cb->_promote_co(coroutine, CO data));

    colod_unlock_co(this->lock);
    co_end;

    return ret;
}

#define start_migration(...) co_wrap(_start_migration(__VA_ARGS__))
static int _start_migration(Coroutine *coroutine, ColodClientListener *this) {
    struct {
        const ClientCallbacks *cb;
        gpointer data;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    wait_while(!this->cb || !this->cb->_start_migration_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;

    co_recurse(ret = CO cb->_start_migration_co(coroutine, CO data));

    colod_unlock_co(this->lock);
    co_end;

    return ret;
}

#define reboot(...) co_wrap(_reboot(__VA_ARGS__))
static int _reboot(Coroutine *coroutine, ColodClientListener *this) {
    struct {
        const ClientCallbacks *cb;
        gpointer data;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    wait_while(!this->cb || !this->cb->_reboot_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;

    co_recurse(ret = CO cb->_reboot_co(coroutine, CO data));

    colod_unlock_co(this->lock);
    co_end;

    return ret;
}

#define shutdown(...) co_wrap(_shutdown(__VA_ARGS__))
static int _shutdown(Coroutine *coroutine, ColodClientListener *this, MyTimeout *timeout) {
    struct {
        const ClientCallbacks *cb;
        gpointer data;
        MyTimeout *timeout;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    wait_while(!this->cb || !this->cb->_shutdown_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;

    co_recurse(ret = CO cb->_shutdown_co(coroutine, CO data, timeout));

    colod_unlock_co(this->lock);
    co_end;

    return ret;
}

#define demote(...) co_wrap(_demote(__VA_ARGS__))
static int _demote(Coroutine *coroutine, ColodClientListener *this, MyTimeout *timeout) {
    struct {
        const ClientCallbacks *cb;
        gpointer data;
        MyTimeout *timeout;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    wait_while(!this->cb || !this->cb->_demote_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;

    co_recurse(ret = CO cb->_demote_co(coroutine, CO data, timeout));

    colod_unlock_co(this->lock);
    co_end;

    return ret;
}

#define quit(...) co_wrap(_quit(__VA_ARGS__))
static int _quit(Coroutine *coroutine, ColodClientListener *this, MyTimeout *timeout) {
    struct {
        const ClientCallbacks *cb;
        gpointer data;
        MyTimeout *timeout;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    wait_while(!this->cb || !this->cb->_quit_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;

    co_recurse(ret = CO cb->_quit_co(coroutine, CO data, timeout));

    colod_unlock_co(this->lock);
    co_end;

    return ret;
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

    wait_while(!this->cb || !this->cb->_yank_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;
    co_recurse(ret = CO cb->_yank_co(coroutine, CO data, errp));

    colod_unlock_co(this->lock);
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

    wait_while(!this->cb || !this->cb->_execute_nocheck_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;
    co_recurse(ret = CO cb->_execute_nocheck_co(coroutine, CO data, errp, command));

    colod_unlock_co(this->lock);
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

    wait_while(!this->cb || !this->cb->_execute_co || this->lock.holder);
    colod_lock_co(this->lock);

    CO cb = this->cb;
    CO data = this->cb_data;
    co_recurse(ret = CO cb->_execute_co(coroutine, CO data, errp, command));

    colod_unlock_co(this->lock);
    co_end;

    return ret;
}
#pragma GCC diagnostic pop

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

static MyTimeout *request_timeout(ColodQmpResult *request) {
    if (!has_member(request->json_root, "timeout")) {
        return NULL;
    }

    JsonObject *object = json_node_get_object(request->json_root);
    guint timeout = json_object_get_int_member(object, "timeout");

    return my_timeout_new(timeout);
}

#define handle_start_migration(...) co_wrap(_handle_start_migration(__VA_ARGS__))
static ColodQmpResult * _handle_start_migration(Coroutine *coroutine, ColodClientListener *this) {
    co_begin(ColodQmpResult *, NULL);

    int ret;
    co_recurse(ret = start_migration(coroutine, this));

    if (ret < 0) {
        return create_error_reply("Pending actions");
    }

    return create_reply("{}");

    co_end;
}

#define handle_shutdown(...) co_wrap(_handle_shutdown(__VA_ARGS__))
static ColodQmpResult *_handle_shutdown(Coroutine *coroutine, ColodClientListener *this, ColodQmpResult *request) {
    struct {
        MyTimeout *timeout;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpResult *, NULL);

    CO timeout = request_timeout(request);
    co_recurse(shutdown(coroutine, this, CO timeout));
    if (CO timeout) {
        my_timeout_unref(CO timeout);
    }

    return create_reply("{}");

    co_end;
}

#define handle_demote(...) co_wrap(_handle_demote(__VA_ARGS__))
static ColodQmpResult *_handle_demote(Coroutine *coroutine, ColodClientListener *this, ColodQmpResult *request) {
    struct {
        MyTimeout *timeout;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpResult *, NULL);

    CO timeout = request_timeout(request);
    co_recurse(demote(coroutine, this, CO timeout));
    if (CO timeout) {
        my_timeout_unref(CO timeout);
    }

    return create_reply("{}");

    co_end;
}

#define handle_quit(...) co_wrap(_handle_quit(__VA_ARGS__))
static ColodQmpResult *_handle_quit(Coroutine *coroutine, ColodClientListener *this, ColodQmpResult *request) {
    struct {
        MyTimeout *timeout;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpResult *, NULL);

    CO timeout = request_timeout(request);
    co_recurse(quit(coroutine, this, CO timeout));
    if (CO timeout) {
        my_timeout_unref(CO timeout);
    }

    return create_reply("{}");

    co_end;
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

static ColodQmpResult *handle_set(ColodClient *this,
                                  int (*func)(QmpCommands *, JsonNode *, GError **),
                                  ColodQmpResult *request) {
    GError *local_errp = NULL;
    ColodQmpResult *reply;

    JsonNode *commands = get_commands(request, &local_errp);
    if (!commands) {
        reply = create_error_reply(local_errp->message);
        g_error_free(local_errp);
        return reply;
    }

    int ret = func(this->commands, commands, &local_errp);
    if (ret < 0) {
        reply = create_error_reply(local_errp->message);
        g_error_free(local_errp);
        return reply;
    }

    return create_reply("{}");
}

static ColodQmpResult *handle_set_prepare_secondary(ColodClient *this,
                                                    ColodQmpResult *request) {
    return handle_set(this, qmp_commands_set_prepare_secondary, request);
}

static ColodQmpResult *handle_set_migration_start(ColodClient *this,
                                                  ColodQmpResult *request) {
    return handle_set(this, qmp_commands_set_migration_start, request);
}

static ColodQmpResult *handle_set_migration_switchover(ColodClient *this,
                                                       ColodQmpResult *request) {
    return handle_set(this, qmp_commands_set_migration_switchover, request);
}

static ColodQmpResult *handle_set_primary_failover(ColodClient *this,
                                                   ColodQmpResult *request) {
    return handle_set(this, qmp_commands_set_failover_primary, request);
}

static ColodQmpResult *handle_set_secondary_failover(ColodClient *this,
                                                     ColodQmpResult *request) {
    return handle_set(this, qmp_commands_set_failover_secondary, request);
}

static ColodQmpResult *handle_set_yank(ColodClient *this, ColodQmpResult *request) {
    JsonNode *instances;

    if (!has_member(request->json_root, "instances")) {
        return create_error_reply("Member 'instances' missing");
    }

    instances = get_member_node(request->json_root, "instances");
    if (!JSON_NODE_HOLDS_ARRAY(instances)) {
        return create_error_reply("Member 'instances' must be an array");
    }

    qmp_commands_set_yank_instances(this->commands, instances);

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

#define handle_set_peer(...) co_wrap(_handle_set_peer(__VA_ARGS__))
static ColodQmpResult * _handle_set_peer(Coroutine *coroutine, ColodClientListener *this, ColodQmpResult *request) {
    struct {
        const gchar *peer;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpResult *, NULL);

    if (!has_member(request->json_root, "peer")) {
        return create_error_reply("Member 'peer' missing");
    }

    CO peer = get_member_str(request->json_root, "peer");
    co_recurse(set_peer(coroutine, this, CO peer));

    return create_reply("{}");

    co_end;
}

#define handle_query_peer(...) co_wrap(_handle_query_peer(__VA_ARGS__))
static ColodQmpResult * _handle_query_peer(Coroutine *coroutine, ColodClientListener *this) {
    (void) coroutine;
    gchar *reply;
    reply = g_strdup_printf("{\"return\": "
                            "{\"peer\": \"%s\"}}",
                            peer_manager_get_peer(this->peer));

    ColodQmpResult *result = qmp_parse_result(reply, strlen(reply), NULL);
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
            } else if (!strcmp(command, "start")) {
                co_recurse(start(coroutine, client->parent));
                CO result = create_reply("{}");
            } else if (!strcmp(command, "promote")) {
                co_recurse(promote(coroutine, client->parent));
                CO result = create_reply("{}");
            } else if (!strcmp(command, "start-migration")) {
                co_recurse(CO result = handle_start_migration(coroutine, client->parent));
            } else if (!strcmp(command, "reboot")) {
                co_recurse(reboot(coroutine, client->parent));
                CO result = create_reply("{}");
            } else if (!strcmp(command, "shutdown")) {
                co_recurse(CO result = handle_shutdown(coroutine, client->parent, CO request));
            } else if (!strcmp(command, "demote")) {
                co_recurse(CO result = handle_demote(coroutine, client->parent, CO request));
            } else if (!strcmp(command, "quit")) {
                co_recurse(CO result = handle_quit(coroutine, client->parent, CO request));
            } else if (!strcmp(command, "set-prepare-secondary")) {
                CO result = handle_set_prepare_secondary(client, CO request);
            } else if (!strcmp(command, "set-migration-start")) {
                CO result = handle_set_migration_start(client, CO request);
            } else if (!strcmp(command, "set-migration-switchover")) {
                CO result = handle_set_migration_switchover(client, CO request);
            } else if (!strcmp(command, "set-primary-failover")) {
                CO result = handle_set_primary_failover(client, CO request);
            } else if (!strcmp(command, "set-secondary-failover")) {
                CO result = handle_set_secondary_failover(client, CO request);
            } else if (!strcmp(command, "set-yank")) {
                CO result = handle_set_yank(client, CO request);
            } else if (!strcmp(command, "yank")) {
                co_recurse(CO result = handle_yank_co(coroutine, client->parent));
            } else if (!strcmp(command, "stop")) {
                co_recurse(CO result = handle_stop_co(coroutine, client));
            } else if (!strcmp(command, "cont")) {
                co_recurse(CO result = handle_cont_co(coroutine, client));
            } else if (!strcmp(command, "set-peer")) {
                co_recurse(CO result = handle_set_peer(coroutine, client->parent, CO request));
            } else if (!strcmp(command, "query-peer")) {
                co_recurse(CO result = handle_query_peer(coroutine, client->parent));
            } else if (!strcmp(command, "clear-peer")) {
                co_recurse(clear_peer(coroutine, client->parent));
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
    client->commands = listener->commands;
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

    peer_manager_unref(listener->peer);
    g_free(listener);
}

ColodClientListener *client_listener_new(int socket, QmpCommands *commands,
                                         PeerManager *peer) {
    ColodClientListener *listener;

    listener = g_new0(ColodClientListener, 1);
    listener->socket = socket;
    listener->commands = commands;
    listener->peer = peer_manager_ref(peer);
    listener->listen_source_id = g_unix_fd_add(socket, G_IO_IN,
                                               client_listener_new_client,
                                               listener);

    return listener;
}
