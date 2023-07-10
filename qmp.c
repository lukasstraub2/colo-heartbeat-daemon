/*
 * COLO background daemon qmp
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <assert.h>

#include <glib-2.0/glib.h>
#include <glib-2.0/glib-unix.h>

#include <json-glib-1.0/json-glib/json-glib.h>

#include "qmp.h"
#include "util.h"
#include "coroutine_stack.h"

typedef void (*Callback)(void);
typedef struct QmpCallback {
    QLIST_ENTRY(QmpCallback) next;
    Callback func;
    gpointer user_data;
} QmpCallback;

QLIST_HEAD(QmpCallbackHead, QmpCallback);
struct ColodQmpState {
    GIOChannel *channel;
    CoroutineLock lock;
    guint timeout;
    gchar *yank_instances;
    struct QmpCallbackHead yank_callbacks;
    struct QmpCallbackHead event_callbacks;
};

#define qmp_yank_co(ret, state, errp) \
    co_call_co((ret), _qmp_yank_co, (state), (errp))
static ColodQmpResult *_qmp_yank_co(Coroutine *coroutine, ColodQmpState *state,
                                    GError **errp);

static QmpCallback *qmp_find_notify(struct QmpCallbackHead *head,
                                    Callback func, gpointer user_data) {
    QmpCallback *entry;
    QLIST_FOREACH(entry, head, next) {
        if (entry->func == func && entry->user_data == user_data) {
            return entry;
        }
    }

    return NULL;
}

static void qmp_add_notify(struct QmpCallbackHead *head,
                           Callback func, gpointer user_data) {
    QmpCallback *cb;

    assert(!qmp_find_notify(head, func, user_data));

    cb = g_new0(QmpCallback, 1);
    cb->func = func;
    cb->user_data = user_data;

    QLIST_INSERT_HEAD(head, cb, next);
}

void qmp_add_notify_event(ColodQmpState *state, QmpEventCallback _func,
                          gpointer user_data) {
    Callback func = (Callback) _func;
    qmp_add_notify(&state->event_callbacks, func, user_data);
}

void qmp_add_notify_yank(ColodQmpState *state, QmpYankCallback _func,
                         gpointer user_data) {
    Callback func = (Callback) _func;
    qmp_add_notify(&state->yank_callbacks, func, user_data);
}

void qmp_del_notify_event(ColodQmpState *state, QmpEventCallback _func,
                          gpointer user_data) {
    Callback func = (Callback) _func;
    QmpCallback *cb;

    cb = qmp_find_notify(&state->event_callbacks, func, user_data);
    assert(cb);

    QLIST_REMOVE(cb, next);
}

void qmp_del_notify_yank(ColodQmpState *state, QmpYankCallback _func,
                         gpointer user_data) {
    Callback func = (Callback) _func;
    QmpCallback *cb;

    cb = qmp_find_notify(&state->yank_callbacks, func, user_data);
    assert(cb);

    QLIST_REMOVE(cb, next);
}

static void notify_event(ColodQmpState *state, ColodQmpResult *result) {
    QmpCallback *entry;
    QLIST_FOREACH(entry, &state->event_callbacks, next) {
        QmpEventCallback func = (QmpEventCallback) entry->func;
        func(entry->user_data, result);
    }
}

static void notify_yank(ColodQmpState *state, gboolean success) {
    QmpCallback *entry;
    QLIST_FOREACH(entry, &state->yank_callbacks, next) {
        QmpYankCallback func = (QmpYankCallback) entry->func;
        func(entry->user_data, success);
    }
}

static gboolean has_member(JsonNode *node, const gchar *member) {
    JsonObject *object;

    assert(JSON_NODE_HOLDS_OBJECT(node));
    object = json_node_get_object(node);
    return json_object_has_member(object, member);
}

void qmp_result_free(ColodQmpResult *result) {
    if (!result)
        return;

    json_node_unref(result->json_root);
    g_free(result->line);
    g_free(result);
}

ColodQmpResult *qmp_parse_result(gchar *line, gsize len, GError **errp) {
    ColodQmpResult *result;
    gboolean ret;

    result = g_new0(ColodQmpResult, 1);
    result->line = line;
    result->len = len;

    JsonParser *parser = json_parser_new_immutable();
    ret = json_parser_load_from_data(parser, line, len, errp);
    if (!ret) {
        g_object_unref(parser);
        g_free(result->line);
        g_free(result);
        return NULL;
    }
    result->json_root = json_node_ref(json_parser_get_root(parser));
    g_object_unref(parser);

    if (!JSON_NODE_HOLDS_OBJECT(result->json_root)) {
        colod_error_set(errp, "Result is not a json object: %s", result->line);
        qmp_result_free(result);
        return NULL;
    }

    return result;
}

#define qmp_read_line_co(ret, state, yank, errp) \
    co_call_co((ret), _qmp_read_line_co, (state), (yank), (errp))

static ColodQmpResult *_qmp_read_line_co(Coroutine *coroutine,
                                         ColodQmpState *state,
                                         gboolean yank,
                                         GError **errp) {
    ColodQmpCo *co = co_stack(qmpco);
    ColodQmpResult *result;
    GIOStatus ret;
    GError *local_errp = NULL;

    co_begin(ColodQmpResult *, NULL);

    while (TRUE) {
        colod_channel_read_line_timeout_co(ret, state->channel, &CO line,
                                           &CO len, state->timeout,
                                           &local_errp);
        if (ret != G_IO_STATUS_NORMAL) {
            if (local_errp->domain == COLOD_ERROR &&
                    local_errp->code == COLOD_ERROR_TIMEOUT) {
                if (yank) {
                    g_error_free(local_errp);
                    qmp_yank_co(result, state, errp);
                    return result;
                }
            }
            // TODO notify protcol error
            g_propagate_error(errp, local_errp);
            return NULL;
        }

        result = qmp_parse_result(CO line, CO len, errp);
        if (!result) {
            // TODO notify protcol error
            return NULL;
        }

        if (has_member(result->json_root, "event")) {
            notify_event(state, result);
            qmp_result_free(result);
            continue;
        }

        break;
    }

    co_end;

    return result;
}

#define ___qmp_execute_co(ret, state, yank, errp, command) \
    co_call_co((ret), __qmp_execute_co, (state), (yank), (errp), (command))

static ColodQmpResult *__qmp_execute_co(Coroutine *coroutine,
                                        ColodQmpState *state,
                                        gboolean yank,
                                        GError **errp,
                                        const gchar *command) {
    ColodQmpResult *result;
    GIOStatus ret;

    co_begin(ColodQmpResult *, NULL);

    colod_lock_co(state->lock);
    colod_channel_write_timeout_co(ret, state->channel, command,
                                   strlen(command), state->timeout, errp);
    if (ret != G_IO_STATUS_NORMAL) {
        colod_unlock_co(state->lock);
        return NULL;
    }

    qmp_read_line_co(result, state, yank, errp);
    colod_unlock_co(state->lock);
    if (!result) {
        return NULL;
    }

    co_end;

    return result;
}

ColodQmpResult *_qmp_execute_co(Coroutine *coroutine,
                                ColodQmpState *state,
                                GError **errp,
                                const gchar *command) {
    return __qmp_execute_co(coroutine, state, TRUE, errp, command);
}

static ColodQmpResult *_qmp_yank_co(Coroutine *coroutine, ColodQmpState *state,
                                    GError **errp) {
    ColodQmpCo *co = co_stack(qmpco);
    ColodQmpResult *result = NULL;

    co_begin(ColodQmpResult *, NULL);
    CO command = g_strdup_printf("{'exec-oob': 'yank', 'id': 'yank0', "
                                        "'arguments':{ 'instances': %s }}",
                                 state->yank_instances);
    ___qmp_execute_co(result, state, FALSE, errp, CO command);
    g_free(CO command);
    if (!result) {
        notify_yank(state, FALSE);
        return NULL;
    }

    if (has_member(result->json_root, "id")) {
        // result is the result of the yank command
        qmp_result_free(result);
        qmp_read_line_co(result, state, FALSE, errp);
        if (!result) {
            notify_yank(state, FALSE);
            return NULL;
        }
    } else {
        // result is the result of the timed-out command
        // read the yank command result
        ColodQmpResult *tmp;
        qmp_read_line_co(tmp, state, FALSE, errp);
        if (!tmp) {
            notify_yank(state, FALSE);
            return NULL;
        }
        qmp_result_free(tmp);
    }

    co_end;

    notify_yank(state, TRUE);
    return result;
}

static gboolean _qmp_handshake_readable_co(Coroutine *coroutine);
static gboolean qmp_handshake_readable_co(gpointer data) {
    Coroutine *coroutine = data;
    gboolean ret;

    co_enter(ret, coroutine, _qmp_handshake_readable_co);
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    g_free(coroutine);
    return ret;
}

static gboolean qmp_handshake_readable_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return qmp_handshake_readable_co(data);
}

static gboolean _qmp_handshake_readable_co(Coroutine *coroutine) {
    ColodQmpCo *co = co_stack(qmpco);
    ColodQmpResult *result;
    GError *errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    qmp_read_line_co(result, CO state, FALSE, &errp);
    if (!result) {
        exit(EXIT_FAILURE);
    }
    qmp_result_free(result);

    qmp_execute_co(result, CO state, &errp,
                   "{'execute': 'qmp_capabilities', "
                        "'arguments': {'enable': ['oob']}}\n");
    if (!result) {
        exit(EXIT_FAILURE);
    }
    qmp_result_free(result);

    colod_unlock_co(CO state->lock);

    co_end;

    return G_SOURCE_REMOVE;
}

static Coroutine *qmp_handshake_coroutine(ColodQmpState *state) {
    Coroutine *coroutine;
    ColodQmpCo *co;

    coroutine = g_new0(Coroutine, 1);
    coroutine->cb.plain = qmp_handshake_readable_co;
    coroutine->cb.iofunc = qmp_handshake_readable_co_wrap;
    co = co_stack(qmpco);
    co->state = state;

    state->lock.count = 1;
    state->lock.holder = coroutine;

    g_io_add_watch(state->channel, G_IO_IN, qmp_handshake_readable_co_wrap,
                   coroutine);

    return coroutine;
}

static gboolean _qmp_event_co(Coroutine *coroutine);
static gboolean qmp_event_co(gpointer data) {
    Coroutine *coroutine = data;
    gboolean ret;

    co_enter(ret, coroutine, _qmp_event_co);
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    g_free(coroutine);
    return ret;
}

static gboolean qmp_event_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return qmp_event_co(data);
}

static gboolean _qmp_event_co(Coroutine *coroutine) {
    ColodQmpCo *co = co_stack(qmpco);
    ColodQmpResult *result;
    GIOStatus ret;
    GError *errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    while (TRUE) {
        g_io_add_watch_full(CO state->channel, G_PRIORITY_LOW, G_IO_IN,
                            qmp_event_co_wrap, coroutine, NULL);
        co_yield_int(G_SOURCE_REMOVE);

        while (CO state->lock.holder) {
            co_yield_int(G_SOURCE_CONTINUE);
        }
        colod_lock_co(CO state->lock);

        colod_channel_read_line_timeout_co(ret, CO state->channel, &CO line,
                                           &CO len, CO state->timeout, &errp);
        colod_unlock_co(CO state->lock);
        if (ret != G_IO_STATUS_NORMAL) {
            // TODO notify protcol error
            return G_SOURCE_REMOVE;
        }

        result = qmp_parse_result(CO line, CO len, &errp);
        if (!result) {
            // TODO notify protcol error
            continue;
        }

        assert(has_member(result->json_root, "event"));
        notify_event(CO state, result);
        qmp_result_free(result);
    }

    co_end;

    return G_SOURCE_REMOVE;
}

static Coroutine *qmp_event_coroutine(ColodQmpState *state) {
    Coroutine *coroutine;
    ColodQmpCo *co;

    coroutine = g_new0(Coroutine, 1);
    coroutine->cb.plain = qmp_event_co;
    coroutine->cb.iofunc = qmp_event_co_wrap;
    co = co_stack(qmpco);
    co->state = state;

    g_io_add_watch_full(state->channel, G_PRIORITY_LOW, G_IO_IN,
                        qmp_event_co_wrap, coroutine, NULL);

    return coroutine;
}

ColodQmpState *qmp_new(int fd, GError **errp) {
    ColodQmpState *state;

    state = g_new0(ColodQmpState, 1);
    state->timeout = 5000;
    state->channel = colod_create_channel(fd, errp);
    if (!state->channel) {
        g_free(state);
        return NULL;
    }

    qmp_handshake_coroutine(state);
    qmp_event_coroutine(state);

    return state;
}
