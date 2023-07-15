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
#include "json_util.h"
#include "coroutine_stack.h"

typedef void (*Callback)(void);
typedef struct QmpCallback {
    QLIST_ENTRY(QmpCallback) next;
    Callback func;
    gpointer user_data;
} QmpCallback;

typedef struct QmpChannel {
    GIOChannel *channel;
    CoroutineLock lock;
} QmpChannel;

QLIST_HEAD(QmpCallbackHead, QmpCallback);
struct ColodQmpState {
    QmpChannel channel;
    QmpChannel yank_channel;
    guint timeout;
    JsonNode *yank_matches;
    struct QmpCallbackHead yank_callbacks;
    struct QmpCallbackHead event_callbacks;
    gboolean did_yank;
    GError *error;
    guint inflight;
};

static void qmp_set_error(ColodQmpState *state, GError *error) {
    assert(error);

    if (state->error) {
        g_error_free(state->error);
    }
    state->error = g_error_copy(error);
}

int qmp_get_error(ColodQmpState *state, GError **errp) {
    if (state->error) {
        g_propagate_error(errp, g_error_copy(state->error));
        return -1;
    } else {
        return 0;
    }
}

static void qmp_set_yank(ColodQmpState *state) {
    state->did_yank = TRUE;
}

gboolean qmp_get_yank(ColodQmpState *state) {
    return state->did_yank;
}

void qmp_clear_yank(ColodQmpState *state) {
    state->did_yank = FALSE;
}

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

void qmp_result_free(ColodQmpResult *result) {
    if (!result)
        return;

    json_node_unref(result->json_root);
    g_free(result->line);
    g_free(result);
}

ColodQmpResult *qmp_parse_result(gchar *line, gsize len, GError **errp) {
    ColodQmpResult *result;

    result = g_new0(ColodQmpResult, 1);
    result->line = line;
    result->len = len;

    result->json_root = json_from_string(line, errp);
    if (!result->json_root) {
        g_free(result->line);
        g_free(result);
        return NULL;
    }

    if (!JSON_NODE_HOLDS_OBJECT(result->json_root)) {
        colod_error_set(errp, "Result is not a json object: %s", result->line);
        qmp_result_free(result);
        return NULL;
    }

    return result;
}

#define qmp_read_line_co(ret, state, channel, yank, skip_events, errp) \
    co_call_co((ret), _qmp_read_line_co, (state), (channel), (yank), \
               (skip_events), (errp))

static ColodQmpResult *_qmp_read_line_co(Coroutine *coroutine,
                                         ColodQmpState *state,
                                         QmpChannel *channel,
                                         gboolean yank,
                                         gboolean skip_events,
                                         GError **errp) {
    ColodQmpCo *co = co_stack(qmpco);
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    co_begin(ColodQmpResult *, NULL);

    while (TRUE) {
        colod_channel_read_line_timeout_co(ret, channel->channel, &CO line,
                                           &CO len, state->timeout,
                                           &local_errp);
        if (ret == G_IO_STATUS_ERROR) {
            if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT)) {
                if (yank) {
                    g_error_free(local_errp);
                    local_errp = NULL;

                    qmp_yank_co(ret, state, errp);
                    if (ret < 0) {
                        return NULL;
                    }
                    qmp_read_line_co(result, state, channel, FALSE, skip_events,
                                     errp);
                    return result;
                }
            }
            g_propagate_error(errp, local_errp);
            return NULL;
        }
        if (ret != G_IO_STATUS_NORMAL) {
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_FATAL,
                        "Qmp signaled EOF");
            return NULL;
        }

        result = qmp_parse_result(CO line, CO len, errp);
        if (!result) {
            return NULL;
        }

        if (skip_events && has_member(result->json_root, "event")) {
            notify_event(state, result);
            qmp_result_free(result);
            continue;
        }

        break;
    }

    co_end;

    return result;
}

#define ___qmp_execute_co(ret, state, channel, yank, errp, command) \
    co_call_co((ret), __qmp_execute_co, (state), (channel), (yank), (errp), \
               (command))

static ColodQmpResult *__qmp_execute_co(Coroutine *coroutine,
                                        ColodQmpState *state,
                                        QmpChannel *channel,
                                        gboolean yank,
                                        GError **errp,
                                        const gchar *command) {
    ColodQmpResult *result;
    GIOStatus ret;
    GError *local_errp = NULL;

    co_begin(ColodQmpResult *, NULL);

    state->inflight++;
    colod_lock_co(channel->lock);
    colod_channel_write_timeout_co(ret, channel->channel, command,
                                   strlen(command), state->timeout,
                                   &local_errp);
    if (ret == G_IO_STATUS_ERROR) {
        qmp_set_error(state, local_errp);
        g_propagate_error(errp, local_errp);
        colod_unlock_co(channel->lock);
        state->inflight--;
        return NULL;
    }
    if (ret != G_IO_STATUS_NORMAL) {
        local_errp = g_error_new(COLOD_ERROR, COLOD_ERROR_FATAL,
                                 "Qmp signaled EOF");
        qmp_set_error(state, local_errp);
        g_propagate_error(errp, local_errp);
        colod_unlock_co(channel->lock);
        state->inflight--;
        return NULL;
    }

    qmp_read_line_co(result, state, channel, yank, TRUE, &local_errp);
    colod_unlock_co(channel->lock);
    state->inflight--;
    if (!result) {
        qmp_set_error(state, local_errp);
        g_propagate_error(errp, local_errp);
        return NULL;
    }

    co_end;

    return result;
}

ColodQmpResult *_qmp_execute_co(Coroutine *coroutine,
                                ColodQmpState *state,
                                GError **errp,
                                const gchar *command) {
    return __qmp_execute_co(coroutine, state, &state->channel, TRUE, errp,
                            command);
}

static gchar *pick_yank_instances(JsonNode *result,
                                  JsonNode *yank_matches) {
    JsonArray *array;
    gchar *instances_str;
    JsonNode *instances;
    JsonReader *reader;

    assert(JSON_NODE_HOLDS_OBJECT(result));
    assert(JSON_NODE_HOLDS_ARRAY(yank_matches));

    instances = json_node_new(JSON_NODE_ARRAY);
    array = json_array_new();
    json_node_set_array(instances, array);
    g_object_unref(array);

    reader = json_reader_new(result);

    json_reader_read_member(reader, "return");
    guint count = json_reader_count_elements(reader);
    for (guint i = 0; i < count; i++) {
        json_reader_read_element(reader, i);
        JsonNode *element = json_reader_get_value(reader);
        assert(element);

        if (object_matches_match_array(element, yank_matches)) {
            json_array_add_element(array, element);
        }

        json_reader_end_element(reader);
    }
    json_reader_end_member(reader);
    g_object_unref(reader);

    instances_str = json_to_string(instances, FALSE);
    g_object_unref(instances);
    return instances_str;
}

int _qmp_yank_co(Coroutine *coroutine, ColodQmpState *state,
                 GError **errp) {
    ColodQmpCo *co = co_stack(qmpco);
    ColodQmpResult *result;

    co_begin(int, -1);

    ___qmp_execute_co(result, state, &state->yank_channel, FALSE, errp,
                      "{'exec-oob': 'query-yank', 'id': 'yank0'}\n");
    if (!result) {
        return -1;
    }
    if (has_member(result->json_root, "error")) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_FATAL,
                    "qmp query-yank: %s", result->line);
        qmp_result_free(result);
        return -1;
    }

    gchar *instances = pick_yank_instances(result->json_root,
                                           state->yank_matches);
    CO command = g_strdup_printf("{'exec-oob': 'yank', 'id': 'yank0', "
                                        "'arguments':{ 'instances': %s }}\n",
                                 instances);
    g_free(instances);
    qmp_result_free(result);

    ___qmp_execute_co(result, state, &state->yank_channel, FALSE, errp,
                      CO command);
    g_free(CO command);
    if (!result) {
        return -1;
    }
    if (has_member(result->json_root, "error")) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_FATAL,
                    "qmp yank: %s", result->line);
        qmp_result_free(result);
        return -1;
    }

    qmp_result_free(result);

    co_end;

    return 0;
}

static gboolean _qmp_handshake_readable_co(Coroutine *coroutine);
static gboolean qmp_handshake_readable_co(gpointer data) {
    Coroutine *coroutine = data;
    ColodQmpCo *co = co_stack(qmpco);
    gboolean ret;

    co_enter(ret, coroutine, _qmp_handshake_readable_co);
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    co->state->inflight--;
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
    GError *local_errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    qmp_read_line_co(result, CO state, &CO state->channel, FALSE, TRUE,
                     &local_errp);
    if (!result) {
        colod_unlock_co(CO state->channel.lock);
        qmp_set_error(CO state, local_errp);
        g_error_free(local_errp);
        return G_SOURCE_REMOVE;
    }
    qmp_result_free(result);

    qmp_execute_co(result, CO state, &local_errp,
                   "{'execute': 'qmp_capabilities', "
                        "'arguments': {'enable': ['oob']}}\n");
    colod_unlock_co(CO state->channel.lock);
    if (!result) {
        qmp_set_error(CO state, local_errp);
        g_error_free(local_errp);
        return G_SOURCE_REMOVE;
    }
    if (has_member(result->json_root, "error")) {
        local_errp = g_error_new(COLOD_ERROR, COLOD_ERROR_FATAL,
                                 "qmp_capabilities: %s", result->line);
        qmp_set_error(CO state, local_errp);
        g_error_free(local_errp);
        qmp_result_free(result);
        return G_SOURCE_REMOVE;
    }
    qmp_result_free(result);

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

    assert(!state->channel.lock.count);
    state->channel.lock.count = 1;
    state->channel.lock.holder = coroutine;

    g_io_add_watch(state->channel.channel, G_IO_IN | G_IO_HUP,
                   qmp_handshake_readable_co_wrap, coroutine);

    state->inflight++;
    return coroutine;
}

typedef struct QmpEventCo {
    Coroutine coroutine;
    ColodQmpState *state;
    QmpChannel *channel;
    gboolean discard;
} QmpEventCo;

static gboolean _qmp_event_co(Coroutine *coroutine);
static gboolean qmp_event_co(gpointer data) {
    QmpEventCo *eventco = data;
    Coroutine *coroutine = &eventco->coroutine;
    gboolean ret;

    co_enter(ret, coroutine, _qmp_event_co);
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    eventco->state->inflight--;
    g_free(eventco);
    return ret;
}

static gboolean qmp_event_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return qmp_event_co(data);
}

static gboolean _qmp_event_co(Coroutine *coroutine) {
    QmpEventCo *eventco = (QmpEventCo *) coroutine;
    QmpChannel *channel = eventco->channel;
    ColodQmpCo *co = co_stack(qmpco);
    ColodQmpResult *result;
    GIOStatus ret;
    GError *local_errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    while (TRUE) {
        g_io_add_watch_full(channel->channel, G_PRIORITY_LOW,
                            G_IO_IN | G_IO_HUP, qmp_event_co_wrap, coroutine,
                            NULL);
        co_yield_int(G_SOURCE_REMOVE);

        while (channel->lock.holder) {
            co_yield_int(G_SOURCE_CONTINUE);
        }
        colod_lock_co(channel->lock);

        colod_channel_read_line_timeout_co(ret, channel->channel, &CO line,
                                           &CO len, CO state->timeout,
                                           &local_errp);
        colod_unlock_co(channel->lock);
        if (ret == G_IO_STATUS_ERROR) {
            qmp_set_error(eventco->state, local_errp);
            g_error_free(local_errp);
            return G_SOURCE_REMOVE;
        }
        if (ret != G_IO_STATUS_NORMAL) {
            local_errp = g_error_new(COLOD_ERROR, COLOD_ERROR_FATAL,
                                     "Qmp signaled EOF");
            qmp_set_error(eventco->state, local_errp);
            g_error_free(local_errp);
            return G_SOURCE_REMOVE;
        }

        result = qmp_parse_result(CO line, CO len, &local_errp);
        if (!result) {
            qmp_set_error(eventco->state, local_errp);
            g_error_free(local_errp);
            local_errp = NULL;
            continue;
        }

        if (!has_member(result->json_root, "event")) {
            local_errp = g_error_new(COLOD_ERROR, COLOD_ERROR_FATAL,
                                     "Not an event: %s", result->line);
            qmp_set_error(eventco->state, local_errp);
            g_error_free(local_errp);
            local_errp = NULL;
            qmp_result_free(result);
            continue;
        }

        if (!eventco->discard) {
            notify_event(eventco->state, result);
        }
        qmp_result_free(result);
    }

    co_end;

    return G_SOURCE_REMOVE;
}

static Coroutine *qmp_event_coroutine(ColodQmpState *state, QmpChannel *channel,
                                      gboolean discard) {
    QmpEventCo *eventco;
    Coroutine *coroutine;

    eventco = g_new0(QmpEventCo, 1);
    coroutine = &eventco->coroutine;
    coroutine->cb.plain = qmp_event_co;
    coroutine->cb.iofunc = qmp_event_co_wrap;
    eventco->state = state;
    eventco->channel = channel;
    eventco->discard = discard;

    g_io_add_watch_full(channel->channel, G_PRIORITY_LOW, G_IO_IN | G_IO_HUP,
                        qmp_event_co_wrap, coroutine, NULL);

    state->inflight++;
    return coroutine;
}

void qmp_free(ColodQmpState *state) {
    colod_shutdown_channel(state->yank_channel.channel);
    colod_shutdown_channel(state->channel.channel);

    while (state->inflight) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }

    g_io_channel_unref(state->yank_channel.channel);
    g_io_channel_unref(state->channel.channel);
    g_free(state);
}

ColodQmpState *qmp_new(int fd1, int fd2, GError **errp) {
    ColodQmpState *state;

    state = g_new0(ColodQmpState, 1);
    state->timeout = 5000;
    state->channel.channel = colod_create_channel(fd1, errp);
    if (!state->channel.channel) {
        g_free(state);
        return NULL;
    }

    state->yank_channel.channel = colod_create_channel(fd2, errp);
    if (!state->yank_channel.channel) {
        g_free(state->channel.channel);
        g_free(state);
        return NULL;
    }

    qmp_handshake_coroutine(state);
    qmp_event_coroutine(state, &state->channel, FALSE);
    qmp_event_coroutine(state, &state->yank_channel, TRUE);

    return state;
}
