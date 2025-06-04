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
#include "daemon.h"

typedef struct QmpChannel {
    GIOChannel *channel;
    CoroutineLock lock;
    gboolean discard_events;
} QmpChannel;

struct ColodQmpState {
    QmpChannel channel;
    QmpChannel yank_channel;
    guint timeout;
    JsonNode *yank_instances;
    ColodCallbackHead event_callbacks;
    ColodCallbackHead hup_callbacks;
    guint inflight;
    guint hup_source_id;
};

void qmp_add_notify_event(ColodQmpState *state, QmpEventCallback _func,
                          gpointer user_data) {
    ColodCallbackFunc func = (ColodCallbackFunc) _func;
    colod_callback_add(&state->event_callbacks, func, user_data);
}

void qmp_del_notify_event(ColodQmpState *state, QmpEventCallback _func,
                          gpointer user_data) {
    ColodCallbackFunc func = (ColodCallbackFunc) _func;
    colod_callback_del(&state->event_callbacks, func, user_data);
}

void qmp_add_notify_hup(ColodQmpState *state, QmpYankCallback _func,
                        gpointer user_data) {
    ColodCallbackFunc func = (ColodCallbackFunc) _func;
    colod_callback_add(&state->hup_callbacks, func, user_data);
}

void qmp_del_notify_hup(ColodQmpState *state, QmpYankCallback _func,
                        gpointer user_data) {
    ColodCallbackFunc func = (ColodCallbackFunc) _func;
    colod_callback_del(&state->hup_callbacks, func, user_data);
}

static void notify_event(ColodQmpState *state, ColodQmpResult *result) {
    ColodCallback *entry, *next_entry;
    QLIST_FOREACH_SAFE(entry, &state->event_callbacks, next, next_entry) {
        QmpEventCallback func = (QmpEventCallback) entry->func;
        func(entry->user_data, result);
    }
}

static void notify_hup(ColodQmpState *state) {
    ColodCallback *entry, *next_entry;
    QLIST_FOREACH_SAFE(entry, &state->hup_callbacks, next, next_entry) {
        QmpYankCallback func = (QmpYankCallback) entry->func;
        func(entry->user_data);
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

#define qmp_read_line_co(...) \
    co_wrap(_qmp_read_line_co(__VA_ARGS__))
static ColodQmpResult *_qmp_read_line_co(Coroutine *coroutine,
                                         ColodQmpState *state,
                                         QmpChannel *channel,
                                         gboolean yank,
                                         gboolean skip_events,
                                         GError **errp) {
    struct {
        gchar *line;
        gsize len;
    } *co;
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpResult *, NULL);

    while (TRUE) {
        co_recurse(ret = colod_channel_read_line_timeout_co(coroutine,
                            channel->channel, &CO line, &CO len,
                            state->timeout, &local_errp));
        if (ret < 0) {
            log_error(local_errp->message);
            if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT)) {
                if (yank) {
                    g_error_free(local_errp);
                    local_errp = NULL;

                    co_recurse(ret = qmp_yank_co(coroutine, state, &local_errp));
                    if (ret < 0) {
                        colod_trace("%s:%u: %s\n", __func__, __LINE__, local_errp->message);
                        g_propagate_error(errp, local_errp);
                        return NULL;
                    }
                    co_recurse(result = qmp_read_line_co(coroutine, state, channel,
                                                         FALSE, skip_events, errp));
                    if (result) {
                        result->did_yank = TRUE;
                    }
                    return result;
                }
            }
            g_propagate_error(errp, local_errp);
            return NULL;
        }

        result = qmp_parse_result(CO line, CO len, &local_errp);
        if (!result) {
            log_error(local_errp->message);
            g_propagate_error(errp, local_errp);
            return NULL;
        }

        if (has_member(result->json_root, "event")) {
            if (!object_matches_json(result->json_root, "{'event': 'MIGRATION_PASS'}")
                    && !channel->discard_events) {
                colod_trace("%s", result->line);
            }
        } else {
            colod_trace("%s", result->line);
        }

        if (skip_events && has_member(result->json_root, "event")) {
            if (!channel->discard_events) {
                notify_event(state, result);
            }
            qmp_result_free(result);
            if (!channel->discard_events) {
                g_idle_add(coroutine->cb, coroutine);
                co_yield_int(G_SOURCE_REMOVE);
            }
            continue;
        }

        break;
    }

    co_end;

    return result;
}

#define qmp_execute_rec_co(...) co_wrap(_qmp_execute_rec_co(__VA_ARGS__))
static ColodQmpResult *_qmp_execute_rec_co(Coroutine *coroutine,
                                           ColodQmpState *state,
                                           QmpChannel *channel,
                                           gboolean yank,
                                           GError **errp,
                                           const gchar *command) {
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    co_begin(ColodQmpResult *, NULL);

    state->inflight++;
    colod_lock_co(channel->lock);
    colod_trace("%s", command);
    co_recurse(ret = colod_channel_write_timeout_co(coroutine, channel->channel,
                                   command, strlen(command), state->timeout,
                                   &local_errp));
    if (ret < 0) {
        log_error(local_errp->message);
        g_propagate_prefixed_error(errp, local_errp, "qmp: ");
        colod_unlock_co(channel->lock);
        state->inflight--;
        return NULL;
    }

    co_recurse(result = qmp_read_line_co(coroutine, state, channel, yank, TRUE,
                                         &local_errp));
    colod_unlock_co(channel->lock);
    state->inflight--;
    if (!result) {
        g_propagate_prefixed_error(errp, local_errp, "qmp: ");
        return NULL;
    }

    co_end;

    return result;
}

ColodQmpResult *_qmp_execute_co(Coroutine *coroutine,
                                ColodQmpState *state,
                                GError **errp,
                                const gchar *command) {
    ColodQmpResult *result;

    result = _qmp_execute_rec_co(coroutine, state, &state->channel, TRUE, errp,
                                 command);
    if (coroutine->yield) {
        return NULL;
    }
    if (!result) {
        return NULL;
    }
    if (has_member(result->json_root, "error")) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_QMP,
                    "qmp command returned error: %s %s",
                    command, result->line);
        qmp_result_free(result);
        return NULL;
    }

    return result;
}

ColodQmpResult *_qmp_execute_nocheck_co(Coroutine *coroutine,
                                        ColodQmpState *state,
                                        GError **errp,
                                        const gchar *command) {
    return _qmp_execute_rec_co(coroutine, state, &state->channel, TRUE, errp,
                               command);
}

static gchar *pick_yank_instances(JsonNode *result,
                                  JsonNode *yank_matches) {
    JsonArray *result_array;
    JsonArray *output_array;
    gchar *output_str;
    JsonNode *output_node;

    assert(JSON_NODE_HOLDS_OBJECT(result));
    assert(JSON_NODE_HOLDS_ARRAY(yank_matches));

    output_node = json_node_alloc();
    output_array = json_array_new();
    json_node_init_array(output_node, output_array);

    result = get_member_node(result, "return");
    result_array = json_node_get_array(result);
    guint count = json_array_get_length(result_array);
    for (guint i = 0; i < count; i++) {
        JsonNode *element = json_array_get_element(result_array, i);
        assert(element);

        if (object_matches_match_array(element, yank_matches)) {
            json_array_add_element(output_array, element);
        }
    }

    output_str = json_to_string(output_node, FALSE);
    json_node_unref(output_node);
    return output_str;
}

int _qmp_yank_co(Coroutine *coroutine, ColodQmpState *state,
                 GError **errp) {
    struct {
        gchar *command;
    } *co;
    ColodQmpResult *result;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    co_recurse(result = qmp_execute_rec_co(coroutine, state, &state->yank_channel,
                                           FALSE, errp,
                                           "{'exec-oob': 'query-yank', 'id': 'yank0'}\n"));
    if (!result) {
        return -1;
    }
    if (has_member(result->json_root, "error")) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_FATAL,
                    "qmp query-yank: %s", result->line);
        qmp_result_free(result);
        return -1;
    }

    gchar *instances = pick_yank_instances(result->json_root, state->yank_instances);
    CO command = g_strdup_printf("{'exec-oob': 'yank', 'id': 'yank0', "
                                        "'arguments':{ 'instances': %s }}\n",
                                 instances);
    g_free(instances);
    qmp_result_free(result);

    co_recurse(result = qmp_execute_rec_co(coroutine, state, &state->yank_channel,
                                           FALSE, errp, CO command));
    if (!result) {
        g_free(CO command);
        return -1;
    }
    if (has_member(result->json_root, "error")) {
        const char *class = get_member_member_str(result->json_root, "error", "class");
        if (!strcmp(class, "DeviceNotFound")) {
            g_free(CO command);
            qmp_result_free(result);
            int ret;
            co_recurse(ret = qmp_yank_co(coroutine, state, errp))
            return ret;
        }
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_FATAL,
                    "qmp yank: %s: %s", CO command, result->line);
        qmp_result_free(result);
        g_free(CO command);
        return -1;
    }
    g_free(CO command);

    qmp_result_free(result);

    co_end;

    return 0;
}

typedef struct QmpCoroutine {
    Coroutine coroutine;
    ColodQmpState *state;
    QmpChannel *channel;
} QmpCoroutine;

static gboolean _qmp_handshake_readable_co(Coroutine *coroutine);
static gboolean qmp_handshake_readable_co(gpointer data) {
    QmpCoroutine *qmpco = data;
    Coroutine *coroutine = &qmpco->coroutine;
    gboolean ret;

    co_enter(coroutine, ret = _qmp_handshake_readable_co(coroutine));
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    colod_assert_remove_one_source(coroutine);
    qmpco->state->inflight--;
    g_free(coroutine);
    return ret;
}

static gboolean _qmp_handshake_readable_co(Coroutine *coroutine) {
    QmpCoroutine *qmpco = (QmpCoroutine *) coroutine;
    ColodQmpState *qmp = qmpco->state;
    ColodQmpResult *result;
    GError *local_errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    co_recurse(result = qmp_read_line_co(coroutine, qmp, qmpco->channel,
                                         FALSE, TRUE, &local_errp));
    if (!result) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        colod_unlock_co(qmpco->channel->lock);
        return G_SOURCE_REMOVE;
    }
    qmp_result_free(result);

    co_recurse(result = qmp_execute_rec_co(coroutine, qmp, qmpco->channel, FALSE, &local_errp,
                                           "{'execute': 'qmp_capabilities', "
                                           "'arguments': {'enable': ['oob']}}\n"));
    colod_unlock_co(qmpco->channel->lock);
    if (!result) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return G_SOURCE_REMOVE;
    }
    if (has_member(result->json_root, "error")) {
        log_error_fmt("qmp_capabilities: %s", result->line);
        qmp_result_free(result);
        return G_SOURCE_REMOVE;
    }
    qmp_result_free(result);

    co_end;

    return G_SOURCE_REMOVE;
}

static Coroutine *qmp_handshake_coroutine(ColodQmpState *state,
                                          QmpChannel *channel) {
    QmpCoroutine *qmpco;
    Coroutine *coroutine;

    qmpco = g_new0(QmpCoroutine, 1);
    coroutine = &qmpco->coroutine;
    coroutine->cb = qmp_handshake_readable_co;
    qmpco->state = state;
    qmpco->channel = channel;

    assert(!channel->lock.count);
    channel->lock.count = 1;
    channel->lock.holder = coroutine;

    g_idle_add(qmp_handshake_readable_co, coroutine);

    state->inflight++;
    return coroutine;
}

typedef struct ColodWaitState {
    Coroutine *coroutine;
    JsonNode *match;
    ColodQmpState *state;
    gboolean fired;
} ColodWaitState;

static void qmp_wait_event_cb(gpointer data, ColodQmpResult *result) {
    ColodWaitState *state = data;

    if (object_matches(result->json_root, state->match)) {
        state->fired = TRUE;
        g_idle_add_full(G_PRIORITY_HIGH, state->coroutine->cb,
                        state->coroutine, NULL);
        qmp_del_notify_event(state->state, qmp_wait_event_cb, state);
    }
}

int _qmp_wait_event_co(Coroutine *coroutine, ColodQmpState *state,
                       guint timeout, const gchar *match, GError **errp) {
    struct {
        ColodWaitState *wait_state;
        guint timeout_source_id;
    } *co;
    JsonNode *parsed;
    int ret = 0;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    parsed = json_from_string(match, NULL);
    assert(parsed);

    CO wait_state = g_new0(ColodWaitState, 1);
    CO wait_state->coroutine = coroutine;
    CO wait_state->match = parsed;
    CO wait_state->state = state;
    qmp_add_notify_event(state, qmp_wait_event_cb, CO wait_state);
    CO timeout_source_id = 0;
    if (timeout) {
        CO timeout_source_id = g_timeout_add(timeout, coroutine->cb,
                                             coroutine);
    }

    co_yield_int(G_SOURCE_REMOVE);
    if (!CO wait_state->fired) {
        if (g_source_get_id(g_main_current_source()) == CO timeout_source_id) {
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT,
                        "Timeout reached while waiting for qmp event: %s",
                        match);
        } else {
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT,
                        "Got interrupted while waiting for qmp event: %s",
                        match);
        }
        qmp_del_notify_event(state, qmp_wait_event_cb, CO wait_state);
        ret = -1;
    }

    if (timeout) {
        g_source_remove(CO timeout_source_id);
    }
    json_node_unref(CO wait_state->match);
    g_free(CO wait_state);

    co_end;

    return ret;
}

static gboolean _qmp_event_co(Coroutine *coroutine);
static gboolean qmp_event_co(gpointer data) {
    QmpCoroutine *qmpco = data;
    Coroutine *coroutine = &qmpco->coroutine;
    gboolean ret;

    co_enter(coroutine, ret = _qmp_event_co(coroutine));
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    colod_assert_remove_one_source(coroutine);
    qmpco->state->inflight--;
    g_free(qmpco);
    return ret;
}

static gboolean _qmp_event_co(Coroutine *coroutine) {
    QmpCoroutine *qmpco = (QmpCoroutine *) coroutine;
    QmpChannel *channel = qmpco->channel;
    ColodQmpResult *result;
    GError *local_errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    while (TRUE) {
        g_io_add_watch_full(channel->channel, G_PRIORITY_DEFAULT_IDLE,
                            G_IO_IN | G_IO_HUP, coroutine_giofunc_cb, coroutine,
                            NULL);
        co_yield_int(G_SOURCE_REMOVE);

        while (channel->lock.holder) {
            co_yield_int(G_SOURCE_CONTINUE);
        }
        colod_lock_co(channel->lock);

        co_recurse(result = qmp_read_line_co(coroutine, qmpco->state, channel,
                                             FALSE, FALSE, &local_errp));
        colod_unlock_co(channel->lock);
        if (!result) {
            log_error(local_errp->message);
            g_error_free(local_errp);
            return G_SOURCE_REMOVE;
        }
        if (!has_member(result->json_root, "event")) {
            log_error_fmt("Not an event: %s", result->line);
            qmp_result_free(result);
            continue;
        }

        if (!channel->discard_events) {
            notify_event(qmpco->state, result);
        }
        qmp_result_free(result);
    }

    co_end;

    return G_SOURCE_REMOVE;
}

static gboolean qmp_hup_cb(G_GNUC_UNUSED GIOChannel *channel,
                           G_GNUC_UNUSED GIOCondition revents,
                           gpointer data) {
    ColodQmpState *state = data;

    log_error("qemu quit");
    notify_hup(state);

    state->hup_source_id = 0;
    return G_SOURCE_REMOVE;
}

static Coroutine *qmp_event_coroutine(ColodQmpState *state,
                                      QmpChannel *channel) {
    QmpCoroutine *qmpco;
    Coroutine *coroutine;

    qmpco = g_new0(QmpCoroutine, 1);
    coroutine = &qmpco->coroutine;
    coroutine->cb = qmp_event_co;
    qmpco->state = state;
    qmpco->channel = channel;

    g_io_add_watch_full(channel->channel, G_PRIORITY_DEFAULT_IDLE,
                        G_IO_IN | G_IO_HUP, coroutine_giofunc_cb, coroutine,
                        NULL);

    state->inflight++;
    return coroutine;
}

void qmp_set_yank_instances(ColodQmpState *state, JsonNode *instances) {
    if (state->yank_instances) {
        json_node_unref(state->yank_instances);
    }
    state->yank_instances = json_node_ref(instances);
}

void qmp_set_timeout(ColodQmpState *state, guint timeout) {
    assert(timeout);

    state->timeout = timeout;
}

static void qmp_free(gpointer _this) {
    ColodQmpState *state = _this;
    if (state->hup_source_id) {
        g_source_remove(state->hup_source_id);
    }

    colod_callback_clear(&state->event_callbacks);
    colod_callback_clear(&state->hup_callbacks);

    colod_shutdown_channel(state->yank_channel.channel);
    colod_shutdown_channel(state->channel.channel);

    while (state->inflight) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }

    g_io_channel_unref(state->yank_channel.channel);
    g_io_channel_unref(state->channel.channel);
    if (state->yank_instances) {
        json_node_unref(state->yank_instances);
    }
}

ColodQmpState *qmp_new(int fd, int yank_fd, guint timeout, GError **errp) {
    ColodQmpState *state;

    state = g_rc_box_new0(ColodQmpState);
    state->timeout = timeout;
    state->channel.channel = colod_create_channel(fd, errp);
    if (!state->channel.channel) {
        qmp_unref(state);
        return NULL;
    }

    state->yank_channel.channel = colod_create_channel(yank_fd, errp);
    state->yank_channel.discard_events = TRUE;
    if (!state->yank_channel.channel) {
        g_io_channel_unref(state->channel.channel);
        qmp_unref(state);
        return NULL;
    }

    qmp_handshake_coroutine(state, &state->channel);
    qmp_handshake_coroutine(state, &state->yank_channel);
    qmp_event_coroutine(state, &state->channel);
    qmp_event_coroutine(state, &state->yank_channel);

    state->hup_source_id = g_io_add_watch(state->channel.channel, G_IO_HUP,
                                          qmp_hup_cb, state);

    return state;
}

ColodQmpState *qmp_ref(ColodQmpState *this) {
    return g_rc_box_acquire(this);
}

void qmp_unref(ColodQmpState *this) {
    g_rc_box_release_full(this, qmp_free);
}
