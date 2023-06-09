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

void qmp_result_free(ColodQmpResult *result) {
    if (!result)
        return;

    json_node_unref(result->json_root);
    g_free(result->line);
    g_free(result);
}

#define qmp_read_line_co(ret, state, errp) \
    co_call_co((ret), _qmp_read_line_co, (state), (errp))

static ColodQmpResult *_qmp_read_line_co(Coroutine *coroutine,
                                         ColodQmpState *state,
                                         GError **errp) {
    ColodQmpCo *co = co_stack(qmpco);
    ColodQmpResult *result;
    GIOStatus ret;

    assert(errp);

    co_begin(ColodQmpResult *, NULL);
    colod_channel_read_line_timeout_co(ret, state->channel, &CO line, &CO len,
                                       state->timeout, errp);
    if (ret != G_IO_STATUS_NORMAL) {
        if ((*errp)->domain == COLOD_ERROR &&
                (*errp)->code == COLOD_ERROR_TIMEOUT) {
            g_error_free(*errp);
            *errp = NULL;
            // yank...
        }
        return NULL;
    }

    result = g_new0(ColodQmpResult, 1);
    result->line = CO line;
    result->len = CO len;

    JsonParser *parser = json_parser_new_immutable();
    json_parser_load_from_data(parser, CO line, CO len, errp);
    if (*errp) {
        g_object_unref(parser);
        g_free(result->line);
        g_free(result);
        return NULL;
    }
    result->json_root = json_node_ref(json_parser_get_root(parser));
    g_object_unref(parser);

    co_end;

    return result;
}

ColodQmpResult *_qmp_execute_co(Coroutine *coroutine,
                                ColodQmpState *state,
                                GError **errp,
                                const gchar *command) {
    ColodQmpResult *result;
    GIOStatus ret;

    assert(errp);

    co_begin(ColodQmpResult *, NULL);

    colod_lock_co(state->lock);
    colod_channel_write_co(ret, state->channel, command, strlen(command),
                           errp);
    if (ret != G_IO_STATUS_NORMAL) {
        colod_unlock_co(state->lock);
        return NULL;
    }

    qmp_read_line_co(result, state, errp);
    if (*errp) {
        return NULL;
    }

    colod_unlock_co(state->lock);

    co_end;

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

static gboolean _qmp_handshake_readable_co(Coroutine *coroutine) {
    ColodQmpCo *co = co_stack(qmpco);
    GError *errp = NULL;
    ColodQmpResult *result;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    qmp_read_line_co(result, CO state, &errp);
    if (errp) {
        exit(EXIT_FAILURE);
    }
    qmp_result_free(result);

    qmp_execute_co(result, CO state, &errp,
                   "{'execute': 'qmp_capabilities', "
                        "'arguments': {'enable': ['oob']}}\n");
    qmp_result_free(result);

    colod_unlock_co(CO state->lock);

    co_end;

    return G_SOURCE_REMOVE;
}

static gboolean qmp_handshake_readable_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return qmp_handshake_readable_co(data);
}

ColodQmpState *qmp_new(int fd, GError **errp) {
    ColodQmpState *state;
    Coroutine *coroutine;
    ColodQmpCo *co;

    assert(errp);

    state = g_new0(ColodQmpState, 1);
    state->timeout = 5000;
    state->channel = colod_create_channel(fd, errp);
    if (*errp) {
        g_free(state);
        return NULL;
    }

    coroutine = g_new0(Coroutine, 1);
    coroutine->cb.plain = qmp_handshake_readable_co;
    coroutine->cb.iofunc = qmp_handshake_readable_co_wrap;
    co = co_stack(qmpco);
    co->state = state;

    state->lock.count = 1;
    state->lock.holder = coroutine;

    g_io_add_watch(state->channel, G_IO_IN, qmp_handshake_readable_co_wrap,
                   coroutine);
    return state;
}
