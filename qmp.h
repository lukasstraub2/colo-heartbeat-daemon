/*
 * COLO background daemon qmp
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef QMP_H
#define QMP_H

#include <glib-2.0/glib.h>

#include <json-glib-1.0/json-glib/json-glib.h>

#include "coroutine.h"
#include "coutil.h"

typedef struct ColodQmpState {
    GIOChannel *channel;
    CoroutineLock lock;
    guint timeout;
} ColodQmpState;

typedef struct ColodQmpCo {
    ColodQmpState *state;
    gchar *line;
    gsize len;
} ColodQmpCo;

typedef struct ColodQmpResult {
    JsonNode *json_root;
    gchar *line;
    gsize len;
} ColodQmpResult;

void qmp_result_free(ColodQmpResult *result);

ColodQmpState *qmp_new(int fd, GError **errp);

#define qmp_execute_co(ret, state, errp, command) \
    co_call_co((ret), _qmp_execute_co, (state), (errp), (command))
ColodQmpResult *_qmp_execute_co(Coroutine *coroutine,
                                ColodQmpState *state,
                                GError **errp,
                                const gchar *command);

#endif // QMP_H
