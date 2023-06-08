/*
 * COLO background daemon coroutine utils
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef COUTIL_H
#define COUTIL_H

#include <glib-2.0/glib.h>

#include "coroutine.h"

typedef struct CoroutineUtilCo {
    gsize offset;
} CoroutineUtilCo;

#define colod_lock_co(variable) \
    do { \
        while ((variable)) { \
            progress_source_add(coroutine->cb.plain, coroutine); \
            co_yield(GINT_TO_POINTER(G_SOURCE_REMOVE)); \
        } \
        (variable) = TRUE; \
    } while(0)

#define colod_unlock_co(variable) \
    (variable) = FALSE;

#define colod_channel_read_line_co(ret, channel, line, len, errp) \
    co_call_co((ret), _colod_channel_read_line_co, \
               (channel), (line), (len), (errp))

#define colod_channel_write_co(ret, channel, buf, len, errp) \
    co_call_co((ret), _colod_channel_write_co, \
               (channel), (buf), (len), (errp))

GIOStatus _colod_channel_read_line_co(Coroutine *coroutine,
                                      GIOChannel *channel, gchar **line,
                                      gsize *len, GError **errp);

GIOStatus _colod_channel_write_co(Coroutine *coroutine,
                                  GIOChannel *channel, gchar *buf,
                                  gsize len, GError **errp);

#endif // COUTIL_H
