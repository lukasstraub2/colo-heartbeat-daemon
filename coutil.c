/*
 * COLO background daemon coroutine utils
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include "coutil.h"

#include <glib-2.0/glib.h>

#include "coroutine_stack.h"

GIOStatus _colod_channel_read_line_co(Coroutine *coroutine,
                                      GIOChannel *channel, gchar **line,
                                      gsize *len, GError **errp) {
    CoroutineUtilCo *co = co_stack(utilco);
    GIOStatus ret;

    co_begin(GIOStatus, 0);

    g_io_add_watch(channel, G_IO_IN, coroutine->cb.iofunc, coroutine);
    co_yield(GINT_TO_POINTER(G_SOURCE_REMOVE));

    while (TRUE) {
        ret = g_io_channel_read_line(channel, line, len, NULL, errp);
        if ((ret == G_IO_STATUS_NORMAL && *len == 0) ||
                ret == G_IO_STATUS_AGAIN) {
            co_yield(GINT_TO_POINTER(G_SOURCE_CONTINUE));
        } else {
            break;
        }
    }
    co_end;

    return ret;
}

GIOStatus _colod_channel_write_co(Coroutine *coroutine,
                                  GIOChannel *channel, gchar *buf,
                                  gsize len, GError **errp) {
    CoroutineUtilCo *co = co_stack(utilco);
    GIOStatus ret;
    gsize write_len;
    CO offset = 0;

    co_begin(GIOStatus, 0);

    g_io_add_watch(channel, G_IO_OUT, coroutine->cb.iofunc, coroutine);
    co_yield(GINT_TO_POINTER(G_SOURCE_REMOVE));

    while (CO offset < len) {
        ret = g_io_channel_write_chars(channel,
                                       buf + CO offset,
                                       len - CO offset,
                                       &write_len, errp);
        CO offset += write_len;
        if (ret == G_IO_STATUS_NORMAL || ret == G_IO_STATUS_AGAIN) {
            if (write_len == 0) {
                co_yield(GINT_TO_POINTER(G_SOURCE_CONTINUE));
            }
        } else {
            break;
        }
    }

    if (ret != G_IO_STATUS_NORMAL) {
        return ret;
    }

    ret = g_io_channel_flush(channel, errp);
    while(ret == G_IO_STATUS_AGAIN) {
        co_yield(GINT_TO_POINTER(G_SOURCE_CONTINUE));
        ret = g_io_channel_flush(channel, errp);
    }
    co_end;

    return ret;
}
