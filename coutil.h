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
#include "util.h"

typedef struct CoroutineLock {
    Coroutine *holder;
    unsigned int count;
} CoroutineLock;

#define colod_lock_co(lock) \
    do { \
        if ((lock).holder == coroutine) { \
            assert((lock).count); \
            (lock).count++; \
            break; \
        } \
        while ((lock).holder) { \
            progress_source_add(coroutine->cb, coroutine); \
            co_yield_int(G_SOURCE_REMOVE); \
        } \
        assert((lock).count == 0); \
        (lock).holder = coroutine; \
        (lock).count++; \
    } while(0)

#define colod_unlock_co(lock) \
    do { \
        assert((lock).holder == coroutine && (lock).count); \
        (lock).count--; \
        if (!(lock).count) { \
            (lock).holder = NULL; \
        } \
    } while(0)

#define colod_wait_co(...) \
    co_wrap(_colod_wait_co(__VA_ARGS__))
int _colod_wait_co(Coroutine *coroutine, GPid pid, guint timeout, GError **errp);

#define colod_execute_sync_timeout_co(...) \
    co_wrap(_colod_execute_sync_timeout_co(__VA_ARGS__))
int _colod_execute_sync_timeout_co(Coroutine *coroutine, MyArray *argv,
                                   guint timeout, GError **errp);

#define colod_execute_sync_co(...) \
    co_wrap(_colod_execute_sync_co(__VA_ARGS__))
int _colod_execute_sync_co(Coroutine *coroutine, MyArray *argv, GError **errp);

#define colod_channel_read_line_timeout_co(...) \
    co_wrap(_colod_channel_read_line_timeout_co(__VA_ARGS__))
int _colod_channel_read_line_timeout_co(Coroutine *coroutine,
                                        GIOChannel *channel,
                                        gchar **line,
                                        gsize *len,
                                        guint timeout,
                                        GError **errp);

#define colod_channel_read_line_co(...) \
    co_wrap(_colod_channel_read_line_co(__VA_ARGS__))
int _colod_channel_read_line_co(Coroutine *coroutine,
                                GIOChannel *channel, gchar **line,
                                gsize *len, GError **errp);

#define colod_channel_write_timeout_co(...) \
    co_wrap(_colod_channel_write_timeout_co(__VA_ARGS__))
int _colod_channel_write_timeout_co(Coroutine *coroutine,
                                    GIOChannel *channel,
                                    const gchar *buf,
                                    gsize len,
                                    guint timeout,
                                    GError **errp);

#define colod_channel_write_co(...) \
    co_wrap(_colod_channel_write_co(__VA_ARGS__))
int _colod_channel_write_co(Coroutine *coroutine,
                            GIOChannel *channel, const gchar *buf,
                            gsize len, GError **errp);

#endif // COUTIL_H
