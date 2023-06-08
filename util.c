/*
 * Utilities
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

// For O_CLOEXEC
#define _GNU_SOURCE

#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

G_DEFINE_QUARK(colod-error-quark, colod_error)

int colod_fd_set_blocking(int fd, gboolean blocking, GError **errp) {
    int flags, ret;

    ret = fcntl(fd, F_GETFL, 0);
    if (ret < 0) {
        colod_error_set(errp, "Failed to get file flags: %s",
                        g_strerror(errno));
        return -1;
    }
    flags = ret;

    if (blocking) {
        flags &= ~O_NONBLOCK;
    } else {
        flags |= O_NONBLOCK;
    }

    ret = fcntl(fd, F_SETFL, flags);
    if (ret < 0) {
        colod_error_set(errp, "Failed to set file flags: %s",
                        g_strerror(errno));
        return -1;
    }

    return 0;
}

static gboolean progress_source_prepare(G_GNUC_UNUSED GSource *source,
                                        gint *timeout)
{
    *timeout = -1;

    return FALSE;
}

static gboolean progress_source_check(G_GNUC_UNUSED GSource *source)
{
    return TRUE;
}

static gboolean progress_source_dispatch(G_GNUC_UNUSED GSource *source,
                                         GSourceFunc callback,
                                         gpointer user_data)
{
    return (*callback)(user_data);
}

static void progress_source_finalize(G_GNUC_UNUSED GSource *source)
{
    // NOOP
}

GSourceFuncs progress_source_funcs = {
    progress_source_prepare,
    progress_source_check,
    progress_source_dispatch,
    progress_source_finalize,
    NULL, NULL
};

gint progress_source_add(GSourceFunc func, gpointer data) {
    GMainContext *context = g_main_context_default();
    GSource *source;

    source = g_source_new(&progress_source_funcs, sizeof(GSource));
    g_source_set_priority(source, G_PRIORITY_DEFAULT_IDLE);
    g_source_set_callback(source, func, data, NULL);
    return g_source_attach(source, context);
}

struct FDSource {
    GSource source;
    GIOCondition events;
    gpointer tag;
};

static gboolean fd_source_prepare(G_GNUC_UNUSED GSource *source,
                                  gint *timeout)
{
    *timeout = -1;

    return FALSE;
}

static gboolean fd_source_check(GSource *source)
{
    struct FDSource *ssource = (struct FDSource*) source;

    GIOCondition revents = g_source_query_unix_fd(source, ssource->tag);
    return revents & ssource->events;
}

static gboolean fd_source_dispatch(G_GNUC_UNUSED GSource *source,
                                   GSourceFunc callback,
                                   gpointer user_data)
{
    return (*callback)(user_data);
}

static void fd_source_finalize(G_GNUC_UNUSED GSource *source)
{
    // NOOP
}

GSourceFuncs fd_source_funcs = {
    fd_source_prepare,
    fd_source_check,
    fd_source_dispatch,
    fd_source_finalize,
    NULL, NULL
};

GSource *fd_source_create(int fd, GIOCondition events) {
    GSource *source;
    struct FDSource *ssource;

    source = g_source_new(&fd_source_funcs, sizeof(struct FDSource));
    ssource = (struct FDSource *) source;
    ssource->events = events;
    ssource->tag = g_source_add_unix_fd(source, fd, events);

    return source;
}
