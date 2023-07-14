/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef DAEMON_H
#define DAEMON_H

#include <glib-2.0/glib.h>

#include <corosync/cpg.h>
#include <corosync/corotypes.h>

#include "base_types.h"
#include "qmp.h"

typedef enum ColodRole {
    ROLE_PRIMARY,
    ROLE_SECONDARY
} ColodRole;

typedef struct ColodContext {
    /* Parameters */
    gboolean daemonize, syslog;

    /* Variables */
    GMainContext *mainctx;
    GMainLoop *mainloop;
    char *node_name, *instance_name, *base_dir, *qmp_path;

    int qmp_fd, mngmt_listen_fd, cpg_fd;
    guint cpg_source_id;

    ColodClientListener *listener;

    ColodQmpState *qmpstate;
    ColodRole role;
    gboolean replication;

    cpg_handle_t cpg_handle;
} ColodContext;

void colod_syslog(ColodContext *ctx, int pri, const char *fmt, ...)
     __attribute__ ((__format__ (__printf__, 3, 4)));

#define colod_check_health_co(result, ctx, errp) \
    co_call_co((result), _colod_check_health_co, (ctx), (errp))
int _colod_check_health_co(Coroutine *coroutine, ColodContext *ctx,
                           GError **errp);

void colod_quit(ColodContext *ctx);

#endif // DAEMON_H
