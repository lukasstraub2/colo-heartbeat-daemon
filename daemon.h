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

#include "coutil.h"

typedef struct ColodContext {
    /* Parameters */
    gboolean daemonize;

    /* Variables */
    GMainContext *mainctx;
    GMainLoop *mainloop;
    char *node_name, *instance_name, *base_dir, *qmp_path;

    int qmp_fd, mngmt_listen_fd, cpg_fd;
    guint qmp_source_id, mngmt_listen_source_id, cpg_source_id;


    GIOChannel *qmp_channel;
    gboolean qmp_lock;
    cpg_handle_t cpg_handle;
} ColodContext;

typedef struct ColodClientCo {
    ColodContext *ctx;
    GIOChannel *client_channel;
    gsize read_len;
    gchar *line;
    size_t write_len;
} ColodClientCo;

#endif // DAEMON_H
