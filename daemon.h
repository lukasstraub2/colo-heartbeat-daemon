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

#include "qmp.h"

typedef enum ColodRole {
    ROLE_PRIMARY,
    ROLE_SECONDARY
} ColodRole;

typedef struct ColodContext {
    /* Parameters */
    gboolean daemonize;

    /* Variables */
    GMainContext *mainctx;
    GMainLoop *mainloop;
    char *node_name, *instance_name, *base_dir, *qmp_path;

    int qmp_fd, mngmt_listen_fd, cpg_fd;
    guint mngmt_listen_source_id, cpg_source_id;

    ColodQmpState *qmpstate;
    ColodRole role;
    gboolean replication;

    cpg_handle_t cpg_handle;
} ColodContext;

typedef struct ColodClientCo {
    ColodContext *ctx;
    GIOChannel *client_channel;
    gsize read_len;
    gchar *line;
    union {
        ColodQmpResult *result;
        ColodQmpResult *qemu_status;
    };
    union {
        ColodQmpResult *request;
        ColodQmpResult *colo_status;
    };
} ColodClientCo;

#endif // DAEMON_H
