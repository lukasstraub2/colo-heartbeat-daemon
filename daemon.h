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

#include <syslog.h>
#include <stdio.h>

#include <glib-2.0/glib.h>

#include "base_types.h"
#include "util.h"
#include "qmp.h"
#include "qmpcommands.h"

typedef struct ColodContext {
    /* Parameters */
    const gchar *node_name, *instance_name, *base_dir;
    const gchar *qemu, *qemu_img, *listen_address, *active_hidden_dir;
    const gchar *monitor_interface;
    const gchar *advanced_config;
    const gchar *qemu_options;
    gboolean daemonize;
    guint qmp_timeout_low, qmp_timeout_high;
    guint command_timeout;
    guint watchdog_interval;
    gboolean do_trace;

    /* Variables */
    int mngmt_listen_fd;

    QmpCommands *commands;
    ColodClientListener *listener;
    Cpg *cpg;
    PeerManager *peer;
} ColodContext;

void colod_syslog(int pri, const char *fmt, ...)
     __attribute__ ((__format__ (__printf__, 2, 3)));
#define log_error(message) \
    colod_syslog(LOG_ERR, "%s:%u: %s", __func__, __LINE__, message)
#define log_error_fmt(fmt, ...) \
    colod_syslog(LOG_ERR, "%s:%u: " fmt, __func__, __LINE__, ##__VA_ARGS__)

void colod_trace(const char *fmt, ...);

void daemon_mainloop(ColodContext *mctx);
int daemon_main(int argc, char **argv);

#endif // DAEMON_H
