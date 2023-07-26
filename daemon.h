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

#include <glib-2.0/glib.h>

#include <corosync/cpg.h>
#include <corosync/corotypes.h>

#include "base_types.h"
#include "util.h"
#include "qmp.h"

typedef enum ColodRole {
    ROLE_PRIMARY,
    ROLE_SECONDARY
} ColodRole;

typedef enum ColodEvent ColodEvent;

typedef struct ColodContext {
    /* Parameters */
    gboolean daemonize;
    gboolean disable_cpg;
    guint qmp_timeout_low, qmp_timeout_high;
    guint checkpoint_interval;
    guint watchdog_interval;

    /* Variables */
    GMainContext *mainctx;
    GMainLoop *mainloop;
    char *node_name, *instance_name, *base_dir, *qmp_path;

    int qmp1_fd, qmp2_fd, mngmt_listen_fd, cpg_fd;
    guint cpg_source_id;

    ColodWatchdog *watchdog;
    Coroutine *raise_timeout_coroutine;
    JsonNode *migration_commands;
    JsonNode *failover_primary_commands, *failover_secondary_commands;
    Coroutine *main_coroutine;
    ColodQueue events, critical_events;
    gboolean pending_action, transitioning;
    gboolean failed, yellow;

    ColodClientListener *listener;

    ColodQmpState *qmp;
    ColodRole role;
    gboolean replication;

    cpg_handle_t cpg_handle;
} ColodContext;

typedef struct ColodCo {
    JsonNode *commands;
    union {
        ColodQmpResult *result;
        ColodQmpResult *qemu_status;
    };
    union {
        ColodQmpResult *request;
        ColodQmpResult *colo_status;
    };
} ColodCo;

typedef struct ColodArrayCo {
    JsonReader *reader;
    gchar *line;
    guint i, count;
} ColodArrayCo;

void colod_syslog(int pri, const char *fmt, ...)
     __attribute__ ((__format__ (__printf__, 2, 3)));
#define log_error(message) \
    colod_syslog(LOG_ERR, "%s: %s", __func__, message)
#define log_error_fmt(fmt, ...) \
    colod_syslog(LOG_ERR, "%s: " fmt, __func__, ##__VA_ARGS__)

#define colod_check_health_co(result, ctx, errp) \
    co_call_co((result), _colod_check_health_co, (ctx), (errp))
int _colod_check_health_co(Coroutine *coroutine, ColodContext *ctx,
                           GError **errp);

void colod_set_migration_commands(ColodContext *ctx, JsonNode *commands);
void colod_set_primary_commands(ColodContext *ctx, JsonNode *commands);
void colod_set_secondary_commands(ColodContext *ctx, JsonNode *commands);

int colod_start_migration(ColodContext *ctx);

#define colod_execute_nocheck_co(result, ctx, errp, command) \
    co_call_co((result), _colod_execute_nocheck_co, (ctx), (errp), (command))
ColodQmpResult *_colod_execute_nocheck_co(Coroutine *coroutine,
                                          ColodContext *ctx,
                                          GError **errp,
                                          const gchar *command);

#define colod_execute_co(result, ctx, errp, command) \
    co_call_co((result), _colod_execute_co, (ctx), (errp), (command))
ColodQmpResult *_colod_execute_co(Coroutine *coroutine,
                                  ColodContext *ctx,
                                  GError **errp,
                                  const gchar *command);

void colod_quit(ColodContext *ctx);

#endif // DAEMON_H
