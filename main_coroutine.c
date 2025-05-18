/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <stdlib.h>

#include <glib-2.0/glib.h>

#include "main_coroutine.h"
#include "daemon.h"
#include "coroutine.h"
#include "coroutine_stack.h"
#include "client.h"
#include "watchdog.h"
#include "cpg.h"
#include "json_util.h"
#include "eventqueue.h"
#include "raise_timeout_coroutine.h"
#include "yellow_coroutine.h"
#include "qemulauncher.h"
#include "qmpexectx.h"
#include "peer_manager.h"

typedef enum MainState {
    STATE_SECONDARY_STARTUP,
    STATE_SECONDARY_WAIT,
    STATE_SECONDARY_COLO_RUNNING,
    STATE_PRIMARY_STARTUP,
    STATE_PRIMARY_WAIT,
    STATE_PRIMARY_START_MIGRATION,
    STATE_PRIMARY_COLO_RUNNING,
    STATE_FAILOVER_SYNC,
    STATE_FAILOVER,
    STATE_FAILED,
    STATE_QUIT
} MainState;

struct ColodMainCoroutine {
    Coroutine coroutine;
    gboolean mainco_running;
    const ColodContext *ctx;
    EventQueue *queue;
    guint wake_source_id;
    QemuLauncher *launcher;
    ColodQmpState *qmp;
    ColodRaiseCoroutine *raise_timeout_coroutine;
    YellowCoroutine *yellow_co;
    ColodWatchdog *watchdog;

    MainState state;
    gboolean transitioning;
    gboolean failed, yellow;
    gboolean qemu_quit;
    gboolean primary;
    gboolean replication;

    Coroutine *wake_on_exit;
    gboolean return_quit;
};

#define colod_trace_source(data) \
    _colod_trace_source((data), __func__, __LINE__)
static void _colod_trace_source(gpointer data, const gchar *func,
                                int line) {
    GMainContext *mainctx = g_main_context_default();
    GSource *found = g_main_context_find_source_by_user_data(mainctx, data);
    const gchar *found_name = colod_source_name_or_null(found);

    GSource *current = g_main_current_source();
    const gchar *current_name = colod_source_name_or_null(current);

    colod_trace("%s:%u: found source \"%s\", current source \"%s\"\n",
                func, line, found_name, current_name);
}

void colod_main_query_status(ColodMainCoroutine *this, ColodState *ret) {
    PeerManager *peer = this->ctx->peer;
    ret->running = TRUE;
    ret->primary = this->primary;
    ret->replication = this->replication;
    ret->failed = this->failed;
    ret->peer_failover = peer_manager_failover(peer);
    ret->peer_failed = peer_manager_failed(peer);
}

static void __colod_query_status(gpointer data, ColodState *ret) {
    return colod_main_query_status(data, ret);
}

static const gchar *event_str(ColodEvent event) {
    switch (event) {
        case EVENT_FAILED: return "EVENT_FAILED";
        case EVENT_QUIT: return "EVENT_QUIT";

        case EVENT_FAILOVER_SYNC: return "EVENT_FAILOVER_SYNC";
        case EVENT_FAILOVER_WIN: return "EVENT_FAILOVER_WIN";
        case EVENT_YELLOW: return "EVENT_YELLOW";
        case EVENT_UNYELLOW: return "EVENT_UNYELLOW";
        case EVENT_START_MIGRATION: return "EVENT_START_MIGRATION";
        case EVENT_MAX: abort();
    }
    abort();
}

static EventQueue *colod_eventqueue_new() {
    return eventqueue_new(32, EVENT_FAILED, EVENT_QUIT, 0);
}

static gboolean event_always_interrupting(ColodEvent event) {
    switch (event) {
        case EVENT_FAILED:
        case EVENT_QUIT:
            return TRUE;
        break;

        default:
            return FALSE;
        break;
    }
}

static MainState handle_always_interrupting(ColodEvent event) {
    switch (event) {
        case EVENT_FAILED: return STATE_FAILED;
        case EVENT_QUIT: return STATE_QUIT;
        default: abort();
    }
}

static gboolean event_failover(ColodEvent event) {
    return event == EVENT_FAILOVER_SYNC;
}

static MainState handle_event_failover(ColodEvent event) {
    if (event == EVENT_FAILOVER_SYNC) {
        return STATE_FAILOVER_SYNC;
    } else {
        abort();
    }
}

static gboolean event_yellow(ColodEvent event) {
    return event == EVENT_YELLOW || event == EVENT_UNYELLOW;
}

static void event_wake_source_destroy_cb(gpointer data) {
    ColodMainCoroutine *this = data;

    this->wake_source_id = 0;
}

#define colod_event_queue(ctx, event, reason) \
    _colod_event_queue((ctx), (event), (reason), __func__, __LINE__)
static void _colod_event_queue(ColodMainCoroutine *this, ColodEvent event,
                               const gchar *reason, const gchar *func,
                               int line) {
    const Event *last_event;

    colod_trace("%s:%u: queued %s (%s)\n", func, line, event_str(event),
                reason);

    if (this->mainco_running && !this->wake_source_id) {
        if (!eventqueue_pending(this->queue)
                || eventqueue_event_interrupting(this->queue, event)) {
            colod_trace("%s:%u: Waking main coroutine\n", __func__, __LINE__);
            this->wake_source_id = g_idle_add_full(G_PRIORITY_DEFAULT_IDLE,
                                                   this->coroutine.cb, this,
                                                   event_wake_source_destroy_cb);
            g_source_set_name_by_id(this->wake_source_id, "wake for event");
        }
    }

    last_event = eventqueue_last(this->queue);
    if (last_event && last_event->event == event) {
        colod_trace("%s:%u: Ratelimiting events\n", __func__, __LINE__);
        return;
    }

    eventqueue_add(this->queue, event, NULL);
}

static gboolean _colod_eventqueue_pending(ColodMainCoroutine *this) {
    return eventqueue_pending(this->queue);
}

#define _colod_eventqueue_remove(this) \
    __colod_eventqueue_remove(this, __func__, __LINE__)
static ColodEvent __colod_eventqueue_remove(ColodMainCoroutine *this,
                                            const gchar *func, int line) {
    Event *event;
    ColodEvent _event;

    event = eventqueue_remove(this->queue);
    _event = event->event;
    g_free(event);
    colod_trace("%s:%u: got %s\n", func, line, event_str(_event));
    return _event;
}

#define colod_event_wait(coroutine, ctx) \
    co_wrap(_colod_event_wait(coroutine, ctx, __func__, __LINE__))
static ColodEvent _colod_event_wait(Coroutine *coroutine,
                                    ColodMainCoroutine *this,
                                    const gchar *func, int line) {
    guint source_id = g_source_get_id(g_main_current_source());

    if (source_id == this->wake_source_id) {
        this->wake_source_id = 0;
    }

    if (!_colod_eventqueue_pending(this) || this->wake_source_id) {
        coroutine->yield = TRUE;
        coroutine->yield_value = GINT_TO_POINTER(G_SOURCE_REMOVE);
        return EVENT_FAILED;
    }

    return __colod_eventqueue_remove(this, func, line);
}

#define colod_qmp_event_wait_co(...) \
    co_wrap(_colod_qmp_event_wait_co(__VA_ARGS__))
static int _colod_qmp_event_wait_co(Coroutine *coroutine,
                                    ColodMainCoroutine *this,
                                    guint timeout, const gchar* match,
                                    GError **errp) {
    int ret;
    GError *local_errp = NULL;

    co_begin(int, -1)
    while (TRUE) {
        co_recurse(ret = qmp_wait_event_co(coroutine, this->qmp, timeout, match,
                                           &local_errp));

        if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
            assert(eventqueue_pending(this->queue));
            if (!eventqueue_pending_interrupt(this->queue)) {
                g_error_free(local_errp);
                local_errp = NULL;
                continue;
            }
            g_propagate_error(errp, local_errp);
            return ret;
        } else if (ret < 0) {
            g_propagate_error(errp, local_errp);
            return ret;
        }

        break;
    }
    co_end;

    return ret;
}

static int __colod_yank_co(Coroutine *coroutine, gpointer data, GError **errp) {
    ColodMainCoroutine *this = data;
    GError *local_errp = NULL;
    int ret;

    co_begin(int, -1);

    colod_main_ref(this);
    co_recurse(ret = qmp_yank_co(coroutine, this->qmp, &local_errp));
    if (ret < 0) {
        colod_event_queue(this, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
    } else {
        colod_event_queue(this, EVENT_FAILOVER_SYNC, "did yank");
    }

    colod_main_unref(this);
    return ret;
    co_end;
}

#define _colod_execute_nocheck_co(...) co_wrap(__colod_execute_nocheck_co(__VA_ARGS__))
static ColodQmpResult *__colod_execute_nocheck_co(Coroutine *coroutine, gpointer data,
                                                  GError **errp, const gchar *command) {
    ColodMainCoroutine *this = data;
    ColodQmpResult *result;
    GError *local_errp = NULL;

    co_begin(ColodQmpResult *, NULL);

    colod_main_ref(this);
    colod_watchdog_refresh(this->watchdog);

    co_recurse(result = qmp_execute_nocheck_co(coroutine, this->qmp, &local_errp, command));
    if (!result) {
        colod_event_queue(this, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        colod_main_unref(this);
        return NULL;
    }

    if (result->did_yank) {
        colod_event_queue(this, EVENT_FAILOVER_SYNC, "did yank");
    }

    colod_main_unref(this);
    return result;
    co_end;
}

static ColodQmpResult *__colod_execute_co(Coroutine *coroutine, gpointer data,
                                          GError **errp, const gchar *command) {
    ColodQmpResult *result;

    co_begin(ColodQmpResult *, NULL);

    co_recurse(result = _colod_execute_nocheck_co(coroutine, data, errp, command));
    if (!result) {
        return NULL;
    }
    if (has_member(result->json_root, "error")) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_QMP,
                    "qmp command returned error: %s %s",
                    command, result->line);
        qmp_result_free(result);
        return NULL;
    }

    return result;
    co_end;
}

static gboolean qemu_runnng(const gchar *status) {
    return !strcmp(status, "running")
            || !strcmp(status, "finish-migrate")
            || !strcmp(status, "colo")
            || !strcmp(status, "prelaunch")
            || !strcmp(status, "paused");
}

#define qemu_query_status_co(...) \
    co_wrap(_qemu_query_status_co(__VA_ARGS__))
static QmpEctx *_qemu_query_status_co(Coroutine *coroutine, ColodMainCoroutine *this,
                                      gboolean *primary, gboolean *replication,
                                      GError **errp) {
    struct {
        QmpEctx *ectx;
        ColodQmpResult *qemu_status, *colo_status;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(QmpEctx *, NULL);

    CO ectx = qmp_ectx_new(this->qmp);
    qmp_ectx_set_ignore_yank(CO ectx);

    co_recurse(CO qemu_status = qmp_ectx(coroutine, CO ectx, "{'execute': 'query-status'}\n"));
    co_recurse(CO colo_status = qmp_ectx(coroutine, CO ectx, "{'execute': 'query-colo-status'}\n"));

    if (qmp_ectx_failed(CO ectx)) {
        qmp_result_free(CO qemu_status);
        qmp_result_free(CO colo_status);
        qmp_ectx_unref(CO ectx, errp);
        return NULL;
    }

    co_end;

    const gchar *status, *colo_mode, *colo_reason;
    status = get_member_member_str(CO qemu_status->json_root,
                                   "return", "status");
    colo_mode = get_member_member_str(CO colo_status->json_root,
                                      "return", "mode");
    colo_reason = get_member_member_str(CO colo_status->json_root,
                                        "return", "reason");
    if (!status || !colo_mode || !colo_reason) {
        colod_error_set(errp, "Failed to parse query-status "
                        "and query-colo-status output");
        qmp_result_free(CO qemu_status);
        qmp_result_free(CO colo_status);
        qmp_ectx_unref(CO ectx, NULL);
        return NULL;
    }

    if (!strcmp(status, "inmigrate") || !strcmp(status, "shutdown")) {
        *primary = FALSE;
        *replication = FALSE;
    } else if (qemu_runnng(status) && !strcmp(colo_mode, "none")
               && (!strcmp(colo_reason, "none")
                   || !strcmp(colo_reason, "request"))) {
        *primary = TRUE;
        *replication = FALSE;
    } else if (qemu_runnng(status) &&!strcmp(colo_mode, "primary")) {
        *primary = TRUE;
        *replication = TRUE;
    } else if (qemu_runnng(status) && !strcmp(colo_mode, "secondary")) {
        *primary = FALSE;
        *replication = TRUE;
    } else {
        colod_error_set(errp, "Unknown qemu status: %s, %s",
                        CO qemu_status->line, CO colo_status->line);
        qmp_result_free(CO qemu_status);
        qmp_result_free(CO colo_status);
        qmp_ectx_unref(CO ectx, NULL);
        return NULL;
    }

    qmp_result_free(CO qemu_status);
    qmp_result_free(CO colo_status);
    return CO ectx;
}

#define colod_check_health_co(...) \
    co_wrap(_colod_check_health_co(__VA_ARGS__))
static int _colod_check_health_co(Coroutine *coroutine, ColodMainCoroutine *this,
                                  GError **errp) {
    gboolean primary;
    gboolean replication;
    QmpEctx *ret;
    GError *local_errp = NULL;

    ret = _qemu_query_status_co(coroutine, this, &primary, &replication,
                                &local_errp);
    if (coroutine->yield) {
        return -1;
    }
    if (!ret) {
        colod_event_queue(this, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return -1;
    }

    if (qmp_ectx_did_yank(ret)) {
        colod_event_queue(this, EVENT_FAILOVER_SYNC, "did yank");
    }
    qmp_ectx_unref(ret, NULL);

    if (!this->transitioning &&
            (this->primary != primary ||
             this->replication != replication)) {
        colod_error_set(&local_errp, "qemu status mismatch: (%s, %s)"
                        " Expected: (%s, %s)",
                        bool_to_json(primary), bool_to_json(replication),
                        bool_to_json(this->primary),
                        bool_to_json(this->replication));
        colod_event_queue(this, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return -1;
    }

    return 0;
}

static int __colod_check_health_co(Coroutine *coroutine, gpointer data,
                                   GError **errp) {
    ColodMainCoroutine *this = data;
    int ret;

    co_begin(int, -1);

    colod_main_ref(this);
    co_recurse(ret = colod_check_health_co(coroutine, this, errp));

    colod_main_unref(this);
    return ret;
    co_end;
}

static int colod_start_migration(ColodMainCoroutine *this) {
    if (this->state != STATE_PRIMARY_WAIT) {
        return -1;
    }

    colod_event_queue(this, EVENT_START_MIGRATION, "client request");
    return 0;
}

static int __colod_start_migration(Coroutine *coroutine, gpointer data) {
    return colod_start_migration(data);
}

#define colod_failover_co(...) \
    co_wrap(_colod_failover_co(__VA_ARGS__))
static MainState _colod_failover_co(Coroutine *coroutine,
                                    ColodMainCoroutine *this) {
    struct {
        QmpEctx *ectx;
        MyArray *commands;
    } *co;
    QmpCommands *qmpcommands = this->ctx->commands;

    co_frame(co, sizeof(*co));
    co_begin(MainState, STATE_FAILED);

    CO ectx = qmp_ectx_new(this->qmp);
    qmp_ectx_set_ignore_yank(CO ectx);
    qmp_ectx_set_ignore_qmp_error(CO ectx);
    eventqueue_set_interrupting(this->queue, 0);

    co_recurse(qmp_ectx_yank(coroutine, CO ectx));

    this->transitioning = TRUE;
    if (this->primary) {
        CO commands = qmp_commands_get_failover_primary(qmpcommands);
    } else {
        CO commands = qmp_commands_get_failover_secondary(qmpcommands);
    }
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO commands));
    my_array_unref(CO commands);

    if (qmp_ectx_failed(CO ectx)) {
        qmp_ectx_log_error(CO ectx);
        qmp_ectx_unref(CO ectx, NULL);
        return STATE_FAILED;
    }
    qmp_ectx_unref(CO ectx, NULL);

    peer_manager_clear_peer(this->ctx->peer);

    co_end;

    return STATE_PRIMARY_WAIT;
}

#define colod_failover_sync_co(...) \
    co_wrap(_colod_failover_sync_co(__VA_ARGS__))
static MainState _colod_failover_sync_co(Coroutine *coroutine,
                                         ColodMainCoroutine *this) {

    co_begin(MainState, STATE_FAILED);

    colod_cpg_send(this->ctx->cpg, MESSAGE_FAILOVER);

    while (TRUE) {
        ColodEvent event;
        co_recurse(event = colod_event_wait(coroutine, this));
        if (event == EVENT_FAILOVER_WIN) {
            return STATE_FAILOVER;
        } else if (event_always_interrupting(event)) {
            return handle_always_interrupting(event);
        }
    }

    co_end;

    return STATE_FAILED;
}

#define colod_secondary_startup_co(...) \
    co_wrap(_colod_secondary_startup_co(__VA_ARGS__));
static MainState _colod_secondary_startup_co(Coroutine *coroutine,
                                             ColodMainCoroutine *this) {
    GError *local_errp = NULL;
    ColodQmpResult *result;

    co_begin(MainState, STATE_FAILED);

    co_recurse(result = qmp_execute_co(coroutine, this->qmp, &local_errp,
                            "{'execute': 'migrate-set-capabilities',"
                            "'arguments': {'capabilities': ["
                                "{'capability': 'events', 'state': true }]}}\n"));
    if (!result) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return STATE_FAILED;
    }

    if (result->did_yank) {
        log_error("Yank during startup");
        qmp_result_free(result);
        return STATE_FAILED;
    }
    qmp_result_free(result);

    return STATE_SECONDARY_WAIT;
    co_end;
}

#define colod_secondary_wait_co(...) \
    co_wrap(_colod_secondary_wait_co(__VA_ARGS__))
static MainState _colod_secondary_wait_co(Coroutine *coroutine,
                                          ColodMainCoroutine *this) {
    GError *local_errp = NULL;
    int ret;

    co_begin(MainState, STATE_FAILED);

    /*
     * We need to be interrupted and discard all these events or
     * else they will be delayed until we are in another state like
     * colo_running and wreak havoc.
     */
    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC,
                                EVENT_FAILOVER_WIN, EVENT_YELLOW,
                                EVENT_UNYELLOW, 0);

    while (TRUE) {
        co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 0,
                                        "{'event': 'RESUME'}", &local_errp));

        if (ret < 0) {
            // Interrupted
            ColodEvent event;
            g_error_free(local_errp);
            assert(eventqueue_pending(this->queue));
            co_recurse(event = colod_event_wait(coroutine, this));

            if (event_always_interrupting(event)) {
                return handle_always_interrupting(event);
            }
            continue;
        }

        break;
    }
    co_end;

    peer_manager_clear_failed(this->ctx->peer);
    colod_raise_timeout_coroutine(&this->raise_timeout_coroutine, this->qmp,
                                  this->ctx);

    return STATE_SECONDARY_COLO_RUNNING;
}

#define colod_colo_running_co(...) \
    co_wrap(_colod_colo_running_co(__VA_ARGS__))
static MainState _colod_colo_running_co(Coroutine *coroutine,
                                        ColodMainCoroutine *this) {
    struct {
        guint source_id;
    } *co;
    GError *local_errp = NULL;
    int ret;
    PeerManager *peer = this->ctx->peer;

    co_frame(co, sizeof(*co));
    co_begin(MainState, STATE_FAILED);

    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC, 0);

    if (this->primary) {
        co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 0,
                                        "{'event': 'RESUME'}", &local_errp));

        if (ret < 0) {
            // Interrupted
            g_error_free(local_errp);
            goto handle_event;
        }

        co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 0,
                                        "{'event': 'RESUME'}", &local_errp));

        if (ret < 0) {
            // Interrupted
            g_error_free(local_errp);
            goto handle_event;
        }

        CO source_id = g_timeout_add(10000, coroutine->cb, coroutine);
        g_source_set_name_by_id(CO source_id, "Waiting before failing to yellow");
        co_yield_int(G_SOURCE_REMOVE);

        guint source_id = g_source_get_id(g_main_current_source());
        if (source_id != CO source_id) {
            // Interrupted
            g_source_remove(CO source_id);
            goto handle_event;
        }

        if (this->yellow && !peer_manager_yellow(peer)) {
            return STATE_FAILED;
        }
    }

handle_event:
    while (TRUE) {
        ColodEvent event;
        co_recurse(event = colod_event_wait(coroutine, this));

        if (event_failover(event)) {
            return handle_event_failover(event);
        } else if (event_always_interrupting(event)) {
            return handle_always_interrupting(event);
        } else if (event_yellow(event)) {
            if (this->primary && this->yellow && !peer_manager_yellow(peer)) {
                return STATE_FAILED;
            }
        }
    }

    co_end;
}

#define colod_primary_wait_co(...) \
    co_wrap(_colod_primary_wait_co(__VA_ARGS__))
static MainState _colod_primary_wait_co(Coroutine *coroutine,
                                        ColodMainCoroutine *this) {
    co_begin(MainState, STATE_FAILED);
    while (TRUE) {
        ColodEvent event;
        co_recurse(event = colod_event_wait(coroutine, this));

        if (event == EVENT_START_MIGRATION) {
        } else if (event_always_interrupting(event)) {
            return handle_always_interrupting(event);
        }
    }
    co_end;
}

static gboolean eventqueue_interrupt(gpointer data) {
    ColodMainCoroutine *this = data;
    return eventqueue_pending_interrupt(this->queue);
}

#define colod_primary_start_migration_co(...) \
    co_wrap(_colod_primary_start_migration_co(__VA_ARGS__))
static MainState _colod_primary_start_migration_co(Coroutine *coroutine,
                                                   ColodMainCoroutine *this) {
    struct {
        ColodEvent event;
        MyArray *commands;
        QmpEctx *ectx;
    } *co;
    QmpCommands *qmpcommands = this->ctx->commands;
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(MainState, STATE_FAILED);

    CO ectx = qmp_ectx_new(this->qmp);
    qmp_ectx_set_interrupt_cb(CO ectx, eventqueue_interrupt, this);
    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC, 0);

    co_recurse(result = qmp_ectx(coroutine, CO ectx,
                    "{'execute': 'migrate-set-capabilities',"
                    "'arguments': {'capabilities': ["
                        "{'capability': 'events', 'state': true },"
                        "{'capability': 'pause-before-switchover', 'state': true}]}}\n"));
    qmp_result_free(result);

    CO commands = qmp_commands_get_migration_start(qmpcommands, "dummy address");
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO commands));
    my_array_unref(CO commands);

    if (qmp_ectx_failed(CO ectx)) {
        goto ectx_failed;
    }

    this->transitioning = TRUE;
    co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 5*60*1000,
                    "{'event': 'MIGRATION',"
                    " 'data': {'status': 'pre-switchover'}}",
                    &local_errp));
    if (ret < 0) {
        goto wait_error;
    }

    CO commands = qmp_commands_get_migration_switchover(qmpcommands);
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO commands));
    my_array_unref(CO commands);

    colod_raise_timeout_coroutine(&this->raise_timeout_coroutine, this->qmp,
                                  this->ctx);

    co_recurse(result = qmp_ectx(coroutine, CO ectx,
                    "{'execute': 'migrate-continue',"
                    "'arguments': {'state': 'pre-switchover'}}\n"));
    qmp_result_free(result);

    if (qmp_ectx_failed(CO ectx)) {
        qmp_set_timeout(this->qmp, this->ctx->qmp_timeout_low);
        goto ectx_failed;
    }

    co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 10000,
                    "{'event': 'MIGRATION',"
                    " 'data': {'status': 'colo'}}",
                    &local_errp));
    if (ret < 0) {
        qmp_set_timeout(this->qmp, this->ctx->qmp_timeout_low);
        goto wait_error;
    }

    qmp_ectx_unref(CO ectx, NULL);
    return STATE_PRIMARY_COLO_RUNNING;

ectx_failed:
    if (qmp_ectx_did_interrupt(CO ectx)) {
        qmp_ectx_unref(CO ectx, NULL);

        goto handle_event;
    } else if (qmp_ectx_did_yank(CO ectx) || qmp_ectx_did_qmp_error(CO ectx)) {
        qmp_ectx_unref(CO ectx, NULL);

        goto failover;
    } else {
        assert(qmp_ectx_did_error(CO ectx));
        qmp_ectx_log_error(CO ectx);
        qmp_ectx_unref(CO ectx, NULL);
        return STATE_FAILED;
    }

wait_error:
    qmp_ectx_unref(CO ectx, NULL);
    assert(local_errp);
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
        g_error_free(local_errp);
        local_errp = NULL;
        goto handle_event;
    } else if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT)) {
        g_error_free(local_errp);
        local_errp = NULL;
        goto failover;
    } else {
        abort();
    }

handle_event:
    assert(eventqueue_pending_interrupt(this->queue));
    co_recurse(CO event = colod_event_wait(coroutine, this));
    if (event_failover(CO event)) {
        goto failover;
    } else {
        return handle_always_interrupting(CO event);
    }

failover:
    co_recurse(result = qmp_execute_co(coroutine, this->qmp, &local_errp,
                    "{'execute': 'migrate_cancel'}\n"));
    if (!result) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return STATE_FAILED;
    }
    qmp_result_free(result);

    return STATE_FAILOVER_SYNC;
    co_end;
}

static void colod_quit(ColodMainCoroutine *this) {
    colod_event_queue(this, EVENT_QUIT, "client request");
}

static int __colod_quit(Coroutine *coroutine, gpointer data, MyTimeout *timeout) {
    colod_quit(data);
    return 0;
}

static gboolean _colod_main_co(Coroutine *coroutine, ColodMainCoroutine *this);
static gboolean colod_main_co(gpointer data) {
    ColodMainCoroutine *this = data;
    Coroutine *coroutine = data;
    gboolean ret;

    co_enter(coroutine, ret = _colod_main_co(coroutine, this));
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    this->return_quit = ret;
    if (this->wake_on_exit) {
        g_idle_add(this->wake_on_exit->cb, this->wake_on_exit);
    }

    this->mainco_running = FALSE;
    colod_assert_remove_one_source(this);
    colod_main_unref(this);
    return G_SOURCE_REMOVE;
}

static gboolean _colod_main_co(Coroutine *coroutine, ColodMainCoroutine *this) {
    MainState new_state;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    if (this->primary) {
        colod_syslog(LOG_INFO, "starting in primary mode");
        new_state = STATE_PRIMARY_STARTUP;
    } else {
        colod_syslog(LOG_INFO, "starting in secondary mode");
        new_state = STATE_SECONDARY_STARTUP;
    }

    colod_cpg_send(this->ctx->cpg, MESSAGE_HELLO);

    while (TRUE) {
        this->transitioning = FALSE;
        this->state = new_state;
        if (this->state == STATE_SECONDARY_STARTUP) {
            co_recurse(new_state = colod_secondary_startup_co(coroutine,
                                                                this));
        } else if (this->state == STATE_SECONDARY_WAIT) {
            co_recurse(new_state = colod_secondary_wait_co(coroutine, this));
        } else if (this->state == STATE_SECONDARY_COLO_RUNNING) {
            this->replication = TRUE;
            co_recurse(new_state = colod_colo_running_co(coroutine, this));
        } else if (this->state == STATE_PRIMARY_STARTUP) {
            new_state = STATE_PRIMARY_WAIT;
        } else if (this->state == STATE_PRIMARY_WAIT) {
            // Now running primary standalone
            this->primary = TRUE;
            this->replication = FALSE;

            co_recurse(new_state = colod_primary_wait_co(coroutine, this));
        } else if (this->state == STATE_PRIMARY_START_MIGRATION) {
            co_recurse(new_state = colod_primary_start_migration_co(coroutine,
                                                                      this));
        } else if (this->state == STATE_PRIMARY_COLO_RUNNING) {
            this->replication = TRUE;
            co_recurse(new_state = colod_colo_running_co(coroutine, this));
        } else if (this->state == STATE_FAILOVER_SYNC) {
            co_recurse(new_state = colod_failover_sync_co(coroutine, this));
        } else if (this->state == STATE_FAILOVER) {
            co_recurse(new_state = colod_failover_co(coroutine, this));
        } else if (this->state == STATE_FAILED) {
            log_error("qemu failed");
            this->failed = TRUE;
            colod_cpg_send(this->ctx->cpg, MESSAGE_FAILED);

            qmp_set_timeout(this->qmp, this->ctx->qmp_timeout_low);
            ColodQmpResult *result;
            co_recurse(result = qmp_execute_co(coroutine, this->qmp, NULL,
                                               "{'execute': 'stop'}\n"));
            qmp_result_free(result);

            while (_colod_eventqueue_pending(this)) {
                ColodEvent event;
                co_recurse(event = colod_event_wait(coroutine, this));
                if (event == EVENT_QUIT) {
                    return TRUE;
                }
            }

            return FALSE;
        } else if (this->state == STATE_QUIT) {
            return TRUE;
        }
    }

    co_end;
}

static void colod_hup_cb(gpointer data) {
    ColodMainCoroutine *this = data;

    log_error("qemu quit");
    this->qemu_quit = TRUE;
    colod_event_queue(this, EVENT_FAILED, "qmp hup");
}

static void colod_qmp_event_cb(gpointer data, ColodQmpResult *result) {
    ColodMainCoroutine *this = data;
    const gchar *event;

    event = get_member_str(result->json_root, "event");

    if (!strcmp(event, "QUORUM_REPORT_BAD")) {
        const gchar *node, *type;
        node = get_member_member_str(result->json_root, "data", "node-name");
        type = get_member_member_str(result->json_root, "data", "type");

        if (!strcmp(node, "nbd0")) {
            if (!!strcmp(type, "read")) {
                colod_event_queue(this, EVENT_FAILOVER_SYNC,
                                  "nbd write/flush error");
            }
        } else {
            if (!!strcmp(type, "read")) {
                this->yellow = TRUE;
                colod_cpg_send(this->ctx->cpg, MESSAGE_YELLOW);
                yellow_shutdown(this->yellow_co);
                colod_event_queue(this, EVENT_YELLOW,
                                  "local disk write/flush error");
            }
        }
    } else if (!strcmp(event, "MIGRATION")) {
        const gchar *status;
        status = get_member_member_str(result->json_root, "data", "status");
        if (!strcmp(status, "failed")
                && this->state == STATE_PRIMARY_START_MIGRATION) {
            colod_event_queue(this, EVENT_FAILOVER_SYNC,
                              "migration failed qmp event");
        }
    } else if (!strcmp(event, "COLO_EXIT")) {
        const gchar *reason;
        reason = get_member_member_str(result->json_root, "data", "reason");

        if (!strcmp(reason, "error")) {
            colod_event_queue(this, EVENT_FAILOVER_SYNC, "COLO_EXIT qmp event");
        }
    } else if (!strcmp(event, "RESET")) {
        colod_raise_timeout_coroutine(&this->raise_timeout_coroutine, this->qmp,
                                      this->ctx);
    }
}

static void colod_failover_cb(gpointer data, ColodEvent event) {
    ColodMainCoroutine *this = data;
    colod_event_queue(this, event, "Got failover msg");
}

static void colod_cpg_event_cb(gpointer data, ColodMessage message,
                               gboolean message_from_this_node,
                               gboolean peer_left_group) {
    ColodMainCoroutine *this = data;

    if (message_from_this_node) {
        return;
    }

    if (message == MESSAGE_FAILED || peer_left_group) {
        colod_event_queue(this, EVENT_FAILOVER_SYNC, "got MESSAGE_FAILED or peer left group");
    } else if (message == MESSAGE_HELLO) {
        if (this->yellow) {
            colod_cpg_send(this->ctx->cpg, MESSAGE_YELLOW);
        }
    } else if (message == MESSAGE_YELLOW) {
        colod_event_queue(this, EVENT_YELLOW, "peer yellow state change");
    } else if (message == MESSAGE_UNYELLOW) {
        colod_event_queue(this, EVENT_YELLOW, "peer yellow state change");
    }
}

static void colod_yellow_event_cb(gpointer data, ColodEvent event) {
    ColodMainCoroutine *this = data;

    if (event == EVENT_YELLOW) {
        this->yellow = TRUE;
        colod_event_queue(this, EVENT_YELLOW, "link down event");
    } else if (event == EVENT_UNYELLOW) {
        this->yellow = FALSE;
        colod_event_queue(this, EVENT_UNYELLOW, "link up event");
    } else {
        abort();
    }
}

const ClientCallbacks colod_client_callbacks = {
    __colod_query_status,
    __colod_check_health_co,
    NULL,
    NULL,
    __colod_start_migration,
    NULL,
    NULL,
    NULL,
    __colod_quit,
    __colod_yank_co,
    __colod_execute_nocheck_co,
    __colod_execute_co
};

void colod_main_client_register(ColodMainCoroutine *this) {
    colod_main_ref(this);
    client_register(this->ctx->listener, &colod_client_callbacks, this);
}

void colod_main_client_unregister(ColodMainCoroutine *this) {
    client_unregister(this->ctx->listener, &colod_client_callbacks, this);
    colod_main_unref(this);
}

int _colod_main_enter(Coroutine *coroutine, ColodMainCoroutine *this) {
    co_begin(int, -1);

    assert(!this->wake_on_exit);
    this->wake_on_exit = coroutine;
    this->mainco_running = TRUE;

    colod_main_ref(this);
    g_idle_add(colod_main_co, this);

    co_yield_int(G_SOURCE_REMOVE);
    this->wake_on_exit = NULL;

    co_end;

    return this->return_quit;
}

ColodMainCoroutine *colod_main_new(const ColodContext *ctx, QemuLauncher *launcher,
                                   ColodQmpState *qmp, GError **errp) {
    ColodMainCoroutine *this;
    Coroutine *coroutine;

    this = g_rc_box_new0(ColodMainCoroutine);
    coroutine = &this->coroutine;
    coroutine->cb = colod_main_co;
    this->ctx = ctx;
    this->launcher = qemu_launcher_ref(launcher);
    this->qmp = qmp_ref(qmp);

    this->yellow_co = yellow_coroutine_new(ctx->cpg, ctx, 500, 1000, errp);
    if (!this->yellow_co) {
        g_free(this);
        return NULL;
    }

    this->queue = colod_eventqueue_new();

    this->primary = ctx->primary_startup;
    qmp_add_notify_event(this->qmp, colod_qmp_event_cb, this);
    qmp_add_notify_hup(this->qmp, colod_hup_cb, this);

    peer_manager_add_notify(ctx->peer, colod_failover_cb, this);
    colod_cpg_add_notify(ctx->cpg, colod_cpg_event_cb, this);

    yellow_add_notify(this->yellow_co, colod_yellow_event_cb, this);

    this->watchdog = colod_watchdog_new(ctx, this->qmp,
                                        __colod_check_health_co, this);
    return this;
}

static void colod_main_free(gpointer _this) {
    ColodMainCoroutine *this = _this;
    assert(!this->mainco_running);

    yellow_del_notify(this->yellow_co, colod_yellow_event_cb, this);
    yellow_coroutine_free(this->yellow_co);

    colod_cpg_del_notify(this->ctx->cpg, colod_cpg_event_cb, this);
    peer_manager_del_notify(this->ctx->peer, colod_failover_cb, this);

    qmp_del_notify_hup(this->qmp, colod_hup_cb, this);
    qmp_del_notify_event(this->qmp, colod_qmp_event_cb, this);
    colod_raise_timeout_coroutine_free(&this->raise_timeout_coroutine);

    colod_watchdog_free(this->watchdog);

    qmp_unref(this->qmp);
    qemu_launcher_unref(this->launcher);

    eventqueue_free(this->queue);
}

ColodMainCoroutine *colod_main_ref(ColodMainCoroutine *this) {
    return g_rc_box_acquire(this);
}

void colod_main_unref(ColodMainCoroutine *this) {
    g_rc_box_release_full(this, colod_main_free);
}
