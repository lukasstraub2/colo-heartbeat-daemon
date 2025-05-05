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
    STATE_FAILED_PEER_FAILOVER,
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
    gboolean failed, peer_failed;
    gboolean yellow, peer_yellow;
    gboolean qemu_quit;
    gboolean peer_failover;
    gboolean primary;
    gboolean replication;
    gchar *peer;

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
    ret->running = TRUE;
    ret->primary = this->primary;
    ret->replication = this->replication;
    ret->failed = this->failed;
    ret->peer_failover = this->peer_failover;
    ret->peer_failed = this->peer_failed;
}

static void __colod_query_status(gpointer data, ColodState *ret) {
    return colod_main_query_status(data, ret);
}

static void colod_peer_failed(ColodMainCoroutine *this) {
    this->peer_failed = TRUE;
}

static void colod_set_peer(ColodMainCoroutine *this, const gchar *peer) {
    g_free(this->peer);
    this->peer = g_strdup(peer);
    this->peer_failed = FALSE;
    this->peer_yellow = FALSE;
}

static int __colod_set_peer(Coroutine *coroutine, gpointer data, const gchar *peer) {
    (void) coroutine;
    colod_set_peer(data, peer);
    return 0;
}

static gchar *__colod_get_peer(Coroutine *coroutine, gpointer data) {
    ColodMainCoroutine *this = data;
    (void) coroutine;
    return g_strdup(this->peer);
}

static void colod_clear_peer(ColodMainCoroutine *this) {
    g_free(this->peer);
    this->peer = g_strdup("");
}

static int __colod_clear_peer(Coroutine *coroutine, gpointer data) {
    (void) coroutine;
    colod_clear_peer(data);
    return 0;
}

static const gchar *event_str(ColodEvent event) {
    switch (event) {
        case EVENT_FAILED: return "EVENT_FAILED";
        case EVENT_PEER_FAILOVER: return "EVENT_PEER_FAILOVER";
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
    return eventqueue_new(32, EVENT_FAILED, EVENT_PEER_FAILOVER, EVENT_QUIT, 0);
}

static gboolean event_always_interrupting(ColodEvent event) {
    switch (event) {
        case EVENT_FAILED:
        case EVENT_PEER_FAILOVER:
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
        case EVENT_PEER_FAILOVER: return STATE_FAILED_PEER_FAILOVER;
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

#define colod_yank(...) \
    co_wrap(_colod_yank_co(__VA_ARGS__))
static int _colod_yank_co(Coroutine *coroutine, ColodMainCoroutine *this, GError **errp) {
    int ret;
    GError *local_errp = NULL;

    ret = _qmp_yank_co(coroutine, this->qmp, &local_errp);
    if (coroutine->yield) {
        return -1;
    }
    if (ret < 0) {
        colod_event_queue(this, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
    } else {
        colod_event_queue(this, EVENT_FAILOVER_SYNC, "did yank");
    }

    return ret;
}

static int __colod_yank_co(Coroutine *coroutine, gpointer data, GError **errp) {
    ColodMainCoroutine *this = data;
    int ret;

    co_begin(int, -1);

    colod_main_ref(this);
    co_recurse(ret = colod_yank(coroutine, this, errp));

    co_end;

    colod_main_unref(this);
    return ret;
}

#define colod_execute_nocheck_co(...) \
    co_wrap(_colod_execute_nocheck_co(__VA_ARGS__))
static ColodQmpResult *_colod_execute_nocheck_co(Coroutine *coroutine,
                                                 ColodMainCoroutine *this,
                                                 GError **errp,
                                                 const gchar *command) {
    ColodQmpResult *result;
    GError *local_errp = NULL;

    colod_watchdog_refresh(this->watchdog);

    result = _qmp_execute_nocheck_co(coroutine, this->qmp, &local_errp, command);
    if (coroutine->yield) {
        return NULL;
    }
    if (!result) {
        colod_event_queue(this, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return NULL;
    }

    if (result->did_yank) {
        colod_event_queue(this, EVENT_FAILOVER_SYNC, "did yank");
    }

    return result;
}

static ColodQmpResult *__colod_execute_nocheck_co(Coroutine *coroutine,
                                                  gpointer data,
                                                  GError **errp,
                                                  const gchar *command) {
    ColodMainCoroutine *this = data;
    ColodQmpResult * ret;

    co_begin(ColodQmpResult *, NULL);

    colod_main_ref(this);
    co_recurse(ret = colod_execute_nocheck_co(coroutine, this, errp, command));

    co_end;

    colod_main_unref(this);
    return ret;
}

#define colod_execute_co(...) \
    co_wrap(_colod_execute_co(__VA_ARGS__))
static ColodQmpResult *_colod_execute_co(Coroutine *coroutine,
                                         ColodMainCoroutine *this,
                                         GError **errp,
                                         const gchar *command) {
    ColodQmpResult *result;

    result = _colod_execute_nocheck_co(coroutine, this, errp, command);
    if (coroutine->yield) {
        return NULL;
    }
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
}

static ColodQmpResult *__colod_execute_co(Coroutine *coroutine,
                                          gpointer data,
                                          GError **errp,
                                          const gchar *command) {
    ColodMainCoroutine *this = data;
    ColodQmpResult * ret;

    co_begin(ColodQmpResult *, NULL);

    colod_main_ref(this);
    co_recurse(ret = colod_execute_co(coroutine, this, errp, command));

    co_end;

    colod_main_unref(this);
    return ret;
}

#define colod_execute_array_co(...) \
    co_wrap(_colod_execute_array_co(__VA_ARGS__))
static int _colod_execute_array_co(Coroutine *coroutine, ColodMainCoroutine *this,
                                   MyArray *array, gboolean ignore_errors,
                                   GError **errp) {
    struct {
        int i;
    } *co;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    assert(!errp || !*errp);

    for (CO i = 0; CO i < array->size; CO i++) {
        if (eventqueue_pending_interrupt(this->queue)) {
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT,
                        "Got interrupting event while executing array");
            return -1;
            break;
        }

        ColodQmpResult *result;
        co_recurse(result = colod_execute_co(coroutine, this, &local_errp, array->array[CO i]));
        if (ignore_errors &&
                g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
            colod_syslog(LOG_WARNING, "Ignoring qmp error: %s",
                         local_errp->message);
            g_error_free(local_errp);
            local_errp = NULL;
        } else if (!result) {
            g_propagate_error(errp, local_errp);
            return -1;
        }
        qmp_result_free(result);
    }

    co_end;

    return 0;
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
static int _qemu_query_status_co(Coroutine *coroutine, ColodMainCoroutine *this,
                                 gboolean *primary, gboolean *replication,
                                 GError **errp) {
    struct {
        ColodQmpResult *qemu_status, *colo_status;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    co_recurse(CO qemu_status = colod_execute_co(coroutine, this, errp,
                                                 "{'execute': 'query-status'}\n"));
    if (!CO qemu_status) {
        return -1;
    }

    co_recurse(CO colo_status = colod_execute_co(coroutine, this, errp,
                                                 "{'execute': 'query-colo-status'}\n"));
    if (!CO colo_status) {
        qmp_result_free(CO qemu_status);
        return -1;
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
        return -1;
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
        return -1;
    }

    qmp_result_free(CO qemu_status);
    qmp_result_free(CO colo_status);
    return 0;
}

#define colod_check_health_co(...) \
    co_wrap(_colod_check_health_co(__VA_ARGS__))
static int _colod_check_health_co(Coroutine *coroutine, ColodMainCoroutine *this,
                                  GError **errp) {
    gboolean primary;
    gboolean replication;
    int ret;
    GError *local_errp = NULL;

    ret = _qemu_query_status_co(coroutine, this, &primary, &replication,
                                &local_errp);
    if (coroutine->yield) {
        return -1;
    }
    if (ret < 0) {
        colod_event_queue(this, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return -1;
    }

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

    co_end;

    colod_main_unref(this);
    return ret;
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

#define colod_stop_co(...) \
    co_wrap(_colod_stop_co(__VA_ARGS__))
static int _colod_stop_co(Coroutine *coroutine, ColodMainCoroutine *this,
                          GError **errp) {
    ColodQmpResult *result;

    result = _colod_execute_co(coroutine, this, errp, "{'execute': 'stop'}\n");
    if (coroutine->yield) {
        return -1;
    }
    if (!result) {
        return -1;
    }
    qmp_result_free(result);

    return 0;
}

#define colod_failover_co(...) \
    co_wrap(_colod_failover_co(__VA_ARGS__))
static MainState _colod_failover_co(Coroutine *coroutine,
                                    ColodMainCoroutine *this) {
    struct {
        MyArray *commands;
    } *co;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(MainState, STATE_FAILED);

    eventqueue_set_interrupting(this->queue, 0);

    co_recurse(ret = qmp_yank_co(coroutine, this->qmp, &local_errp));
    if (ret < 0) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return STATE_FAILED;
    }

    if (this->primary) {
        CO commands = qmp_commands_get_failover_primary(this->ctx->commands);
    } else {
        CO commands = qmp_commands_get_failover_secondary(this->ctx->commands);
    }
    this->transitioning = TRUE;
    co_recurse(ret = colod_execute_array_co(coroutine, this, CO commands,
                                            TRUE, &local_errp));
    my_array_unref(CO commands);
    if (ret < 0) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return STATE_FAILED;
    }

    colod_clear_peer(this);

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

    result =_colod_execute_co(coroutine, this, &local_errp,
                            "{'execute': 'migrate-set-capabilities',"
                            "'arguments': {'capabilities': ["
                                "{'capability': 'events', 'state': true }]}}\n");
    if (coroutine->yield) {
        return 0;
    }

    if (!result) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return STATE_FAILED;
    }
    qmp_result_free(result);

    return STATE_SECONDARY_WAIT;
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
            } else if (event_failover(event)) {
                this->peer_failed = FALSE;
            }
            continue;
        }

        break;
    }
    co_end;

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

        if (this->yellow && !this->peer_yellow) {
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
            if (this->primary && this->yellow && !this->peer_yellow) {
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
            return STATE_PRIMARY_START_MIGRATION;
        } else if (event == EVENT_FAILED) {
            return STATE_FAILED;
        } else if (event == EVENT_PEER_FAILOVER) {
            /*
             * Failover message from node that lost failover sync may
             * arrive later, when we've failed over.
             */
        } else if (event == EVENT_QUIT) {
            return STATE_QUIT;
        }
    }
    co_end;
}

#define colod_primary_start_migration_co(...) \
    co_wrap(_colod_primary_start_migration_co(__VA_ARGS__))
static MainState _colod_primary_start_migration_co(Coroutine *coroutine,
                                                   ColodMainCoroutine *this) {
    struct {
        ColodEvent event;
        MyArray *commands;
    } *co;
    ColodQmpState *qmp = this->qmp;
    ColodQmpResult *qmp_result;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(MainState, STATE_FAILED);

    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC, 0);

    co_recurse(qmp_result = colod_execute_co(coroutine, this, &local_errp,
                    "{'execute': 'migrate-set-capabilities',"
                    "'arguments': {'capabilities': ["
                        "{'capability': 'events', 'state': true },"
                        "{'capability': 'pause-before-switchover', 'state': true}]}}\n"));
    if (!qmp_result) {
        goto qmp_error;
    }
    qmp_result_free(qmp_result);
    if (eventqueue_pending_interrupt(this->queue)) {
        goto handle_event;
    }

    CO commands = qmp_commands_get_migration_start(this->ctx->commands, "dummy");
    co_recurse(ret = colod_execute_array_co(coroutine, this, CO commands,
                                            FALSE, &local_errp));
    my_array_unref(CO commands);
    if (ret < 0) {
        goto qmp_error;
    }
    if (eventqueue_pending_interrupt(this->queue)) {
        goto handle_event;
    }

    this->transitioning = TRUE;
    co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 5*60*1000,
                    "{'event': 'MIGRATION',"
                    " 'data': {'status': 'pre-switchover'}}",
                    &local_errp));
    if (ret < 0) {
        goto qmp_error;
    }

    CO commands = qmp_commands_get_migration_switchover(this->ctx->commands);
    co_recurse(ret = colod_execute_array_co(coroutine, this,
                                            CO commands,
                                            FALSE, &local_errp));
    my_array_unref(CO commands);
    if (ret < 0) {
        goto qmp_error;
    }
    if (eventqueue_pending_interrupt(this->queue)) {
        goto handle_event;
    }

    colod_raise_timeout_coroutine(&this->raise_timeout_coroutine, this->qmp,
                                  this->ctx);

    co_recurse(qmp_result = colod_execute_co(coroutine, this, &local_errp,
                    "{'execute': 'migrate-continue',"
                    "'arguments': {'state': 'pre-switchover'}}\n"));
    if (!qmp_result) {
        qmp_set_timeout(qmp, this->ctx->qmp_timeout_low);
        goto qmp_error;
    }
    qmp_result_free(qmp_result);
    if (eventqueue_pending_interrupt(this->queue)) {
        qmp_set_timeout(qmp, this->ctx->qmp_timeout_low);
        goto handle_event;
    }

    co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 10000,
                    "{'event': 'MIGRATION',"
                    " 'data': {'status': 'colo'}}",
                    &local_errp));
    if (ret < 0) {
        qmp_set_timeout(qmp, this->ctx->qmp_timeout_low);
        goto qmp_error;
    }

    return STATE_PRIMARY_COLO_RUNNING;

qmp_error:
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
        g_error_free(local_errp);
        local_errp = NULL;
        goto handle_event;
    } else if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        local_errp = NULL;
        CO event = EVENT_FAILOVER_SYNC;
        goto failover;
    } else {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return STATE_FAILED;
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
    co_recurse(qmp_result = colod_execute_co(coroutine, this, &local_errp,
                    "{'execute': 'migrate_cancel'}\n"));
    if (!qmp_result) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return STATE_FAILED;
    }
    qmp_result_free(qmp_result);
    assert(event_failover(CO event));
    return handle_event_failover(CO event);

    co_end;

    return STATE_FAILED;
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
    int ret;
    GError *local_errp = NULL;
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
        } else if (this->state == STATE_FAILED_PEER_FAILOVER) {
            this->peer_failover = TRUE;
            new_state = STATE_FAILED;
        } else if (this->state == STATE_FAILED) {
            log_error("qemu failed");
            this->failed = TRUE;
            colod_cpg_send(this->ctx->cpg, MESSAGE_FAILED);

            qmp_set_timeout(this->qmp, this->ctx->qmp_timeout_low);
            co_recurse(ret = colod_stop_co(coroutine, this, &local_errp));
            if (ret < 0) {
                g_error_free(local_errp);
                local_errp = NULL;
            }

            while (_colod_eventqueue_pending(this)) {
                ColodEvent event;
                co_recurse(event = colod_event_wait(coroutine, this));
                if (event == EVENT_PEER_FAILOVER) {
                    this->peer_failover = TRUE;
                } else if (event == EVENT_QUIT) {
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

static void colod_cpg_event_cb(gpointer data, ColodMessage message,
                               gboolean message_from_this_node,
                               gboolean peer_left_group) {
    ColodMainCoroutine *this = data;

    if (peer_left_group) {
        log_error("Peer failed");
        colod_peer_failed(this);
        colod_event_queue(this, EVENT_FAILOVER_SYNC, "peer left cpg group");
    } else if (message == MESSAGE_FAILOVER) {
        if (message_from_this_node) {
            colod_event_queue(this, EVENT_FAILOVER_WIN, "Got our failover msg");
        } else {
            colod_event_queue(this, EVENT_PEER_FAILOVER, "Got peer failover msg");
        }
    } else if (message == MESSAGE_FAILED) {
        if (!message_from_this_node) {
            log_error("Peer failed");
            colod_peer_failed(this);
            colod_event_queue(this, EVENT_FAILOVER_SYNC, "got MESSAGE_FAILED");
        }
    } else if (message == MESSAGE_HELLO) {
        if (this->yellow) {
            colod_cpg_send(this->ctx->cpg, MESSAGE_YELLOW);
        }
    } else if (message == MESSAGE_YELLOW) {
        if (!message_from_this_node) {
            this->peer_yellow = TRUE;
            colod_event_queue(this, EVENT_YELLOW, "peer yellow state change");
        }
    } else if (message == MESSAGE_UNYELLOW) {
        if (!message_from_this_node) {
            this->peer_yellow = FALSE;
            colod_event_queue(this, EVENT_YELLOW, "peer yellow state change");
        }
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
    __colod_set_peer,
    __colod_get_peer,
    __colod_clear_peer,
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
    this->peer = g_strdup("");
    qmp_add_notify_event(this->qmp, colod_qmp_event_cb, this);
    qmp_add_notify_hup(this->qmp, colod_hup_cb, this);

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

    qmp_del_notify_hup(this->qmp, colod_hup_cb, this);
    qmp_del_notify_event(this->qmp, colod_qmp_event_cb, this);
    colod_raise_timeout_coroutine_free(&this->raise_timeout_coroutine);

    colod_watchdog_free(this->watchdog);

    qmp_unref(this->qmp);
    qemu_launcher_unref(this->launcher);

    eventqueue_free(this->queue);
    g_free(this->peer);
}

ColodMainCoroutine *colod_main_ref(ColodMainCoroutine *this) {
    return g_rc_box_acquire(this);
}

void colod_main_unref(ColodMainCoroutine *this) {
    g_rc_box_release_full(this, colod_main_free);
}
