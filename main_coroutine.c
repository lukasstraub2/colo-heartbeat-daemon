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
#include "cluster_resource.h"

typedef enum MainState {
    STATE_SECONDARY_WAIT,
    STATE_PRIMARY_STARTUP,
    STATE_PRIMARY_WAIT,
    STATE_PRIMARY_RESYNC,
    STATE_PRIMARY_CONT_REPL,
    STATE_PRIMARY_START_MIGRATION,
    STATE_COLO_RUNNING,
    STATE_FAILOVER_SYNC,
    STATE_SHUTDOWN,
    STATE_GUEST_SHUTDOWN,
    STATE_GUEST_REBOOT,
    STATE_FAILED,
    STATE_QUIT,
    STATE_RETURN_NONE
} MainState;

struct ColodMainCache {
    gboolean valid;
    MainState new_state;
};

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
    guint link_broken_delay_id;
    guint link_broken_delay2_id;

    MainState state;
    gboolean transitioning;
    gboolean failed, yellow;
    gboolean qemu_quit;
    gboolean guest_shutdown;
    gboolean guest_reboot;
    gboolean peer_reboot_restart;
    gboolean peer_shutdown_done;
    gboolean primary;
    gboolean replication;

    Coroutine *wake_on_exit;
    MainReturn main_return;

    MainReturn command;
    Coroutine *command_wake;

    ColodMainCache cache;
};

static void colod_link_broken_delay_stop(ColodMainCoroutine *this);

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
        case EVENT_KICK: return "EVENT_KICK";
        case EVENT_START_MIGRATION: return "EVENT_START_MIGRATION";
        case EVENT_SHUTDOWN: return "EVENT_SHUTDOWN";
        case EVENT_GUEST_SHUTDOWN: return "EVENT_GUEST_SHUTDOWN";
        case EVENT_MAX: abort();
    }
    abort();
}

static EventQueue *colod_eventqueue_new() {
    return eventqueue_new(32, EVENT_FAILED, EVENT_QUIT, EVENT_GUEST_SHUTDOWN, 0);
}

static gboolean event_always_interrupting(ColodEvent event) {
    switch (event) {
        case EVENT_FAILED:
        case EVENT_QUIT:
        case EVENT_GUEST_SHUTDOWN:
            return TRUE;
        break;

        default:
            return FALSE;
        break;
    }
}

static MainState handle_always_interrupting(ColodMainCoroutine *this, ColodEvent event) {
    switch (event) {
        case EVENT_FAILED: return STATE_FAILED;
        case EVENT_QUIT: return STATE_QUIT;
        case EVENT_GUEST_SHUTDOWN:
            if (this->guest_reboot) {
                return STATE_GUEST_REBOOT;
            } else {
                return STATE_GUEST_SHUTDOWN;
            }
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

static gboolean event_command(ColodEvent event) {
    return event == EVENT_START_MIGRATION || event == EVENT_SHUTDOWN;
}

static MainState handle_event_command(ColodEvent event) {
    switch (event) {
        case EVENT_START_MIGRATION: return STATE_PRIMARY_RESYNC;
        case EVENT_SHUTDOWN: return STATE_SHUTDOWN;
        default: abort();
    }
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
            this->wake_source_id = g_idle_add_full(G_PRIORITY_HIGH,
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

    co_begin(ColodEvent, EVENT_FAILED);

    guint source_id = g_source_get_id(g_main_current_source());
    if (source_id == this->wake_source_id) {
        this->wake_source_id = 0;
    }

    while (!eventqueue_pending(this->queue) || this->wake_source_id) {
        co_yield(G_SOURCE_REMOVE);
        guint source_id = g_source_get_id(g_main_current_source());
        if (source_id == this->wake_source_id) {
            this->wake_source_id = 0;
        }
    }

    return __colod_eventqueue_remove(this, func, line);
    co_end;
}

#define colod_interrupting_wait(coroutine, ctx) \
    co_wrap(_colod_interrupting_wait(coroutine, ctx, __func__, __LINE__))
static ColodEvent _colod_interrupting_wait(Coroutine *coroutine,
                                           ColodMainCoroutine *this,
                                           const gchar *func, int line) {

    co_begin(ColodEvent, EVENT_FAILED);

    guint source_id = g_source_get_id(g_main_current_source());
    if (source_id == this->wake_source_id) {
        this->wake_source_id = 0;
    }

    while (!eventqueue_pending_interrupt(this->queue) || this->wake_source_id) {
        co_yield(G_SOURCE_REMOVE);
        guint source_id = g_source_get_id(g_main_current_source());
        if (source_id == this->wake_source_id) {
            this->wake_source_id = 0;
        }
    }

    return __colod_eventqueue_remove(this, func, line);
    co_end;
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

#define wait_while_timeout(...) co_wrap(_wait_while_timeout(__VA_ARGS__))
static int _wait_while_timeout(Coroutine *coroutine, gboolean expr, guint timeout) {
    struct {
        guint timeout_source_id, progress_source_id;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO timeout_source_id = g_timeout_add(timeout, coroutine->cb, coroutine);
    g_source_set_name_by_id(CO timeout_source_id, "wait_while timeout");
    CO progress_source_id = progress_source_add(coroutine->cb, coroutine);
    g_source_set_name_by_id(CO progress_source_id, "wait_while progress");

    while (TRUE) {
        co_yield_int(G_SOURCE_REMOVE);

        guint source_id = g_source_get_id(g_main_current_source());
        if (source_id == CO timeout_source_id) {
            g_source_remove(CO progress_source_id);
            if (expr) {
                return -1;
            } else {
                return 0;
            }
        } else if (source_id == CO progress_source_id) {
            if (expr) {
                CO progress_source_id = progress_source_add(coroutine->cb, coroutine);
                g_source_set_name_by_id(CO progress_source_id, "wait_while progress");
            } else {
                g_source_remove(CO timeout_source_id);
                return 0;
            }
        } else {
            colod_trace("%s:%u: Got woken by unknown source\n", __func__, __LINE__);
        }
    }
    co_end;
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

static gboolean qemu_running(const gchar *status) {
    return !strcmp(status, "running")
            || !strcmp(status, "finish-migrate")
            || !strcmp(status, "colo")
            || !strcmp(status, "prelaunch")
            || !strcmp(status, "paused");
}

static gboolean ignore_state(MainState state) {
    switch (state) {
        case STATE_SECONDARY_WAIT: return FALSE;
        case STATE_PRIMARY_STARTUP: return TRUE;
        case STATE_PRIMARY_WAIT: return FALSE;
        case STATE_PRIMARY_RESYNC: return FALSE;
        case STATE_PRIMARY_CONT_REPL: return FALSE;
        case STATE_PRIMARY_START_MIGRATION: return FALSE;
        case STATE_COLO_RUNNING: return FALSE;
        case STATE_FAILOVER_SYNC: return FALSE;
        case STATE_SHUTDOWN: return TRUE;
        case STATE_GUEST_SHUTDOWN: return TRUE;
        case STATE_GUEST_REBOOT: return TRUE;
        case STATE_FAILED: return TRUE;
        case STATE_QUIT: return TRUE;
        case STATE_RETURN_NONE: return TRUE;
    }
    abort();
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

    if (ignore_state(this->state)) {
        *primary = this->primary;
        *replication = this->replication;
        return qmp_ectx_new(this->qmp);
    }

    co_recurse(CO qemu_status = qmp_ectx(coroutine, CO ectx, "{'execute': 'query-status'}\n"));
    co_recurse(CO colo_status = qmp_ectx(coroutine, CO ectx, "{'execute': 'query-colo-status'}\n"));

    if (ignore_state(this->state)) {
        qmp_ectx_failed(CO ectx);
        qmp_ectx_unref(CO ectx, NULL);
        *primary = this->primary;
        *replication = this->replication;
        return qmp_ectx_new(this->qmp);
    }

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

    if ((!strcmp(status, "inmigrate") || !strcmp(status, "shutdown"))
            && !strcmp(colo_mode, "none")) {
        *primary = FALSE;
        *replication = FALSE;
    } else if (qemu_running(status) && !strcmp(colo_mode, "none")
               && (!strcmp(colo_reason, "none")
                   || !strcmp(colo_reason, "request"))) {
        *primary = TRUE;
        *replication = FALSE;
    } else if (qemu_running(status) && !strcmp(colo_mode, "primary")) {
        *primary = TRUE;
        *replication = TRUE;
    } else if ((!strcmp(status, "inmigrate") || qemu_running(status))
               && !strcmp(colo_mode, "secondary")) {
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

#define colod_check_health_co(...) co_wrap(_colod_check_health_co(__VA_ARGS__))
static int _colod_check_health_co(Coroutine *coroutine, ColodMainCoroutine *this,
                                  GError **errp) {
    gboolean primary;
    gboolean replication;
    QmpEctx *ret;
    GError *local_errp = NULL;

    co_begin(int, -1);

    co_recurse(ret = qemu_query_status_co(coroutine, this, &primary, &replication, &local_errp));
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
    co_end;
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

#define deliver_command(...) co_wrap(_deliver_command(__VA_ARGS__))
static int _deliver_command(Coroutine *coroutine, ColodMainCoroutine *this, ColodEvent event,
                            MainReturn command, MyTimeout *timeout) {
    (void) timeout;

    co_begin(int, -1);

    colod_main_ref(this);
    assert(!this->command_wake);
    this->command = command;
    this->command_wake = coroutine;

    colod_event_queue(this, event, "client request");

    co_yield_int(G_SOURCE_REMOVE);
    this->command_wake = NULL;
    colod_main_unref(this);

    return 0;
    co_end;
}

static void wake_command(ColodMainCoroutine *this, MainReturn command) {
    if (this->command_wake) {
        assert(this->command == command);
        g_idle_add(this->command_wake->cb, this->command_wake);
    }
}

static int __colod_start_migration(Coroutine *coroutine, gpointer data) {
    ColodMainCoroutine *this = data;

    co_begin(int, -1);

    while (this->state != STATE_PRIMARY_WAIT) {
        progress_source_add(coroutine->cb, coroutine);
        co_yield_int(G_SOURCE_REMOVE);
    }

    co_recurse(deliver_command(coroutine, this, EVENT_START_MIGRATION, MAIN_NONE, NULL));
    return 0;
    co_end;
}

static int __colod_shutdown(Coroutine *coroutine, gpointer data, MyTimeout *timeout) {
    ColodMainCoroutine *this = data;
    (void) timeout;

    co_begin(int, -1);

    colod_main_ref(this);
    assert(!this->command_wake);
    this->command = MAIN_NONE;
    this->command_wake = coroutine;

    colod_cpg_send(this->ctx->cpg, MESSAGE_SHUTDOWN_REQUEST);

    co_yield_int(G_SOURCE_REMOVE);
    this->command_wake = NULL;
    colod_main_unref(this);

    return 0;
    co_end;
}

static int __colod_quit(Coroutine *coroutine, gpointer data, MyTimeout *timeout) {
    ColodMainCoroutine *this = data;

    co_begin(int, -1);

    co_recurse(deliver_command(coroutine, this, EVENT_QUIT, MAIN_QUIT, timeout));
    return 0;
    co_end;
}

static int __colod_demote(Coroutine *coroutine, gpointer data, MyTimeout *timeout) {
    ColodMainCoroutine *this = data;

    co_begin(int, -1);

    co_recurse(deliver_command(coroutine, this, EVENT_QUIT, MAIN_DEMOTE, timeout));
    return 0;
    co_end;
}

static int __colod_promote(Coroutine *coroutine, gpointer data) {
    ColodMainCoroutine *this = data;

    co_begin(int, -1);

    co_recurse(deliver_command(coroutine, this, EVENT_QUIT, MAIN_PROMOTE, NULL));
    return 0;
    co_end;
}

const ClientCallbacks colod_client_callbacks = {
    __colod_query_status,
    __colod_check_health_co,
    __colod_promote,
    __colod_start_migration,
    NULL,
    __colod_shutdown,
    __colod_demote,
    __colod_quit,
    __colod_yank_co,
    __colod_execute_nocheck_co,
    __colod_execute_co
};

#define colod_failover_co(...) co_wrap(_colod_failover_co(__VA_ARGS__))
static int _colod_failover_co(Coroutine* coroutine, ColodMainCoroutine *this) {
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
        return -1;
    }
    qmp_ectx_unref(CO ectx, NULL);

    return 0;
    co_end;
}

#define colod_failover_sync_co(...) co_wrap(_colod_failover_sync_co(__VA_ARGS__))
static MainState _colod_failover_sync_co(Coroutine *coroutine,
                                         ColodMainCoroutine *this) {
    co_begin(MainState, STATE_FAILED);

    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_WIN, 0);
    colod_cpg_send(this->ctx->cpg, MESSAGE_FAILOVER);

    while (TRUE) {
        ColodEvent event;
        co_recurse(event = colod_interrupting_wait(coroutine, this));
        if (event == EVENT_FAILOVER_WIN) {
            break;
        } else if (event_always_interrupting(event)) {
            return handle_always_interrupting(this, event);
        } else {
            abort();
        }
    }

    int ret;
    co_recurse(ret = colod_failover_co(coroutine, this));
    if (ret < 0) {
        return STATE_FAILED;
    }

    colod_link_broken_delay_stop(this);
    peer_manager_clear_peer(this->ctx->peer);

    return STATE_PRIMARY_WAIT;
    co_end;
}

#define colod_secondary_wait_co(...) \
    co_wrap(_colod_secondary_wait_co(__VA_ARGS__))
static MainState _colod_secondary_wait_co(Coroutine *coroutine,
                                          ColodMainCoroutine *this) {
    struct {
        guint timeout;
    } *co;
    GError *local_errp = NULL;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(MainState, STATE_FAILED);

    /*
     * We need to be interrupted and discard all these events or
     * else they will be delayed until we are in another state like
     * colo_running and wreak havoc.
     */
    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC,
                                EVENT_FAILOVER_WIN, EVENT_KICK,
                                EVENT_SHUTDOWN, 0);

    while (TRUE) {
        co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 0,
                                                 "{'event': 'MIGRATION', 'data': {'status': 'active'}}",
                                                 &local_errp));

        if (ret < 0) {
            // Interrupted
            ColodEvent event;
            g_error_free(local_errp);
            local_errp = NULL;
            assert(eventqueue_pending(this->queue));
            co_recurse(event = colod_event_wait(coroutine, this));

            if (event_always_interrupting(event)) {
                return handle_always_interrupting(this, event);
            } else if (event == EVENT_SHUTDOWN) {
                eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC, 0);

                co_recurse(wait_while_timeout(coroutine, !this->peer_shutdown_done, this->ctx->command_timeout - 10*1000));
                return STATE_RETURN_NONE;
            }
            continue;
        }

        break;
    }

    eventqueue_set_interrupting(this->queue, EVENT_SHUTDOWN, 0);
    this->transitioning = TRUE;

    co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 5*60*1000,
                                             "{'event': 'RESUME'}",
                                             &local_errp));
    if (ret < 0) {
        if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
            g_error_free(local_errp);
            local_errp = NULL;

            ColodEvent event;
            co_recurse(event = colod_event_wait(coroutine, this));
            if (event_always_interrupting(event)) {
                return handle_always_interrupting(this, event);
            } else if (event == EVENT_SHUTDOWN) {
                eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC, 0);

                co_recurse(wait_while_timeout(coroutine, !this->peer_shutdown_done, this->ctx->command_timeout - 10*1000));
                return STATE_RETURN_NONE;
            } else {
                abort();
            }
        } else if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT)) {
            g_error_free(local_errp);
            local_errp = NULL;
            return STATE_FAILED;
        }
    }

    peer_manager_clear_failed(this->ctx->peer);
    peer_manager_clear_failover(this->ctx->peer);
    colod_raise_timeout_coroutine(&this->raise_timeout_coroutine, this->qmp,
                                  this->ctx);

    return STATE_COLO_RUNNING;
    co_end;
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

    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC,
                                EVENT_GUEST_SHUTDOWN, EVENT_SHUTDOWN, 0);

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

        CO source_id = g_timeout_add(10*1000, coroutine->cb, coroutine);
        g_source_set_name_by_id(CO source_id, "Waiting before failing to yellow");
        co_yield_int(G_SOURCE_REMOVE);

        guint source_id = g_source_get_id(g_main_current_source());
        if (source_id != CO source_id) {
            // Interrupted
            g_source_remove(CO source_id);
            goto handle_event;
        }

        if (this->yellow && strlen(peer_manager_get_peer(peer))
                && !peer_manager_yellow(peer)) {
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
            this->cache.new_state = STATE_PRIMARY_CONT_REPL;
            return handle_always_interrupting(this, event);
        } else if (event_command(event)) {
            return handle_event_command(event);
        }

        if (this->primary && this->yellow
                && strlen(peer_manager_get_peer(peer))
                && !peer_manager_yellow(peer)) {
            return STATE_FAILED;
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

        if (event_always_interrupting(event)) {
            return handle_always_interrupting(this, event);
        } else if (event_command(event)) {
            return handle_event_command(event);
        }
    }
    co_end;
}

static gboolean eventqueue_interrupt(gpointer data) {
    ColodMainCoroutine *this = data;
    return eventqueue_pending_interrupt(this->queue);
}

#define colod_primary_start_resync(...) co_wrap(_colod_primary_start_resync(__VA_ARGS__))
static MainState _colod_primary_start_resync(Coroutine *coroutine, ColodMainCoroutine *this) {
    struct {
        MyArray *commands;
        QmpEctx *ectx;
    } *co;
    QmpCommands *qmpcommands = this->ctx->commands;
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(MainState, STATE_FAILED);

    if (!strlen(peer_manager_get_peer(this->ctx->peer))) {
        return STATE_PRIMARY_WAIT;
    }

    CO ectx = qmp_ectx_new(this->qmp);
    qmp_ectx_set_interrupt_cb(CO ectx, eventqueue_interrupt, this);
    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC, 0);

    CO commands = qmp_commands_adhoc(qmpcommands, peer_manager_get_peer(this->ctx->peer),
                                     "{'execute': 'blockdev-add', 'arguments': {'driver': 'nbd', 'node-name': 'nbd0', 'server': {'type': 'inet', 'host': '@@ADDRESS@@', 'port': '@@NBD_PORT@@'}, 'export': 'parent0', 'detect-zeroes': 'on'}}",
                                     "@@DECL_BLK_MIRROR_PROP@@ {'device': 'colo-disk0', 'job-id': 'resync', 'target': 'nbd0', 'sync': 'full', 'on-target-error': 'report', 'on-source-error': 'ignore', 'auto-dismiss': false}",
                                     "{'execute': 'blockdev-mirror', 'arguments': @@BLK_MIRROR_PROP@@}",
                                     NULL);
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO commands));
    my_array_unref(CO commands);

    if (qmp_ectx_failed(CO ectx)) {
        goto ectx_failed;
    }

    co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 24*60*60*1000,
                    "{'event': 'JOB_STATUS_CHANGE',"
                    " 'data': {'status': 'ready', 'id': 'resync'}}",
                    &local_errp));
    if (ret < 0) {
        goto wait_error;
    }

    CO commands = qmp_commands_adhoc(qmpcommands, peer_manager_get_peer(this->ctx->peer),
                                     "{'execute': 'stop'}",
                                     "{'execute': 'block-job-cancel', 'arguments': {'device': 'resync'}}",
                                     NULL);
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO commands));
    my_array_unref(CO commands);

    if (qmp_ectx_failed(CO ectx)) {
        goto ectx_failed;
    }

    co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 10*1000,
                    "{'event': 'JOB_STATUS_CHANGE',"
                    " 'data': {'status': 'concluded', 'id': 'resync'}}",
                    &local_errp));
    if (ret < 0) {
        goto wait_error;
    }

    CO commands = qmp_commands_adhoc(qmpcommands, peer_manager_get_peer(this->ctx->peer),
                                     "{'execute': 'block-job-dismiss', 'arguments': {'id': 'resync'}}",
                                     "{'execute': 'x-blockdev-change', 'arguments': {'parent': 'quorum0', 'node': 'nbd0'}}",
                                     "{'execute': 'cont'}",
                                     NULL);
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO commands));
    my_array_unref(CO commands);

    if (qmp_ectx_failed(CO ectx)) {
        goto ectx_failed;
    }

    return STATE_PRIMARY_START_MIGRATION;

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
    ColodEvent event;
    co_recurse(event = colod_event_wait(coroutine, this));
    if (event_failover(event)) {
        goto failover;
    } else {
        this->cache.new_state = STATE_PRIMARY_RESYNC;
        return handle_always_interrupting(this, event);
    }

failover:
    CO ectx = qmp_ectx_new(this->qmp);
    qmp_ectx_set_ignore_yank(CO ectx);
    qmp_ectx_set_ignore_qmp_error(CO ectx);
    eventqueue_set_interrupting(this->queue, 0);

    co_recurse(qmp_ectx_yank(coroutine, CO ectx));

    co_recurse(result = qmp_ectx(coroutine, CO ectx,
                                 "{'execute': 'block-job-cancel', 'arguments': {'device': 'resync', 'force': true}}}\n"));
    if (result) {
        qmp_result_free(result);

        co_recurse(ret = colod_qmp_event_wait_co(coroutine, this, 10*1000,
                        "{'event': 'JOB_STATUS_CHANGE',"
                        " 'data': {'status': 'concluded', 'id': 'resync'}}",
                        &local_errp));
        if (ret < 0) {
            if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
                g_error_free(local_errp);
                local_errp = NULL;
                goto handle_event;
            } else if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT)) {
                g_error_free(local_errp);
                local_errp = NULL;
                return STATE_FAILED;
            } else {
                abort();
            }
        }
    }

    CO commands = qmp_commands_adhoc(qmpcommands, "dummy address",
                                     "{'execute': 'block-job-dismiss', 'arguments': {'id': 'resync'}}",
                                     "{'execute': 'x-blockdev-change', 'arguments': {'parent': 'quorum0', 'child': 'children.1'}}",
                                     "{'execute': 'blockdev-del', 'arguments': {'node-name': 'nbd0'}}",
                                     "{'execute': 'cont'}",
                                     NULL);
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO commands));
    my_array_unref(CO commands);

    if (qmp_ectx_failed(CO ectx)) {
        qmp_ectx_log_error(CO ectx);
        qmp_ectx_unref(CO ectx, NULL);
        return STATE_FAILED;
    }
    qmp_ectx_unref(CO ectx, NULL);

    peer_manager_clear_peer(this->ctx->peer);

    return STATE_PRIMARY_WAIT;
    co_end;
}

#define colod_primary_cont_repl(...) co_wrap(_colod_primary_cont_repl(__VA_ARGS__))
static MainState _colod_primary_cont_repl(Coroutine *coroutine, ColodMainCoroutine *this) {
    struct {
        MyArray *commands;
        QmpEctx *ectx;
    } *co;
    QmpCommands *qmpcommands = this->ctx->commands;

    co_frame(co, sizeof(*co));
    co_begin(MainState, STATE_FAILED);

    if (!strlen(peer_manager_get_peer(this->ctx->peer))) {
        return STATE_PRIMARY_WAIT;
    }

    CO ectx = qmp_ectx_new(this->qmp);
    qmp_ectx_set_interrupt_cb(CO ectx, eventqueue_interrupt, this);
    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC, 0);

    CO commands = qmp_commands_adhoc(qmpcommands, peer_manager_get_peer(this->ctx->peer),
                                     "{'execute': 'blockdev-add', 'arguments': {'driver': 'nbd', 'node-name': 'nbd0', 'server': {'type': 'inet', 'host': '@@ADDRESS@@', 'port': '@@NBD_PORT@@'}, 'export': 'parent0', 'detect-zeroes': 'on'}}",
                                     "{'execute': 'x-blockdev-change', 'arguments': {'parent': 'quorum0', 'node': 'nbd0'}}",
                                     NULL);
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO commands));
    my_array_unref(CO commands);

    if (qmp_ectx_failed(CO ectx)) {
        goto ectx_failed;
    }

    return STATE_PRIMARY_START_MIGRATION;

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

handle_event:
    assert(eventqueue_pending_interrupt(this->queue));
    ColodEvent event;
    co_recurse(event = colod_event_wait(coroutine, this));
    if (event_failover(event)) {
        goto failover;
    } else {
        this->cache.new_state = STATE_PRIMARY_CONT_REPL;
        return handle_always_interrupting(this, event);
    }

failover:
    CO ectx = qmp_ectx_new(this->qmp);
    qmp_ectx_set_ignore_yank(CO ectx);
    qmp_ectx_set_ignore_qmp_error(CO ectx);
    eventqueue_set_interrupting(this->queue, 0);

    co_recurse(qmp_ectx_yank(coroutine, CO ectx));

    CO commands = qmp_commands_adhoc(qmpcommands, "dummy address",
                                     "{'execute': 'x-blockdev-change', 'arguments': {'parent': 'quorum0', 'child': 'children.1'}}",
                                     "{'execute': 'blockdev-del', 'arguments': {'node-name': 'nbd0'}}",
                                     "{'execute': 'cont'}",
                                     NULL);
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO commands));
    my_array_unref(CO commands);

    if (qmp_ectx_failed(CO ectx)) {
        qmp_ectx_log_error(CO ectx);
        qmp_ectx_unref(CO ectx, NULL);
        return STATE_FAILED;
    }
    qmp_ectx_unref(CO ectx, NULL);

    peer_manager_clear_peer(this->ctx->peer);

    return STATE_PRIMARY_WAIT;
    co_end;
}

#define colod_primary_start_migration_co(...) \
    co_wrap(_colod_primary_start_migration_co(__VA_ARGS__))
static MainState _colod_primary_start_migration_co(Coroutine *coroutine,
                                                   ColodMainCoroutine *this) {
    struct {
        MyArray *commands;
        QmpEctx *ectx;
        gboolean filter_rewriter;
    } *co;
    QmpCommands *qmpcommands = this->ctx->commands;
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(MainState, STATE_FAILED);

    if (!strlen(peer_manager_get_peer(this->ctx->peer))) {
        return STATE_FAILOVER_SYNC;
    }

    CO ectx = qmp_ectx_new(this->qmp);
    qmp_ectx_set_interrupt_cb(CO ectx, eventqueue_interrupt, this);
    eventqueue_set_interrupting(this->queue, EVENT_FAILOVER_SYNC, 0);

    co_recurse(result = qmp_execute_co(coroutine, this->qmp, &local_errp,
                                       "{'execute': 'qom-list', 'arguments': {'path': '/objects/rew0'}}\n"));
    if (!result) {
        if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
            CO filter_rewriter = FALSE;
            g_error_free(local_errp);
            local_errp = NULL;
        } else {
            qmp_ectx_unref(CO ectx, NULL);
            return STATE_FAILED;
        }
    } else {
        CO filter_rewriter = TRUE;
    }
    qmp_result_free(result);

    co_recurse(result = qmp_ectx(coroutine, CO ectx,
                    "{'execute': 'migrate-set-capabilities',"
                    "'arguments': {'capabilities': ["
                        "{'capability': 'events', 'state': true },"
                        "{'capability': 'pause-before-switchover', 'state': true}]}}\n"));
    qmp_result_free(result);

    CO commands = qmp_commands_get_migration_start(qmpcommands, peer_manager_get_peer(this->ctx->peer), CO filter_rewriter);
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

    peer_manager_clear_failover_win(this->ctx->peer);
    return STATE_COLO_RUNNING;

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
    ColodEvent event;
    co_recurse(event = colod_event_wait(coroutine, this));
    if (event_failover(event)) {
        goto failover;
    } else {
        this->cache.new_state = STATE_PRIMARY_CONT_REPL;
        return handle_always_interrupting(this, event);
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

#define colod_quit_co(...) co_wrap(_colod_quit_co(__VA_ARGS__))
static int _colod_quit_co(Coroutine *coroutine, ColodMainCoroutine *this) {
    struct {
        MyTimeout *timeout;
        guint timeout_ms;
    } *co;
    ColodQmpResult *result;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO timeout = my_timeout_new(10*1000);

    qmp_set_timeout(this->qmp, MIN(5*1000, this->ctx->qmp_timeout_low));
    co_recurse(result = qmp_execute_co(coroutine, this->qmp, &local_errp,
                                       "{'execute': 'quit'}\n"));
    if (!result) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        local_errp = NULL;
        qemu_launcher_kill(this->launcher);
    }
    qmp_result_free(result);

    int ret;
    CO timeout_ms = my_timeout_remaining_ms(CO timeout);
    co_recurse(ret = qemu_launcher_wait_co(coroutine, this->launcher, CO timeout_ms, &local_errp));
    if (ret < 0) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        local_errp = NULL;
    }

    my_timeout_unref(CO timeout);
    return 0;
    co_end;
}

#define colod_shutdown_co(...) co_wrap(_colod_shutdown_co(__VA_ARGS__))
static int _colod_shutdown_co(Coroutine *coroutine, ColodMainCoroutine *this) {
    struct {
        MyTimeout *timeout;
        guint timeout_ms;
    } *co;
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    eventqueue_set_interrupting(this->queue, 0);
    CO timeout = my_timeout_new(this->ctx->command_timeout);

    co_recurse(result = qmp_execute_co(coroutine, this->qmp, NULL,
                                       "{'execute': 'system_powerdown'}\n"));
    if (!result) {
        goto stop;
    }
    qmp_result_free(result);

    if (this->replication) {
        while (!this->guest_shutdown) {
            CO timeout_ms = my_timeout_remaining_minus_ms(CO timeout, 10*1000);
            co_recurse(ret = qmp_wait_event_co(coroutine, this->qmp, CO timeout_ms,
                                               "{'event': 'RESUME'}", &local_errp));
            if (ret < 0) {
                if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
                    g_error_free(local_errp);
                    continue;
                }
                g_error_free(local_errp);
            }

            break;
        }

        co_recurse(result = qmp_execute_co(coroutine, this->qmp, NULL,
                                           "{'execute': 'system_powerdown'}\n"));
        if (!result) {
            goto stop;
        }
        qmp_result_free(result);
    }

    CO timeout_ms = my_timeout_remaining_minus_ms(CO timeout, 10*1000);
    co_recurse(wait_while_timeout(coroutine, !this->guest_shutdown, CO timeout_ms));
    if (peer_manager_failover(this->ctx->peer)) {
        my_timeout_unref(CO timeout);
        return -1;
    }

    if (!this->primary) {
        CO timeout_ms = my_timeout_remaining_minus_ms(CO timeout, 10*1000);
        co_recurse(wait_while_timeout(coroutine, !this->peer_shutdown_done, CO timeout_ms));
        if (peer_manager_failover(this->ctx->peer)) {
            my_timeout_unref(CO timeout);
            return -1;
        }
    }

stop:
    colod_cpg_send(this->ctx->cpg, MESSAGE_SHUTDOWN_DONE);
    my_timeout_unref(CO timeout);
    return 0;
    co_end;
}

#define colod_guest_shutdown_co(...) co_wrap(_colod_guest_shutdown_co(__VA_ARGS__))
static int _colod_guest_shutdown_co(Coroutine *coroutine, ColodMainCoroutine *this) {
    struct {
        MyTimeout *timeout;
        guint timeout_ms;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);
    GError *local_errp = NULL;
    ColodQmpResult *result;

    eventqueue_set_interrupting(this->queue, 0);
    CO timeout = my_timeout_new(this->ctx->command_timeout);

    if (this->primary) {
        int ret;
        co_recurse(ret = cluster_resource_stop(coroutine, this->ctx->instance_name, &local_errp));
        if (ret < 0) {
            log_error(local_errp->message);
            g_error_free(local_errp);
            my_timeout_unref(CO timeout);
            return -1;
        }
    } else {
        if (this->replication) {
            CO timeout_ms = my_timeout_remaining_minus_ms(CO timeout, 10*1000);
            co_recurse(wait_while_timeout(coroutine, !this->guest_shutdown, CO timeout_ms));
        }
        co_recurse(result = qmp_execute_co(coroutine, this->qmp, NULL,
                                           "{'execute': 'yank', 'arguments': {"
                                           " 'instances': [{ 'type': 'migration' }]}}\n"));
        qmp_result_free(result);
        co_recurse(result = qmp_execute_co(coroutine, this->qmp, NULL,
                                           "{'execute': 'stop'}\n"));
        qmp_result_free(result);
        co_recurse(wait_while_timeout(coroutine, !this->peer_shutdown_done, this->ctx->command_timeout - 10*1000));
    }

    if (peer_manager_failover(this->ctx->peer)) {
        return -1;
    }

    colod_cpg_send(this->ctx->cpg, MESSAGE_SHUTDOWN_DONE);
    return 0;
    co_end;
}

#define colod_guest_reboot_co(...) co_wrap(_colod_guest_reboot_co(__VA_ARGS__))
static int _colod_guest_reboot_co(Coroutine *coroutine, ColodMainCoroutine *this) {
    co_begin(int, -1);
    GError *local_errp = NULL;
    ColodQmpResult *result;

    eventqueue_set_interrupting(this->queue, 0);

    if (this->primary) {
        colod_cpg_send(this->ctx->cpg, MESSAGE_SHUTDOWN_DONE);
        if (strlen(peer_manager_get_peer(this->ctx->peer))
                && !peer_manager_failed(this->ctx->peer)) {
            co_recurse(wait_while_timeout(coroutine, !this->peer_reboot_restart, this->ctx->command_timeout - 10*1000));
            co_recurse(wait_while_timeout(coroutine, TRUE, 5*1000));
        }
    } else {
        if (this->replication) {
            co_recurse(wait_while_timeout(coroutine, !this->guest_shutdown, this->ctx->command_timeout - 10*1000));
        }
        co_recurse(result = qmp_execute_co(coroutine, this->qmp, NULL,
                                           "{'execute': 'yank', 'arguments': {"
                                           " 'instances': [{ 'type': 'migration' }]}}\n"));
        qmp_result_free(result);
        co_recurse(result = qmp_execute_co(coroutine, this->qmp, NULL,
                                           "{'execute': 'stop'}\n"));
        qmp_result_free(result);
        co_recurse(wait_while_timeout(coroutine, !this->peer_shutdown_done, this->ctx->command_timeout - 10*1000));
    }

    if (peer_manager_failover(this->ctx->peer)) {
        return -1;
    }

    if (this->primary
            && (this->cache.new_state == STATE_PRIMARY_RESYNC
             || this->cache.new_state == STATE_PRIMARY_CONT_REPL)) {
        this->cache.valid = TRUE;
    }

    colod_cpg_send(this->ctx->cpg, MESSAGE_REBOOT_RESTART);
    return 0;
    co_end;
}

static MainReturn handle_pending_command(ColodMainCoroutine *this, MainReturn ret) {
    if (this->command_wake) {
        this->cache.valid = FALSE;
        g_idle_add(this->command_wake->cb, this->command_wake);
        return this->command;
    }

    return ret;
}

static MainReturn _colod_main_co(Coroutine *coroutine, ColodMainCoroutine *this);
static gboolean colod_main_co(gpointer data) {
    ColodMainCoroutine *this = data;
    Coroutine *coroutine = data;
    MainReturn ret;

    co_enter(coroutine, ret = _colod_main_co(coroutine, this));
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    peer_manager_clear_shutdown(this->ctx->peer);

    this->main_return = ret;
    if (this->wake_on_exit) {
        g_idle_add(this->wake_on_exit->cb, this->wake_on_exit);
    }

    colod_main_client_unregister(this);
    colod_link_broken_delay_stop(this);

    this->mainco_running = FALSE;
    colod_assert_remove_one_source(this);
    colod_main_unref(this);
    return G_SOURCE_REMOVE;
}

static MainReturn _colod_main_co(Coroutine *coroutine, ColodMainCoroutine *this) {
    MainState new_state;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    if (this->primary) {
        if (this->cache.valid
                && strlen(peer_manager_get_peer(this->ctx->peer))
                && !peer_manager_failed(this->ctx->peer)) {
            colod_syslog(LOG_INFO, "starting in primary mode and continuing replication");
            new_state = this->cache.new_state;
        } else {
            colod_syslog(LOG_INFO, "starting in primary mode");
            new_state = STATE_PRIMARY_STARTUP;
        }
    } else {
        colod_syslog(LOG_INFO, "starting in secondary mode");
        new_state = STATE_SECONDARY_WAIT;
    }
    memset(&this->cache, 0, sizeof(this->cache));

    colod_cpg_send(this->ctx->cpg, MESSAGE_HELLO);

    while (TRUE) {
        this->transitioning = FALSE;
        this->state = new_state;
        if (this->state == STATE_SECONDARY_WAIT) {
            co_recurse(new_state = colod_secondary_wait_co(coroutine, this));
        } else if (this->state == STATE_PRIMARY_STARTUP) {
            ColodQmpResult *result;
            GError *local_errp = NULL;
            co_recurse(result = qmp_execute_co(coroutine, this->qmp, &local_errp,
                                               "{'execute': 'cont'}\n"));
            if (!result) {
                log_error(local_errp->message);
                g_error_free(local_errp);
                new_state = STATE_FAILED;
                continue;
            }
            qmp_result_free(result);

            new_state = STATE_PRIMARY_WAIT;
        } else if (this->state == STATE_PRIMARY_WAIT) {
            // Now running primary standalone
            this->primary = TRUE;
            this->replication = FALSE;

            co_recurse(new_state = colod_primary_wait_co(coroutine, this));
        } else if (this->state == STATE_PRIMARY_RESYNC) {
            wake_command(this, MAIN_NONE);
            co_recurse(new_state = colod_primary_start_resync(coroutine, this));
        } else if (this->state == STATE_PRIMARY_CONT_REPL) {
            co_recurse(new_state = colod_primary_cont_repl(coroutine, this));
        } else if (this->state == STATE_PRIMARY_START_MIGRATION) {
            co_recurse(new_state = colod_primary_start_migration_co(coroutine,
                                                                      this));
        } else if (this->state == STATE_COLO_RUNNING) {
            this->replication = TRUE;
            co_recurse(new_state = colod_colo_running_co(coroutine, this));
        } else if (this->state == STATE_FAILOVER_SYNC) {
            co_recurse(new_state = colod_failover_sync_co(coroutine, this));
        } else if (this->state == STATE_SHUTDOWN) {
            this->transitioning = TRUE;

            int ret;
            co_recurse(ret = colod_shutdown_co(coroutine, this));
            if (ret < 0) {
                new_state = STATE_FAILED;
            } else {
                co_recurse(colod_quit_co(coroutine, this));
                return handle_pending_command(this, MAIN_NONE);
            }
        } else if (this->state == STATE_GUEST_SHUTDOWN) {
            this->transitioning = TRUE;
            colod_cpg_send(this->ctx->cpg, MESSAGE_SHUTDOWN);

            int ret;
            co_recurse(ret = colod_guest_shutdown_co(coroutine, this));
            if (ret < 0) {
                new_state = STATE_FAILED;
            } else {
                co_recurse(colod_quit_co(coroutine, this));
                return handle_pending_command(this, MAIN_NONE);
            }
        } else if (this->state == STATE_GUEST_REBOOT) {
            this->transitioning = TRUE;
            colod_cpg_send(this->ctx->cpg, MESSAGE_REBOOT);

            int ret;
            co_recurse(ret = colod_guest_reboot_co(coroutine, this));
            if (ret < 0) {
                new_state = STATE_FAILED;
            } else {
                co_recurse(colod_quit_co(coroutine, this));
                if (this->primary) {
                    return handle_pending_command(this, MAIN_PROMOTE);
                } else {
                    return handle_pending_command(this, MAIN_DEMOTE);
                }
            }
        } else if (this->state == STATE_FAILED) {
            log_error("qemu failed");
            this->failed = TRUE;
            colod_cpg_send(this->ctx->cpg, MESSAGE_FAILED);

            qmp_set_timeout(this->qmp, this->ctx->qmp_timeout_low);
            co_recurse(colod_quit_co(coroutine, this));

            return handle_pending_command(this, MAIN_NONE);
        } else if (this->state == STATE_QUIT) {
            if (this->replication) {
                colod_cpg_send(this->ctx->cpg, MESSAGE_FAILED);
                co_recurse(wait_while_timeout(coroutine,
                                              !peer_manager_failover(this->ctx->peer)
                                                && !peer_manager_failed(this->ctx->peer),
                                              this->ctx->command_timeout - 10*1000));
            }
            co_recurse(colod_quit_co(coroutine, this));
            return handle_pending_command(this, MAIN_QUIT);
        } else if (this->state == STATE_RETURN_NONE) {
            co_recurse(colod_quit_co(coroutine, this));
            return handle_pending_command(this, MAIN_NONE);
        }
    }

    co_end;
}

static void colod_hup_cb(gpointer data) {
    ColodMainCoroutine *this = data;

    this->qemu_quit = TRUE;
    colod_event_queue(this, EVENT_FAILED, "qmp hup");
}

static void delay_destroy_cb(gpointer data) {
    ColodMainCoroutine *this = data;

    this->link_broken_delay_id = 0;
    colod_main_unref(this);
}

static void delay2_destroy_cb(gpointer data) {
    ColodMainCoroutine *this = data;

    this->link_broken_delay2_id = 0;
    colod_main_unref(this);
}

static gboolean colo_link_broken_delay2(gpointer data) {
    ColodMainCoroutine *this = data;
    colod_event_queue(this, EVENT_FAILOVER_SYNC, "COLO_EXIT qmp event delay2");
    return G_SOURCE_REMOVE;
}

static gboolean colo_link_broken_delay(gpointer data) {
    ColodMainCoroutine *this = data;

    if (peer_manager_shutdown(this->ctx->peer)) {
        this->link_broken_delay2_id = g_timeout_add_full(G_PRIORITY_DEFAULT,
                                                         MIN(30*1000, this->ctx->command_timeout - 10*1000),
                                                         colo_link_broken_delay2,
                                                         this, delay2_destroy_cb);
        colod_main_ref(this);
    } else {
        colod_event_queue(this, EVENT_FAILOVER_SYNC, "COLO_EXIT qmp event delay");
    }
    return G_SOURCE_REMOVE;
}

static void colod_link_broken_delay_stop(ColodMainCoroutine *this) {
    if (this->link_broken_delay_id) {
        g_source_remove(this->link_broken_delay_id);
    }
    if (this->link_broken_delay2_id) {
        g_source_remove(this->link_broken_delay2_id);
    }
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
                colod_event_queue(this, EVENT_KICK,
                                  "local disk write/flush error");
            }
        }
    } else if (!strcmp(event, "MIGRATION")) {
        const gchar *status;
        status = get_member_member_str(result->json_root, "data", "status");
        if (!strcmp(status, "failed") && this->state == STATE_PRIMARY_START_MIGRATION) {
            colod_event_queue(this, EVENT_FAILOVER_SYNC, "migration failed qmp event");
        }
    } else if (!strcmp(event, "COLO_EXIT")) {
        const gchar *reason;
        reason = get_member_member_str(result->json_root, "data", "reason");

        if (!strcmp(reason, "error")) {
            this->link_broken_delay_id = g_timeout_add_full(G_PRIORITY_DEFAULT,
                                                            1000, colo_link_broken_delay,
                                                            this, delay_destroy_cb);
            colod_main_ref(this);
        }
    } else if (!strcmp(event, "SHUTDOWN")) {
        const gchar *reason = get_member_member_str(result->json_root, "data", "reason");
        this->guest_shutdown = TRUE;
        if (!strcmp(reason, "guest-shutdown")) {
            this->guest_reboot = FALSE;
        } else if (!strcmp(reason, "guest-reset") && !strcmp(reason, "host-qmp-system-reset")) {
            this->guest_reboot = TRUE;
        } else {
            return;
        }
        colod_event_queue(this, EVENT_GUEST_SHUTDOWN, "guest shutdown");
    } else if (!strcmp(event, "RESET")) {
        colod_raise_timeout_coroutine(&this->raise_timeout_coroutine, this->qmp,
                                      this->ctx);
    } else if (!strcmp(event, "BLOCK_JOB_COMPLETED")) {
        const gchar *id = get_member_member_str(result->json_root, "data", "device");
        if (!strcmp(id, "resync")) {
            JsonObject *obj = json_node_get_object(result->json_root);
            JsonObject *data = json_object_get_object_member(obj, "data");
            assert(data);
            if (json_object_has_member(data, "error")) {
                colod_event_queue(this, EVENT_FAILOVER_SYNC, "block job failed");
            }
        }
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

    if (message == MESSAGE_SHUTDOWN_REQUEST) {
        if (this->state == STATE_PRIMARY_RESYNC
                || this->state == STATE_PRIMARY_START_MIGRATION
                || this->state == STATE_PRIMARY_CONT_REPL) {
            colod_event_queue(this, EVENT_FAILOVER_SYNC, "Failover before shutdown");
        }
        colod_event_queue(this, EVENT_SHUTDOWN, "cpg shutdown request");
    } else if (message_from_this_node) {
        return;
    } else if (message == MESSAGE_FAILED || peer_left_group) {
        colod_event_queue(this, EVENT_FAILOVER_SYNC, "got MESSAGE_FAILED or peer left group");
    } else if (message == MESSAGE_HELLO) {
        if (this->yellow) {
            colod_cpg_send(this->ctx->cpg, MESSAGE_YELLOW);
        } else {
            colod_cpg_send(this->ctx->cpg, MESSAGE_UNYELLOW);
        }
    } else if (message == MESSAGE_YELLOW) {
        colod_event_queue(this, EVENT_KICK, "peer yellow state change");
    } else if (message == MESSAGE_UNYELLOW) {
        colod_event_queue(this, EVENT_KICK, "peer yellow state change");
    } else if (message == MESSAGE_SHUTDOWN && !this->primary) {
        this->guest_reboot = FALSE;
        colod_event_queue(this, EVENT_GUEST_SHUTDOWN, "peer shutdown");
    } else if (message == MESSAGE_REBOOT && !this->primary) {
        this->guest_reboot = TRUE;
        colod_event_queue(this, EVENT_GUEST_SHUTDOWN, "peer reboot");
    } else if (message == MESSAGE_REBOOT_RESTART) {
        this->peer_reboot_restart = TRUE;
    } else if (message == MESSAGE_SHUTDOWN_DONE) {
        this->peer_shutdown_done = TRUE;
    }
}

static void colod_yellow_event_cb(gpointer data, YellowStatus event) {
    ColodMainCoroutine *this = data;

    if (event == STATUS_YELLOW) {
        this->yellow = TRUE;
        colod_event_queue(this, EVENT_KICK, "link down event");
    } else if (event == STATUS_UNYELLOW) {
        this->yellow = FALSE;
        colod_event_queue(this, EVENT_KICK, "link up event");
    } else {
        abort();
    }
}

void colod_main_client_register(ColodMainCoroutine *this) {
    colod_main_ref(this);
    client_register(this->ctx->listener, &colod_client_callbacks, this);
}

void colod_main_client_unregister(ColodMainCoroutine *this) {
    client_unregister(this->ctx->listener, &colod_client_callbacks, this);
    colod_main_unref(this);
}

MainReturn _colod_main_enter(Coroutine *coroutine, ColodMainCoroutine *this) {
    co_begin(int, -1);

    assert(!this->wake_on_exit);
    this->wake_on_exit = coroutine;
    this->mainco_running = TRUE;

    colod_main_client_register(this);

    colod_main_ref(this);
    g_idle_add_full(G_PRIORITY_HIGH, colod_main_co, this, NULL);

    co_yield_int(G_SOURCE_REMOVE);
    this->wake_on_exit = NULL;

    co_end;

    return this->main_return;
}

ColodMainCache *colod_main_get_cache(ColodMainCoroutine *this) {
    if (!this->cache.valid) {
        return NULL;
    }

    ColodMainCache *ret = g_new0(ColodMainCache, 1);
    *ret = this->cache;
    return ret;
}

ColodMainCoroutine *colod_main_new(const ColodContext *ctx, QemuLauncher *launcher,
                                   ColodQmpState *qmp, gboolean primary,
                                   ColodMainCache *cache, GError **errp) {
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
        colod_main_unref(this);
        return NULL;
    }

    this->queue = colod_eventqueue_new();

    this->primary = primary;
    if (cache) {
        this->cache = *cache;
        g_free(cache);
    }
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
