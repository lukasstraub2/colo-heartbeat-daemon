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
#include "json_util.h"

static const gchar *event_str(ColodEvent event) {
    switch (event) {
        case EVENT_NONE: return "EVENT_NONE";
        case EVENT_FAILED: return "EVENT_FAILED";
        case EVENT_QEMU_QUIT: return "EVENT_QEMU_QUIT";
        case EVENT_PEER_FAILOVER: return "EVENT_PEER_FAILOVER";
        case EVENT_FAILOVER_SYNC: return "EVENT_FAILOVER_SYNC";
        case EVENT_PEER_FAILED: return "EVENT_PEER_FAILED";
        case EVENT_FAILOVER_WIN: return "EVENT_FAILOVER_WIN";
        case EVENT_QUIT: return "EVENT_QUIT";
        case EVENT_AUTOQUIT: return "EVENT_AUTOQUIT";
        case EVENT_YELLOW: return "EVENT_YELLOW";
        case EVENT_START_MIGRATION: return "EVENT_START_MIGRATION";
        case EVENT_DID_FAILOVER: return "EVENT_DID_FAILOVER";
    }
    abort();
}

static gboolean event_escalate(ColodEvent event) {
    switch (event) {
        case EVENT_NONE:
        case EVENT_FAILED:
        case EVENT_QEMU_QUIT:
        case EVENT_PEER_FAILOVER:
        case EVENT_QUIT:
        case EVENT_AUTOQUIT:
        case EVENT_YELLOW:
        case EVENT_START_MIGRATION:
        case EVENT_DID_FAILOVER:
            return TRUE;
        break;

        default:
            return FALSE;
        break;
    }
}

static gboolean event_critical(ColodEvent event) {
    switch (event) {
        case EVENT_NONE:
        case EVENT_FAILOVER_WIN:
        case EVENT_YELLOW:
        case EVENT_START_MIGRATION:
        case EVENT_DID_FAILOVER:
            return FALSE;
        break;

        default:
            return TRUE;
        break;
    }
}

static gboolean event_failed(ColodEvent event) {
    switch (event) {
        case EVENT_FAILED:
        case EVENT_QEMU_QUIT:
        case EVENT_PEER_FAILOVER:
            return TRUE;
        break;

        default:
            return FALSE;
        break;
    }
}

static gboolean event_failover(ColodEvent event) {
    return event == EVENT_FAILOVER_SYNC || event == EVENT_PEER_FAILED;
}

static gboolean colod_event_pending(ColodContext *ctx) {
    return !queue_empty(&ctx->events) || !queue_empty(&ctx->critical_events);
}

#define colod_event_queue(ctx, event, reason) \
    _colod_event_queue((ctx), (event), (reason), __func__, __LINE__)
void _colod_event_queue(ColodContext *ctx, ColodEvent event,
                        const gchar *reason, const gchar *func,
                        int line) {
    ColodQueue *queue;

    colod_trace("%s:%u: queued %s (%s)\n", func, line, event_str(event),
                reason);

    if (event_critical(event)) {
        queue = &ctx->critical_events;
    } else {
        queue = &ctx->events;
    }

    if (queue_empty(queue) && ctx->main_coroutine) {
        colod_trace("%s:%u: Waking main coroutine\n", __func__, __LINE__);
        g_idle_add(ctx->main_coroutine->cb.plain, ctx->main_coroutine);
    }

    if (!queue_empty(queue)) {
        // ratelimit
        if (queue_peek(queue) == event) {
            colod_trace("%s:%u: Ratelimiting events\n", __func__, __LINE__);
            return;
        }
    }

    queue_add(queue, event);
    assert(colod_event_pending(ctx));
}

#define colod_event_wait(coroutine, ctx) \
    co_wrap(_colod_event_wait(coroutine, ctx, __func__, __LINE__))
static ColodEvent _colod_event_wait(Coroutine *coroutine, ColodContext *ctx,
                                    const gchar *func, int line) {
    ColodQueue *queue = &ctx->events;

    if (!colod_event_pending(ctx)) {
        coroutine->yield = TRUE;
        coroutine->yield_value = GINT_TO_POINTER(G_SOURCE_REMOVE);
        return EVENT_FAILED;
    }

    if (!queue_empty(&ctx->critical_events)) {
        queue = &ctx->critical_events;
    }

    ColodEvent event = queue_remove(queue);
    colod_trace("%s:%u: got %s\n", func, line, event_str(event));
    return event;
}

static gboolean colod_critical_pending(ColodContext *ctx) {
    return !queue_empty(&ctx->critical_events);
}

#define colod_qmp_event_wait_co(...) \
    co_wrap(_colod_qmp_event_wait_co(__VA_ARGS__))
static int _colod_qmp_event_wait_co(Coroutine *coroutine, ColodContext *ctx,
                                    guint timeout, const gchar* match,
                                    GError **errp) {
    int ret;
    GError *local_errp = NULL;

    while (TRUE) {
        ret = _qmp_wait_event_co(coroutine, ctx->qmp, timeout, match,
                                 &local_errp);
        if (coroutine->yield) {
            return -1;
        }
        if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
            assert(colod_event_pending(ctx));
            if (!colod_critical_pending(ctx)) {
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

    return ret;
}

void colod_set_migration_commands(ColodContext *ctx, JsonNode *commands) {
    if (ctx->migration_commands) {
        json_node_unref(ctx->migration_commands);
    }
    ctx->migration_commands = json_node_ref(commands);
}

void colod_set_primary_commands(ColodContext *ctx, JsonNode *commands) {
    if (ctx->failover_primary_commands) {
        json_node_unref(ctx->failover_primary_commands);
    }
    ctx->failover_primary_commands = json_node_ref(commands);
}

void colod_set_secondary_commands(ColodContext *ctx, JsonNode *commands) {
    if (ctx->failover_secondary_commands) {
        json_node_unref(ctx->failover_secondary_commands);
    }
    ctx->failover_secondary_commands = json_node_ref(commands);
}

int _colod_yank_co(Coroutine *coroutine, ColodContext *ctx, GError **errp) {
    int ret;
    GError *local_errp = NULL;

    ret = _qmp_yank_co(coroutine, ctx->qmp, &local_errp);
    if (coroutine->yield) {
        return -1;
    }
    if (ret < 0) {
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
    } else {
        qmp_clear_yank(ctx->qmp);
        colod_event_queue(ctx, EVENT_FAILOVER_SYNC, "did yank");
    }

    return ret;
}

ColodQmpResult *_colod_execute_nocheck_co(Coroutine *coroutine,
                                          ColodContext *ctx,
                                          GError **errp,
                                          const gchar *command) {
    ColodQmpResult *result;
    int ret;
    GError *local_errp = NULL;

    colod_watchdog_refresh(ctx->watchdog);

    result = _qmp_execute_nocheck_co(coroutine, ctx->qmp, &local_errp, command);
    if (coroutine->yield) {
        return NULL;
    }
    if (!result) {
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return NULL;
    }

    ret = qmp_get_error(ctx->qmp, &local_errp);
    if (ret < 0) {
        qmp_result_free(result);
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return NULL;
    }

    if (qmp_get_yank(ctx->qmp)) {
        qmp_clear_yank(ctx->qmp);
        colod_event_queue(ctx, EVENT_FAILOVER_SYNC, "did yank");
    }

    return result;
}

ColodQmpResult *_colod_execute_co(Coroutine *coroutine,
                                  ColodContext *ctx,
                                  GError **errp,
                                  const gchar *command) {
    ColodQmpResult *result;

    result = _colod_execute_nocheck_co(coroutine, ctx, errp, command);
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


#define colod_execute_array_co(...) \
    co_wrap(_colod_execute_array_co(__VA_ARGS__))
static int _colod_execute_array_co(Coroutine *coroutine, ColodContext *ctx,
                                   JsonNode *array_node, gboolean ignore_errors,
                                   GError **errp) {
    ColodArrayCo *co;
    int ret = 0;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    assert(!errp || !*errp);
    assert(JSON_NODE_HOLDS_ARRAY(array_node));

    CO array = json_node_get_array(array_node);
    CO count = json_array_get_length(CO array);
    for (CO i = 0; CO i < CO count; CO i++) {
        JsonNode *node = json_array_get_element(CO array, CO i);
        assert(node);

        gchar *tmp = json_to_string(node, FALSE);
        CO line = g_strdup_printf("%s\n", tmp);
        g_free(tmp);

        ColodQmpResult *result;
        co_recurse(result = colod_execute_co(coroutine, ctx, &local_errp, CO line));
        if (ignore_errors &&
                g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
            colod_syslog(LOG_WARNING, "Ignoring qmp error: %s",
                         local_errp->message);
            g_error_free(local_errp);
            local_errp = NULL;
        } else if (!result) {
            g_propagate_error(errp, local_errp);
            g_free(CO line);
            ret = -1;
            break;
        }
        qmp_result_free(result);
    }

    co_end;

    return ret;
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
static int _qemu_query_status_co(Coroutine *coroutine, ColodContext *ctx,
                                 gboolean *primary, gboolean *replication,
                                 GError **errp) {
    ColodCo *co;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    co_recurse(CO qemu_status = colod_execute_co(coroutine, ctx, errp,
                                                 "{'execute': 'query-status'}\n"));
    if (!CO qemu_status) {
        return -1;
    }

    co_recurse(CO colo_status = colod_execute_co(coroutine, ctx, errp,
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

int _colod_check_health_co(Coroutine *coroutine, ColodContext *ctx,
                           GError **errp) {
    gboolean primary;
    gboolean replication;
    int ret;
    GError *local_errp = NULL;

    ret = _qemu_query_status_co(coroutine, ctx, &primary, &replication,
                                &local_errp);
    if (coroutine->yield) {
        return -1;
    }
    if (ret < 0) {
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return -1;
    }

    if (!ctx->transitioning &&
            (ctx->primary != primary || ctx->replication != replication)) {
        colod_error_set(&local_errp, "qemu status mismatch: (%s, %s)"
                        " Expected: (%s, %s)",
                        bool_to_json(primary), bool_to_json(replication),
                        bool_to_json(ctx->primary),
                        bool_to_json(ctx->replication));
        colod_event_queue(ctx, EVENT_FAILED, local_errp->message);
        g_propagate_error(errp, local_errp);
        return -1;
    }

    return 0;
}

int colod_start_migration(ColodContext *ctx) {
    if (ctx->pending_action || ctx->replication) {
        return -1;
    }

    colod_event_queue(ctx, EVENT_START_MIGRATION, "client request");
    return 0;
}

void colod_autoquit(ColodContext *ctx) {
    colod_watchdog_inc_inhibit(ctx->watchdog);
    colod_event_queue(ctx, EVENT_AUTOQUIT, "client request");
}

void colod_qemu_failed(ColodContext *ctx) {
    colod_event_queue(ctx, EVENT_FAILED, "?");
}

typedef struct ColodRaiseCoroutine {
    Coroutine coroutine;
    ColodContext *ctx;
} ColodRaiseCoroutine;

static gboolean _colod_raise_timeout_co(Coroutine *coroutine,
                                        ColodContext *ctx);
static gboolean colod_raise_timeout_co(gpointer data) {
    ColodRaiseCoroutine *raiseco = data;
    Coroutine *coroutine = &raiseco->coroutine;
    ColodContext *ctx = raiseco->ctx;
    gboolean ret;

    co_enter(coroutine, ret = _colod_raise_timeout_co(coroutine, ctx));
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    qmp_set_timeout(ctx->qmp, ctx->qmp_timeout_low);

    g_source_remove_by_user_data(coroutine);
    assert(!g_source_remove_by_user_data(coroutine));
    g_free(ctx->raise_timeout_coroutine);
    ctx->raise_timeout_coroutine = NULL;
    return ret;
}

static gboolean colod_raise_timeout_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return colod_raise_timeout_co(data);
}

static gboolean _colod_raise_timeout_co(Coroutine *coroutine,
                                        ColodContext *ctx) {
    int ret;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    co_recurse(ret = qmp_wait_event_co(coroutine, ctx->qmp, 0,
                                       "{'event': 'STOP'}", NULL));
    if (ret < 0) {
        return G_SOURCE_REMOVE;
    }

    co_recurse(ret = qmp_wait_event_co(coroutine, ctx->qmp, 0,
                                       "{'event': 'RESUME'}", NULL));
    if (ret < 0) {
        return G_SOURCE_REMOVE;
    }

    co_end;

    return G_SOURCE_REMOVE;
}

void colod_raise_timeout_coroutine_free(ColodContext *ctx) {
    if (!ctx->raise_timeout_coroutine) {
        return;
    }

    g_idle_add(colod_raise_timeout_co, ctx->raise_timeout_coroutine);

    while (ctx->raise_timeout_coroutine) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }
}

Coroutine *colod_raise_timeout_coroutine(ColodContext *ctx) {
    ColodRaiseCoroutine *raiseco;
    Coroutine *coroutine;

    if (ctx->raise_timeout_coroutine) {
        return NULL;
    }

    qmp_set_timeout(ctx->qmp, ctx->qmp_timeout_high);

    raiseco = g_new0(ColodRaiseCoroutine, 1);
    coroutine = &raiseco->coroutine;
    coroutine->cb.plain = colod_raise_timeout_co;
    coroutine->cb.iofunc = colod_raise_timeout_co_wrap;
    raiseco->ctx = ctx;
    ctx->raise_timeout_coroutine = coroutine;

    g_idle_add(colod_raise_timeout_co, raiseco);
    return coroutine;
}

#define colod_stop_co(...) \
    co_wrap(_colod_stop_co(__VA_ARGS__))
static int _colod_stop_co(Coroutine *coroutine, ColodContext *ctx,
                          GError **errp) {
    ColodQmpResult *result;

    result = _colod_execute_co(coroutine, ctx, errp,
                               "{'execute': 'stop'}\n");
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
static ColodEvent _colod_failover_co(Coroutine *coroutine, ColodContext *ctx) {
    ColodCo *co;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(ColodEvent, EVENT_FAILED);

    co_recurse(ret = qmp_yank_co(coroutine, ctx->qmp, &local_errp));
    if (ret < 0) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return EVENT_FAILED;
    }

    if (ctx->primary) {
        CO commands = ctx->failover_primary_commands;
    } else {
        CO commands = ctx->failover_secondary_commands;
    }
    ctx->transitioning = TRUE;
    co_recurse(ret = colod_execute_array_co(coroutine, ctx, CO commands,
                                            TRUE, &local_errp));
    ctx->transitioning = FALSE;
    if (ret < 0) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return EVENT_FAILED;
    }

    co_end;

    return EVENT_DID_FAILOVER;
}

#define colod_failover_sync_co(...) \
    co_wrap(_colod_failover_sync_co(__VA_ARGS__))
static ColodEvent _colod_failover_sync_co(Coroutine *coroutine,
                                          ColodContext *ctx) {

    co_begin(ColodEvent, EVENT_FAILED);

    colod_cpg_send(ctx, MESSAGE_FAILOVER);

    while (TRUE) {
        ColodEvent event;
        co_recurse(event = colod_event_wait(coroutine, ctx));
        if (event == EVENT_FAILOVER_WIN) {
            break;
        } else if (event == EVENT_PEER_FAILED) {
            break;
        } else if (event_critical(event) && event_escalate(event)) {
            return event;
        }
    }

    ColodEvent event;
    co_recurse(event = colod_failover_co(coroutine, ctx));
    return event;

    co_end;

    return EVENT_FAILED;
}

#define colod_start_migration_co(...) \
    co_wrap(_colod_start_migration_co(__VA_ARGS__))
static ColodEvent _colod_start_migration_co(Coroutine *coroutine,
                                            ColodContext *ctx) {
    ColodCo *co;
    ColodQmpState *qmp = ctx->qmp;
    ColodQmpResult *qmp_result;
    ColodEvent result;
    int ret;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(ColodEvent, EVENT_FAILED);
    co_recurse(qmp_result = colod_execute_co(coroutine, ctx, &local_errp,
                    "{'execute': 'migrate-set-capabilities',"
                    "'arguments': {'capabilities': ["
                        "{'capability': 'events', 'state': true },"
                        "{'capability': 'pause-before-switchover', 'state': true}]}}\n"));
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
        goto qmp_error;
    } else if (!qmp_result) {
        goto qemu_failed;
    }
    qmp_result_free(qmp_result);
    if (colod_critical_pending(ctx)) {
        goto handle_event;
    }

    co_recurse(ret = colod_qmp_event_wait_co(coroutine, ctx, 5*60*1000,
                    "{'event': 'MIGRATION',"
                    " 'data': {'status': 'pre-switchover'}}",
                    &local_errp));
    if (ret < 0) {
        goto qmp_error;
    }

    co_recurse(ret = colod_execute_array_co(coroutine, ctx, ctx->migration_commands,
                                            FALSE, &local_errp));
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
        goto qmp_error;
    } else if (ret < 0) {
        goto qemu_failed;
    }
    if (colod_critical_pending(ctx)) {
        goto handle_event;
    }

    colod_raise_timeout_coroutine(ctx);

    co_recurse(qmp_result = colod_execute_co(coroutine, ctx, &local_errp,
                    "{'execute': 'migrate-continue',"
                    "'arguments': {'state': 'pre-switchover'}}\n"));
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
        qmp_set_timeout(qmp, ctx->qmp_timeout_low);
        goto qmp_error;
    } else if (!qmp_result) {
        qmp_set_timeout(qmp, ctx->qmp_timeout_low);
        goto qemu_failed;
    }
    qmp_result_free(qmp_result);
    if (colod_critical_pending(ctx)) {
        qmp_set_timeout(qmp, ctx->qmp_timeout_low);
        goto handle_event;
    }

    ctx->transitioning = TRUE;
    co_recurse(ret = colod_qmp_event_wait_co(coroutine, ctx, 10000,
                    "{'event': 'MIGRATION',"
                    " 'data': {'status': 'colo'}}",
                    &local_errp));
    ctx->transitioning = FALSE;
    if (ret < 0) {
        qmp_set_timeout(qmp, ctx->qmp_timeout_low);
        goto qmp_error;
    }

    return EVENT_NONE;

qmp_error:
    if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_INTERRUPT)) {
        g_error_free(local_errp);
        local_errp = NULL;
        assert(colod_critical_pending(ctx));
        co_recurse(CO event = colod_event_wait(coroutine, ctx));
        if (event_failover(CO event)) {
            goto failover;
        } else {
            return CO event;
        }
    } else {
        log_error(local_errp->message);
        g_error_free(local_errp);
    }
    CO event = EVENT_PEER_FAILED;
    goto failover;

qemu_failed:
    log_error(local_errp->message);
    g_error_free(local_errp);
    return EVENT_FAILED;

handle_event:
    assert(colod_critical_pending(ctx));
    co_recurse(CO event = colod_event_wait(coroutine, ctx));
    if (event_failover(CO event)) {
        goto failover;
    } else {
        return CO event;
    }

failover:
    co_recurse(qmp_result = colod_execute_co(coroutine, ctx, &local_errp,
                    "{'execute': 'migrate_cancel'}\n"));
    if (!qmp_result) {
        goto qemu_failed;
    }
    qmp_result_free(qmp_result);
    assert(event_failover(CO event));
    if (CO event == EVENT_FAILOVER_SYNC) {
        co_recurse(result = colod_failover_sync_co(coroutine, ctx));
    } else {
        co_recurse(result = colod_failover_co(coroutine, ctx));
    }
    return result;

    co_end;

    return EVENT_FAILED;
}

#define colod_replication_wait_co(...) \
    co_wrap(_colod_replication_wait_co(__VA_ARGS__))
static ColodEvent _colod_replication_wait_co(Coroutine *coroutine,
                                             ColodContext *ctx) {
    ColodEvent event;
    int ret;
    ColodQmpResult *qmp_result;
    GError *local_errp = NULL;

    co_begin(ColodEvent, EVENT_FAILED);

    co_recurse(qmp_result = colod_execute_co(coroutine, ctx, &local_errp,
                        "{'execute': 'migrate-set-capabilities',"
                        "'arguments': {'capabilities': ["
                            "{'capability': 'events', 'state': true }]}}\n"));
    if (!qmp_result) {
        log_error(local_errp->message);
        g_error_free(local_errp);
        return EVENT_FAILED;
    }
    qmp_result_free(qmp_result);

    while (TRUE) {
        ctx->transitioning = TRUE;
        co_recurse(ret = colod_qmp_event_wait_co(coroutine, ctx, 0,
                                      "{'event': 'RESUME'}", &local_errp));
        ctx->transitioning = FALSE;
        if (ret < 0) {
            g_error_free(local_errp);
            assert(colod_event_pending(ctx));
            co_recurse(event = colod_event_wait(coroutine, ctx));
            if (event_critical(event) && event_escalate(event)) {
                return event;
            }
            continue;
        }
        break;
    }

    colod_raise_timeout_coroutine(ctx);

    co_end;

    return EVENT_NONE;
}

#define colod_replication_running_co(...) \
    co_wrap(_colod_replication_running_co(__VA_ARGS__))
static ColodEvent _colod_replication_running_co(Coroutine *coroutine,
                                                ColodContext *ctx) {
    co_begin(ColodEvent, EVENT_FAILED);

    while (TRUE) {
        ColodEvent event;
        co_recurse(event = colod_event_wait(coroutine, ctx));
        if (event == EVENT_FAILOVER_SYNC) {
            co_recurse(event = colod_failover_sync_co(coroutine, ctx));
            return event;
        } else if (event == EVENT_PEER_FAILED) {
            co_recurse(event = colod_failover_co(coroutine, ctx));
            return event;
        } else if (event_critical(event) && event_escalate(event)) {
            return event;
        }
    }

    co_end;

    return EVENT_FAILED;
}

void colod_quit(ColodContext *ctx) {
    g_main_loop_quit(ctx->mainloop);
}

void do_autoquit(ColodContext *ctx) {
    client_listener_free(ctx->listener);
    exit(EXIT_SUCCESS);
}

typedef struct ColodMainCoroutine {
    Coroutine coroutine;
    ColodContext *ctx;
} ColodMainCoroutine;

static gboolean _colod_main_co(Coroutine *coroutine, ColodContext *ctx);
static gboolean colod_main_co(gpointer data) {
    ColodMainCoroutine *mainco = data;
    Coroutine *coroutine = data;
    ColodContext *ctx = mainco->ctx;
    gboolean ret;

    co_enter(coroutine, ret = _colod_main_co(coroutine, ctx));
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    g_source_remove_by_user_data(coroutine);
    assert(!g_source_remove_by_user_data(coroutine));
    ctx->main_coroutine = NULL;
    g_free(coroutine);
    return ret;
}

static gboolean colod_main_co_wrap(
        G_GNUC_UNUSED GIOChannel *channel,
        G_GNUC_UNUSED GIOCondition revents,
        gpointer data) {
    return colod_main_co(data);
}

static gboolean _colod_main_co(Coroutine *coroutine, ColodContext *ctx) {
    ColodEvent event = EVENT_NONE;
    int ret;
    GError *local_errp = NULL;

    co_begin(gboolean, G_SOURCE_CONTINUE);

    if (!ctx->primary) {
        colod_syslog(LOG_INFO, "starting in secondary mode");
        while (TRUE) {
            co_recurse(event = colod_replication_wait_co(coroutine, ctx));
            assert(event_escalate(event));
            if (event_failed(event)) {
                goto failed;
            } else if (event == EVENT_QUIT) {
                return G_SOURCE_REMOVE;
            } else if (event == EVENT_AUTOQUIT) {
                goto autoquit;
            } else if (event == EVENT_DID_FAILOVER) {
                break;
            }
            ctx->replication = TRUE;

            co_recurse(event = colod_replication_running_co(coroutine, ctx));
            assert(event_escalate(event));
            assert(event != EVENT_NONE);
            if (event_failed(event)) {
                goto failed;
            } else if (event == EVENT_QUIT) {
                return G_SOURCE_REMOVE;
            } else if (event == EVENT_AUTOQUIT) {
                goto autoquit;
            } else if (event == EVENT_DID_FAILOVER) {
                break;
            } else {
                abort();
            }
        }
    } else {
        colod_syslog(LOG_INFO, "starting in primary mode");
    }

    // Now running primary standalone
    ctx->primary = TRUE;
    ctx->replication = FALSE;

    while (TRUE) {
        co_recurse(event = colod_event_wait(coroutine, ctx));
        if (event == EVENT_START_MIGRATION) {
            ctx->pending_action = TRUE;
            co_recurse(event = colod_start_migration_co(coroutine, ctx));
            assert(event_escalate(event));
            ctx->pending_action = FALSE;
            if (event_failed(event)) {
                goto failed;
            } else if (event == EVENT_QUIT) {
                return G_SOURCE_REMOVE;
            } else if (event == EVENT_AUTOQUIT) {
                goto autoquit;
            } else if (event == EVENT_DID_FAILOVER) {
                continue;
            }
            ctx->replication = TRUE;

            co_recurse(event = colod_replication_running_co(coroutine, ctx));
            assert(event_escalate(event));
            assert(event != EVENT_NONE);
            if (event_failed(event)) {
                 goto failed;
             } else if (event == EVENT_QUIT) {
                 return G_SOURCE_REMOVE;
             } else if (event == EVENT_AUTOQUIT) {
                 goto autoquit;
             } else if (event == EVENT_DID_FAILOVER) {
                ctx->replication = FALSE;
                continue;
             } else {
                 abort();
             }
        } else if (event_failed(event)) {
            if (event != EVENT_PEER_FAILOVER) {
                goto failed;
            }
        } else if (event == EVENT_QUIT) {
            return G_SOURCE_REMOVE;
        } else if (event == EVENT_AUTOQUIT) {
            goto autoquit;
        }
    }

failed:
    qmp_set_timeout(ctx->qmp, ctx->qmp_timeout_low);
    ret = qmp_get_error(ctx->qmp, &local_errp);
    if (ret < 0) {
        log_error_fmt("qemu failed: %s", local_errp->message);
        g_error_free(local_errp);
        local_errp = NULL;
    }

    ctx->failed = TRUE;
    colod_cpg_send(ctx, MESSAGE_FAILED);

    if (event == EVENT_NONE) {
        log_error("Failed with EVENT_NONE");
    }
    if (event == EVENT_PEER_FAILOVER) {
        ctx->peer_failover = TRUE;
    }
    if (event != EVENT_QEMU_QUIT) {
        co_recurse(ret = colod_stop_co(coroutine, ctx, &local_errp));
        if (ret < 0) {
            if (event == EVENT_PEER_FAILOVER) {
                log_error_fmt("Failed to stop qemu in response to "
                              "peer failover: %s", local_errp->message);
            }
            g_error_free(local_errp);
            local_errp = NULL;
        }
    }

    while (TRUE) {
        ColodEvent event;
        co_recurse(event = colod_event_wait(coroutine, ctx));
        if (event == EVENT_PEER_FAILOVER) {
            ctx->peer_failover = TRUE;
        } else if (event == EVENT_QUIT) {
            return G_SOURCE_REMOVE;
        } else if (event == EVENT_AUTOQUIT) {
            if (ctx->qemu_quit) {
                do_autoquit(ctx);
            } else {
                goto autoquit;
            }
        }
    }

autoquit:
    ctx->failed = TRUE;
    colod_cpg_send(ctx, MESSAGE_FAILED);

    while (TRUE) {
        ColodEvent event;
        co_recurse(event = colod_event_wait(coroutine, ctx));
        if (event == EVENT_PEER_FAILOVER) {
            ctx->peer_failover = TRUE;
        } else if (event == EVENT_QUIT) {
            return G_SOURCE_REMOVE;
        } else if (event == EVENT_QEMU_QUIT) {
            do_autoquit(ctx);
        }
    }

    co_end;

    return G_SOURCE_REMOVE;
}

Coroutine *colod_main_coroutine(ColodContext *ctx) {
    ColodMainCoroutine *mainco;
    Coroutine *coroutine;

    assert(!ctx->main_coroutine);

    mainco = g_new0(ColodMainCoroutine, 1);
    coroutine = &mainco->coroutine;
    coroutine->cb.plain = colod_main_co;
    coroutine->cb.iofunc = colod_main_co_wrap;
    mainco->ctx = ctx;
    ctx->main_coroutine = coroutine;

    g_idle_add(colod_main_co, mainco);
    return coroutine;
}

void colod_main_free(ColodContext *ctx) {
    colod_event_queue(ctx, EVENT_QUIT, "teardown");

    while (ctx->main_coroutine) {
        g_main_context_iteration(g_main_context_default(), TRUE);
    }
}
