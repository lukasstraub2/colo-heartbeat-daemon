/*
 * Utilities
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include "yellow_coroutine.h"
#include "coroutine_stack.h"
#include "cpg.h"
#include "netlink.h"

struct YellowCoroutine {
    Coroutine coroutine;
    gboolean quit;
    Cpg *cpg;
    const ColodContext *ctx;
    ColodNetlink *netlink;
    ColodCallbackHead callbacks;
    guint timeout1, timeout2;
};

void yellow_add_notify(YellowCoroutine *this, YellowCallback _func,
                       gpointer user_data) {
    ColodCallbackFunc func = (ColodCallbackFunc) _func;
    colod_callback_add(&this->callbacks, func, user_data);
}

void yellow_del_notify(YellowCoroutine *this, YellowCallback _func,
                       gpointer user_data) {
    ColodCallbackFunc func = (ColodCallbackFunc) _func;
    colod_callback_del(&this->callbacks, func, user_data);
}

static void notify(YellowCoroutine *this, YellowStatus event) {
    ColodCallback *entry, *next_entry;
    QLIST_FOREACH_SAFE(entry, &this->callbacks, next, next_entry) {
        YellowCallback func = (YellowCallback) entry->func;
        func(entry->user_data, event);
    }
}

static void yellow_send_target_message(Cpg *cpg, YellowStatus target_event) {
    if (target_event == STATUS_YELLOW) {
        colod_cpg_send(cpg, MESSAGE_YELLOW);
    } else {
        colod_cpg_send(cpg, MESSAGE_UNYELLOW);
    }
}

static void yellow_send_revert_message(Cpg *cpg, YellowStatus target_event) {
    if (target_event == STATUS_YELLOW) {
        colod_cpg_send(cpg, MESSAGE_UNYELLOW);
    } else {
        colod_cpg_send(cpg, MESSAGE_YELLOW);
    }
}

#define yellow_delay_co(...) \
    co_wrap(_yellow_delay_co(__VA_ARGS__))
static int _yellow_delay_co(Coroutine *coroutine, YellowCoroutine *this,
                            YellowStatus target_event, YellowStatus event) {
    struct {
        guint source_id;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);
    while (TRUE) {
        assert(event);
        if (event == STATUS_QUIT) {
            return -1;
        } else if (event != target_event) {
            co_yield(0);
            continue;
        }
        assert(event == target_event);

        CO source_id = g_timeout_add(this->timeout1, coroutine->cb, this);
        co_yield(0);

        while (event == target_event) {
            co_yield(0);
        }
        if (event) {
            g_source_remove(CO source_id);
            continue;
        }
        // No event
        yellow_send_target_message(this->cpg, target_event);

        CO source_id = g_timeout_add(this->timeout2, coroutine->cb, this);
        co_yield(0);

        while (event == target_event) {
            co_yield(0);
        }
        if (event) {
            yellow_send_revert_message(this->cpg, target_event);
            g_source_remove(CO source_id);
            continue;
        }

        break;
    }

    co_end;

    return 0;
}

static int _yellow_co(Coroutine *coroutine, YellowCoroutine *this,
                      YellowStatus event) {
    int ret;

    co_begin(gboolean, FALSE);

    while (TRUE) {
        co_recurse(ret = yellow_delay_co(coroutine, this, STATUS_YELLOW, event));
        if (ret < 0) {
            return 0;
        }
        notify(this, STATUS_YELLOW);
        co_yield(0);

        co_recurse(ret = yellow_delay_co(coroutine, this, STATUS_UNYELLOW, event));
        if (ret < 0) {
            return 0;
        }
        notify(this, STATUS_UNYELLOW);
        co_yield(0);
    }

    co_end;
}

static gboolean yellow_co(gpointer data) {
    YellowCoroutine *this = data;
    Coroutine *coroutine = &this->coroutine;

    co_enter(coroutine, _yellow_co(coroutine, this, 0));
    assert(coroutine->yield);
    return G_SOURCE_REMOVE;
}

static void yellow_queue_event(YellowCoroutine *this, YellowStatus event) {
    Coroutine *coroutine = &this->coroutine;

    assert(event == STATUS_QUIT || event == STATUS_YELLOW || event == STATUS_UNYELLOW);
    co_enter(coroutine, _yellow_co(coroutine, this, event));
    if (coroutine->yield) {
        return;
    }

    this->quit = TRUE;
}

static void yellow_netlink_event_cb(gpointer data, const char *ifname,
                                    gboolean up) {
    YellowCoroutine *this = data;

    if (!this->ctx->monitor_interface) {
        return;
    }

    if (!strcmp(ifname, this->ctx->monitor_interface)) {
        if (up) {
            yellow_queue_event(this, STATUS_UNYELLOW);
        } else {
            yellow_queue_event(this, STATUS_YELLOW);
        }
    }
}

YellowCoroutine *yellow_coroutine_new(Cpg *cpg, const ColodContext *ctx,
                                      guint timeout1, guint timeout2,
                                      GError **errp) {
    int ret;
    YellowCoroutine *this;
    Coroutine *coroutine;

    this = g_new0(YellowCoroutine, 1);
    coroutine = &this->coroutine;
    coroutine->cb = yellow_co;
    this->cpg = cpg;
    this->ctx = ctx;
    this->timeout1 = timeout1;
    this->timeout2 = timeout2;

    this->netlink = netlink_new(errp);
    if (!this->netlink) {
        g_free(this);
        return NULL;
    }

    netlink_add_notify(this->netlink, yellow_netlink_event_cb, this);
    ret = netlink_request_status(this->netlink, errp);
    if (ret < 0) {
        yellow_coroutine_free(this);
        return NULL;
    }

    return this;
}

void yellow_shutdown(YellowCoroutine *this) {
    if (this->quit) {
        return;
    }

    netlink_del_notify(this->netlink, yellow_netlink_event_cb, this);
    yellow_queue_event(this, STATUS_QUIT);
    assert(this->quit);
}

void yellow_coroutine_free(YellowCoroutine *this) {
    yellow_shutdown(this);

    colod_callback_clear(&this->callbacks);

    netlink_free(this->netlink);
    g_free(this);
}
