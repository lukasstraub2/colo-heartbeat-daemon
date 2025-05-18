
#include <glib-2.0/glib.h>

#include "peer_manager.h"
#include "coroutine_stack.h"
#include "cpg.h"

typedef struct PeerStatus PeerStatus;
struct PeerStatus {
    gboolean failed, yellow, failover;
};

struct PeerManager {
    Coroutine coroutine;
    Cpg *cpg;

    char *peer_name;
    PeerStatus peer;

    gboolean failover_win;
    ColodCallbackHead callbacks;
};

void peer_manager_add_notify(PeerManager *this, PeerManagerCb _func, gpointer data) {
    ColodCallbackFunc func = (ColodCallbackFunc) _func;
    colod_callback_add(&this->callbacks, func, data);
    peer_manager_ref(this);
}

void peer_manager_del_notify(PeerManager *this, PeerManagerCb _func, gpointer data) {
    ColodCallbackFunc func = (ColodCallbackFunc) _func;
    colod_callback_del(&this->callbacks, func, data);
    peer_manager_unref(this);
}

static void peer_manager_notify(PeerManager *this, ColodEvent event) {
    ColodCallback *entry, *next_entry;
    QLIST_FOREACH_SAFE(entry, &this->callbacks, next, next_entry) {
        PeerManagerCb func = (PeerManagerCb) entry->func;
        func(entry->user_data, event);
    }
}

static gboolean peer_manager_clear_failover_win(gpointer data) {
    PeerManager *this = data;

    this->failover_win = FALSE;

    peer_manager_unref(this);
    return G_SOURCE_REMOVE;
}

static void peer_manager_cpg_cb(gpointer data, ColodMessage message,
                                gboolean message_from_this_node,
                                gboolean peer_left_group) {
    PeerManager *this = data;

    if (message == MESSAGE_FAILOVER) {
        if (message_from_this_node) {
            this->failover_win = TRUE;
            peer_manager_notify(this, EVENT_FAILOVER_WIN);
            g_timeout_add(60*1000, peer_manager_clear_failover_win, this);
            peer_manager_ref(this);
        } else if (this->failover_win) {
            this->failover_win = FALSE;
        } else {
            peer_manager_notify(this, EVENT_FAILED);
            this->peer.failover = TRUE;
        }
    } else if (message_from_this_node) {
        return;
    } else if (message == MESSAGE_FAILED || peer_left_group) {
        log_error("Peer failed");
        this->peer.failed = TRUE;
    } else if (message == MESSAGE_YELLOW) {
        this->peer.yellow = TRUE;
    } else if (message == MESSAGE_UNYELLOW) {
        this->peer.yellow = FALSE;
    }
}

void peer_manager_set_failed(PeerManager *this) {
    this->peer.failed = TRUE;
}

void peer_manager_clear_failed(PeerManager *this) {
    this->peer.failed = FALSE;
}

void peer_manager_set_peer(PeerManager *this, const gchar *peer) {
    g_free(this->peer_name);
    this->peer_name = g_strdup(peer);
    memset(&this->peer, 0, sizeof(this->peer));
}

void peer_manager_clear_peer(PeerManager *this) {
    g_free(this->peer_name);
    this->peer_name = g_strdup("");
}

char *peer_manager_get_peer(PeerManager *this) {
    return g_strdup(this->peer_name);
}

gboolean peer_manager_failed(PeerManager *this) {
    return this->peer.failed;
}

gboolean peer_manager_yellow(PeerManager *this) {
    return this->peer.yellow;
}

gboolean peer_manager_failover(PeerManager *this) {
    return this->peer.failover;
}

PeerManager *peer_manager_new(Cpg *cpg) {
    PeerManager *this = g_rc_box_new0(PeerManager);

    this->cpg = cpg;
    this->peer_name = g_strdup("");

    colod_cpg_add_notify(this->cpg, peer_manager_cpg_cb, this);

    return this;
}

void peer_manager_free(gpointer data) {
    PeerManager *this = data;

    g_free(this->peer_name);

    colod_cpg_del_notify(this->cpg, peer_manager_cpg_cb, this);
    colod_callback_clear(&this->callbacks);
}

PeerManager *peer_manager_ref(PeerManager *this) {
    return g_rc_box_acquire(this);
}

void peer_manager_unref(PeerManager *this) {
    g_rc_box_release_full(this, peer_manager_free);
}
