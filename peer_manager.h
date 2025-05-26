#pragma once

#include "base_types.h"
#include "eventqueue.h"

typedef void (*PeerManagerCb)(gpointer data, ColodEvent event);

void peer_manager_add_notify(PeerManager *this, PeerManagerCb _func, gpointer data);
void peer_manager_del_notify(PeerManager *this, PeerManagerCb _func, gpointer data);

void peer_manager_set_failed(PeerManager *this);
void peer_manager_clear_failed(PeerManager *this);
void peer_manager_set_peer(PeerManager *this, const gchar *peer);
void peer_manager_clear_peer(PeerManager *this);
const char *peer_manager_get_peer(PeerManager *this);
gboolean peer_manager_failed(PeerManager *this);
gboolean peer_manager_yellow(PeerManager *this);
gboolean peer_manager_failover(PeerManager *this);
gboolean peer_manager_shutdown(PeerManager *this);

PeerManager *peer_manager_new(Cpg *cpg);
void peer_manager_free(gpointer data);
PeerManager *peer_manager_ref(PeerManager *this);
void peer_manager_unref(PeerManager *this);
