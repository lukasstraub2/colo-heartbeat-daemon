/*
 * COLO background daemon client handling
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef CLIENT_H
#define CLIENT_H

#include <glib-2.0/glib.h>

#include "base_types.h"
#include "daemon.h"
#include "main_coroutine.h"

typedef struct MyTimeout MyTimeout;

guint my_timeout_remaining_ms(MyTimeout *this);
guint my_timeout_remaining_minus_ms(MyTimeout *this, guint minus);
MyTimeout *my_timeout_new(guint timeout_ms);
void my_timeout_free(gpointer data);
MyTimeout *my_timeout_ref(MyTimeout *this);
void my_timeout_unref(MyTimeout *this);

typedef struct ClientCallbacks ClientCallbacks;

struct ClientCallbacks {
    void (*query_status)(gpointer data, ColodState *ret);
    int (*_check_health_co)(Coroutine *coroutine, gpointer data, GError **errp);

    int (*_set_peer_co)(Coroutine *coroutine, gpointer data, const gchar *peer);
    const gchar *(*_get_peer_co)(Coroutine *coroutine, gpointer data);
    int (*_clear_peer_co)(Coroutine *coroutine, gpointer data);

    int (*_start_co)(Coroutine *coroutine, gpointer data);
    int (*_promote_co)(Coroutine *coroutine, gpointer data);
    int (*_start_migration_co)(Coroutine *coroutine, gpointer data);
    int (*_reboot_co)(Coroutine *coroutine, gpointer data);
    int (*_shutdown_co)(Coroutine *coroutine, gpointer data, MyTimeout *timeout);
    int (*_demote_co)(Coroutine *coroutine, gpointer data, MyTimeout *timeout);
    int (*_quit_co)(Coroutine *coroutine, gpointer data, MyTimeout *timeout);

    int (*_yank_co)(Coroutine *coroutine, gpointer data, GError **errp);

    ColodQmpResult *(*_execute_nocheck_co)(Coroutine *coroutine, gpointer data,
                                          GError **errp, const gchar *command);

    ColodQmpResult *(*_execute_co)(Coroutine *coroutine, gpointer data,
                                   GError **errp, const gchar *command);
};

void client_register(ColodClientListener *this, const ClientCallbacks *cb, gpointer data);
void client_unregister(ColodClientListener *this, const ClientCallbacks *cb, gpointer data);

void client_listener_free(ColodClientListener *listener);
ColodClientListener *client_listener_new(int socket, QmpCommands *commands);

#endif // CLIENT_H
