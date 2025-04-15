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

typedef struct ClientCallbacks ClientCallbacks;

struct ClientCallbacks {
    int (*_check_health_co)(Coroutine *coroutine, gpointer data, GError **errp);
    void (*query_status)(gpointer data, ColodState *ret);

    void (*set_peer)(gpointer data, const gchar *peer);
    const gchar *(*get_peer)(gpointer data);
    void (*clear_peer)(gpointer data);

    int (*start_migration)(gpointer data);
    void (*autoquit)(gpointer data);
    void (*quit)(gpointer data);
    void (*client_cont_failed)(gpointer data);

    int (*_yank_co)(Coroutine *coroutine, gpointer data, GError **errp);

    ColodQmpResult *(*_execute_nocheck_co)(Coroutine *coroutine, gpointer data,
                                          GError **errp, const gchar *command);

    ColodQmpResult *(*_execute_co)(Coroutine *coroutine, gpointer data,
                                   GError **errp, const gchar *command);
};

void client_register(ColodClientListener *this, const ClientCallbacks *cb, gpointer data);
void client_unregister(ColodClientListener *this, const ClientCallbacks *cb, gpointer data);

void client_listener_free(ColodClientListener *listener);
ColodClientListener *client_listener_new(int socket, const ColodContext *ctx);

#endif // CLIENT_H
