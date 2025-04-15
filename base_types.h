/*
 * COLO background daemon base types
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef BASE_TYPES_H
#define BASE_TYPES_H

#include <glib-2.0/glib.h>

struct ColodState {
    gboolean running;
    gboolean primary;
    gboolean replication, failed, peer_failover, peer_failed;
};

typedef struct ColodState ColodState;
typedef struct Formater Formater;
typedef struct QmpCommands QmpCommands;
typedef struct ColodMainCoroutine ColodMainCoroutine;
typedef struct ColodClientListener ColodClientListener;
typedef struct ColodQmpState ColodQmpState;
typedef struct ColodWatchdog ColodWatchdog;
typedef struct Cpg Cpg;

#endif // BASE_TYPES_H
