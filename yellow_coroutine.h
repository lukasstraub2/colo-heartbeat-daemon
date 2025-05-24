/*
 * Utilities
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef YELLOW_COROUTINE_H
#define YELLOW_COROUTINE_H

#include "cpg.h"

typedef enum YellowStatus {
    STATUS_NONE,
    STATUS_QUIT,
    STATUS_YELLOW,
    STATUS_UNYELLOW,
} YellowStatus;

typedef struct YellowCoroutine YellowCoroutine;

typedef void (*YellowCallback)(gpointer data, YellowStatus event);

void yellow_add_notify(YellowCoroutine *this, YellowCallback _func,
                       gpointer user_data);
void yellow_del_notify(YellowCoroutine *this, YellowCallback _func,
                       gpointer user_data);

void yellow_shutdown(YellowCoroutine *this);
YellowCoroutine *yellow_coroutine_new(Cpg *cpg, const ColodContext *ctx,
                                      guint timeout1, guint timeout2,
                                      GError **errp);
void yellow_coroutine_free(YellowCoroutine *this);

#endif // YELLOW_COROUTINE_H
