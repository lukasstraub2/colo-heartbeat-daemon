#ifndef MAIN_COROUTINE_H
#define MAIN_COROUTINE_H

#include "daemon.h"

void colod_query_status(ColodMainCoroutine *this, ColodState *ret);

void colod_client_register(ColodMainCoroutine *this);
void colod_client_unregister(ColodMainCoroutine *this);

ColodMainCoroutine *colod_main_new(const ColodContext *ctx, GError **errp);
ColodMainCoroutine *colod_main_ref(ColodMainCoroutine *this);
void colod_main_unref(ColodMainCoroutine *this);

#endif // MAIN_COROUTINE_H
