#ifndef MAIN_COROUTINE_H
#define MAIN_COROUTINE_H

#include "daemon.h"

typedef enum MainReturn {
    MAIN_NONE,
    MAIN_DEMOTE,
    MAIN_PROMOTE,
    MAIN_QUIT,
} MainReturn;

#define colod_main_enter(...) co_wrap(_colod_main_enter(__VA_ARGS__))
MainReturn _colod_main_enter(Coroutine *coroutine, ColodMainCoroutine *this);

void colod_main_query_status(ColodMainCoroutine *this, ColodState *ret);

void colod_main_client_register(ColodMainCoroutine *this);
void colod_main_client_unregister(ColodMainCoroutine *this);

ColodMainCoroutine *colod_main_new(const ColodContext *ctx, QemuLauncher *launcher,
                                   ColodQmpState *qmp, gboolean primary, GError **errp);
ColodMainCoroutine *colod_main_ref(ColodMainCoroutine *this);
void colod_main_unref(ColodMainCoroutine *this);

#endif // MAIN_COROUTINE_H
