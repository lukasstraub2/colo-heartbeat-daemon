#ifndef MAIN_COROUTINE_H
#define MAIN_COROUTINE_H

#include "daemon.h"

enum ColodEvent {
    EVENT_NONE = 0,
    EVENT_FAILED,
    EVENT_QEMU_QUIT,
    EVENT_PEER_FAILOVER,
    EVENT_FAILOVER_SYNC,
    EVENT_PEER_FAILED,
    EVENT_FAILOVER_WIN,
    EVENT_QUIT,
    EVENT_AUTOQUIT,
    EVENT_YELLOW,
    EVENT_START_MIGRATION,
    EVENT_DID_FAILOVER
};

#define colod_event_queue(ctx, event, reason) \
    _colod_event_queue((ctx), (event), (reason), __func__, __LINE__)
void _colod_event_queue(ColodContext *ctx, ColodEvent event,
                        const gchar *reason, const gchar *func,
                        int line);

void colod_raise_timeout_coroutine_free(ColodContext *ctx);
Coroutine *colod_raise_timeout_coroutine(ColodContext *ctx);

Coroutine *colod_main_coroutine(ColodContext *ctx);
void colod_main_free(ColodContext *ctx);

#endif // MAIN_COROUTINE_H
