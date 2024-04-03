#ifndef WATCHDOG_H
#define WATCHDOG_H

#include "daemon.h"
#include "coroutine_stack.h"

typedef struct ColodWatchdog {
    Coroutine coroutine;
    ColodContext *ctx;
    guint interval;
    guint timer_id;
    guint inhibit;
    gboolean quit;
} ColodWatchdog;

void colod_watchdog_refresh(ColodWatchdog *state);
void colod_watchdog_inc_inhibit(ColodWatchdog *state);
void colod_watchdog_dec_inhibit(ColodWatchdog *state);

void colo_watchdog_free(ColodWatchdog *state);
ColodWatchdog *colod_watchdog_new(ColodContext *ctx);

#endif // WATCHDOG_H
