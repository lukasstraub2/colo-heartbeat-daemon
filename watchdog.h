#ifndef WATCHDOG_H
#define WATCHDOG_H

#include "daemon.h"
#include "coroutine_stack.h"

typedef int (*WatchdogCheckHealth)(Coroutine *coroutine, gpointer data, GError **errp);

void colod_watchdog_refresh(ColodWatchdog *state);
void colod_watchdog_free(ColodWatchdog *state);
ColodWatchdog *colod_watchdog_new(const ColodContext *ctx,
                                  WatchdogCheckHealth cb, gpointer data);

#endif // WATCHDOG_H
