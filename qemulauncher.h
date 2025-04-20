/*
 * COLO background daemon native qemu launcher
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef QEMU_LAUNCHER_H
#define QEMU_LAUNCHER_H

#include "base_types.h"
#include "coroutine.h"

#define qemu_launcher_wait_co(...) \
    co_wrap(_qemu_launcher_wait_co(__VA_ARGS__))
int _qemu_launcher_wait_co(Coroutine *coroutine, QemuLauncher *this, guint timeout, GError **errp);

int qemu_launcher_kill(QemuLauncher *this);

#define qemu_launcher_launch_primary(...) \
    co_wrap(_qemu_launcher_launch_primary(__VA_ARGS__))
ColodQmpState *_qemu_launcher_launch_primary(Coroutine *coroutine, QemuLauncher *this, GError **errp);
#define qemu_launcher_launch_secondary(...) \
    co_wrap(_qemu_launcher_launch_secondary(__VA_ARGS__))
ColodQmpState *_qemu_launcher_launch_secondary(Coroutine *coroutine, QemuLauncher *this, GError **errp);
void qemu_launcher_set_disk_size(QemuLauncher *this, char *disk_size);

QemuLauncher *qemu_launcher_new(QmpCommands *commands, const char *base_dir, guint qmp_timeout);
QemuLauncher *qemu_launcher_ref(QemuLauncher *this);
void qemu_launcher_unref(QemuLauncher *this);

#endif
