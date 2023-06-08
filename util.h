/*
 * Utilities
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef UTIL_H
#define UTIL_H

#include <stdint.h>
#include <glib-2.0/glib.h>

typedef enum ColodError {
    COLOD_ERROR_FATAL,
} ColodError;

#define COLOD_ERROR (colod_error_quark())

#define colod_error_set(errp, fmt, ...) \
    g_set_error(errp, COLOD_ERROR, COLOD_ERROR_FATAL, \
                fmt, ##__VA_ARGS__);

GQuark colod_error_quark();

size_t colod_write_full(int fd, const uint8_t *buf, size_t count);
size_t colod_read_full(int fd, uint8_t *buf, size_t count);
gboolean colod_write_pidfile(const char *path, GError **errp);
int os_daemonize(void);
int os_daemonize_post_init(int pipe, GError **errp);

int colod_fd_set_blocking(int fd, gboolean blocking, GError **errp);
GSource *fd_source_create(int fd, GIOCondition events);
gint progress_source_add(GSourceFunc func, gpointer data);

#endif // UTIL_H
