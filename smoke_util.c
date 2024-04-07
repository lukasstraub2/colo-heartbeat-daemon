/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include "base_types.h"
#include "smoke_util.h"
#include "coutil.h"
#include "daemon.h"

void _ch_write_co(Coroutine *coroutine, GIOChannel *channel,
                  const gchar *buf, guint timeout) {
    GIOStatus ret;
    GError *local_errp = NULL;
    ret = _colod_channel_write_timeout_co(coroutine, channel, buf, strlen(buf),
                                          timeout, &local_errp);
    if (coroutine->yield) {
        return;
    }

    if (ret == G_IO_STATUS_ERROR) {
        log_error(local_errp->message);
        abort();
    } else if (ret != G_IO_STATUS_NORMAL) {
        colod_syslog(0, "channel write: EOF");
        abort();
    }

    return;
}

void _ch_readln_co(Coroutine *coroutine, GIOChannel *channel,
                   gchar **buf, gsize *len, guint timeout) {
    GIOStatus ret;
    GError *local_errp = NULL;

    ret = _colod_channel_read_line_timeout_co(coroutine, channel, buf, len,
                                              timeout, &local_errp);
    if (coroutine->yield) {
        return;
    }

    if (ret == G_IO_STATUS_ERROR) {
        log_error(local_errp->message);
        abort();
    } else if (ret != G_IO_STATUS_NORMAL) {
        colod_syslog(0, "channel read: EOF");
        abort();
    }

    return;
}
