/*
 * COLO background daemon coroutine utils
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <signal.h>
#include <glib-2.0/glib.h>

#include "coutil.h"
#include "util.h"
#include "daemon.h"

#include "coroutine_stack.h"

GSource *get_source_by_id(guint id) {
    return g_main_context_find_source_by_id(g_main_context_default(), id);
}

struct WaitSourceTmp {
    Coroutine *coroutine;
    guint timeout_source_id, wait_source_id;
    GSource *wait_source;
    GPid pid;
    int status;
};

static void colod_execute_wait_cb(GPid pid, gint status, gpointer data) {
    struct WaitSourceTmp *tmp = data;

    assert(pid == tmp->pid);
    tmp->status = status;
    tmp->coroutine->cb(tmp->coroutine);
    g_spawn_close_pid(pid);
}

int _colod_wait_co(Coroutine *coroutine, GPid pid, guint timeout, GError **errp) {
    struct WaitSourceTmp *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO coroutine = coroutine;
    CO pid = pid;

    if (timeout) {
        CO timeout_source_id = g_timeout_add(timeout, coroutine->cb, coroutine);
        g_source_set_name_by_id(CO timeout_source_id, "child wait timeout");
    }

    CO wait_source_id = g_child_watch_add(CO pid, colod_execute_wait_cb, co);
    CO wait_source = get_source_by_id(CO wait_source_id);
    g_source_set_name_by_id(CO wait_source_id, "child wait watch");

    while (TRUE) {
        co_yield_int(G_SOURCE_REMOVE);

        guint source_id = g_source_get_id(g_main_current_source());
        if (timeout && source_id == CO timeout_source_id) {
            g_source_set_callback(CO wait_source, G_SOURCE_FUNC(coroutine_wait_cb),
                                  coroutine, NULL);

            ret = kill(CO pid, SIGKILL);
            if (ret < 0) {
                log_error("kill() failed for timed out child wait");
            }

            while (TRUE) {
                co_yield_int(G_SOURCE_REMOVE);

                if (g_main_current_source() == CO wait_source) {
                    break;
                }
            }

            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT,
                        "child wait timed out");
            return -1;
        } else if (source_id != CO wait_source_id) {
            colod_trace("%s:%u: Got woken by unknown source\n",
                        __func__, __LINE__);
            continue;
        }

        break;
    }

    if (timeout) {
        g_source_remove(CO timeout_source_id);
    }

    return co->status;

    co_end;
}

int _colod_execute_sync_timeout_co(Coroutine *coroutine, MyArray *argv,
                                   guint timeout, GError **errp) {
    struct {
        GPid pid;
    }*co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    ret = g_spawn_async(NULL, (char **)argv->array, NULL,
                        G_SPAWN_SEARCH_PATH | G_SPAWN_DO_NOT_REAP_CHILD,
                        NULL, NULL, &CO pid, errp);
    my_array_unref(argv);
    if (!ret) {
        return -1;
    }

    co_recurse(ret = colod_wait_co(coroutine, CO pid, timeout, errp));
    return ret;

    co_end;
}

int _colod_execute_sync_co(Coroutine *coroutine, MyArray *argv, GError **errp) {
    return _colod_execute_sync_timeout_co(coroutine, argv, 0, errp);
}

int _colod_channel_read_line_timeout_co(Coroutine *coroutine,
                                        GIOChannel *channel,
                                        gchar **line,
                                        gsize *len,
                                        guint timeout,
                                        GError **errp) {
    struct {
        guint timeout_source_id, io_source_id;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(int, 0);

    if (timeout) {
        CO timeout_source_id = g_timeout_add(timeout, coroutine->cb,
                                             coroutine);
        g_source_set_name_by_id(CO timeout_source_id, "channel read timeout");
    }

    while (TRUE) {
        GIOStatus ret = g_io_channel_read_line(channel, line, len, NULL, errp);

        if ((ret == G_IO_STATUS_NORMAL && *len == 0) ||
                ret == G_IO_STATUS_AGAIN) {
            CO io_source_id = g_io_add_watch(channel,
                                             G_IO_IN | G_IO_HUP,
                                             coroutine_giofunc_cb,
                                             coroutine);
            g_source_set_name_by_id(CO io_source_id, "channel read io watch");
            co_yield_int(G_SOURCE_REMOVE);

            guint source_id = g_source_get_id(g_main_current_source());
            if (timeout && source_id == CO timeout_source_id) {
                g_source_remove(CO io_source_id);
                g_set_error(errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT,
                            "Channel read timed out");
                goto err;
            } else if (source_id != CO io_source_id) {
                g_source_remove(CO io_source_id);
                colod_trace("%s:%u: Got woken by unknown source\n",
                            __func__, __LINE__);
            }
        } else if (ret == G_IO_STATUS_NORMAL) {
            break;
        } else if (ret == G_IO_STATUS_ERROR) {
            goto err;
        } else if (ret == G_IO_STATUS_EOF) {
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_EOF, "Channel got EOF");
            goto err;
        }
    }

    if (timeout) {
        g_source_remove(CO timeout_source_id);
    }
    co_end;

    return 0;

err:
    if (timeout) {
        g_source_remove(CO timeout_source_id);
    }

    return -1;
}

int _colod_channel_read_line_co(Coroutine *coroutine,
                                GIOChannel *channel, gchar **line,
                                gsize *len, GError **errp) {
    return _colod_channel_read_line_timeout_co(coroutine, channel, line,
                                               len, 0, errp);
}

int _colod_channel_write_timeout_co(Coroutine *coroutine,
                                    GIOChannel *channel,
                                    const gchar *buf,
                                    gsize len,
                                    guint timeout,
                                    GError **errp) {
    struct {
        guint timeout_source_id, io_source_id;
        gsize offset;
    } *co;
    gsize write_len;

    co_frame(co, sizeof(*co));
    co_begin(int, 0);

    if (timeout) {
        CO timeout_source_id = g_timeout_add(timeout, coroutine->cb,
                                             coroutine);
        g_source_set_name_by_id(CO timeout_source_id, "channel write timeout");
    }

    CO offset = 0;
    while (CO offset < len) {
        GIOStatus ret = g_io_channel_write_chars(channel,
                                                 buf + CO offset,
                                                 len - CO offset,
                                                 &write_len, errp);
        CO offset += write_len;

        if (ret == G_IO_STATUS_NORMAL || ret == G_IO_STATUS_AGAIN) {
            if (write_len == 0) {
                CO io_source_id = g_io_add_watch(channel, G_IO_OUT | G_IO_HUP,
                                                 coroutine_giofunc_cb,
                                                 coroutine);
                g_source_set_name_by_id(CO io_source_id, "channel write io watch");
                co_yield_int(G_SOURCE_REMOVE);

                guint source_id = g_source_get_id(g_main_current_source());
                if (timeout && source_id == CO timeout_source_id) {
                    g_source_remove(CO io_source_id);
                    g_set_error(errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT,
                                "Channel write timed out");
                    goto err;
                } else if (source_id != CO io_source_id) {
                    g_source_remove(CO io_source_id);
                    colod_trace("%s:%u: Got woken by unknown source\n",
                                __func__, __LINE__);
                }
            }
        } else if (ret == G_IO_STATUS_ERROR) {
            goto err;
        } else if (ret == G_IO_STATUS_EOF) {
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_EOF, "Channel got EOF");
            goto err;
        }
    }

    while (TRUE) {
        GIOStatus ret = g_io_channel_flush(channel, errp);

        if (ret == G_IO_STATUS_AGAIN) {
            CO io_source_id = g_io_add_watch(channel, G_IO_OUT | G_IO_HUP,
                                             coroutine_giofunc_cb,
                                             coroutine);
            g_source_set_name_by_id(CO io_source_id, "channel flush io watch");
            co_yield_int(G_SOURCE_REMOVE);

            guint source_id = g_source_get_id(g_main_current_source());
            if (timeout && source_id == CO timeout_source_id) {
                g_source_remove(CO io_source_id);
                g_set_error(errp, COLOD_ERROR, COLOD_ERROR_TIMEOUT,
                            "Channel write timed out");
                goto err;
            } else if (source_id != CO io_source_id) {
                g_source_remove(CO io_source_id);
                colod_trace("%s:%u: Got woken by unknown source\n",
                            __func__, __LINE__);
            }
        } else if (ret == G_IO_STATUS_NORMAL) {
            break;
        } else if (ret == G_IO_STATUS_ERROR) {
            goto err;
        } else if (ret == G_IO_STATUS_EOF) {
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_EOF, "Channel got EOF");
            goto err;
        }
    }

    if (timeout) {
        g_source_remove(CO timeout_source_id);
    }

    co_end;

    return 0;

err:
    if (timeout) {
        g_source_remove(CO timeout_source_id);
    }

    return -1;
}

int _colod_channel_write_co(Coroutine *coroutine,
                            GIOChannel *channel, const gchar *buf,
                            gsize len, GError **errp) {
    return _colod_channel_write_timeout_co(coroutine, channel, buf, len, 0,
                                           errp);
}
