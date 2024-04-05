/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <glib-2.0/glib.h>
#include <glib-2.0/glib-unix.h>

#include <corosync/cpg.h>
#include <corosync/corotypes.h>

#include "cpg.h"
#include "daemon.h"
#include "main_coroutine.h"

struct Cpg {
    cpg_handle_t handle;
    guint source_id;
    ColodContext *ctx;
};

static void colod_cpg_deliver(cpg_handle_t handle,
                              G_GNUC_UNUSED const struct cpg_name *group_name,
                              uint32_t nodeid,
                              G_GNUC_UNUSED uint32_t pid,
                              void *msg,
                              size_t msg_len) {
    Cpg *cpg;
    uint32_t conv;
    uint32_t myid;

    cpg_context_get(handle, (void**) &cpg);
    cpg_local_get(handle, &myid);

    if (msg_len != sizeof(conv)) {
        log_error_fmt("Got message of invalid length %zu", msg_len);
        return;
    }
    conv = ntohl((*(uint32_t*)msg));

    switch (conv) {
        case MESSAGE_FAILOVER:
            if (nodeid == myid) {
                colod_event_queue(cpg->ctx->main_coroutine,
                                  EVENT_FAILOVER_WIN, "");
            } else {
                colod_event_queue(cpg->ctx->main_coroutine,
                                  EVENT_PEER_FAILOVER, "");
            }
        break;

        case MESSAGE_FAILED:
            if (nodeid != myid) {
                log_error("Peer failed");
                colod_peer_failed(cpg->ctx->main_coroutine);
                colod_event_queue(cpg->ctx->main_coroutine, EVENT_PEER_FAILED,
                                  "got MESSAGE_FAILED");
            }
        break;
    }
}

static void colod_cpg_confchg(cpg_handle_t handle,
    G_GNUC_UNUSED const struct cpg_name *group_name,
    G_GNUC_UNUSED const struct cpg_address *member_list,
    G_GNUC_UNUSED size_t member_list_entries,
    G_GNUC_UNUSED const struct cpg_address *left_list,
    size_t left_list_entries,
    G_GNUC_UNUSED const struct cpg_address *joined_list,
    G_GNUC_UNUSED size_t joined_list_entries) {
    Cpg *cpg;

    cpg_context_get(handle, (void**) &cpg);

    if (left_list_entries) {
        log_error("Peer failed");
        colod_peer_failed(cpg->ctx->main_coroutine);
        colod_event_queue(cpg->ctx->main_coroutine, EVENT_PEER_FAILED,
                          "peer left cpg group");
    }
}

static void colod_cpg_totem_confchg(G_GNUC_UNUSED cpg_handle_t handle,
                                    G_GNUC_UNUSED struct cpg_ring_id ring_id,
                                    G_GNUC_UNUSED uint32_t member_list_entries,
                                    G_GNUC_UNUSED const uint32_t *member_list) {

}

static gboolean colod_cpg_readable(G_GNUC_UNUSED gint fd,
                                   G_GNUC_UNUSED GIOCondition events,
                                   gpointer data) {
    Cpg *cpg = data;
    cpg_dispatch(cpg->handle, CS_DISPATCH_ALL);
    return G_SOURCE_CONTINUE;
}

void colod_cpg_send(Cpg *cpg, uint32_t message) {
    struct iovec vec;
    uint32_t conv = htonl(message);

    vec.iov_len = sizeof(conv);
    vec.iov_base = &conv;
    cpg_mcast_joined(cpg->handle, CPG_TYPE_AGREED, &vec, 1);
}

cpg_model_v1_data_t cpg_data = {
    CPG_MODEL_V1,
    colod_cpg_deliver,
    colod_cpg_confchg,
    colod_cpg_totem_confchg,
    0
};

Cpg *colod_open_cpg(ColodContext *ctx, GError **errp) {
    cs_error_t ret;
    struct cpg_name name;
    Cpg *cpg;

    if (strlen(ctx->instance_name) +1 >= sizeof(name.value)) {
        colod_error_set(errp, "Instance name too long");
        return NULL;
    }
    strcpy(name.value, ctx->instance_name);
    name.length = strlen(name.value);

    cpg = g_new0(Cpg, 1);
    cpg->ctx = ctx;

    ret = cpg_model_initialize(&cpg->handle, CPG_MODEL_V1,
                               (cpg_model_data_t*) &cpg_data, cpg);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to initialize cpg: %s", cs_strerror(ret));
        g_free(cpg);
        return NULL;
    }

    ret = cpg_join(cpg->handle, &name);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to join cpg group: %s", cs_strerror(ret));
        cpg_finalize(cpg->handle);
        g_free(cpg);
        return NULL;
    }

    return cpg;
}

Cpg *cpg_new(Cpg *cpg, GError **errp) {
    cs_error_t ret;
    int fd;

    ret = cpg_fd_get(cpg->handle, &fd);
    if (ret != CS_OK) {
        colod_error_set(errp, "Failed to get cpg file descriptor: %s",
                        cs_strerror(ret));
        return NULL;
    }

    cpg->source_id = g_unix_fd_add(fd, G_IO_IN | G_IO_HUP, colod_cpg_readable, cpg);
    return cpg;
}

void cpg_free(Cpg *cpg) {
    g_source_remove(cpg->source_id);
    g_free(cpg);
}
