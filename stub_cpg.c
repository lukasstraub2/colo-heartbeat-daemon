/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <glib-2.0/glib.h>

#include "cpg.h"
#include "daemon.h"

struct Cpg {
    int dummy;
};


void colod_cpg_send(G_GNUC_UNUSED Cpg *cpg, G_GNUC_UNUSED uint32_t message) {}

Cpg *colod_open_cpg(G_GNUC_UNUSED ColodContext *ctx, G_GNUC_UNUSED GError **errp) {
    return g_new0(Cpg, 1);
}

Cpg *cpg_new(Cpg *cpg, G_GNUC_UNUSED GError **errp) {
    return cpg;
}

void cpg_free(Cpg *cpg) {
    g_free(cpg);
}
