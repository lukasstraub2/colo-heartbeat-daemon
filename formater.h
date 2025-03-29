/*
 * COLO background daemon formater
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#pragma once

#include <glib-2.0/glib.h>
#include <json-glib-1.0/json-glib/json-glib.h>

#include "base_types.h"
#include "util.h"

MyArray *formater_format(Formater *this, const MyArray *entry);

char *formater_qmp_sock(const char *base_dir);
char *formater_qmp_yank_sock(const char *base_dir);

Formater *formater_new(const char *instance_name, const char *base_dir,
                       const char *active_hidden_dir, const char *address,
                       const char *listen_address, const char *qemu_binary,
                       const char *qemu_img_binary, const char *disk_size,
                       gboolean filter_rewriter, gboolean newline,
                       JsonNode *comp_prop,
                       JsonNode *mig_cap, JsonNode *mig_prop,
                       JsonNode *throttle_prop, JsonNode *blk_mirror_prop,
                       const int base_port);
void formater_free(Formater *this);
