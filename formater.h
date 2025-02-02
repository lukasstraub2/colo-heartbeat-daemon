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

Formater *formater_new(const char *base_dir, const char *address,
                       const char *listen_address, const int base_port,
                       gboolean filter_rewriter,
                       JsonNode *comp_prop,
                       JsonNode *mig_cap, JsonNode *mig_prop,
                       JsonNode *throttle_prop, JsonNode *blk_mirror_prop);
void formater_free(Formater *this);
