/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef QMPCOMMANDS_H
#define QMPCOMMANDS_H

#include <glib-2.0/glib.h>
#include <json-glib-1.0/json-glib/json-glib.h>

#include "base_types.h"
#include "util.h"

int qmp_commands_set_qemu_primary(QmpCommands *this, JsonNode *commands, GError **errp);
int qmp_commands_set_qemu_secondary(QmpCommands *this, JsonNode *commands, GError **errp);
int qmp_commands_set_prepare_primary(QmpCommands *this, JsonNode *commands, GError **errp);
int qmp_commands_set_prepare_secondary(QmpCommands *this, JsonNode *commands, GError **errp);
int qmp_commands_set_migration_start(QmpCommands *this, JsonNode *commands, GError **errp);
int qmp_commands_set_migration_switchover(QmpCommands *this, JsonNode *commands, GError **errp);
int qmp_commands_set_failover_primary(QmpCommands *this, JsonNode *commands, GError **errp);
int qmp_commands_set_failover_secondary(QmpCommands *this, JsonNode *commands, GError **errp);

MyArray *qmp_commands_cmdline(QmpCommands *this, const char *address,
                              const char *disk_size, ...);
MyArray *qmp_commands_adhoc(QmpCommands *this, ...);
MyArray *qmp_commands_get_qemu_primary(QmpCommands *this);
MyArray *qmp_commands_get_qemu_secondary(QmpCommands *this);
MyArray *qmp_commands_get_qemu_dummy(QmpCommands *this);
MyArray *qmp_commands_get_prepare_primary(QmpCommands *this);
MyArray *qmp_commands_get_prepare_secondary(QmpCommands *this);
MyArray *qmp_commands_get_migration_start(QmpCommands *this, const char *address);
MyArray *qmp_commands_get_migration_switchover(QmpCommands *this);
MyArray *qmp_commands_get_failover_primary(QmpCommands *this);
MyArray *qmp_commands_get_failover_secondary(QmpCommands *this);

void qmp_commands_set_filter_rewriter(QmpCommands *this, gboolean filter_rewriter);
void qmp_commands_set_comp_prop(QmpCommands *this, JsonNode *prop);
void qmp_commands_set_mig_cap(QmpCommands *this, JsonNode *prop);
void qmp_commands_set_mig_prop(QmpCommands *this, JsonNode *prop);
void qmp_commands_set_throttle_prop(QmpCommands *this, JsonNode *prop);
void qmp_commands_set_blk_mirror_prop(QmpCommands *this, JsonNode *prop);
void qmp_commands_set_qemu_options(QmpCommands *this, JsonNode *prop);
int qmp_commands_set_qemu_options_str(QmpCommands *this, const char *_qemu_options, GError **errp);
void qmp_commands_set_yank_instances(QmpCommands *this, JsonNode *prop);
JsonNode *qmp_commands_get_yank_instances(QmpCommands *this);

int qmp_commands_read_config(QmpCommands *this, const char *config_str, const char *qemu_options, GError **errp);

QmpCommands *qmp_commands_new(const char *instance_name, const char *base_dir,
                              const char *active_hidden_dir,
                              const char *listen_address,
                              const char *qemu_binary,
                              const char *qemu_img_binary,
                              int base_port);
void qmp_commands_free(QmpCommands *this);

#endif // QMPCOMMANDS_H
