/*
 * COLO background daemon
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <glib-2.0/glib.h>
#include <json-glib-1.0/json-glib/json-glib.h>
#include <assert.h>

#include "qmpcommands.h"
#include "util.h"
#include "formater.h"

struct QmpCommands {
    char *base_dir;
    char *listen_address;
    int base_port;
    gboolean filter_rewriter;
    JsonNode *comp_prop;
    JsonNode *mig_cap;
    JsonNode *mig_prop;
    JsonNode *throttle_prop;
    JsonNode *blk_mirror_prop;

    MyArray *prepare_primary, *prepare_secondary;
    MyArray *migration_start, *migration_switchover;
    MyArray *failover_primary, *failover_secondary;
};

static int qmp_commands_format_check(MyArray *new) {
    Formater *fmt = formater_new("", "", "", 9000, FALSE, NULL, NULL,
                                 NULL, NULL, NULL);
    MyArray *formated = formater_format(fmt, new);

    if (!formated) {
        formater_free(fmt);
        return -1;
    }
    my_array_unref(formated);
    formater_free(fmt);

    return 0;
}

static int qmp_commands_set_json(MyArray **entry, JsonNode *commands, GError **errp) {
    MyArray *new = my_array_new(g_free);
    JsonArray *array;
    int size;
    int ret = 0;

    assert(!errp || !*errp);
    assert(entry && *entry);

    if (!JSON_NODE_HOLDS_ARRAY(commands)) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_QMP,
                    "Expected array of strings");
        ret = -1;
        goto out;
    }

    array = json_node_get_array(commands);
    size = json_array_get_length(array);
    for (int i = 0; i < size; i++) {
        JsonNode *node = json_array_get_element(array, i);
        const gchar *str = json_node_get_string(node);
        if (!str) {
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_QMP,
                        "Expected array of strings");
            ret = -1;
            goto out;
        }

        my_array_append(new, g_strdup(str));
    }

    ret = qmp_commands_format_check(new);
    if (ret < 0) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_QMP, "Invalid format");
        ret = -1;
        goto out;
    }

    my_array_unref(*entry);
    *entry = my_array_ref(new);

out:
    my_array_unref(new);

    return ret;
}

static MyArray *qmp_commands_static(int dummy, ...) {
    va_list args;
    MyArray *array = my_array_new(g_free);

    va_start(args, dummy);
    while (TRUE) {
        const char *str = va_arg(args, const char *);
        if (!str) {
            break;
        }

        my_array_append(array, g_strdup(str));
    }
    va_end(args);

    int ret = qmp_commands_format_check(array);
    assert(ret == 0);

    return array;
}

int qmp_commands_set_prepare_primary(QmpCommands *this, JsonNode *commands,
                                     GError **errp) {
    return qmp_commands_set_json(&this->prepare_primary, commands, errp);
}

int qmp_commands_set_prepare_secondary(QmpCommands *this, JsonNode *commands,
                                       GError **errp) {
    return qmp_commands_set_json(&this->prepare_secondary, commands, errp);
}

int qmp_commands_set_migration_start(QmpCommands *this, JsonNode *commands,
                                     GError **errp) {
    return qmp_commands_set_json(&this->migration_start, commands, errp);
}

int qmp_commands_set_migration_switchover(QmpCommands *this, JsonNode *commands,
                                          GError **errp) {
    return qmp_commands_set_json(&this->migration_switchover, commands, errp);
}

int qmp_commands_set_failover_primary(QmpCommands *this, JsonNode *commands,
                                      GError **errp) {
    return qmp_commands_set_json(&this->failover_primary, commands, errp);
}

int qmp_commands_set_failover_secondary(QmpCommands *this, JsonNode *commands,
                                        GError **errp) {
    return qmp_commands_set_json(&this->failover_secondary, commands, errp);
}

static MyArray *qmp_commands_format(const QmpCommands *this,
                                    const MyArray *entry,
                                    const char *address) {
    Formater *fmt = formater_new(
                this->base_dir,
                address,
                this->listen_address,
                this->base_port,
                this->filter_rewriter,
                this->comp_prop,
                this->mig_cap,
                this->mig_prop,
                this->throttle_prop,
                this->blk_mirror_prop);

    MyArray *ret = formater_format(fmt, entry);

    formater_free(fmt);
    return ret;
}

MyArray *qmp_commands_adhoc(QmpCommands *this, ...) {
    va_list args;
    MyArray *array = my_array_new(g_free);

    va_start(args, this);
    while (TRUE) {
        const char *str = va_arg(args, const char *);
        if (!str) {
            break;
        }

        my_array_append(array, g_strdup(str));
    }
    va_end(args);

    MyArray *ret = qmp_commands_format(this, array, "");
    my_array_unref(array);
    return ret;
}

MyArray *qmp_commands_get_prepare_primary(QmpCommands *this) {
    return qmp_commands_format(this, this->prepare_primary, "");
}

MyArray *qmp_commands_get_prepare_secondary(QmpCommands *this) {
    return qmp_commands_format(this, this->prepare_secondary, "");
}

MyArray *qmp_commands_get_migration_start(QmpCommands *this,
                                          const char *address) {
    return qmp_commands_format(this, this->migration_start, address);
}

MyArray *qmp_commands_get_migration_switchover(QmpCommands *this) {
    return qmp_commands_format(this, this->migration_switchover, "");
}

MyArray *qmp_commands_get_failover_primary(QmpCommands *this) {
    return qmp_commands_format(this, this->failover_primary, "");
}

MyArray *qmp_commands_get_failover_secondary(QmpCommands *this) {
    return qmp_commands_format(this, this->failover_secondary, "");
}

static JsonNode *qmp_commands_set_prop(JsonNode *prop) {
    if (!prop) {
        return NULL;
    }

    assert(JSON_NODE_HOLDS_OBJECT(prop));
    return json_node_ref(prop);
}

static JsonNode *qmp_commands_set_array(JsonNode *prop) {
    if (!prop) {
        return NULL;
    }

    assert(JSON_NODE_HOLDS_ARRAY(prop));
    return json_node_ref(prop);
}

static void qmp_commands_node_unref(JsonNode *prop) {
    if (prop) {
        json_node_unref(prop);
    }
}

void qmp_commands_set_filter_rewriter(QmpCommands *this, gboolean filter_rewriter) {
    this->filter_rewriter = filter_rewriter;
}

void qmp_commands_set_comp_prop(QmpCommands *this, JsonNode *prop) {
    qmp_commands_node_unref(this->comp_prop);
    this->comp_prop = qmp_commands_set_prop(prop);
}

void qmp_commands_set_mig_cap(QmpCommands *this, JsonNode *prop) {
    qmp_commands_node_unref(this->mig_cap);
    this->mig_cap = qmp_commands_set_array(prop);
}

void qmp_commands_set_mig_prop(QmpCommands *this, JsonNode *prop) {
    qmp_commands_node_unref(this->mig_prop);
    this->mig_prop = qmp_commands_set_prop(prop);
}

void qmp_commands_set_throttle_prop(QmpCommands *this, JsonNode *prop) {
    qmp_commands_node_unref(this->throttle_prop);
    this->throttle_prop = qmp_commands_set_prop(prop);
}

void qmp_commands_set_blk_mirror_prop(QmpCommands *this, JsonNode *prop) {
    qmp_commands_node_unref(this->blk_mirror_prop);
    this->blk_mirror_prop = qmp_commands_set_prop(prop);
}

QmpCommands *qmp_commands_new(const char *base_dir, const char *listen_address,
                              int base_port) {
    QmpCommands *this = g_new0(QmpCommands, 1);

    this->base_dir = g_strdup(base_dir);
    this->listen_address = g_strdup(listen_address);
    this->base_port = base_port;

    this->prepare_primary = qmp_commands_static(0,
        "@@DECL_THROTTLE_PROP@@ {}",
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/throttle0', 'property': 'limits', 'value': @@THROTTLE_PROP@@}}",
        NULL);

    this->prepare_secondary = qmp_commands_static(0,
        "@@DECL_THROTTLE_PROP@@ {}",
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/throttle0', 'property': 'limits', 'value': @@THROTTLE_PROP@@}}",
        "{'execute': 'migrate-set-capabilities', 'arguments': {'capabilities': [{'capability': 'x-colo', 'state': True}]}}",
        "{'execute': 'migrate-set-capabilities', 'arguments': {'capabilities': @@MIG_CAP@@}}",
        "@@DECL_MIG_PROP@@ {}",
        "{'execute': 'migrate-set-parameters', 'arguments': @@MIG_PROP@@}",
        "{'execute': 'nbd-server-start', 'arguments': {'addr': {'type': 'inet', 'data': {'host': '@@LISTEN_ADDRESS@@', 'port': '@@NBD_PORT@@'}}}}",
        "{'execute': 'nbd-server-add', 'arguments': {'device': 'parent0', 'writable': True}}",
        "{'execute': 'migrate-incoming', 'arguments': {'uri': 'tcp:@@LISTEN_ADDRESS@@:@@MIGRATE_PORT@@'}}",
        NULL);

    this->migration_start = qmp_commands_static(0,
        "{'execute': 'migrate-set-capabilities', 'arguments': {'capabilities': [{'capability': 'x-colo', 'state': True}]}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_pri_in0..', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'unix', 'data': {'path': '@@COMP_PRI_SOCK@@'}}, 'server': True}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_pri_in0', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'unix', 'data': {'path': '@@COMP_PRI_SOCK@@'}}, 'server': False}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_out0..', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'unix', 'data': {'path': '@@COMP_OUT_SOCK@@'}}, 'server': True}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_out0', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'unix', 'data': {'path': '@@COMP_OUT_SOCK@@'}}, 'server': False}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'mirror0', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'inet', 'data': {'host': '@@ADDRESS@@', 'port': '@@MIRROR_PORT@@'}}, 'server': False, 'nodelay': True}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_sec_in0', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'inet', 'data': {'host': '@@ADDRESS@@', 'port': '@@COMPARE_IN_PORT@@'}}, 'server': False, 'nodelay': True}}}}",
        "@@IF_REWRITER@@ {'execute': 'object-add', 'arguments': {'qom-type': 'filter-mirror', 'id': 'mirror0', 'status': 'off', 'insert': 'before', 'position': 'id=rew0', 'netdev': 'hn0', 'queue': 'tx', 'outdev': 'mirror0'}}",
        "@@IF_REWRITER@@ {'execute': 'object-add', 'arguments': {'qom-type': 'filter-redirector', 'id': 'comp_out0', 'insert': 'before', 'position': 'id=rew0', 'netdev': 'hn0', 'queue': 'rx', 'indev': 'comp_out0..'}}",
        "@@IF_REWRITER@@ {'execute': 'object-add', 'arguments': {'qom-type': 'filter-redirector', 'id': 'comp_pri_in0', 'status': 'off', 'insert': 'before', 'position': 'id=rew0', 'netdev': 'hn0', 'queue': 'rx', 'outdev': 'comp_pri_in0..'}}",
        "@@IF_NOT_REWRITER@@ {'execute': 'object-add', 'arguments': {'qom-type': 'filter-mirror', 'id': 'mirror0', 'status': 'off', 'netdev': 'hn0', 'queue': 'tx', 'outdev': 'mirror0'}}",
        "@@IF_NOT_REWRITER@@ {'execute': 'object-add', 'arguments': {'qom-type': 'filter-redirector', 'id': 'comp_out0', 'netdev': 'hn0', 'queue': 'rx', 'indev': 'comp_out0..'}}",
        "@@IF_NOT_REWRITER@@ {'execute': 'object-add', 'arguments': {'qom-type': 'filter-redirector', 'id': 'comp_pri_in0', 'status': 'off', 'netdev': 'hn0', 'queue': 'rx', 'outdev': 'comp_pri_in0..'}}",
        "{'execute': 'object-add', 'arguments': {'qom-type': 'iothread', 'id': 'iothread1'}}",
        "@@DECL_COMP_PROP@@ {'qom-type': 'colo-compare', 'id': 'comp0', 'primary_in': 'comp_pri_in0', 'secondary_in': 'comp_sec_in0', 'outdev': 'comp_out0', 'iothread': 'iothread1'}",
        "{'execute': 'object-add', 'arguments': @@COMP_PROP@@}",
        "{'execute': 'migrate', 'arguments': {'uri': 'tcp:@@ADDRESS@@:@@MIGRATE_PORT@@'}}",
        NULL);

    this->migration_switchover = qmp_commands_static(0,
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/mirror0', 'property': 'status', 'value': 'on'}}"
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/comp_pri_in0', 'property': 'status', 'value': 'on'}}",
        NULL);

    this->failover_primary = qmp_commands_static(0,
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/mirror0', 'property': 'status', 'value': 'off'}}",
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/comp_pri_in0', 'property': 'status', 'value': 'off'}}",
        "{'execute': 'x-blockdev-change', 'arguments': {'parent': 'quorum0', 'child': 'children.1'}}",
        "{'execute': 'x-colo-lost-heartbeat'}",
        "{'execute': 'blockdev-del', 'arguments': {'node-name': 'nbd0'}}",
        "{'execute': 'object-del', 'arguments': {'id': 'mirror0'}}",
        "{'execute': 'object-del', 'arguments': {'id': 'comp_pri_in0'}}",
        "{'execute': 'object-del', 'arguments': {'id': 'comp_out0'}}",
        "{'execute': 'object-del', 'arguments': {'id': 'comp0'}}",
        "{'execute': 'object-del', 'arguments': {'id': 'iothread1'}}",
        "{'execute': 'chardev-remove', 'arguments': {'id': 'mirror0'}}",
        "{'execute': 'chardev-remove', 'arguments': {'id': 'comp_sec_in0'}}",
        "{'execute': 'chardev-remove', 'arguments': {'id': 'comp_pri_in0..'}}",
        "{'execute': 'chardev-remove', 'arguments': {'id': 'comp_pri_in0'}}",
        "{'execute': 'chardev-remove', 'arguments': {'id': 'comp_out0..'}}",
        "{'execute': 'chardev-remove', 'arguments': {'id': 'comp_out0'}}",
        NULL);

    this->failover_secondary = qmp_commands_static(0,
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/drop0', 'property': 'status', 'value': 'off'}}",
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/comp_sec_in0', 'property': 'status', 'value': 'off'}}",
        "{'execute': 'nbd-server-stop'}",
        "{'execute': 'x-colo-lost-heartbeat'}",
        "{'execute': 'object-del', 'arguments': {'id': 'mirror0'}}",
        "{'execute': 'object-del', 'arguments': {'id': 'drop0'}}",
        "{'execute': 'object-del', 'arguments': {'id': 'comp_sec_in0'}}",
        "{'execute': 'chardev-remove', 'arguments': {'id': 'mirror0'}}",
        "{'execute': 'chardev-remove', 'arguments': {'id': 'comp_sec_in0'}}",
        NULL);

    return this;
}

void qmp_commands_free(QmpCommands *this) {
    g_free(this->base_dir);
    g_free(this->listen_address);
    qmp_commands_node_unref(this->comp_prop);
    qmp_commands_node_unref(this->mig_cap);
    qmp_commands_node_unref(this->mig_prop);
    qmp_commands_node_unref(this->throttle_prop);
    qmp_commands_node_unref(this->blk_mirror_prop);

    my_array_unref(this->prepare_primary);
    my_array_unref(this->prepare_secondary);
    my_array_unref(this->migration_start);
    my_array_unref(this->migration_switchover);
    my_array_unref(this->failover_primary);
    my_array_unref(this->failover_secondary);

    g_free(this);
}
