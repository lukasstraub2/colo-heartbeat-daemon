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

struct QmpCommands {
    char *base_dir;
    char *listen_address;
    int base_port;
    gboolean filter_rewriter;

    MyArray *prepare_secondary;
    MyArray *migration_start, *migration_switchover;
    MyArray *failover_primary, *failover_secondary;
};

static int qmp_commands_set_json(MyArray **entry, JsonNode *commands, GError **errp) {
    JsonArray *array;
    int size;
    assert(!errp || !*errp);
    assert(entry && *entry);

    if (!JSON_NODE_HOLDS_ARRAY(commands)) {
        g_set_error(errp, COLOD_ERROR, COLOD_ERROR_QMP,
                    "Expected array of strings");
        return -1;
    }

    array = json_node_get_array(commands);
    size = json_array_get_length(array);
    for (int i = 0; i < size; i++) {
        JsonNode *node = json_array_get_element(array, i);
        const gchar *str = json_node_get_string(node);
        if (!str) {
            g_set_error(errp, COLOD_ERROR, COLOD_ERROR_QMP,
                        "Expected array of strings");
            return -1;
        }
    }

    my_array_unref(*entry);
    *entry = my_array_new(g_free);

    for (int i = 0; i < size; i++) {
        JsonNode *node = json_array_get_element(array, i);
        const gchar *str = json_node_get_string(node);
        assert(str);

        my_array_append(*entry, g_strdup_printf("%s\n", str));
    }

    return 0;
}

static MyArray *qmp_commands_static(int dummy, ...) {
    va_list args;
    MyArray *ret = my_array_new(g_free);

    va_start(args, dummy);
    while (TRUE) {
        const char *str = va_arg(args, const char *);
        if (!str) {
            break;
        }

        my_array_append(ret, g_strdup_printf("%s\n", str));
    }
    va_end(args);

    return ret;
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

static MyArray *qmp_commands_format(const MyArray *entry,
                                    const char *base_dir,
                                    const char *address,
                                    const char *listen_address,
                                    const int base_port,
                                    gboolean filter_rewriter) {
    MyArray *ret = my_array_new(g_free);
    char *comp_pri_sock = g_build_filename(base_dir, "comp-pri-in0.sock", NULL);
    char *comp_out_sock = g_build_filename(base_dir, "comp-out0.sock", NULL);
    char *nbd_port = g_strdup_printf("%i", base_port);
    char *migrate_port = g_strdup_printf("%i", base_port + 1);
    char *mirror_port = g_strdup_printf("%i", base_port + 2);
    char *compare_in_port = g_strdup_printf("%i", base_port + 3);

    for (int i = 0; i < entry->size; i++) {
        const char *str = entry->array[i];
        gboolean if_rewriter = !!g_strstr_len(str, -1, "@@IF_REWRITER@@");
        gboolean if_not_rewriter = !!g_strstr_len(str, -1, "@@IF_NOT_REWRITER@@");

        if (filter_rewriter) {
            if (if_not_rewriter) {
                continue;
            }
        } else {
            if (if_rewriter) {
                continue;
            }
        }

        GString *command = g_string_new(str);

        g_string_replace(command, "@@IF_REWRITER@@", "", 0);
        g_string_replace(command, "@@IF_NOT_REWRITER@@", "", 0);

        g_string_replace(command, "@@ADDRESS@@", address, 0);
        g_string_replace(command, "@@LISTEN_ADDRESS@@", listen_address, 0);
        g_string_replace(command, "@@COMP_PRI_SOCK@@", comp_pri_sock, 0);
        g_string_replace(command, "@@COMP_OUT_SOCK@@", comp_out_sock, 0);

        g_string_replace(command, "@@NBD_PORT@@", nbd_port, 0);
        g_string_replace(command, "@@MIGRATE_PORT@@", migrate_port, 0);
        g_string_replace(command, "@@MIRROR_PORT@@", mirror_port, 0);
        g_string_replace(command, "@@COMPARE_IN_PORT@@", compare_in_port, 0);

        my_array_append(ret, g_string_free(command, FALSE));
    }

    g_free(comp_pri_sock);
    g_free(comp_out_sock);
    g_free(nbd_port);
    g_free(migrate_port);
    g_free(mirror_port);
    g_free(compare_in_port);
    return ret;
}

MyArray *qmp_commands_get_prepare_secondary(QmpCommands *this) {
    return qmp_commands_format(this->prepare_secondary, this->base_dir, "",
                               this->listen_address, this->base_port,
                               this->filter_rewriter);
}

MyArray *qmp_commands_get_migration_start(QmpCommands *this,
                                          const char *address) {
    return qmp_commands_format(this->migration_start, this->base_dir, address,
                               this->listen_address, this->base_port,
                               this->filter_rewriter);
}

MyArray *qmp_commands_get_migration_switchover(QmpCommands *this) {
    return qmp_commands_format(this->migration_switchover, this->base_dir, "",
                               this->listen_address, this->base_port,
                               this->filter_rewriter);
}

MyArray *qmp_commands_get_failover_primary(QmpCommands *this) {
    return qmp_commands_format(this->failover_primary, this->base_dir, "",
                               this->listen_address, this->base_port,
                               this->filter_rewriter);
}

MyArray *qmp_commands_get_failover_secondary(QmpCommands *this) {
    return qmp_commands_format(this->failover_secondary, this->base_dir, "",
                               this->listen_address, this->base_port,
                               this->filter_rewriter);
}

QmpCommands *qmp_commands_new(const char *base_dir, const char *listen_address,
                              int base_port, gboolean filter_rewriter) {
    QmpCommands *this = g_new0(QmpCommands, 1);

    this->base_dir = g_strdup(base_dir);
    this->listen_address = g_strdup(listen_address);
    this->base_port = base_port;
    this->filter_rewriter = filter_rewriter;

    this->prepare_secondary = qmp_commands_static(0,
        "{'execute': 'migrate-set-capabilities', 'arguments': {'capabilities': [{'capability': 'x-colo', 'state': True}]}}",
        "{'execute': 'nbd-server-start', 'arguments': {'addr': {'type': 'inet', 'data': {'host': '@@LISTEN_ADDRESS@@', 'port': '@@NBD_PORT@@'}}}}",
        "{'execute': 'nbd-server-add', 'arguments': {'device': 'parent0', 'writable': True}}",
        "{'execute': 'migrate-incoming', 'arguments': {'uri': 'tcp:@@LISTEN_ADDRESS@@:@@MIGRATE_PORT@@'}}",
        NULL);
    assert(this->prepare_secondary);

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
        "{'execute': 'object-add', 'arguments': {'qom-type': 'colo-compare', 'id': 'comp0', 'primary_in': 'comp_pri_in0', 'secondary_in': 'comp_sec_in0', 'outdev': 'comp_out0', 'iothread': 'iothread1'}}",
        "{'execute': 'migrate', 'arguments': {'uri': 'tcp:@@ADDRESS@@:@@MIGRATE_PORT@@'}}",
        NULL);
    assert(this->migration_start);

    this->migration_switchover = qmp_commands_static(0,
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/mirror0', 'property': 'status', 'value': 'on'}}"
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/comp_pri_in0', 'property': 'status', 'value': 'on'}}",
        NULL);
    assert(this->migration_switchover);

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
    assert(this->failover_primary);

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
    assert(this->failover_secondary);

    return this;
}

void qmp_commands_free(QmpCommands *this) {
    g_free(this->base_dir);
    g_free(this->listen_address);

    my_array_unref(this->prepare_secondary);
    my_array_unref(this->migration_start);
    my_array_unref(this->migration_switchover);
    my_array_unref(this->failover_primary);
    my_array_unref(this->failover_secondary);

    g_free(this);
}
