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
    JsonNode *comp_prop;
    JsonNode *mig_cap;
    JsonNode *mig_prop;
    JsonNode *throttle_prop;
    JsonNode *blk_mirror_prop;

    MyArray *prepare_secondary;
    MyArray *migration_start, *migration_switchover;
    MyArray *failover_primary, *failover_secondary;
};

typedef struct Formater {
    const char *base_dir;
    const char *address;
    const char *listen_address;
    gboolean filter_rewriter;
    gboolean newline;
    JsonNode *comp_prop;
    JsonNode *mig_prop;
    JsonNode *throttle_prop;
    JsonNode *blk_mirror_prop;

    char *decl_comp_prop;
    char *decl_mig_cap;
    char *decl_mig_prop;
    char *decl_throttle_prop;
    char *decl_blk_mirror_prop;

    char *comp_pri_sock;
    char *comp_out_sock;
    char *nbd_port;
    char *migrate_port;
    char *mirror_port;
    char *compare_in_port;
} Formater;

static JsonNode *formater_set_prop(JsonNode *prop) {
    if (!prop) {
        return json_from_string("{}", NULL);
    }

    assert(JSON_NODE_HOLDS_OBJECT(prop));
    return json_node_ref(prop);
}

static Formater *formater_new(const char *base_dir, const char *address,
                              const char *listen_address, const int base_port,
                              gboolean filter_rewriter,
                              JsonNode *comp_prop,
                              JsonNode *mig_cap, JsonNode *mig_prop,
                              JsonNode *throttle_prop, JsonNode *blk_mirror_prop) {
    Formater *this = g_new0(Formater, 1);

    this->base_dir = base_dir;
    this->address = address;
    this->listen_address = listen_address;
    this->filter_rewriter = filter_rewriter;
    this->newline = TRUE;
    this->comp_prop = formater_set_prop(comp_prop);
    if (mig_cap) {
        this->decl_mig_cap = json_to_string(mig_cap, FALSE);
    } else {
        this->decl_mig_cap = g_strdup("[]");
    }
    this->mig_prop = formater_set_prop(mig_prop);
    this->throttle_prop = formater_set_prop(throttle_prop);
    this->blk_mirror_prop = formater_set_prop(blk_mirror_prop);

    this->comp_pri_sock = g_build_filename(this->base_dir, "comp-pri-in0.sock", NULL);
    this->comp_out_sock = g_build_filename(this->base_dir, "comp-out0.sock", NULL);
    this->nbd_port = g_strdup_printf("%i", base_port);
    this->migrate_port = g_strdup_printf("%i", base_port + 1);
    this->mirror_port = g_strdup_printf("%i", base_port + 2);
    this->compare_in_port = g_strdup_printf("%i", base_port + 3);

    return this;
}

static void formater_free(Formater *this) {
    json_node_unref(this->comp_prop);
    json_node_unref(this->mig_prop);
    json_node_unref(this->throttle_prop);
    json_node_unref(this->blk_mirror_prop);

    g_free(this->decl_comp_prop);
    g_free(this->decl_mig_cap);
    g_free(this->decl_mig_prop);
    g_free(this->decl_throttle_prop);
    g_free(this->decl_blk_mirror_prop);

    g_free(this->comp_pri_sock);
    g_free(this->comp_out_sock);
    g_free(this->nbd_port);
    g_free(this->migrate_port);
    g_free(this->mirror_port);
    g_free(this->compare_in_port);

    g_free(this);
}

static MyArray *formater_format(Formater *this, const MyArray *entry);

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

        my_array_append(new, g_strdup_printf("%s\n", str));
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

        my_array_append(array, g_strdup_printf("%s\n", str));
    }
    va_end(args);

    int ret = qmp_commands_format_check(array);
    assert(ret == 0);

    return array;
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

static const char *decl_fmts[] = {
    "@@DECL_COMP_PROP@@",
    "@@DECL_MIG_PROP@@",
    "@@DECL_THROTTLE_PROP@@",
    "@@DECL_BLK_MIRROR_PROP@@"
};

static const char *prop_fmts[] = {
    "@@COMP_PROP@@",
    "@@MIG_PROP@@",
    "@@THROTTLE_PROP@@",
    "@@BLK_MIRROR_PROP@@"
};

static char **props(Formater *this, int i) {
    char **props[] = {
        &this->decl_comp_prop,
        &this->decl_mig_prop,
        &this->decl_throttle_prop,
        &this->decl_blk_mirror_prop
    };

    return props[i];
};

static gboolean formater_is_decl(const char *str) {
    for (int i = 0; i < (int) (sizeof(decl_fmts)/sizeof(decl_fmts[0])); i++) {
        const char *decl = decl_fmts[i];

        if (strstr(str, decl)) {
            return TRUE;
        }
    }

    return FALSE;
}

static void formater_update(JsonObject* object G_GNUC_UNUSED,
                     const gchar* member_name,
                     JsonNode* member_node, gpointer user_data) {
    JsonObject *to = user_data;

    json_object_set_member(to, member_name, json_node_ref(member_node));
};

static int formater_handle_decl(Formater *this, const char *_str) {
    JsonNode *froms[] = {
        this->comp_prop,
        this->mig_prop,
        this->throttle_prop,
        this->blk_mirror_prop
    };

    for (int i = 0; i < (int) (sizeof(decl_fmts)/sizeof(decl_fmts[0])); i++) {
        const char *decl_fmt = decl_fmts[i];
        char **prop = props(this, i);
        JsonObject *from = json_node_get_object(froms[i]);

        if (!strstr(_str, decl_fmt)) {
            continue;
        }

        if (*prop) {
            return -1;
        }

        GString *str = g_string_new(_str);

        g_string_replace(str, decl_fmt, "", 0);

        if (strstr(str->str, "@@")) {
            g_string_free(str, TRUE);
            return -1;
        }

        JsonNode *json = json_from_string(str->str, NULL);
        if (!json) {
            g_string_free(str, TRUE);
            return -1;
        }

        if (!JSON_NODE_HOLDS_OBJECT(json)) {
            json_node_unref(json);
            g_string_free(str, TRUE);
            return -1;
        }

        JsonObject *to = json_node_get_object(json);
        json_object_foreach_member(from, formater_update, to);

        *prop = json_to_string(json, FALSE);
        json_node_unref(json);

        g_string_free(str, TRUE);
        break;
    }

    return 0;
}

static int formater_replace_props(Formater *this, GString *command) {
    for (int i = 0; i < (int) (sizeof(prop_fmts)/sizeof(prop_fmts[0])); i++) {
           const char *prop_fmt = prop_fmts[i];
           char **prop = props(this, i);

           guint num = g_string_replace(command, prop_fmt,
                                        (*prop ? *prop: "{}"), 0);
           if (num && !*prop) {
               return -1;
           }
    }

    return 0;
}

static int formater_format_one(Formater *this, MyArray *out, const char *str) {
    gboolean if_rewriter = !!strstr(str, "@@IF_REWRITER@@");
    gboolean if_not_rewriter = !!strstr(str, "@@IF_NOT_REWRITER@@");

    if (formater_is_decl(str)) {
        return formater_handle_decl(this, str);
    }

    if (this->filter_rewriter) {
        if (if_not_rewriter) {
            return 0;
        }
    } else {
        if (if_rewriter) {
            return 0;
        }
    }

    GString *command = g_string_new(str);

    g_string_replace(command, "@@IF_REWRITER@@", "", 0);
    g_string_replace(command, "@@IF_NOT_REWRITER@@", "", 0);

    g_string_replace(command, "@@ADDRESS@@", this->address, 0);
    g_string_replace(command, "@@LISTEN_ADDRESS@@", this->listen_address, 0);
    g_string_replace(command, "@@COMP_PRI_SOCK@@", this->comp_pri_sock, 0);
    g_string_replace(command, "@@COMP_OUT_SOCK@@", this->comp_out_sock, 0);

    g_string_replace(command, "@@NBD_PORT@@", this->nbd_port, 0);
    g_string_replace(command, "@@MIGRATE_PORT@@", this->migrate_port, 0);
    g_string_replace(command, "@@MIRROR_PORT@@", this->mirror_port, 0);
    g_string_replace(command, "@@COMPARE_IN_PORT@@", this->compare_in_port, 0);

    g_string_replace(command, "@@MIG_CAP@@", this->decl_mig_cap, 0);

    int ret = formater_replace_props(this, command);
    if (ret < 0) {
        g_string_free(command, TRUE);
        return -1;
    }

    if (strstr(command->str, "@@")) {
        g_string_free(command, TRUE);
        return -1;
    }

    my_array_append(out, g_string_free(command, FALSE));

    return 0;
}

static MyArray *formater_format(Formater *this, const MyArray *entry) {
    MyArray *array = my_array_new(g_free);

    for (int i = 0; i < entry->size; i++) {
        int ret = formater_format_one(this, array, entry->array[i]);
        if (ret < 0) {
            my_array_unref(array);
            return NULL;
        }
    }

    return array;
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

    this->prepare_secondary = qmp_commands_static(0,
        "{'execute': 'migrate-set-capabilities', 'arguments': {'capabilities': [{'capability': 'x-colo', 'state': True}]}}",
        "{'execute': 'migrate-set-capabilities', 'arguments': {'capabilities': @@MIG_CAP@@}}",
        "@@DECL_MIG_PROP@@ {}",
        "{'execute': 'migrate-set-parameters', 'arguments': @@MIG_PROP@@}",
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
        "@@DECL_COMP_PROP@@ {'qom-type': 'colo-compare', 'id': 'comp0', 'primary_in': 'comp_pri_in0', 'secondary_in': 'comp_sec_in0', 'outdev': 'comp_out0', 'iothread': 'iothread1'}",
        "{'execute': 'object-add', 'arguments': @@COMP_PROP@@}",
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
    qmp_commands_node_unref(this->comp_prop);
    qmp_commands_node_unref(this->mig_cap);
    qmp_commands_node_unref(this->mig_prop);
    qmp_commands_node_unref(this->throttle_prop);
    qmp_commands_node_unref(this->blk_mirror_prop);

    my_array_unref(this->prepare_secondary);
    my_array_unref(this->migration_start);
    my_array_unref(this->migration_switchover);
    my_array_unref(this->failover_primary);
    my_array_unref(this->failover_secondary);

    g_free(this);
}
