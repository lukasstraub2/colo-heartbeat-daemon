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
    char *instance_name;
    char *base_dir;
    char *active_hidden_dir;
    char *listen_address;
    char *qemu_binary;
    char *qemu_img_binary;
    int base_port;
    gboolean filter_rewriter;
    JsonNode *comp_prop;
    JsonNode *mig_cap;
    JsonNode *mig_prop;
    JsonNode *throttle_prop;
    JsonNode *blk_mirror_prop;
    JsonNode *qemu_options;
    JsonNode *yank_instances;

    MyArray *qemu_primary, *qemu_secondary;
    MyArray *qemu_dummy;

    MyArray *prepare_primary, *prepare_secondary;
    MyArray *migration_start, *migration_switchover;
    MyArray *failover_primary, *failover_secondary;
};

static int qmp_commands_format_check(MyArray *new) {
    Formater *fmt = formater_new(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                 FALSE, TRUE, NULL, NULL, NULL, NULL, NULL, NULL,
                                 9000);
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

int qmp_commands_set_qemu_primary(QmpCommands *this, JsonNode *commands,
                                  GError **errp) {
    return qmp_commands_set_json(&this->qemu_primary, commands, errp);
}

int qmp_commands_set_qemu_secondary(QmpCommands *this, JsonNode *commands,
                                    GError **errp) {
    return qmp_commands_set_json(&this->qemu_secondary, commands, errp);
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

static MyArray *_qmp_commands_format(const QmpCommands *this,
                                     gboolean newline,
                                     const MyArray *entry,
                                     const char *address,
                                     const char *disk_size) {
    Formater *fmt = formater_new(
                this->instance_name,
                this->base_dir,
                this->active_hidden_dir,
                address,
                this->listen_address,
                this->qemu_binary,
                this->qemu_img_binary,
                disk_size,
                this->filter_rewriter,
                newline,
                this->comp_prop,
                this->mig_cap,
                this->mig_prop,
                this->throttle_prop,
                this->blk_mirror_prop,
                this->qemu_options,
                this->base_port);

    MyArray *ret = formater_format(fmt, entry);

    formater_free(fmt);
    return ret;
}

static MyArray *qmp_commands_format(const QmpCommands *this,
                                    const MyArray *entry,
                                    const char *address,
                                    const char *disk_size) {
    return _qmp_commands_format(this, TRUE, entry, address, disk_size);
}

static MyArray *qmp_commands_format_cmdline(const QmpCommands *this,
                                            const MyArray *entry,
                                            const char *address,
                                            const char *disk_size) {
    MyArray *ret = _qmp_commands_format(this, FALSE, entry, address, disk_size);
    if (!ret) {
        return NULL;
    }

    my_array_append(ret, NULL);
    return ret;
}

MyArray *qmp_commands_cmdline(QmpCommands *this, const char *address,
                              const char *disk_size, ...) {
    va_list args;
    MyArray *array = my_array_new(g_free);

    va_start(args, disk_size);
    while (TRUE) {
        const char *str = va_arg(args, const char *);
        if (!str) {
            break;
        }

        my_array_append(array, g_strdup(str));
    }
    va_end(args);

    MyArray *ret = qmp_commands_format_cmdline(this, array, address, disk_size);
    my_array_unref(array);

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

    MyArray *ret = qmp_commands_format(this, array, NULL, NULL);
    my_array_unref(array);
    return ret;
}

MyArray *qmp_commands_get_qemu_primary(QmpCommands *this) {
    return qmp_commands_format_cmdline(this, this->qemu_primary, NULL, NULL);
}

MyArray *qmp_commands_get_qemu_secondary(QmpCommands *this) {
    return qmp_commands_format_cmdline(this, this->qemu_secondary, NULL, NULL);
}

MyArray *qmp_commands_get_qemu_dummy(QmpCommands *this) {
    return qmp_commands_format_cmdline(this, this->qemu_dummy, NULL, NULL);
}

MyArray *qmp_commands_get_prepare_primary(QmpCommands *this) {
    return qmp_commands_format(this, this->prepare_primary, NULL, NULL);
}

MyArray *qmp_commands_get_prepare_secondary(QmpCommands *this) {
    return qmp_commands_format(this, this->prepare_secondary, NULL, NULL);
}

MyArray *qmp_commands_get_migration_start(QmpCommands *this,
                                          const char *address) {
    return qmp_commands_format(this, this->migration_start, address, NULL);
}

MyArray *qmp_commands_get_migration_switchover(QmpCommands *this) {
    return qmp_commands_format(this, this->migration_switchover, NULL, NULL);
}

MyArray *qmp_commands_get_failover_primary(QmpCommands *this) {
    return qmp_commands_format(this, this->failover_primary, NULL, NULL);
}

MyArray *qmp_commands_get_failover_secondary(QmpCommands *this) {
    return qmp_commands_format(this, this->failover_secondary, NULL, NULL);
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

void qmp_commands_set_qemu_options(QmpCommands *this, JsonNode *prop) {
    qmp_commands_node_unref(this->qemu_options);
    this->qemu_options = qmp_commands_set_array(prop);
}

void qmp_commands_set_yank_instances(QmpCommands *this, JsonNode *prop) {
    qmp_commands_node_unref(this->yank_instances);
    this->yank_instances = qmp_commands_set_array(prop);
}

JsonNode *qmp_commands_get_yank_instances(QmpCommands *this) {
    return json_node_ref(this->yank_instances);
}

static void _json_object_update(JsonObject* object G_GNUC_UNUSED,
                                const gchar* member_name,
                                JsonNode* member_node, gpointer user_data) {
    JsonObject *to = user_data;

    json_object_set_member(to, member_name, json_node_ref(member_node));
};

static void json_object_update(JsonObject *to, JsonObject *from) {
    json_object_foreach_member(from, _json_object_update, to);
}

static JsonNode* _parse_config(const char* config_str, GError** errp) {
    JsonObject* object = NULL;

    JsonNode* node = json_from_string(config_str, errp);
    if (!node) {
        return NULL;
    }

    if (!JSON_NODE_HOLDS_OBJECT(node)) {
        json_node_unref(node);
        colod_error_set(errp, "not an object")
        return NULL;
    }

    object = json_node_get_object(node);
    if (json_object_has_member(object, "include")) {
        JsonNode* include_node = json_object_get_member(object, "include");

        if (!JSON_NODE_HOLDS_VALUE(include_node) || json_node_get_value_type(include_node) != G_TYPE_STRING) {
            json_node_unref(node);
            colod_error_set(errp, "invalid include member")
            return NULL;
        }

        const char* include_path = json_node_get_string(include_node);
        gchar* include_str_content = NULL;

        int ret = g_file_get_contents(include_path, &include_str_content, NULL, errp);
        if (!ret) {
            json_node_unref(node);
            return NULL;
        }

        JsonNode* included_node = _parse_config(include_str_content, errp);
        g_free(include_str_content);
        if (!included_node) {
            json_node_unref(node);
            return NULL;
        }

        assert(JSON_NODE_HOLDS_OBJECT(included_node));

        JsonObject* included_object = json_node_get_object(included_node);
        json_object_update(included_object, object);

        json_object_remove_member(included_object, "include");

        json_node_unref(node);
        node = included_node;
    }

    return node;
}

static JsonNode* parse_config(const char* config_str, const char* qemu_options, GError** errp) {
    JsonObject* config = json_object_new();

    json_object_set_string_member(config, "qemu-options-str", qemu_options);
    json_object_set_boolean_member(config, "vnet-hdr", FALSE);
    json_object_set_boolean_member(config, "filter-rewriter", TRUE);
    json_object_set_object_member(config, "colo-compare-options", json_object_new());
    json_object_set_object_member(config, "migration-parameters", json_object_new());
    json_object_set_array_member(config, "migration-capabilities", json_array_new());
    json_object_set_object_member(config, "throttle-limits", json_object_new());
    json_object_set_object_member(config, "blockdev-mirror-arguments", json_object_new());

    JsonNode* parsed_node = _parse_config(config_str, errp);
    if (!parsed_node) {
        json_object_unref(config);
        return NULL;
    }

    assert(JSON_NODE_HOLDS_OBJECT(parsed_node));
    JsonObject* parsed_object = json_node_get_object(parsed_node);
    json_object_update(config, parsed_object);
    json_node_unref(parsed_node);

    if (json_object_has_member(config, "qemu-options-str")) {
        JsonNode* qemu_options_node = json_object_get_member(config, "qemu-options-str");
        if (JSON_NODE_HOLDS_ARRAY(qemu_options_node)) {
            JsonArray* qemu_options_array = json_node_get_array(qemu_options_node);
            GString* joined_string = g_string_new("");
            guint len = json_array_get_length(qemu_options_array);
            for (guint i = 0; i < len; i++) {
                JsonNode* element_node = json_array_get_element(qemu_options_array, i);
                if (JSON_NODE_HOLDS_VALUE(element_node) && json_node_get_value_type(element_node) == G_TYPE_STRING) {
                    g_string_append(joined_string, json_node_get_string(element_node));
                }
            }
            json_object_set_string_member(config, "qemu-options-str", joined_string->str);
            g_string_free(joined_string, TRUE);
        }
    }

    JsonNode *ret = json_node_alloc();
    json_node_init_object(ret, config);
    json_object_unref(config);
    return ret;
}

int qmp_commands_set_qemu_options_str(QmpCommands *this, const char *_qemu_options, GError **errp) {
    int len;
    char **argv;
    int ret;

    ret = g_shell_parse_argv(_qemu_options, &len, &argv, errp);
    if (!ret) {
        return -1;
    }

    JsonNode *qemu_options = json_node_alloc();
    JsonArray *array = json_array_sized_new(len);
    json_node_init_array(qemu_options, array);
    json_array_unref(array);

    for (int i = 0; i < len; i++) {
        json_array_add_string_element(array, argv[i]);
    }

    qmp_commands_set_qemu_options(this, qemu_options);
    json_node_unref(qemu_options);

    g_strfreev(argv);
    return 0;
}

static int check_config(JsonNode *config, GError **errp) {
    if (!JSON_NODE_HOLDS_OBJECT(config)) {
        colod_error_set(errp, "config must be an object");
        return -1;
    }

    JsonObject *object = json_node_get_object(config);
    JsonNode *node;

    node = json_object_get_member(object, "qemu-options-str");
    if (json_node_get_value_type(node) != G_TYPE_STRING) {
        colod_error_set(errp, "qemu-options-str must be a string");
        return -1;
    }

    node = json_object_get_member(object, "filter-rewriter");
    if (json_node_get_value_type(node) != G_TYPE_BOOLEAN) {
        colod_error_set(errp, "filter-rewriter must be a boolean");
        return -1;
    }

    node = json_object_get_member(object, "colo-compare-options");
    if (!JSON_NODE_HOLDS_OBJECT(node)) {
        colod_error_set(errp, "colo-compare-options must be an object");
        return -1;
    }

    node = json_object_get_member(object, "migration-parameters");
    if (!JSON_NODE_HOLDS_OBJECT(node)) {
        colod_error_set(errp, "migration-parameters must be an object");
        return -1;
    }

    node = json_object_get_member(object, "migration-capabilities");
    if (!JSON_NODE_HOLDS_ARRAY(node)) {
        colod_error_set(errp, "migration-capabilities must be a list");
        return -1;
    }

    node = json_object_get_member(object, "throttle-limits");
    if (!JSON_NODE_HOLDS_OBJECT(node)) {
        colod_error_set(errp, "throttle-limits must be an object");
        return -1;
    }

    node = json_object_get_member(object, "blockdev-mirror-arguments");
    if (!JSON_NODE_HOLDS_OBJECT(node)) {
        colod_error_set(errp, "blockdev-mirror-arguments must be an object");
        return -1;
    }

    return 0;
}

int qmp_commands_read_config(QmpCommands *this, const char *config_str, const char *qemu_options, GError **errp) {
    int ret;
    JsonNode *config = parse_config(config_str, qemu_options, errp);
    if (!config) {
        return -1;
    }

    ret = check_config(config, errp);
    if (ret < 0) {
        json_node_unref(config);
        return -1;
    }

    JsonObject *object = json_node_get_object(config);

    this->filter_rewriter = json_object_get_boolean_member(object, "filter-rewriter");

    ret = qmp_commands_set_qemu_options_str(this, json_object_get_string_member(object, "qemu-options-str"),
                                                errp);
    if (ret < 0) {
        json_node_unref(config);
        return -1;
    }

    qmp_commands_set_comp_prop(this, json_object_get_member(object, "colo-compare-options"));
    qmp_commands_set_mig_cap(this, json_object_get_member(object, "migration-capabilities"));
    qmp_commands_set_mig_prop(this, json_object_get_member(object, "migration-parameters"));
    qmp_commands_set_throttle_prop(this, json_object_get_member(object, "throttle-limits"));
    qmp_commands_set_blk_mirror_prop(this, json_object_get_member(object, "blockdev-mirror-arguments"));

    json_node_unref(config);
    return 0;
}

QmpCommands *qmp_commands_new(const char *instance_name, const char *base_dir,
                              const char *active_hidden_dir,
                              const char *listen_address,
                              const char *qemu_binary,
                              const char *qemu_img_binary,
                              int base_port) {
    QmpCommands *this = g_new0(QmpCommands, 1);

    this->instance_name = g_strdup(instance_name);
    this->base_dir = g_strdup(base_dir);
    this->active_hidden_dir = g_strdup(active_hidden_dir);
    this->listen_address = g_strdup(listen_address);
    this->qemu_binary = g_strdup(qemu_binary);
    this->qemu_img_binary = g_strdup(qemu_img_binary);
    this->base_port = base_port;

    this->qemu_primary = qmp_commands_static(0,
        "@@QEMU_BINARY@@",
        "@@QEMU_OPTIONS@@",
        "-drive", "if=none,node-name=quorum0,driver=quorum,read-pattern=fifo,vote-threshold=1,children.0=parent0",
        "-drive", "if=none,node-name=colo-disk0,driver=throttle,throttle-group=throttle0,file.driver=raw,file.file=quorum0",
        "-no-shutdown",
        "-no-reboot",
        "-qmp", "unix:@@QMP_SOCK@@,server=on,wait=off",
        "-qmp", "unix:@@QMP_YANK_SOCK@@,server=on,wait=off",
        "-object", "throttle-group,id=throttle0",
        NULL);

    this->qemu_secondary = qmp_commands_static(0,
        "@@QEMU_BINARY@@",
        "@@QEMU_OPTIONS@@",
        "-chardev", "socket,id=mirror0,host=@@LISTEN_ADDRESS@@,port=@@MIRROR_PORT@@,server=on,wait=off,nodelay=on",
        "-chardev", "socket,id=comp_sec_in0,host=@@LISTEN_ADDRESS@@,port=@@COMPARE_IN_PORT@@,server=on,wait=off,nodelay=on",
        "-object", "filter-redirector,id=mirror0,netdev=hn0,queue=tx,indev=mirror0",
        "-object", "filter-drop,id=drop0,netdev=hn0,queue=rx",
        "-object", "filter-redirector,id=comp_sec_in0,netdev=hn0,queue=rx,outdev=comp_sec_in0",
        "@@IF_REWRITER@@-object", "@@IF_REWRITER@@filter-rewriter,id=rew0,netdev=hn0,queue=all",
        "-drive", "if=none,node-name=childs0,top-id=colo-disk0,driver=replication,mode=secondary,file.driver=qcow2,file.file.filename=@@ACTIVE_IMAGE@@,"
        "file.backing.driver=qcow2,file.backing.file.filename=@@HIDDEN_IMAGE@@,file.backing.backing=parent0",
        "-drive", "if=none,node-name=quorum0,driver=quorum,read-pattern=fifo,vote-threshold=1,children.0=childs0",
        "-drive", "if=none,node-name=colo-disk0,driver=throttle,throttle-group=throttle0,file.driver=raw,file.file=quorum0",
        "-incoming", "defer",
        "-no-shutdown",
        "-no-reboot",
        "-qmp", "unix:@@QMP_SOCK@@,server=on,wait=off",
        "-qmp", "unix:@@QMP_YANK_SOCK@@,server=on,wait=off",
        "-object", "throttle-group,id=throttle0",
        NULL);

    this->qemu_dummy = qmp_commands_static(0,
        "@@QEMU_BINARY@@",
        "@@QEMU_OPTIONS@@",
        "-drive", "if=none,node-name=colo-disk0,driver=null-co",
        "-S",
        "-qmp", "unix:@@QMP_SOCK@@,server=on,wait=off",
        "-qmp", "unix:@@QMP_YANK_SOCK@@,server=on,wait=off",
        NULL);

    this->prepare_primary = qmp_commands_static(0,
        "@@DECL_THROTTLE_PROP@@ {}",
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/throttle0', 'property': 'limits', 'value': @@THROTTLE_PROP@@}}",
        NULL);

    this->prepare_secondary = qmp_commands_static(0,
        "@@DECL_THROTTLE_PROP@@ {}",
        "{'execute': 'qom-set', 'arguments': {'path': '/objects/throttle0', 'property': 'limits', 'value': @@THROTTLE_PROP@@}}",
        "{'execute': 'migrate-set-capabilities', 'arguments': {'capabilities': [{'capability': 'x-colo', 'state': true}]}}",
        "{'execute': 'migrate-set-capabilities', 'arguments': {'capabilities': @@MIG_CAP@@}}",
        "@@DECL_MIG_PROP@@ {}",
        "{'execute': 'migrate-set-parameters', 'arguments': @@MIG_PROP@@}",
        "{'execute': 'nbd-server-start', 'arguments': {'addr': {'type': 'inet', 'data': {'host': '@@LISTEN_ADDRESS@@', 'port': '@@NBD_PORT@@'}}}}",
        "{'execute': 'nbd-server-add', 'arguments': {'device': 'parent0', 'writable': true}}",
        "{'execute': 'migrate-incoming', 'arguments': {'uri': 'tcp:@@LISTEN_ADDRESS@@:@@MIGRATE_PORT@@'}}",
        NULL);

    this->migration_start = qmp_commands_static(0,
        "{'execute': 'migrate-set-capabilities', 'arguments': {'capabilities': [{'capability': 'x-colo', 'state': true}]}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_pri_in0..', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'unix', 'data': {'path': '@@COMP_PRI_SOCK@@'}}, 'server': true}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_pri_in0', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'unix', 'data': {'path': '@@COMP_PRI_SOCK@@'}}, 'server': false}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_out0..', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'unix', 'data': {'path': '@@COMP_OUT_SOCK@@'}}, 'server': true}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_out0', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'unix', 'data': {'path': '@@COMP_OUT_SOCK@@'}}, 'server': false}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'mirror0', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'inet', 'data': {'host': '@@ADDRESS@@', 'port': '@@MIRROR_PORT@@'}}, 'server': false, 'nodelay': true}}}}",
        "{'execute': 'chardev-add', 'arguments': {'id': 'comp_sec_in0', 'backend': {'type': 'socket', 'data': {'addr': {'type': 'inet', 'data': {'host': '@@ADDRESS@@', 'port': '@@COMPARE_IN_PORT@@'}}, 'server': false, 'nodelay': true}}}}",
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

    this->yank_instances = json_from_string(
                "[{'type': 'block-node', 'node-name': 'nbd0'}, "
                "{'type': 'chardev', 'id': 'mirror0'}, "
                "{'type': 'chardev', 'id': 'comp_sec_in0'}, "
                "{'type': 'migration'}]", NULL);
    assert(this->yank_instances);

    return this;
}

void qmp_commands_free(QmpCommands *this) {
    g_free(this->instance_name);
    g_free(this->base_dir);
    g_free(this->active_hidden_dir);
    g_free(this->listen_address);
    g_free(this->qemu_binary);
    g_free(this->qemu_img_binary);
    qmp_commands_node_unref(this->comp_prop);
    qmp_commands_node_unref(this->mig_cap);
    qmp_commands_node_unref(this->mig_prop);
    qmp_commands_node_unref(this->throttle_prop);
    qmp_commands_node_unref(this->blk_mirror_prop);
    qmp_commands_node_unref(this->qemu_options);
    qmp_commands_node_unref(this->yank_instances);

    my_array_unref(this->qemu_primary);
    my_array_unref(this->qemu_secondary);
    my_array_unref(this->qemu_dummy);
    my_array_unref(this->prepare_primary);
    my_array_unref(this->prepare_secondary);
    my_array_unref(this->migration_start);
    my_array_unref(this->migration_switchover);
    my_array_unref(this->failover_primary);
    my_array_unref(this->failover_secondary);

    g_free(this);
}
