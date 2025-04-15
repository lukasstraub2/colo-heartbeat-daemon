/*
 * COLO background daemon formater
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <glib-2.0/glib.h>
#include <json-glib-1.0/json-glib/json-glib.h>
#include <assert.h>

#include "formater.h"
#include "util.h"

struct Formater {
    const char *instance_name;
    const char *base_dir;
    const char *active_hidden_dir;
    const char *address;
    const char *listen_address;
    const char *qemu_binary;
    const char *qemu_img_binary;
    const char *disk_size;
    gboolean filter_rewriter;
    gboolean newline;
    JsonNode *comp_prop;
    JsonNode *mig_prop;
    JsonNode *throttle_prop;
    JsonNode *blk_mirror_prop;
    JsonNode *qemu_options;

    char *decl_comp_prop;
    char *decl_mig_cap;
    char *decl_mig_prop;
    char *decl_throttle_prop;
    char *decl_blk_mirror_prop;

    char *active_image;
    char *hidden_image;
    char *qmp_sock;
    char *qmp_yank_sock;
    char *comp_pri_sock;
    char *comp_out_sock;
    char *nbd_port;
    char *migrate_port;
    char *mirror_port;
    char *compare_in_port;
};

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

static int formater_qemu_options(Formater *this, MyArray *out) {
    JsonArray *array = json_node_get_array(this->qemu_options);

    int len = json_array_get_length(array);
    for (int i = 0; i < len; i++) {
        JsonNode *node = json_array_get_element(array, i);
        my_array_append(out, g_strdup(json_node_get_string(node)));
    }

    return 0;
}

static int formater_format_one(Formater *this, MyArray *out, const char *str) {
    gboolean if_rewriter = !!strstr(str, "@@IF_REWRITER@@");
    gboolean if_not_rewriter = !!strstr(str, "@@IF_NOT_REWRITER@@");
    gboolean qemu_options = !!strstr(str, "@@QEMU_OPTIONS@@");

    if (qemu_options) {
        return formater_qemu_options(this, out);
    }

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
    g_string_replace(command, "@@QEMU_BINARY@@", this->qemu_binary, 0);
    g_string_replace(command, "@@QEMU_IMG_BINARY@@", this->qemu_img_binary, 0);
    g_string_replace(command, "@@DISK_SIZE", this->disk_size, 0);

    g_string_replace(command, "@@ACTIVE_IMAGE@@", this->active_image, 0);
    g_string_replace(command, "@@HIDDEN_IMAGE@@", this->hidden_image, 0);
    g_string_replace(command, "@@QMP_SOCK@@", this->qmp_sock, 0);
    g_string_replace(command, "@@QMP_YANK_SOCK@@", this->qmp_yank_sock, 0);
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

    if (this->newline) {
        g_string_append_c(command, '\n');
    }

    my_array_append(out, g_string_free(command, FALSE));

    return 0;
}

MyArray *formater_format(Formater *this, const MyArray *entry) {
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

static JsonNode *formater_set_prop(JsonNode *prop) {
    if (!prop) {
        return json_from_string("{}", NULL);
    }

    assert(JSON_NODE_HOLDS_OBJECT(prop));
    return json_node_ref(prop);
}

static JsonNode *formater_set_array(JsonNode *prop) {
    if (!prop) {
        return json_from_string("[]", NULL);
    }

    assert(JSON_NODE_HOLDS_ARRAY(prop));
    return json_node_ref(prop);
}

static const char *formater_set_string(const char *str) {
    if (!str) {
        return "";
    }

    return str;
}

char *formater_qmp_sock(const char *base_dir) {
    return g_build_filename(base_dir, "qmp.sock", NULL);
}

char *formater_qmp_yank_sock(const char *base_dir) {
    return g_build_filename(base_dir, "qmp-yank.sock", NULL);
}

Formater *formater_new(const char *instance_name, const char *base_dir,
                       const char *active_hidden_dir, const char *address,
                       const char *listen_address, const char *qemu_binary,
                       const char *qemu_img_binary, const char *disk_size,
                       gboolean filter_rewriter, gboolean newline,
                       JsonNode *comp_prop,
                       JsonNode *mig_cap, JsonNode *mig_prop,
                       JsonNode *throttle_prop, JsonNode *blk_mirror_prop,
                       JsonNode *qemu_options, const int base_port) {
    Formater *this = g_new0(Formater, 1);

    this->instance_name = formater_set_string(instance_name);
    this->base_dir = formater_set_string(base_dir);
    this->active_hidden_dir = formater_set_string(active_hidden_dir);
    this->address = formater_set_string(address);
    this->listen_address = formater_set_string(listen_address);
    this->qemu_binary = formater_set_string(qemu_binary);
    this->qemu_img_binary = formater_set_string(qemu_img_binary);
    this->disk_size = formater_set_string(disk_size);
    this->filter_rewriter = filter_rewriter;
    this->newline = newline;

    this->comp_prop = formater_set_prop(comp_prop);
    if (mig_cap) {
        this->decl_mig_cap = json_to_string(mig_cap, FALSE);
    } else {
        this->decl_mig_cap = g_strdup("[]");
    }
    this->mig_prop = formater_set_prop(mig_prop);
    this->throttle_prop = formater_set_prop(throttle_prop);
    this->blk_mirror_prop = formater_set_prop(blk_mirror_prop);
    this->qemu_options = formater_set_array(qemu_options);

    char *tmp = g_strdup_printf("%s-active.qcow2", this->instance_name);
    this->active_image = g_build_filename(this->active_hidden_dir, tmp, NULL);
    g_free(tmp);
    tmp = g_strdup_printf("%s-hidden.qcow2", this->instance_name);
    this->hidden_image = g_build_filename(this->active_hidden_dir, tmp, NULL);
    g_free(tmp);
    this->qmp_sock = formater_qmp_sock(this->base_dir);
    this->qmp_yank_sock = formater_qmp_yank_sock(this->base_dir);
    this->comp_pri_sock = g_build_filename(this->base_dir, "comp-pri-in0.sock", NULL);
    this->comp_out_sock = g_build_filename(this->base_dir, "comp-out0.sock", NULL);
    this->nbd_port = g_strdup_printf("%i", base_port);
    this->migrate_port = g_strdup_printf("%i", base_port + 1);
    this->mirror_port = g_strdup_printf("%i", base_port + 2);
    this->compare_in_port = g_strdup_printf("%i", base_port + 3);

    return this;
}

void formater_free(Formater *this) {
    json_node_unref(this->comp_prop);
    json_node_unref(this->mig_prop);
    json_node_unref(this->throttle_prop);
    json_node_unref(this->blk_mirror_prop);
    json_node_unref(this->qemu_options);

    g_free(this->decl_comp_prop);
    g_free(this->decl_mig_cap);
    g_free(this->decl_mig_prop);
    g_free(this->decl_throttle_prop);
    g_free(this->decl_blk_mirror_prop);

    g_free(this->active_image);
    g_free(this->hidden_image);
    g_free(this->qmp_sock);
    g_free(this->qmp_yank_sock);
    g_free(this->comp_pri_sock);
    g_free(this->comp_out_sock);
    g_free(this->nbd_port);
    g_free(this->migrate_port);
    g_free(this->mirror_port);
    g_free(this->compare_in_port);

    g_free(this);
}
