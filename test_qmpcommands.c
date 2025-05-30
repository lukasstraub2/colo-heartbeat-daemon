/*
 * QmpCommands tests
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <assert.h>
#include <stdio.h>
#include <glib-2.0/glib.h>

#include "qmpcommands.h"

static QmpCommands *test_qmp_commands_new() {
    return qmp_commands_new("colo-test", "/tmp", "/dev/shm", "0.0.0.0",
                            "/opt/qemu-colo/bin/qemu", "/usr/bin/qemu", 9000);
}

static void test_a() {
    int ret;
    GError *local_errp = NULL;
    QmpCommands *commands = test_qmp_commands_new();

    JsonNode *json = json_from_string("\"lol\"", NULL);
    assert(json);
    ret = qmp_commands_set_migration_start(commands, json, &local_errp);
    assert(ret < 0);
    assert(local_errp);
    g_error_free(local_errp);
    local_errp = NULL;
    json_node_unref(json);

    json = json_from_string("[1, 2, 3]", NULL);
    assert(json);
    ret = qmp_commands_set_migration_start(commands, json, &local_errp);
    assert(ret < 0);
    assert(local_errp);
    g_error_free(local_errp);
    local_errp = NULL;
    json_node_unref(json);

    json = json_from_string("['', '', '']", NULL);
    assert(json);
    ret = qmp_commands_set_migration_start(commands, json, &local_errp);
    assert(ret == 0);
    assert(!local_errp);
    json_node_unref(json);

    qmp_commands_free(commands);
}

static void test_b() {
    int ret;
    QmpCommands *commands = test_qmp_commands_new();

    JsonNode *json = json_from_string("['some @@ADDRESS@@', "
                                      "'and :@@NBD_PORT@@: then', "
                                      "'@@LISTEN_ADDRESS@@', "
                                      "'and @@COMP_PRI_SOCK@@']", NULL);
    assert(json);

    ret = qmp_commands_set_migration_start(commands, json, NULL);
    assert(ret == 0);
    ret = qmp_commands_set_prepare_secondary(commands, json, NULL);
    assert(ret == 0);

    MyArray *array = qmp_commands_get_migration_start(commands, "address", FALSE);
    assert(!strcmp(array->array[0], "some address\n"));
    assert(!strcmp(array->array[1], "and :9000: then\n"));
    assert(!strcmp(array->array[2], "0.0.0.0\n"));
    assert(!strcmp(array->array[3], "and /tmp/comp-pri-in0.sock\n"));
    my_array_unref(array);

    array = qmp_commands_get_prepare_secondary(commands);
    assert(!strcmp(array->array[0], "some \n"));
    assert(!strcmp(array->array[1], "and :9000: then\n"));
    assert(!strcmp(array->array[2], "0.0.0.0\n"));
    assert(!strcmp(array->array[3], "and /tmp/comp-pri-in0.sock\n"));
    my_array_unref(array);

    json_node_unref(json);
    qmp_commands_free(commands);
}

static void test_c(gboolean filter_rewriter) {
    int ret;
    QmpCommands *commands = test_qmp_commands_new();
    qmp_commands_set_filter_rewriter(commands, filter_rewriter);
    JsonNode *json = json_from_string("['@@IF_REWRITER@@ rewriter', "
                                      "'@@IF_NOT_REWRITER@@ no rewriter']", NULL);
    assert(json);

    ret = qmp_commands_set_migration_switchover(commands, json, NULL);
    assert(ret == 0);

    MyArray *array = qmp_commands_get_migration_switchover(commands);
    assert(array->size == 1);
    if (filter_rewriter) {
        assert(!strcmp(array->array[0], " rewriter\n"));
    } else {
        assert(!strcmp(array->array[0], " no rewriter\n"));
    }
    my_array_unref(array);

    json_node_unref(json);
    qmp_commands_free(commands);
}

static void test_d() {
    int ret;
    GError *local_errp = NULL;
    QmpCommands *commands = test_qmp_commands_new();

    JsonNode *json = json_from_string("['@@COMP_OUT_SOCK@@ @@unknown@@']", NULL);
    assert(json);
    ret = qmp_commands_set_migration_start(commands, json, &local_errp);
    assert(ret < 0);
    assert(local_errp);
    g_error_free(local_errp);
    local_errp = NULL;
    json_node_unref(json);

    qmp_commands_free(commands);
}

static void test_e() {
    int ret;
    GError *local_errp = NULL;
    QmpCommands *commands = test_qmp_commands_new();

    JsonNode *json = json_from_string("['@@COMP_PROP@@']", NULL);
    assert(json);
    ret = qmp_commands_set_migration_start(commands, json, &local_errp);
    assert(ret < 0);
    assert(local_errp);
    g_error_free(local_errp);
    local_errp = NULL;
    json_node_unref(json);

    json = json_from_string("['@@DECL_COMP_PROP@@ lol']", NULL);
    assert(json);
    ret = qmp_commands_set_migration_start(commands, json, &local_errp);
    assert(ret < 0);
    assert(local_errp);
    g_error_free(local_errp);
    local_errp = NULL;
    json_node_unref(json);

    qmp_commands_free(commands);
}

static void test_f() {
    int ret;
    JsonNode *colo_comp_prop = json_from_string("{\"colo_comp_prop\":\"lol\"}", NULL);
    assert(colo_comp_prop);
    QmpCommands *commands = test_qmp_commands_new();
    qmp_commands_set_comp_prop(commands, colo_comp_prop);
    json_node_unref(colo_comp_prop);

    JsonNode *json = json_from_string("['@@DECL_COMP_PROP@@ {\"test\": \"test\"}', "
                                      "'@@COMP_PROP@@']", NULL);
    assert(json);
    ret = qmp_commands_set_migration_start(commands, json, NULL);
    assert(ret == 0);

    MyArray *array = qmp_commands_get_migration_start(commands, "address", FALSE);
    assert(array->size == 1);
    assert(!strcmp(array->array[0], "{\"test\":\"test\",\"colo_comp_prop\":\"lol\"}\n"));
    my_array_unref(array);

    json_node_unref(json);
    qmp_commands_free(commands);
}

static void test_g() {
    int ret;
    JsonNode *mig_cap = json_from_string("[{\"colo_comp_prop\":\"lol\"}]", NULL);
    assert(mig_cap);
    QmpCommands *commands = test_qmp_commands_new();
    qmp_commands_set_mig_cap(commands, mig_cap);
    json_node_unref(mig_cap);

    JsonNode *json = json_from_string("['@@MIG_CAP@@']", NULL);
    assert(json);
    ret = qmp_commands_set_migration_start(commands, json, NULL);
    assert(ret == 0);

    MyArray *array = qmp_commands_get_migration_start(commands, "address", FALSE);
    assert(array->size == 1);
    assert(!strcmp(array->array[0], "[{\"colo_comp_prop\":\"lol\"}]\n"));
    my_array_unref(array);

    json_node_unref(json);
    qmp_commands_free(commands);
}

static void test_h() {
    JsonNode *blk_mirror_prop = json_from_string("{\"blk_prop\":\"lol\"}", NULL);
    assert(blk_mirror_prop);
    QmpCommands *commands = test_qmp_commands_new();
    qmp_commands_set_blk_mirror_prop(commands, blk_mirror_prop);
    json_node_unref(blk_mirror_prop);

    MyArray *array = qmp_commands_adhoc(commands, "dummy address",
        "@@DECL_BLK_MIRROR_PROP@@ {\"test\": \"test\"}",
        "{'execute': 'blockdev-mirror', 'arguments': @@BLK_MIRROR_PROP@@}",
        NULL);
    assert(array->size == 1);
    assert(!strcmp(array->array[0], "{'execute': 'blockdev-mirror', 'arguments': {\"test\":\"test\",\"blk_prop\":\"lol\"}}\n"));
    my_array_unref(array);

    qmp_commands_free(commands);
}

static void test_i() {
    QmpCommands *commands = test_qmp_commands_new();
    MyArray *array = qmp_commands_cmdline(commands, NULL, NULL,
                                          "@@QEMU_BINARY@@",
                                          "@@IF_REWRITER@@-rewriter",
                                          "-disk",
                                          "@@ACTIVE_IMAGE@@",
                                          NULL);

    assert(array->size == 4);
    assert(!strcmp(array->array[0], "/opt/qemu-colo/bin/qemu"));
    assert(!strcmp(array->array[1], "-disk"));
    assert(!strcmp(array->array[2], "/dev/shm/colo-test-active.qcow2"));
    assert(array->array[3] == NULL);
    my_array_unref(array);

    qmp_commands_free(commands);
}

static void test_j() {
    JsonNode *qemu_options = json_from_string("[\"a\", \"b\"]", NULL);
    assert(qemu_options);
    QmpCommands *commands = test_qmp_commands_new();
    qmp_commands_set_qemu_options(commands, qemu_options);
    json_node_unref(qemu_options);

    MyArray *array = qmp_commands_cmdline(commands, NULL, NULL,
                                          "@@QEMU_OPTIONS@@", NULL);
    assert(array->size == 3);
    assert(!strcmp(array->array[0], "a"));
    assert(!strcmp(array->array[1], "b"));
    assert(array->array[2] == NULL);
    my_array_unref(array);

    qmp_commands_free(commands);
}

static void test_k() {
    QmpCommands *commands = test_qmp_commands_new();
    int ret = qmp_commands_set_qemu_options_str(commands, "a b", NULL);
    assert(ret == 0);

    MyArray *array = qmp_commands_cmdline(commands, NULL, NULL,
                                          "@@QEMU_OPTIONS@@", NULL);
    assert(array->size == 3);
    assert(!strcmp(array->array[0], "a"));
    assert(!strcmp(array->array[1], "b"));
    assert(array->array[2] == NULL);
    my_array_unref(array);

    qmp_commands_free(commands);
}

static void test_l() {
    QmpCommands *commands = test_qmp_commands_new();
    int ret;

    ret = qmp_commands_read_config(commands,
                                   "{'include': 'test/include.json', "
                                   "'colo-compare-options': {'colo_comp_prop': 'lol'}}",
                                   "override this", NULL);
    assert(ret == 0);

    MyArray *array = qmp_commands_adhoc(commands, "dummy address",
        "@@DECL_COMP_PROP@@ {\"test\": \"test\"}",
        "@@COMP_PROP@@",
        "@@MIG_CAP@@",
        "@@DECL_BLK_MIRROR_PROP@@ {\"test\": \"test\"}",
        "{'execute': 'blockdev-mirror', 'arguments': @@BLK_MIRROR_PROP@@}",
        "@@QEMU_OPTIONS@@",
        NULL);
    assert(array->size == 6);
    assert(!strcmp(array->array[0], "{\"test\":\"test\",\"colo_comp_prop\":\"lol\"}\n"));
    assert(!strcmp(array->array[1], "[{\"colo_comp_prop\":\"lol\"}]\n"));
    assert(!strcmp(array->array[2], "{'execute': 'blockdev-mirror', 'arguments': {\"test\":\"test\",\"blk_prop\":\"lol\"}}\n"));
    assert(!strcmp(array->array[3], "a"));
    assert(!strcmp(array->array[4], "b"));
    assert(!strcmp(array->array[5], "ab"));
    my_array_unref(array);

    qmp_commands_free(commands);
}

int main(G_GNUC_UNUSED int argc, G_GNUC_UNUSED char **argv) {
    test_a();
    test_b();
    test_c(TRUE);
    test_c(FALSE);
    test_d();
    test_e();
    test_f();
    test_g();
    test_h();
    test_i();
    test_j();
    test_k();
    test_l();

    return 0;
}
