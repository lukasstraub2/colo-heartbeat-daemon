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

static void test_a() {
    int ret;
    GError *local_errp = NULL;
    QmpCommands *commands = qmp_commands_new("/tmp", "0.0.0.0", 9000);

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
    QmpCommands *commands = qmp_commands_new("/tmp", "0.0.0.0", 9000);

    JsonNode *json = json_from_string("['some @@ADDRESS@@', 'and :@@NBD_PORT@@: then', '@@LISTEN_ADDRESS@@', 'and @@COMP_PRI_SOCK@@']", NULL);
    assert(json);

    ret = qmp_commands_set_migration_start(commands, json, NULL);
    assert(ret == 0);
    ret = qmp_commands_set_prepare_secondary(commands, json, NULL);
    assert(ret == 0);

    MyArray *array = qmp_commands_get_migration_start(commands, "address");
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

int main(G_GNUC_UNUSED int argc, G_GNUC_UNUSED char **argv) {
    test_a();
    test_b();

    return 0;
}
