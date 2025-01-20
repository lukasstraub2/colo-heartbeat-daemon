/*
 * MyArray tests
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <assert.h>
#include <glib-2.0/glib.h>

#include "util.h"

#define MYVAL ((void *) 1234)

static int free_count = 0;

static void free_ptr(gpointer data) {
    assert(data == MYVAL);
    free_count--;
}

static void test_a() {
    MyArray *array = my_array_new(free_ptr);

    for (int i = 0; i < 2000; i++) {
        my_array_append(array, MYVAL);
        free_count++;
    }

    assert(array->size == 2000);
    for (int i = 0; i < array->size; i++) {
        assert(array->array[i] == MYVAL);
    }

    my_array_unref(array);
    assert(free_count == 0);
}

static void test_b() {
    MyArray *array = my_array_new(g_free);

    for (int i = 0; i < 5000; i++) {
        my_array_append(array, g_strdup("Hallo"));
    }

    for (int i = 0; i < array->size; i++) {
        assert(!strcmp(array->array[i], "Hallo"));
    }

    my_array_unref(array);
}

int main(G_GNUC_UNUSED int argc, G_GNUC_UNUSED char **argv) {
    test_a();
    test_b();

    return 0;
}
