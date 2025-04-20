/*
 * COLO background daemon native QemuLauncher test
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <assert.h>
#include <stdio.h>
#include <glib-2.0/glib.h>

#include "coroutine.h"
#include "coroutine_stack.h"
#include "qemulauncher.h"
#include "qmpcommands.h"
#include "qmp.h"

typedef struct TestCoroutine TestCoroutine;
struct TestCoroutine {
    Coroutine coroutine;
    QmpCommands *commands;
    GMainLoop *mainloop;
};

FILE *trace = NULL;
gboolean do_syslog = FALSE;

void colod_trace(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    vfprintf(stderr, fmt, args);
    fflush(stderr);

    va_end(args);
}

void colod_syslog(G_GNUC_UNUSED int pri, const char *fmt, ...) {
    va_list args;

    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    fwrite("\n", 1, 1, stderr);
    va_end(args);
}

static QmpCommands *test_qmp_commands() {
    QmpCommands *commands;
    const char *qemu = "qemu-system-x86_64";
    const char *qemu_img = "qemu-img";
    int ret;

    ret = access("/opt/qemu-colo/bin/qemu-system-x86_64", X_OK);
    if (ret == 0) {
        qemu = "/opt/qemu-colo/bin/qemu-system-x86_64";
        qemu_img = "/opt/qemu-colo/bin/qemu-img";
    }

    commands = qmp_commands_new("colo-test", "/tmp", "/tmp", "127.0.0.1",
                                qemu, qemu_img, 9000);
    assert(commands);

    JsonNode *options = json_from_string("["
                                         "'-enable-kvm', "
                                         "'-drive', 'if=none,node-name=parent0,driver=null-co,size=1g', "
                                         "'-netdev', 'user,id=hn0', "
                                         "'-device', 'e1000,netdev=hn0', "
                                         "'-device', 'virtio-blk,drive=colo-disk0']", NULL);
    assert(options);
    qmp_commands_set_qemu_options(commands, options);
    json_node_unref(options);

    return commands;
}

static QemuLauncher* test_qemu_launcher(QmpCommands *commands) {
    return qemu_launcher_new(commands, "/tmp", 1000);
}

int _test_a(Coroutine *coroutine) {
    struct {
        QmpCommands *commands;
        QemuLauncher *launcher;
        GError *local_errp;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO local_errp = NULL;
    CO commands = test_qmp_commands();
    JsonNode *options = json_from_string("[]", NULL);
    assert(options);
    qmp_commands_set_qemu_options(CO commands, options);
    json_node_unref(options);
    CO launcher = test_qemu_launcher(CO commands);

    ColodQmpState *qmp;
    co_recurse(qmp = qemu_launcher_launch_secondary(coroutine, CO launcher, &CO local_errp));
    assert(!qmp);
    g_error_free(CO local_errp);

    qemu_launcher_unref(CO launcher);
    qmp_commands_free(CO commands);
    return 0;

    co_end;
}

int _test_b(Coroutine *coroutine, QmpCommands *commands) {
    struct {
        QemuLauncher *launcher;
        ColodQmpState *qmp;
        GError *local_errp;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO local_errp = NULL;
    CO launcher = test_qemu_launcher(commands);

    co_recurse(CO qmp = qemu_launcher_launch_secondary(coroutine, CO launcher, &CO local_errp));
    if (!CO qmp) {
        fprintf(stderr, "%s\n", CO local_errp->message);
        abort();
    }

    ColodQmpResult *res;
    co_recurse(res = qmp_execute_co(coroutine, CO qmp, &CO local_errp, "{'execute': 'quit'}\n"));
    if (!res) {
        fprintf(stderr, "%s\n", CO local_errp->message);
        abort();
    }
    qmp_result_free(res);

    int ret;
    co_recurse(ret = qemu_launcher_wait_co(coroutine, CO launcher, 1000, &CO local_errp));
    if (ret < 0) {
        fprintf(stderr, "%s\n", CO local_errp->message);
        abort();
    }

    qmp_unref(CO qmp);
    qemu_launcher_unref(CO launcher);
    return 0;

    co_end;
}

int _test_c(Coroutine *coroutine, QmpCommands *commands) {
    struct {
        QemuLauncher *launcher;
        ColodQmpState *qmp;
        GError *local_errp;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO local_errp = NULL;
    CO launcher = test_qemu_launcher(commands);

    co_recurse(CO qmp = qemu_launcher_launch_primary(coroutine, CO launcher, &CO local_errp));
    if (!CO qmp) {
        fprintf(stderr, "%s\n", CO local_errp->message);
        abort();
    }

    ColodQmpResult *res;
    co_recurse(res = qmp_execute_co(coroutine, CO qmp, &CO local_errp, "{'execute': 'quit'}\n"));
    if (!res) {
        fprintf(stderr, "%s\n", CO local_errp->message);
        abort();
    }
    qmp_result_free(res);

    int ret;
    co_recurse(ret = qemu_launcher_wait_co(coroutine, CO launcher, 1000, &CO local_errp));
    if (ret < 0) {
        fprintf(stderr, "%s\n", CO local_errp->message);
        abort();
    }

    qmp_unref(CO qmp);
    qemu_launcher_unref(CO launcher);
    return 0;

    co_end;
}

int _test_d(Coroutine *coroutine) {
    struct {
        MyArray *argv;
    } *co;
    GError *local_errp = NULL;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO argv = my_array_new(NULL);
    my_array_append(CO argv, "/bin/sleep");
    my_array_append(CO argv, "100");
    my_array_append(CO argv, NULL);

    int ret;
    co_recurse(ret = colod_execute_sync_timeout_co(coroutine, CO argv, 500, &local_errp));
    assert(ret < 0);
    assert(local_errp->domain == COLOD_ERROR && local_errp->code == COLOD_ERROR_TIMEOUT);
    g_error_free(local_errp);
    return 0;

    co_end;
}

static gboolean _test_main_co(Coroutine *coroutine, TestCoroutine *this);
static gboolean test_main_co(gpointer data) {
    Coroutine *coroutine = data;
    gboolean ret;

    co_enter(coroutine, ret = _test_main_co(coroutine, data));
    if (coroutine->yield) {
        return GPOINTER_TO_INT(coroutine->yield_value);
    }

    colod_assert_remove_one_source(coroutine);
    return ret;
}

static gboolean _test_main_co(Coroutine *coroutine, TestCoroutine *this) {

    co_begin(int, -1);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
    co_recurse(_test_a(coroutine));
    co_recurse(_test_b(coroutine, this->commands));
    co_recurse(_test_c(coroutine, this->commands));
    co_recurse(_test_d(coroutine));
#pragma GCC diagnostic pop

    g_main_loop_quit(this->mainloop);
    return G_SOURCE_REMOVE;

    co_end;
}

int main(G_GNUC_UNUSED int argc, G_GNUC_UNUSED char **argv) {
    TestCoroutine _this = {0};
    TestCoroutine *this = &_this;
    Coroutine *coroutine = &this->coroutine;
    int ret;

    ret = access("/usr/bin/qemu-system-x86_64", X_OK);
    if (ret < 0) {
        ret = access("/opt/qemu-colo/bin/qemu-system-x86_64", X_OK);
        if (ret < 0) {
            return 0;
        }
    }

    coroutine->cb = test_main_co;
    this->commands = test_qmp_commands();
    assert(this->commands);
    this->mainloop = g_main_loop_new(g_main_context_default(), FALSE);
    assert(this->mainloop);

    g_idle_add(test_main_co, this);

    g_main_loop_run(this->mainloop);

    g_main_loop_unref(this->mainloop);
    qmp_commands_free(this->commands);
    return 0;
}
