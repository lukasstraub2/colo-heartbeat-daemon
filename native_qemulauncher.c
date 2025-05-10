/*
 * COLO background daemon native qemu launcher
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#include <glib-2.0/glib.h>
#include <unistd.h>
#include <sys/prctl.h>
#include <signal.h>
#include <sys/wait.h>

#include "base_types.h"
#include "qemulauncher.h"
#include "util.h"
#include "qmpcommands.h"
#include "qmp.h"
#include "formater.h"
#include "daemon.h"
#include "json_util.h"

#include "coroutine_stack.h"
#include "qmpexectx.h"

struct QemuLauncher {
    QmpCommands *commands;
    const char *base_dir;
    guint qmp_timeout;
    int pid;
    char *disk_size;
};

static void setup_child(gpointer data) {
    (void) data;
    int ret = prctl(PR_SET_PDEATHSIG, SIGKILL);
    if (ret < 0) {
        char a[] = "prctl(PR_SET_PDEATHSIG) failed\n";
        ret = write(2, a, sizeof(a)); // write() is async-safe
        _exit(1);
    }
}

static int execute_qemu(MyArray *argv, GError **errp) {
    int pid;
    gboolean ret;

    ret = g_spawn_async("/", (char **)argv->array, NULL,
                        G_SPAWN_SEARCH_PATH | G_SPAWN_DO_NOT_REAP_CHILD,
                        setup_child, NULL,
                        &pid, errp);
    my_array_unref(argv);
    if (!ret) {
        return -1;
    }

    return pid;
}

static int open_qmp_sockets(QemuLauncher *this, int *qmp_fd, int *qmp_yank_fd, GError **errp) {
    int ret;

    char *qmp_path = formater_qmp_sock(this->base_dir);
    ret = colod_unix_connect(qmp_path, errp);
    g_free(qmp_path);
    if (ret < 0) {
        return -1;
    }
    *qmp_fd = ret;

    char *qmp_yank_path = formater_qmp_yank_sock(this->base_dir);
    ret = colod_unix_connect(qmp_yank_path, errp);
    g_free(qmp_yank_path);
    if (ret < 0) {
        close(*qmp_fd);
        *qmp_fd = 0;
        return -1;
    }
    *qmp_yank_fd = ret;

    return 0;
}

#define qemu_launcher_launch_co(...) \
    co_wrap(_qemu_launcher_launch_co(__VA_ARGS__))
static ColodQmpState *_qemu_launcher_launch_co(Coroutine *coroutine, QemuLauncher *this,
                                               MyArray *argv, GError **errp) {
    struct {
        int i;
        GError *local_errp;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpState *, NULL);

    CO local_errp = NULL;

    ret = execute_qemu(argv, errp);
    if (ret < 0) {
        return NULL;
    }
    this->pid = ret;

    for (CO i = 0; CO i < 100; CO i++) {
        ColodQmpState *qmp;
        int qmp_fd, qmp_yank_fd;

        guint timeout_source_id = g_timeout_add(100, coroutine->cb, coroutine);
        g_source_set_name_by_id(timeout_source_id, "reconnect sleep timer");
        co_yield_int(G_SOURCE_REMOVE);

        ret = waitpid(this->pid, NULL, WNOHANG);
        if (ret != 0) {
            colod_error_set(errp, "qemu died");
            return NULL;
        }

        ret = open_qmp_sockets(this, &qmp_fd, &qmp_yank_fd, NULL);
        if (ret < 0) {
            continue;
        }

        qmp = qmp_new(qmp_fd, qmp_yank_fd, this->qmp_timeout, &CO local_errp);
        if (!qmp) {
            close(qmp_fd);
            close(qmp_yank_fd);
            break;
        }

        JsonNode *yank_instances = qmp_commands_get_yank_instances(this->commands);
        qmp_set_yank_instances(qmp, yank_instances);
        json_node_unref(yank_instances);

        return qmp;
    }

    qemu_launcher_kill(this);
    co_recurse(ret = qemu_launcher_wait_co(coroutine, this, 0, NULL));

    if (CO local_errp) {
        g_propagate_error(errp, CO local_errp);
    } else {
        colod_error_set(errp, "timeout while trying to connect to qmp");
    }
    return NULL;

    co_end;
}

static char *get_disk_size(ColodQmpResult *res, GError **errp) {
    JsonNode *_array = get_member_node(res->json_root, "return");

    assert(JSON_NODE_HOLDS_ARRAY(_array));
    JsonArray *array = json_node_get_array(_array);

    int len = json_array_get_length(array);
    for (int i = 0; i < len; i++) {
        JsonNode *node = json_array_get_element(array, i);
        const char *node_name = get_member_str(node, "node-name");
        if (strcmp(node_name, "parent0")) {
            continue;
        }

        long ret = get_member_member_int(node, "image", "virtual-size");
        return g_strdup_printf("%lu", ret);
    }

    colod_error_set(errp, "Disk \"parent0\" not found");
    return NULL;
}

#define qemu_launcher_disk_size_co(...) \
    co_wrap(_qemu_launcher_disk_size_co(__VA_ARGS__))
static char *_qemu_launcher_disk_size_co(Coroutine *coroutine, QemuLauncher *this, GError **errp) {
    struct {
        QmpEctx *ectx;
        char *disk_size;
        MyArray *cmdline;
        GError *local_errp;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(char *, NULL);

    CO local_errp = NULL;

    ColodQmpState *_qmp;
    CO cmdline = qmp_commands_get_qemu_dummy(this->commands);
    co_recurse(_qmp = qemu_launcher_launch_co(coroutine, this, CO cmdline, errp));
    if (!_qmp) {
        return NULL;
    }
    CO ectx = qmp_ectx_new(_qmp);
    qmp_unref(_qmp);

    ColodQmpResult *ret;
    co_recurse(ret = qmp_ectx(coroutine, CO ectx,
                              "{'execute': 'query-named-block-nodes', 'arguments': {'flat': true}}\n"));
    if (qmp_ectx_failed(CO ectx)) {
        qmp_ectx_unref(CO ectx, errp);
        return NULL;
    }

    CO disk_size = get_disk_size(ret, &CO local_errp);
    qmp_result_free(ret);

    co_recurse(ret = qmp_ectx(coroutine, CO ectx, "{'execute': 'quit'}\n"));
    if (qmp_ectx_failed(CO ectx)) {
        qmp_ectx_unref(CO ectx, NULL);
        abort();
    }
    qmp_result_free(ret);
    qmp_ectx_unref(CO ectx, NULL);

    int iret;
    co_recurse(iret = qemu_launcher_wait_co(coroutine, this, 1000, NULL));
    if (iret < 0) {
        abort();
    }

    if (!CO disk_size) {
        g_propagate_error(errp, CO local_errp);
        return NULL;
    }

    return CO disk_size;

    co_end;
}

int _qemu_launcher_wait_co(Coroutine *coroutine, QemuLauncher *this, guint timeout, GError **errp) {
    int ret;

    co_begin(int, -1);

    if (!this->pid) {
        colod_error_set(errp, "qemu not running");
        return -1;
    }

    co_recurse(ret = colod_wait_co(coroutine, this->pid, timeout, errp));
    return ret;

    co_end;
}

int qemu_launcher_kill(QemuLauncher *this) {
    return kill(this->pid, SIGKILL);
}

ColodQmpState *_qemu_launcher_launch_primary(Coroutine *coroutine, QemuLauncher *this, GError **errp) {
    struct {
        MyArray *cmdline;
        ColodQmpState *qmp;
        QmpEctx *ectx;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpState *, NULL);

    CO cmdline = qmp_commands_get_qemu_primary(this->commands);
    co_recurse(CO qmp = qemu_launcher_launch_co(coroutine, this, CO cmdline, errp));
    if (!CO qmp) {
        return NULL;
    }
    CO ectx = qmp_ectx_new(CO qmp);

    CO cmdline = qmp_commands_get_prepare_primary(this->commands);
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO cmdline));
    my_array_unref(CO cmdline);
    if (qmp_ectx_failed(CO ectx)) {
        qmp_unref(CO qmp);
        qmp_ectx_unref(CO ectx, errp);
        return NULL;
    }

    qmp_ectx_unref(CO ectx, NULL);
    return CO qmp;

    co_end;
}

ColodQmpState *_qemu_launcher_launch_secondary(Coroutine *coroutine, QemuLauncher *this, GError **errp) {
    struct {
        MyArray *cmdline;
        ColodQmpState *qmp;
        QmpEctx *ectx;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(ColodQmpState *, NULL);
    if (!this->disk_size) {
        char *disk_size;
        co_recurse(disk_size = qemu_launcher_disk_size_co(coroutine, this, errp));
        if (!disk_size) {
            return NULL;
        }

        this->disk_size = disk_size;
    }

    CO cmdline = qmp_commands_cmdline(this->commands, NULL, this->disk_size,
                                      "@@QEMU_IMG_BINARY@@",
                                      "create", "-q", "-f", "qcow2",
                                      "@@ACTIVE_IMAGE@@",
                                      "@@DISK_SIZE@@",
                                      NULL);
    co_recurse(ret = colod_execute_sync_co(coroutine, CO cmdline, errp));
    if (ret != 0) {
        return NULL;
    }

    CO cmdline = qmp_commands_cmdline(this->commands, NULL, this->disk_size,
                                      "@@QEMU_IMG_BINARY@@",
                                      "create", "-q", "-f", "qcow2",
                                      "@@HIDDEN_IMAGE@@",
                                      "@@DISK_SIZE@@",
                                      NULL);
    co_recurse(ret = colod_execute_sync_co(coroutine, CO cmdline, errp));
    if (ret != 0) {
        return NULL;
    }

    CO cmdline = qmp_commands_get_qemu_secondary(this->commands);
    co_recurse(CO qmp = qemu_launcher_launch_co(coroutine, this, CO cmdline, errp));
    if (!CO qmp) {
        return NULL;
    }
    CO ectx = qmp_ectx_new(CO qmp);

    CO cmdline = qmp_commands_get_prepare_secondary(this->commands);
    co_recurse(qmp_ectx_array(coroutine, CO ectx, CO cmdline));
    my_array_unref(CO cmdline);
    if (qmp_ectx_failed(CO ectx)) {
        qmp_unref(CO qmp);
        qmp_ectx_unref(CO ectx, errp);
        return NULL;
    }

    qmp_ectx_unref(CO ectx, NULL);
    return CO qmp;

    co_end;
}

void qemu_launcher_set_disk_size(QemuLauncher *this, char *disk_size) {
    g_free(this->disk_size);
    this->disk_size = g_strdup(disk_size);
}

QemuLauncher *qemu_launcher_new(QmpCommands *commands, const char *base_dir, guint qmp_timeout) {
    QemuLauncher *this = g_rc_box_new0(QemuLauncher);
    this->commands = commands;
    this->base_dir = base_dir;
    this->qmp_timeout = qmp_timeout;

    return this;
}

static void qemu_launcher_free(gpointer data) {
    QemuLauncher *this = data;
    g_free(this->disk_size);
}

QemuLauncher *qemu_launcher_ref(QemuLauncher *this) {
    return g_rc_box_acquire(this);
}

void qemu_launcher_unref(QemuLauncher *this) {
    g_rc_box_release_full(this, qemu_launcher_free);
}
