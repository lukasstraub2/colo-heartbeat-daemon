
#include <glib-2.0/glib.h>

#include "qemulauncher.h"
#include "qmp.h"

struct QemuLauncher {
    int dummy;
    int qmp_timeout;
};

static int qmp_fd, qmp_yank_fd;

void qemu_launcher_stub_set_fd(int _qmp_fd, int _qmp_yank_fd) {
    qmp_fd = _qmp_fd;
    qmp_yank_fd = _qmp_yank_fd;
}

int _qemu_launcher_wait_co(Coroutine *coroutine, QemuLauncher *this, guint timeout, GError **errp) {
    (void) coroutine;
    (void) this;
    (void) timeout;
    (void) errp;
    return 0;
}

int qemu_launcher_kill(QemuLauncher *this) {
    (void) this;
    return 0;
}

ColodQmpState *_qemu_launcher_launch_primary(Coroutine *coroutine, QemuLauncher *this, GError **errp) {
    (void) coroutine;
    (void) errp;

    return qmp_new(qmp_fd, qmp_yank_fd, this->qmp_timeout, errp);
}

ColodQmpState *_qemu_launcher_launch_secondary(Coroutine *coroutine, QemuLauncher *this, GError **errp) {
    (void) coroutine;
    (void) errp;

    return qmp_new(qmp_fd, qmp_yank_fd, this->qmp_timeout, errp);
}

void qemu_launcher_set_disk_size(QemuLauncher *this, char *disk_size) {
    (void) this;
    (void) disk_size;
}

QemuLauncher *qemu_launcher_new(QmpCommands *commands, const char *base_dir,
                                guint qmp_timeout) {
    (void) commands;
    (void) base_dir;
    QemuLauncher *this = g_rc_box_new0(QemuLauncher);
    this->qmp_timeout = qmp_timeout;
    return this;
}

QemuLauncher *qemu_launcher_ref(QemuLauncher *this) {
    return g_rc_box_acquire(this);
}

void qemu_launcher_unref(QemuLauncher *this) {
    g_rc_box_release(this);
}
