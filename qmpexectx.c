
#include <glib-2.0/glib.h>
#include <assert.h>

#include "qmpexectx.h"
#include "coroutine_stack.h"
#include "daemon.h"

struct QmpEctx {
    ColodQmpState *qmp;
    GError *errp;
    GError *qmp_errp;
    gboolean did_yank;
    gboolean did_error;
    gboolean did_qmp_error;
    gboolean did_interrupt;

    gboolean ignore_yank;
    gboolean ignore_qmp_error;
    gboolean unchecked;

    GSourceFunc cb;
    gpointer cb_data;
};

void qmp_ectx_set_ignore_qmp_error(QmpEctx *this) {
    this->ignore_qmp_error = TRUE;
}

gboolean qmp_ectx_get_ignore_qmp_error(QmpEctx *this) {
    return this->ignore_qmp_error;
}

// ignore qmp errors and yank like colod_execute_array_co
void qmp_ectx_set_ignore_yank(QmpEctx *this) {
    this->ignore_yank = TRUE;
}

gboolean qmp_ectx_get_ignore_yank(QmpEctx *this) {
    return this->ignore_yank;
}

void qmp_ectx_set_interrupt_cb(QmpEctx *this, GSourceFunc cb, gpointer user_data) {
    this->cb = cb;
    this->cb_data = user_data;
}

// returns true if something happened and it wasn't ignored
gboolean qmp_ectx_failed(QmpEctx *this) {
    this->unchecked = FALSE;
    return  (!this->ignore_yank && this->did_yank) || this->did_error
            || (!this->ignore_qmp_error && this->did_qmp_error) || this->did_interrupt;
}

// always returns true if any error or yank happened, regardless of ignore_errors
gboolean qmp_ectx_did_any(QmpEctx *this) {
    this->unchecked = FALSE;
    return this->did_yank || this->did_error || this->did_qmp_error || this->did_interrupt;
}

gboolean qmp_ectx_did_yank(QmpEctx *this) {
    this->unchecked = FALSE;
    return this->did_yank;
}

gboolean qmp_ectx_did_error(QmpEctx *this) {
    this->unchecked = FALSE;
    return this->did_error;
}

gboolean qmp_ectx_did_qmp_error(QmpEctx *this) {
    this->unchecked = FALSE;
    return this->did_qmp_error;
}

gboolean qmp_ectx_did_interrupt(QmpEctx *this) {
    this->unchecked = FALSE;
    return this->did_interrupt;
}

void qmp_ectx_get_error(QmpEctx *this, GError **errp) {
    GError *local_errp = NULL;

    this->unchecked = FALSE;

    if (this->errp) {
        local_errp = g_error_copy(this->errp);
    } else if (this->qmp_errp) {
        local_errp = g_error_copy(this->qmp_errp);
    } else {
        if (this->did_yank) {
            colod_error_set(&local_errp, "did yank");
        } else if (this->did_interrupt) {
            colod_error_set(&local_errp, "did interrupt");
        } else {
            return;
        }
    }

    g_propagate_error(errp, local_errp);
}

void _qmp_ectx_log_error(QmpEctx *this, const gchar *func, int line) {
    GError *local_errp = NULL;

    qmp_ectx_get_error(this, &local_errp);
    assert(local_errp);
    colod_syslog(LOG_ERR, "%s:%u: %s", func, line, local_errp->message);
    g_error_free(local_errp);
}

#define qmp_ectx(...) co_wrap(_qmp_ectx(__VA_ARGS__))
ColodQmpResult *_qmp_ectx(Coroutine *coroutine, QmpEctx *this, const gchar *command) {
    GError *local_errp = NULL;
    ColodQmpResult *result;

    co_begin(ColodQmpResult *, NULL);

    this->unchecked = TRUE;

    if (this->cb) {
        this->did_interrupt |= this->cb(this->cb_data);
    }

    if (qmp_ectx_failed(this)) {
        return NULL;
    }

    co_recurse(result = qmp_execute_co(coroutine, this->qmp, &local_errp, command));
    if (!result) {
        if (g_error_matches(local_errp, COLOD_ERROR, COLOD_ERROR_QMP)) {
            if (!this->qmp_errp) {
                this->qmp_errp = g_error_copy(local_errp);
                this->did_qmp_error = TRUE;
            }

            if (!qmp_ectx_failed(this)) {
                colod_syslog(LOG_WARNING, "Ignoring qmp error: %s", local_errp->message);
            }
            g_error_free(local_errp);
            return NULL;
        }

        if (!this->errp) {
            this->errp = g_error_copy(local_errp);
            this->did_error = TRUE;
        }

        g_error_free(local_errp);
        return NULL;
    }

    this->did_yank |= result->did_yank;

    return result;

    co_end;
}

int _qmp_ectx_yank(Coroutine *coroutine, QmpEctx *this) {
    GError *local_errp = NULL;

    co_begin(int, -1);

    assert(this->ignore_yank);
    this->did_yank = TRUE;

    int ret;
    co_recurse(ret = qmp_yank_co(coroutine, this->qmp, &local_errp));
    if (ret < 0) {
        if (!this->errp) {
            this->errp = g_error_copy(local_errp);
            this->did_error = TRUE;
        }
        g_error_free(local_errp);
    }

    return 0;

    co_end;
}

#define qmp_ectx_array(...) co_wrap(_qmp_ectx_array(__VA_ARGS__))
int _qmp_ectx_array(Coroutine *coroutine, QmpEctx *this, MyArray *array) {
    struct {
        int i;
    } *co;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    for (CO i = 0; CO i < array->size; CO i++) {
        ColodQmpResult *result;
        co_recurse(result = qmp_ectx(coroutine, this, array->array[CO i]));
        qmp_result_free(result);
    }

    return 0;

    co_end;
}

QmpEctx *qmp_ectx_new(ColodQmpState *qmp) {
    QmpEctx *this = g_rc_box_new0(QmpEctx);
    this->qmp = qmp_ref(qmp);
    return this;
}

void qmp_ectx_free(gpointer data) {
    QmpEctx *this = data;
    assert(!this->unchecked);

    qmp_unref(this->qmp);
    if (this->errp) {
        g_error_free(this->errp);
    }
    if (this->qmp_errp) {
        g_error_free(this->qmp_errp);
    }
}

QmpEctx *qmp_ectx_ref(QmpEctx *this) {
    return g_rc_box_acquire(this);
}

void qmp_ectx_unref(QmpEctx *this, GError **errp) {
    if (errp) {
        qmp_ectx_get_error(this, errp);
    }
    g_rc_box_release_full(this, qmp_ectx_free);
}
