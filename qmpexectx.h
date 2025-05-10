#pragma once

#include <glib-2.0/glib.h>
#include "qmp.h"

typedef struct QmpEctx QmpEctx;

void qmp_ectx_set_ignore_qmp_error(QmpEctx *this);
gboolean qmp_ectx_get_ignore_qmp_error(QmpEctx *this);
void qmp_ectx_set_ignore_yank(QmpEctx *this);
gboolean qmp_ectx_get_ignore_yank(QmpEctx *this);
void qmp_ectx_set_interrupt_cb(QmpEctx *this, GSourceFunc cb, gpointer user_data);

// returns true if something happened and it wasn't ignored
gboolean qmp_ectx_failed(QmpEctx *this);

// always returns true if any error or yank happened, regardless of ignore_errors
gboolean qmp_ectx_did_any(QmpEctx *this);
gboolean qmp_ectx_did_yank(QmpEctx *this);
gboolean qmp_ectx_did_error(QmpEctx *this);
gboolean qmp_ectx_did_qmp_error(QmpEctx *this);
gboolean qmp_ectx_did_interrupt(QmpEctx *this);
void qmp_ectx_get_error(QmpEctx *this, GError **errp);

#define qmp_ectx_log_error(this) _qmp_ectx_log_error(this, __func__, __LINE__)
void _qmp_ectx_log_error(QmpEctx *this, const gchar *func, int line);

#define qmp_ectx(...) co_wrap(_qmp_ectx(__VA_ARGS__))
ColodQmpResult *_qmp_ectx(Coroutine *coroutine, QmpEctx *this, const gchar *command);

#define qmp_ectx_array(...) co_wrap(_qmp_ectx_array(__VA_ARGS__))
int _qmp_ectx_array(Coroutine *coroutine, QmpEctx *this, MyArray *array);

#define qmp_ectx_yank(...) co_wrap(_qmp_ectx_yank(__VA_ARGS__))
int _qmp_ectx_yank(Coroutine *coroutine, QmpEctx *this);

QmpEctx *qmp_ectx_new(ColodQmpState *qmp);
QmpEctx *qmp_ectx_ref(QmpEctx *this);
void qmp_ectx_unref(QmpEctx *this, GError **errp);
