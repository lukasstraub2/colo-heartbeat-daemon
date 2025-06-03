#ifndef CPG_C
#define CPG_C

#include "daemon.h"

typedef enum ColodMessage {
    MESSAGE_NONE,
    MESSAGE_FAILOVER,
    MESSAGE_FAILED,
    MESSAGE_HELLO,
    MESSAGE_YELLOW,
    MESSAGE_UNYELLOW,
    MESSAGE_SHUTDOWN_REQUEST,
    MESSAGE_SHUTDOWN,
    MESSAGE_SHUTDOWN_DONE,
    MESSAGE_REBOOT,
    MESSAGE_REBOOT_RESTART,
    MESSAGE_MAX
} ColodMessage;

typedef void (*CpgCallback)(gpointer user_data, ColodMessage message,
                            gboolean message_from_this_node,
                            gboolean peer_left_group);

void colod_cpg_add_notify(Cpg *this, CpgCallback _func, gpointer user_data);
void colod_cpg_del_notify(Cpg *this, CpgCallback _func, gpointer user_data);

void colod_cpg_stub_notify(Cpg *this, ColodMessage message,
                           gboolean message_from_this_node,
                           gboolean peer_left_group);

void colod_cpg_send(Cpg *cpg, uint32_t message);
Cpg *colod_open_cpg(ColodContext *ctx, GError **errp);
Cpg *cpg_new(Cpg *cpg, GError **errp);
Cpg *cpg_ref(Cpg *this);
void cpg_unref(Cpg *this);

#endif // CPG_C
