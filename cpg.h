#ifndef CPG_C
#define CPG_C

#include "daemon.h"

typedef enum ColodMessage {
    MESSAGE_FAILOVER,
    MESSAGE_FAILED
} ColodMessage;

void colod_cpg_send(ColodContext *ctx, uint32_t message);
int colod_open_cpg(ColodContext *ctx, GError **errp);
guint cpg_new(cpg_handle_t handle, ColodContext *ctx, GError **errp);
void cpg_free(guint source_id);

#endif // CPG_C
