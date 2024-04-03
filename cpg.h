#ifndef CPG_C
#define CPG_C

#include "daemon.h"

typedef enum ColodMessage {
    MESSAGE_FAILOVER,
    MESSAGE_FAILED
} ColodMessage;

void colod_cpg_send(Cpg *cpg, uint32_t message);
Cpg *colod_open_cpg(ColodContext *ctx, GError **errp);
Cpg *cpg_new(Cpg *cpg, GError **errp);
void cpg_free(Cpg *cpg);

#endif // CPG_C
