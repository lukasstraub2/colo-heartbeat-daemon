
#include <glib-2.0/glib.h>
#include "coroutine_stack.h"
#include "util.h"
#include "coutil.h"
#include "cluster_resource.h"

int _cluster_resource_stop(Coroutine *coroutine, const char *instance_name, GError **errp) {
    struct {
        MyArray *cmdline;
    } *co;
    int ret;

    co_frame(co, sizeof(*co));
    co_begin(int, -1);

    CO cmdline = my_array_new(NULL);
    my_array_append(CO cmdline, "crm_resource");
    my_array_append(CO cmdline, "--resource");
    my_array_append(CO cmdline, (void *) instance_name);
    my_array_append(CO cmdline, "--set-parameter");
    my_array_append(CO cmdline, "target-role");
    my_array_append(CO cmdline, "--meta");
    my_array_append(CO cmdline, "--parameter-value");
    my_array_append(CO cmdline, "Stopped");
    my_array_append(CO cmdline, NULL);
    co_recurse(ret = colod_execute_sync_co(coroutine, CO cmdline, errp));

    return ret;
    co_end;
}
