
#include <glib-2.0/glib.h>

#include "coroutine_stack.h"
#include "cluster_resource.h"

int _cluster_resource_stop(Coroutine *coroutine, const char *instance_name, GError **errp) {
    (void) coroutine;
    (void) instance_name;
    (void) errp;
    abort();
}
