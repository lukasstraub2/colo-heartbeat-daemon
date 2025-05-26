#pragma once

#include <glib-2.0/glib.h>

#include "coroutine_stack.h"

#define cluster_resurce_stop(...) co_wrap(_cluster_resource_stop(__VA_ARGS__))
int _cluster_resource_stop(Coroutine *coroutine, const char *instance_name, GError **errp);
