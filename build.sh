#!/bin/bash

set -e

CFLAGS="-g -O2 -Wall -Wextra -fsanitize=address"
CFLAGS="${CFLAGS} `pkg-config --cflags glib-2.0 json-glib-1.0`"

LDFLAGS="-lcorosync_common -lcpg"
LDFLAGS="${LDFLAGS} `pkg-config --libs glib-2.0 json-glib-1.0`"

FILES="util.c qemu_util.c json_util.c coutil.c qmp.c client.c daemon.c"

gcc $CFLAGS $FILES -o colod $LDFLAGS
