#!/bin/bash

set -e

CFLAGS="-g -Wall -Wextra -pedantic -Wno-unused-function -fsanitize=address"
CFLAGS="${CFLAGS} `pkg-config --cflags glib-2.0`"

LDFLAGS="-lcorosync_common -lcpg"
LDFLAGS="${LDFLAGS} `pkg-config --libs glib-2.0`"

FILES="util.c qemu_util.c coutil.c daemon.c"

gcc $CFLAGS $FILES -o colod $LDFLAGS
