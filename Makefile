CC=gcc

CFLAGS=-g -O2 -Wall -Wextra -fsanitize=address `pkg-config --cflags libnl-3.0 glib-2.0 json-glib-1.0`
CPG_LDFLAGS=-lcorosync_common -lcpg
LDFLAGS=`pkg-config --libs libnl-3.0 glib-2.0 json-glib-1.0`
common_objects=util.o qemu_util.o json_util.o coutil.o qmp.o client.o watchdog.o qmpcommands.o main_coroutine.o daemon.o

%.o: %.c *.h
	$(CC) -c -o $@ $< $(CFLAGS)

all: colod smoketest_quit_early io_watch_test netlink_test

colod: $(common_objects) cpg.o colod.o
	$(CC) -o $@ $^ $(CFLAGS) $(CPG_LDFLAGS) $(LDFLAGS)

smoketest_quit_early: $(common_objects) stub_cpg.o smoke_util.o smoketest_quit_early.o smoketest.o
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

io_watch_test: util.o io_watch_test.o
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

netlink_test: util.o netlink.o netlink_test.o
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

.PHONY: clean

clean:
	rm -f *.o colod smoketest_quit_early io_watch_test netlink_test
