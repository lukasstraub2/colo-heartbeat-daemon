CC=gcc

CFLAGS=-g -O2 -Wall -Wextra -fsanitize=address `pkg-config --cflags glib-2.0 json-glib-1.0`
CPG_LDFLAGS=-lcorosync_common -lcpg
LDFLAGS=`pkg-config --libs glib-2.0 json-glib-1.0`

%.o: %.c *.h
	$(CC) -c -o $@ $< $(CFLAGS)

colod: util.o qemu_util.o json_util.o coutil.o qmp.o client.o watchdog.o cpg.o main_coroutine.o daemon.o
	$(CC) -o $@ $^ $(CFLAGS) $(CPG_LDFLAGS) $(LDFLAGS)

.PHONY: clean

clean:
	rm -f *.o colod
