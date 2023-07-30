
#include <stdio.h>
#include <glib-2.0/glib.h>

#include "coroutine.h"
#include "coroutine_stack.h"

int myco(Coroutine *coroutine) {

    co_begin(int, -1);

    while (1) {
        co_yield_int(0);
        break;
    }

    fprintf(stdout, "Before yield\n");
    co_yield_int(0);
    fprintf(stdout, "After yield\n");

    co_end;

    return 0;
}

int main(int argc, char *argv[])
{
    Coroutine coroutine_stack = {0};
    Coroutine *coroutine = &coroutine_stack;
    int ret;

again:
    fprintf(stdout, "Before co_enter\n");
    co_enter(ret, coroutine, myco);
    if (coroutine->yield) {
        fprintf(stdout, "Yielded!\n");
        goto again;
    }

    fprintf(stderr, "Fell trough!\n");
    return 1;
}
