/*
 * COLO background daemon coroutine core
 *
 * Copyright (c) Lukas Straub <lukasstraub2@web.de>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

#ifndef COROUTINE_H
#define COROUTINE_H

#define CO co->

#define co_begin(yield_type, yield_ret) \
    yield_type coroutine_yield_ret = (yield_ret); \
    switch(co->co.line) { case 0:;

#define co_end \
    }; do {co->co.line = 0;} while(0)

#define _co_yield(value) \
        do { \
            co->co.line=__LINE__; \
            return (value); case __LINE__:; \
        } while (0)

#define _co_yieldV \
        do { \
            co->co.line=__LINE__; \
            return; case __LINE__:; \
        } while (0)

typedef struct CoroutineEntry {
    unsigned int line;
} CoroutineEntry;

typedef struct Coroutine Coroutine;

#endif /* COROUTINE_H */
