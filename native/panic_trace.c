#include "panic_trace.h"
#include <stdlib.h>
#include <string.h>
#if defined(__linux__) || defined(__APPLE__)
#include <execinfo.h>
#endif

/* Upstream V recovers panics but does not expose the discarded panic-site stack.
 * Capture the surviving recovery boundary without relying on compiler internals. */
char *temporal_v_recovery_trace(void) {
#if defined(__linux__) || defined(__APPLE__)
    void *frames[64];
    int count = backtrace(frames, 64);
    char **symbols = count > 0 ? backtrace_symbols(frames, count) : NULL;
    if (!symbols) return NULL;
    const char prefix[] = "Recovered panic; recovery boundary stack (not original panic-site stack):\n";
    const size_t capacity = 16385;
    char *result = malloc(capacity);
    if (result) {
        char *cursor = result;
        memcpy(cursor, prefix, sizeof(prefix) - 1);
        cursor += sizeof(prefix) - 1;
        for (int index = 0; index < count; index++) {
            size_t remaining = capacity - 1 - (size_t)(cursor - result);
            if (remaining < 2) break;
            size_t size = strnlen(symbols[index], remaining - 1);
            memcpy(cursor, symbols[index], size);
            cursor += size;
            *cursor++ = '\n';
        }
        *cursor = '\0';
    }
    free(symbols);
    return result;
#else
    return NULL;
#endif
}
