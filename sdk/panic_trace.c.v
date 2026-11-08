module sdk

#flag @DIR/../native/panic_trace.c
#include "@DIR/../native/panic_trace.h"

fn C.temporal_v_recovery_trace() &char
fn C.free(voidptr)

fn recovery_stack_trace() string {
	trace := C.temporal_v_recovery_trace()
	if trace == unsafe { nil } { return '' }
	defer { unsafe { C.free(trace) } }
	return unsafe { trace.vstring().clone() }
}
