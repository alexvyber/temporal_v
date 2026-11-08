module main

import sync
import temporal_v.sdk
import time

fn C.temporal_v_recovery_trace() &char
fn C.free(voidptr)

fn captured_recovery_trace() string {
	trace := C.temporal_v_recovery_trace()
	assert trace != unsafe { nil }
	defer { unsafe { C.free(trace) } }
	return unsafe { trace.vstring().clone() }
}

struct StackOutcome {
	message string
	trace   string
}

fn sample_failure() StackOutcome {
	return StackOutcome{
		message: 'callback panic'
		trace:   captured_recovery_trace()
	}
}

fn check_stacks(mut group sync.WaitGroup) {
	defer { group.done() }
	for _ in 0 .. 100 {
		first := captured_recovery_trace()
		second := captured_recovery_trace()
		assert first.starts_with('Recovered panic; recovery boundary stack')
		assert first.len <= 16384
		assert second.len > 0
	}
}

fn test_recovery_traces_are_owned_across_threads() {
	mut group := sync.new_waitgroup()
	group.add(8)
	for _ in 0 .. 8 {
		spawn check_stacks(mut group)
	}
	group.wait()
}

fn test_panic_stack_survives_plain_and_protected_failure_conversion() ! {
	result := sample_failure()
	for protected in [false, true] {
		converter := sdk.DataConverter{
			failure_converter: sdk.DefaultFailureConverter{
				encode_common_attributes: protected
			}
		}
		failure := sdk.PanicError{
			message:     result.message
			stack_trace: result.trace
		}
		encoded := converter.encode_failure(failure, 'ignored')!
		if protected {
			assert encoded.stack_trace == ''
		} else {
			assert encoded.stack_trace == result.trace
		}
		decoded := converter.decode_failure(encoded)
		assert decoded.message == result.message
		assert decoded.stack_trace == result.trace
		assert decoded.category == 'PanicError'
	}
}

struct PanicTraceExporter {
	records chan []sdk.TraceSpanData
}

fn (exporter PanicTraceExporter) export(context &sdk.TraceExportContext, spans []sdk.TraceSpanData) ! {
	exporter.records <- spans
}

fn test_panic_stack_reaches_trace_exception_event() ! {
	result := sample_failure()
	records := chan []sdk.TraceSpanData{cap: 2}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter: PanicTraceExporter{
			records: records
		}
	})!
	defer { provider.shutdown(time.second) or {} }
	span := provider.start_span('panic-stack', sdk.TraceSpanOptions{})!
	span.record_exception('PanicError', sdk.PanicError{
		message:     result.message
		stack_trace: result.trace
	})!
	span.finish(sdk.TraceEndOptions{})
	provider.force_flush(time.second)!
	spans := <-records
	assert spans.len == 1
	assert spans[0].events.len == 1
	attribute := spans[0].events[0].attributes['exception.stacktrace'] or {
		return error('missing original panic stack attribute')
	}
	assert attribute is string
	assert attribute == result.trace
}
