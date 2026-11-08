module main

import sync
import sdk
import time

struct StackOutcome {
mut:
	message string
	trace   string
}

@[noinline]
fn callback_leaf() {
	panic('original callback panic')
}

@[noinline]
fn callback_middle() {
	callback_leaf()
}

fn guarded_stack(mut result StackOutcome) {
	defer {
		if message := recover() {
			result.message = message
			result.trace = recovered_panic_stack_trace()
		}
	}
	callback_middle()
}

fn nested_stack(mut result StackOutcome) {
	defer {
		if message := recover() {
			result.message = message
			result.trace = recovered_panic_stack_trace()
		}
	}
	mut original := StackOutcome{}
	guarded_stack(mut original)
	assert original.trace.contains('callback_leaf')
	panic('nested panic')
}

fn check_stacks(mut group sync.WaitGroup) {
	defer { group.done() }
	for _ in 0 .. 100 {
		assert recovered_panic_stack_trace() == ''
		mut result := StackOutcome{}
		guarded_stack(mut result)
		assert result.message == 'original callback panic'
		assert result.trace.contains('callback_leaf'), result.trace
		assert result.trace.contains('callback_middle'), result.trace
		assert result.trace.len < 16384
		assert recovered_panic_stack_trace() == ''
		mut nested := StackOutcome{}
		nested_stack(mut nested)
		assert nested.message == 'nested panic'
		assert nested.trace.len > 0
		assert !nested.trace.contains('callback_leaf')
	}
}

fn test_original_panic_stacks_are_owned_and_thread_local() {
	mut group := sync.new_waitgroup()
	group.add(8)
	for _ in 0 .. 8 {
		spawn check_stacks(mut group)
	}
	group.wait()
}

fn test_panic_stack_survives_plain_and_protected_failure_conversion() ! {
	mut result := StackOutcome{}
	guarded_stack(mut result)
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
	mut result := StackOutcome{}
	guarded_stack(mut result)
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
