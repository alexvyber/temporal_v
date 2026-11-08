module main

import temporal_v.sdk
import os

struct BaggageQueryInterceptor {}

fn (interceptor BaggageQueryInterceptor) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.QueryNext) !sdk.PayloadValue {
	propagation := next.propagation_context()
	span := propagation.span or { return error('query propagation context missing') }
	if span.trace_id != os.getenv('V_TRACE_ID') || span.remote
		|| propagation.baggage.header() != os.getenv('V_TRACE_BAGGAGE') {
		return error('query propagation context changed')
	}
	return next.execute(name, input)!
}

struct BaggageValidatorInterceptor {}

fn (interceptor BaggageValidatorInterceptor) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.UpdateValidatorNext) ! {
	propagation := next.propagation_context()
	span := propagation.span or { return error('validator propagation context missing') }
	expected := os.getenv_opt('V_TRACE_BAGGAGE_UPDATE') or { os.getenv('V_TRACE_BAGGAGE') }
	if span.trace_id != os.getenv('V_TRACE_ID') || span.remote
		|| propagation.baggage.header() != expected {
		return error('validator propagation context changed')
	}
	next.execute(name, input)!
}
