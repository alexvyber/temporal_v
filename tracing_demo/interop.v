module main

import temporal_v.sdk
import os
import x.json2

struct TraceInteropWorkflow {
mut:
	stage int
}

fn trace_interop_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return TraceInteropWorkflow{}
}

fn (mut workflow TraceInteropWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	propagation := context.propagation_context()
	span := propagation.span or { return error('V interop trace context missing') }
	member := propagation.baggage.member('tenant') or { return error('V interop baggage missing') }
	if span.trace_id != os.getenv('V_TRACE_ID') || member.value != 'interop' {
		return error('V interop context changed')
	}
	if event.kind == .signal_received {
		if event.name == 'ready' { workflow.stage++ }
		if event.name == 'finish' { context.complete(sdk.PayloadValue('"interop"'))! }
	}
}

fn (workflow &TraceInteropWorkflow) query_names() []string {
	return ['state']
}

fn (workflow &TraceInteropWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.stage))
}
