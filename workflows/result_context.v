module workflows

import temporal_v.sdk
import x.json2

struct ResultContextState {
	input  sdk.SerializationContext
	result sdk.SerializationContext
}

struct ResultContextWorkflow {}

fn new_result_context_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ResultContextWorkflow{}
}

fn (mut workflow ResultContextWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		context.complete(json2.encode(ResultContextState{
			input:  context.serialization_context()
			result: context.result_serialization_context()
		}))!
	} else if event.kind == .cancel_requested {
		context.cancel()!
	} else {
		return error('unexpected result-context event')
	}
}
