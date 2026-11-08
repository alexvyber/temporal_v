module workflows

import sdk
import json2

struct CancelDetailsWorkflow {}

fn new_cancel_details_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return CancelDetailsWorkflow{}
}

fn (mut workflow CancelDetailsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {}
		.cancel_requested {
			context.cancel(
				details: sdk.Arguments([json2.encode(event.message), 'cancel 世界\x00\xff'])
			)!
		}
		else {
			return error('unexpected cancellation-details event')
		}
	}
}
