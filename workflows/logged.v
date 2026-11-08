module workflows

import alexvyber.temporal_v.sdk
import time

struct LoggedWorkflow {
mut:
	result string
}

fn new_logged_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return LoggedWorkflow{}
}

fn (mut workflow LoggedWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			context.log(.info, 'workflow user started', {
				'test': 'observability'
			})
			context.activity('v.echo', '"metrics"', sdk.ActivityOptions{})!
		}
		.activity_completed {
			workflow.result = event.data
			context.timer(3 * time.second)!
		}
		.timer_fired {
			context.log(.info, 'workflow user completed', {})
			context.complete(workflow.result)!
		}
		else {
			return error('unexpected logged workflow event')
		}
	}
}
