module workflows

import sdk
import time

struct AsyncWorkflow {
mut:
	activity u32
	result   string
}

fn new_async_activity(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return AsyncWorkflow{}
}

fn (mut workflow AsyncWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			workflow.activity = context.activity('v.async', event.data, sdk.ActivityOptions{
				start_to_close:    time.minute
				heartbeat_timeout: 30 * time.second
				cancellation:      .wait_completed
			})!
		}
		.signal_received {
			if event.name == 'cancel' {
				context.cancel_activity(workflow.activity)!
			} else {
				return error('unknown signal')
			}
		}
		.activity_completed {
			workflow.result = event.data
			context.timer(time.second)!
		}
		.activity_failed {
			if failure := event.failure { return error(failure.root_message()) }
			return error(event.message)
		}
		.activity_cancelled {
			context.complete('"async cancelled"')!
		}
		.timer_fired {
			context.complete(workflow.result)!
		}
		else {
			return error('unexpected async Activity workflow event')
		}
	}
}
