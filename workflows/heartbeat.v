module workflows

import alexvyber.temporal_v.sdk
import time

struct HeartbeatWorkflow {}

fn new_heartbeat_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return HeartbeatWorkflow{}
}

fn (mut workflow HeartbeatWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			context.activity('v.heartbeat-state', sdk.Arguments(sdk.TypedArguments{}), sdk.ActivityOptions{
				start_to_close:    10 * time.second
				heartbeat_timeout: 500 * time.millisecond
				retry:             sdk.RetryPolicy{
					initial_interval: 100 * time.millisecond
					maximum_attempts: 3
				}
			})!
		}
		.activity_completed {
			context.complete(event.data)!
		}
		.activity_failed {
			return event.failure or { return error(event.message) }
		}
		else {}
	}
}
