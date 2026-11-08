module workflows

import temporal_v.sdk
import x.json2
import temporal_v.temporal
import time

struct ActivityMetadataWorkflow {
mut:
	result string
}

fn new_activity_metadata(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ActivityMetadataWorkflow{}
}

fn (mut workflow ActivityMetadataWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			local := json2.decode[bool](event.data)!
			retry := sdk.RetryPolicy{
				initial_interval: 500 * time.millisecond
				maximum_attempts: 2
			}
			if local {
				context.local_activity('v.attempt', '{"fail_until":1}', sdk.LocalActivityOptions{
					activity_id:           'custom-local'
					summary:               'local retry summary 世界'
					start_to_close:        20 * time.second
					schedule_to_close:     60 * time.second
					local_retry_threshold: time.millisecond
					retry:                 retry
				})!
			} else {
				context.activity('v.attempt', '{}', sdk.ActivityOptions{
					activity_id:       'custom-remote'
					summary:           'remote summary 世界'
					start_to_close:    20 * time.second
					schedule_to_close: 60 * time.second
					heartbeat_timeout: 3 * time.second
					priority:          temporal.TemporalApiCommonV1_Priority{
						priority_key:    3
						fairness_key:    'tenant-a'
						fairness_weight: 2
					}
					retry:             retry
				})!
			}
		}
		.activity_completed {
			workflow.result = event.data
			context.timer(3 * time.second, sdk.TimerOptions{ summary: 'result ready 世界' })!
		}
		.timer_fired {
			context.complete(workflow.result)!
		}
		.activity_failed {
			return event.failure or { return error(event.message) }
		}
		else {}
	}
}
