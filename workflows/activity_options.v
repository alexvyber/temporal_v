module workflows

import x.json2
import alexvyber.temporal_v.sdk
import time

struct ActivityInput {
	local            bool
	fail_until       u32
	non_retryable    bool
	delay_ms         int
	maximum_attempts int = 3
	retry_ms         i64 = 100
	threshold_ms     i64 = 1000
	cancel_after_ms  i64
}

struct ActivityOptionsWorkflow {
mut:
	sequence       u32
	cancel_timer   u32
	cancel_trigger u32
	result         string
}

fn new_activity_options(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ActivityOptionsWorkflow{}
}

fn (mut workflow ActivityOptionsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[ActivityInput](event.data)!
			retry := sdk.RetryPolicy{
				maximum_attempts: input.maximum_attempts
				initial_interval: input.retry_ms * time.millisecond
			}
			argument := json2.encode(input)
			if input.local {
				workflow.sequence = context.local_activity('v.attempt', argument, sdk.LocalActivityOptions{
					start_to_close:        30 * time.second
					retry:                 retry
					local_retry_threshold: input.threshold_ms * time.millisecond
				})!
			} else {
				workflow.sequence = context.activity('v.attempt', argument, sdk.ActivityOptions{
					retry:        retry
					cancellation: .wait_completed
				})!
			}
			if input.cancel_after_ms > 0 {
				if input.local && input.delay_ms > 0 {
					workflow.cancel_trigger = context.local_activity('v.delay', json2.encode({
						'delay_ms': input.cancel_after_ms
					}), sdk.LocalActivityOptions{})!
				} else {
					workflow.cancel_timer = context.timer(input.cancel_after_ms * time.millisecond)!
				}
			}
		}
		.activity_completed {
			if event.sequence == workflow.cancel_trigger {
				context.cancel_activity(workflow.sequence)!
				return
			}
			workflow.result = event.data
			if workflow.cancel_timer > 0 { context.cancel_timer(workflow.cancel_timer)! }
			context.timer(3 * time.second)!
		}
		.activity_failed {
			if failure := event.failure { return error(failure.root_message()) }
			return error(event.message)
		}
		.activity_cancelled {
			context.complete('"activity cancelled"')!
		}
		.timer_fired {
			if event.sequence == workflow.cancel_timer {
				if !context.cancel_activity(workflow.sequence)! {
					return error('Activity was not pending')
				}
				if context.cancel_activity(workflow.sequence)! {
					return error('Activity cancellation was not idempotent')
				}
			} else {
				context.complete(workflow.result)!
			}
		}
		else {
			return error('unexpected Activity options event')
		}
	}
}

struct DefaultRetryWorkflow {}

fn new_default_retry_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return DefaultRetryWorkflow{}
}

fn (mut workflow DefaultRetryWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[ActivityInput](event.data)!
			retry := sdk.RetryPolicy{
				initial_interval: 100 * time.millisecond
			}
			if input.local {
				context.local_activity('v.attempt', event.data, sdk.LocalActivityOptions{
					retry: retry
				})!
			} else {
				context.activity('v.attempt', event.data, sdk.ActivityOptions{ retry: retry })!
			}
		}
		.activity_completed {
			context.complete(event.data)!
		}
		.activity_failed {
			return error(event.message)
		}
		else {
			return error('unexpected default retry event')
		}
	}
}
