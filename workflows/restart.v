module workflows

import x.json2
import temporal_v.sdk
import temporal_v.example_activities
import os
import time

struct RestartInput {
	name          string
	delay_seconds i64
}

struct Restart {
mut:
	first string
	delay i64
}

fn new_restart(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return Restart{}
}

fn (mut workflow Restart) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[RestartInput](event.data)!
			if input.delay_seconds < 1 || input.delay_seconds > 60 {
				return error('delay_seconds must be between 1 and 60')
			}
			workflow.delay = input.delay_seconds
			context.activity('v.echo', json2.encode(input.name), sdk.ActivityOptions{})!
		}
		.activity_completed {
			if event.sequence == 1 {
				workflow.first = event.data
				context.timer(workflow.delay * time.second)!
			} else {
				context.complete(event.data)!
			}
		}
		.timer_fired {
			context.activity('v.echo', workflow.first, sdk.ActivityOptions{})!
		}
		.activity_failed {
			return error('echo failed: ${event.message}')
		}
		.activity_cancelled {
			return error('echo cancelled')
		}
		.signal_received {
			return error('restart workflow does not accept signals')
		}
		else {
			return error('unsupported restart event')
		}
	}
}

struct ParallelInput {
	wait_for_cancel bool
	name            string
	delay_ms        int = 500
	fail_activity   bool
	fail_workflow   bool
	timeout_seconds i64 = 10
}

struct DelayInput {
	name     string
	delay_ms int
	fail     bool
}

struct Parallel {
mut:
	wait_for_cancel bool
	results         []string
}

fn new_parallel(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return Parallel{}
}

fn (mut workflow Parallel) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[ParallelInput](event.data)!
			workflow.wait_for_cancel = input.wait_for_cancel
			if input.fail_workflow { return error('requested workflow failure') }
			for index in 0 .. 2 {
				context.activity('v.delay', json2.encode(DelayInput{
					name:     '${input.name}-${index}'
					delay_ms: input.delay_ms
					fail:     input.fail_activity && index == 0
				}), sdk.ActivityOptions{
					start_to_close: input.timeout_seconds * time.second
					retry:          sdk.RetryPolicy{
						maximum_attempts: 1
					}
				})!
			}
		}
		.activity_completed {
			workflow.results << json2.decode[string](event.data)!
			if workflow.results.len == 2 && !workflow.wait_for_cancel {
				workflow.results.sort()
				context.complete(json2.encode(workflow.results))!
			}
		}
		.activity_failed {
			return error('Activity failed: ${event.message}')
		}
		.activity_cancelled {
			return error('Activity cancelled')
		}
		else {
			return error('unexpected timer')
		}
	}
}

struct Wait {}

fn new_wait(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return Wait{}
}

fn (mut workflow Wait) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		context.timer(60 * time.second)!
	} else if event.kind == .timer_fired {
		context.complete('"timer finished"')!
	}
}

pub fn register(mut runtime sdk.Runtime) ! {
	runtime.register('v.clock', new_recorded_clock)!
	runtime.register('v.update-context', new_update_context)!
	for name in ['v.nondeterminism.selected', 'v.nondeterminism.other'] {
		runtime.register(name, new_nondeterminism_fixture)!
	}
	runtime.register('v.result-context', new_result_context_workflow)!
	runtime.register('v.cancel-details', new_cancel_details_workflow)!
	runtime.register('v.signal-metadata', new_signal_metadata_workflow)!
	if os.getenv('V_SDK_EXAMPLE_DYNAMIC_HANDLERS') == 'true' {
		runtime.register_dynamic(new_dynamic_workflow)!
		runtime.register_dynamic_local_activity(example_activities.dynamic)!
	}

	runtime.register('v.context-failure-parent', new_context_failure_parent)!
	runtime.register('v.heartbeat-state', new_heartbeat_workflow)!
	runtime.register('v.activity-metadata', new_activity_metadata)!
	runtime.register('v.failure-chain.remote', new_remote_failure_chain)!
	runtime.register('v.failure-chain.local', new_local_failure_chain)!
	runtime.register('v.failure-updates', new_failure_update_workflow)!
	runtime.register('v.binary-error', new_binary_error)!
	runtime.register('v.binary-application-error', new_binary_application_error)!
	runtime.register('v.arguments.remote', new_remote_arguments)!
	runtime.register('v.arguments.local', new_local_arguments)!
	runtime.register('v.arguments.echo', new_arguments_echo)!
	runtime.register('v.arguments.zero', new_arguments_zero)!
	runtime.register('v.arguments.parent', new_arguments_parent)!
	runtime.register('v.arguments.continue', new_arguments_continue)!

	if os.getenv('V_SDK_EXAMPLE_TASK_ERROR_FIXED') == 'true' {
		runtime.register('v.task-failure', new_repaired_workflow)!
	} else {
		runtime.register('v.task-failure', new_blocked_workflow)!
	}

	register_patch(mut runtime)!
	runtime.register('v.async-activity', new_async_activity)!
	runtime.register('v.scopes', new_scope_workflow)!
	runtime.register('v.logged', new_logged_workflow)!
	runtime.register('v.metadata', new_metadata_workflow)!
	runtime.register('v.nexus-target', new_nexus_target)!
	runtime.register('v.nexus', new_nexus_workflow)!
	runtime.register('v.retry', new_retry_workflow)!
	runtime.register('v.child-options', new_child_options)!
	runtime.register('v.activity-options', new_activity_options)!
	runtime.register('v.default-retry', new_default_retry_workflow)!
	runtime.register('v.timer-cancel', new_timer_cancellation)!
	runtime.register('v.random', new_random)!
	runtime.register('v.external', new_external)!
	runtime.register('v.parent', new_parent)!
	runtime.register('v.chain', new_chain)!
	runtime.register('v.counter', new_counter)!
	if os.getenv('V_SDK_EXAMPLE_VERSIONED_TYPES') == 'true' {
		runtime.register('v.versioned.pinned', new_versioned_workflow, versioning_behavior: .pinned)!
		runtime.register('v.versioned.auto', new_versioned_workflow,
			versioning_behavior: .auto_upgrade
		)!
	}
	runtime.register('v.restart', new_restart)!
	runtime.register('v.parallel', new_parallel)!
	runtime.register('v.wait', new_wait)!
}
