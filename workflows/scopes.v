module workflows

import json2
import sdk
import time

struct ScopeInput {
	mode string
}

struct ScopeResult {
	normal_cancelled  bool
	shield_completed  bool
	rejected_new_work bool
}

struct ScopeWorkflow {
mut:
	mode              string
	parent            u32
	normal_scope      u32
	shield_scope      u32
	normal_activity   u32
	shield_activity   u32
	normal_cancelled  bool
	shield_completed  bool
	rejected_new_work bool
	cleanup_started   bool
}

fn new_scope_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ScopeWorkflow{}
}

fn (workflow &ScopeWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name != 'state' { return error('unknown scope query') }
	return json2.encode({
		'cleanup_started': workflow.cleanup_started
	})
}

fn (mut workflow ScopeWorkflow) finish_if_ready(mut context sdk.WorkflowContext) ! {
	if !workflow.normal_cancelled || !workflow.shield_completed { return }
	if workflow.mode == 'cleanup' {
		context.cancel()!
		return
	}
	context.release_scope(workflow.normal_scope)!
	context.release_scope(workflow.shield_scope)!
	context.release_scope(workflow.parent)!
	context.complete(json2.encode(ScopeResult{
		normal_cancelled:  true
		shield_completed:  true
		rejected_new_work: workflow.rejected_new_work
	}))!
}

fn (mut workflow ScopeWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			workflow.mode = json2.decode[ScopeInput](event.data)!.mode
			if workflow.mode == 'manual' {
				workflow.parent = context.cancellation_scope()!
				workflow.normal_scope = context.cancellation_scope(parent: workflow.parent)!
				workflow.shield_scope = context.cancellation_scope(
					parent:   workflow.parent
					shielded: true
				)!
				workflow.normal_activity = context.activity('v.delay',
					'{"name":"normal","delay_ms":5000}', sdk.ActivityOptions{
					scope:        workflow.normal_scope
					cancellation: .wait_completed
				})!
				workflow.shield_activity = context.activity('v.delay',
					'{"name":"shielded","delay_ms":500}', sdk.ActivityOptions{
					scope: workflow.shield_scope
				})!
				context.timer(100 * time.millisecond)!
			} else {
				workflow.normal_activity = context.activity('v.delay',
					'{"name":"normal","delay_ms":5000}', sdk.ActivityOptions{
					cancellation: .wait_completed
				})!
			}
		}
		.cancel_requested {
			if workflow.mode != 'cleanup' {
				context.cancel()!
				return
			}
			if !context.info().cancellation_requested { return error('missing cancellation flag') }
			workflow.shield_scope = context.cancellation_scope(shielded: true)!
			workflow.shield_activity = context.activity('v.delay',
				'{"name":"cleanup","delay_ms":3000}', sdk.ActivityOptions{
				scope: workflow.shield_scope
			})!
			workflow.cleanup_started = true
		}
		.timer_fired {
			if !context.cancel_scope(workflow.parent)! || context.cancel_scope(workflow.parent)! {
				return error('scope cancellation not idempotent')
			}
			context.timer(time.second, scope: workflow.normal_scope) or {
				if err !is sdk.CancelledError { return err }
				workflow.rejected_new_work = true
			}
		}
		.activity_completed {
			if event.sequence != workflow.shield_activity {
				return error('unshielded Activity unexpectedly completed')
			}
			workflow.shield_completed = true
			workflow.finish_if_ready(mut context)!
		}
		.activity_cancelled {
			if event.sequence != workflow.normal_activity {
				return error('shielded Activity cancelled')
			}
			workflow.normal_cancelled = true
			workflow.finish_if_ready(mut context)!
		}
		.activity_failed {
			return error(event.message)
		}
		else {
			return error('unexpected scope event')
		}
	}
}
