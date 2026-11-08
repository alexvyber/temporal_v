module workflows

import x.json2
import alexvyber.temporal_v.sdk
import time

struct TimerCancellation {
mut:
	long_timer u32
}

fn new_timer_cancellation(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return TimerCancellation{}
}

fn (mut workflow TimerCancellation) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		workflow.long_timer = context.timer(60 * time.second)!
		context.timer(100 * time.millisecond)!
	} else if event.kind == .timer_fired {
		if event.sequence == workflow.long_timer { return error('cancelled timer fired') }
		if !context.cancel_timer(workflow.long_timer)! {
			return error('pending timer not cancelled')
		}
		if context.cancel_timer(workflow.long_timer)! { return error('timer cancelled twice') }
		context.complete('"cancelled"')!
	} else {
		return error('unexpected timer cancellation event')
	}
}

struct RandomWorkflow {
mut:
	values []string
}

fn new_random(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RandomWorkflow{}
}

fn (workflow &RandomWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name != 'state' { return error('unknown query') }
	return json2.encode(workflow.values)
}

fn (mut workflow RandomWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		workflow.values << context.random_uuid()!
		workflow.values << context.random_u64()!.str()
		context.timer(3 * time.second)!
	} else if event.kind == .timer_fired {
		workflow.values << context.random_uuid()!
		context.complete(json2.encode(workflow.values))!
	} else {
		return error('unexpected random event')
	}
}

struct ExternalInput {
	namespace   string
	workflow_id string
	run_id      string
	mode        string
	amount      int = 5
}

struct ExternalResult {
	failed  bool
	message string
}

struct External {}

fn new_external(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return External{}
}

fn (mut workflow External) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[ExternalInput](event.data)!
			target := sdk.ExternalWorkflow{
				workflow_id: input.workflow_id
				run_id:      input.run_id
				namespace:   input.namespace
			}
			if input.mode == 'signal' {
				context.signal_external(target, 'add', json2.encode({
					'amount': input.amount
				}))!
			} else if input.mode == 'cancel' {
				context.cancel_external(target, 'requested by V workflow')!
			} else {
				return error('invalid external operation')
			}
		}
		.external_signal_completed, .external_cancel_completed {
			context.timer(2 * time.second)!
		}
		.external_signal_failed, .external_cancel_failed {
			context.complete(json2.encode(ExternalResult{ failed: true, message: event.message }))!
		}
		.timer_fired {
			context.complete(json2.encode(ExternalResult{}))!
		}
		else {
			return error('unexpected external operation event')
		}
	}
}
