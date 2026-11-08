module main

import temporal_v.sdk
import x.json2

struct HandlerState {
	pending      []sdk.PendingUpdateInfo
	all_finished bool
}

struct HandlerLifecycleWorkflow {
mut:
	state HandlerState
}

fn handler_lifecycle_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return HandlerLifecycleWorkflow{}
}

fn (mut workflow HandlerLifecycleWorkflow) snapshot(context &sdk.WorkflowContext) {
	workflow.state = HandlerState{
		pending:      context.pending_updates()
		all_finished: context.all_handlers_finished()
	}
}

fn (mut workflow HandlerLifecycleWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started && event.data == '"next"' {
		context.complete('"continued"')!
		return
	}
	if event.kind == .update_received {
		if context.all_handlers_finished() { return error('accepted Update was not tracked') }
		if event.name == 'immediate' { context.complete_update(event.id, '"immediate"')! }
		if event.name == 'close' { context.complete('"handler-closed"')! }
	}
	if event.kind == .signal_received {
		match event.name {
			'complete' {
				context.complete('"closed"')!
			}
			'continue' {
				context.continue_as_new('"next"', sdk.ContinueOptions{})!
			}
			'cancel' {
				context.cancel()!
			}
			'fail' {
				return sdk.ApplicationError{
					message:       'lifecycle failure'
					non_retryable: true
				}
			}
			'finish-updates' {
				pending := context.pending_updates()
				for update in pending {
					context.complete_update(update.protocol_instance_id, json2.encode(update.id))!
				}
			}
			else {}
		}
	}
	if event.kind == .cancel_requested { context.cancel()! }
	workflow.snapshot(context)
}

fn (workflow HandlerLifecycleWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.state))
}

fn (workflow HandlerLifecycleWorkflow) update_names() []string {
	return ['hold-warn', 'hold-silent', 'immediate', 'close']
}

fn (workflow HandlerLifecycleWorkflow) validate_update(name string, input []string, view sdk.WorkflowContextView) ! {}

fn (workflow HandlerLifecycleWorkflow) update_handler_options(name string) sdk.UpdateHandlerOptions {
	return sdk.UpdateHandlerOptions{
		unfinished_policy: if name == 'hold-silent' {
			.abandon
		} else {
			.warn_and_abandon
		}
	}
}
