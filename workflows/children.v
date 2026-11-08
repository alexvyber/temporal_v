module workflows

import json2
import sdk
import time

struct RetryWorkflow {}

fn new_retry_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RetryWorkflow{}
}

fn (mut workflow RetryWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind != .started { return error('unexpected retry event') }
	if context.info().attempt < 3 {
		return sdk.ApplicationError{
			message:  'retry this workflow'
			category: 'TransientWorkflow'
			details:  [json2.encode(context.info().attempt)]
		}
	}
	context.complete(json2.encode(context.info().attempt))!
}

struct ChildInput {
	mode         string
	close_policy string = 'request_cancel'
}

struct ChildReference {
	workflow_id string
	run_id      string
}

struct ChildOptionsWorkflow {
mut:
	mode     string
	child_id string
}

fn new_child_options(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ChildOptionsWorkflow{}
}

fn (mut workflow ChildOptionsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[ChildInput](event.data)!
			workflow.mode = input.mode
			workflow.child_id = context.info().workflow_id + '-child'
			policy := match input.close_policy {
				'abandon' { sdk.ParentClosePolicy.abandon }
				'terminate' { sdk.ParentClosePolicy.terminate }
				'request_cancel' { sdk.ParentClosePolicy.request_cancel }
				else { return error('invalid parent close policy') }
			}

			options := sdk.ChildOptions{
				static_summary:      'child summary 世界'
				static_details:      'child details 世界'
				parent_close_policy: policy
				cancellation:        .wait_completed
				task_timeout:        5 * time.second
				run_timeout:         30 * time.second
				headers:             {
					'origin': '"parent"'
				}
				memo:                {
					'purpose': '"child-test"'
				}
				retry:               sdk.RetryPolicy{
					maximum_attempts: 3
					initial_interval: 20 * time.millisecond
				}
			}
			type_name := if input.mode == 'retry' { 'v.retry' } else { 'v.wait' }
			sequence := context.child(type_name, workflow.child_id, '{}', options)!
			if input.mode == 'cancel_before' {
				context.cancel_child(sequence, 'cancel before start')!
			}
		}
		.child_started {
			match workflow.mode {
				'close' {
					context.complete(json2.encode(ChildReference{
						workflow_id: workflow.child_id
						run_id:      event.id
					}))!
				}
				'cancel' {
					if !context.cancel_child(event.sequence, 'explicit child cancellation')! {
						return error('child not pending')
					}
					if context.cancel_child(event.sequence, 'duplicate request')! {
						return error('child cancellation not idempotent')
					}
				}
				'duplicate' {
					if event.sequence == 1 {
						context.child('v.wait', workflow.child_id, '{}', sdk.ChildOptions{
							id_reuse: .reject_duplicate
						})!
					}
				}
				else {}
			}
		}
		.child_completed {
			context.complete(event.data)!
		}
		.child_cancelled {
			context.complete('"child cancelled"')!
		}
		.child_failed {
			if workflow.mode == 'duplicate' && event.message.contains('already_exists') {
				context.complete('"duplicate rejected"')!
			} else {
				return error(event.message)
			}
		}
		else {
			return error('unexpected child options event')
		}
	}
}
