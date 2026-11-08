module workflows

import temporal_v.sdk
import x.json2

struct UpdateContextWorkflow {
mut:
	pending map[string]string
}

fn new_update_context(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return UpdateContextWorkflow{}
}

fn (mut workflow UpdateContextWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {}
		.update_received {
			if event.name == 'fail' {
				return sdk.ApplicationError{
					message:  'scoped Update failed'
					category: 'ScopedUpdateFailure'
					details:  [
						'"Update detail"',
						'binary\x00\xff',
					]
					cause:    error('Update root cause')
				}
			}
			value := json2.encode({
				'input':  context.serialization_context()
				'result': context.update_result_serialization_context(event.id)!
			})
			if event.name == 'hold' {
				workflow.pending[event.id] = value
			} else {
				context.complete_update(event.id, value)!
			}
		}
		.signal_received {
			if event.name == 'release' {
				mut ids := workflow.pending.keys()
				ids.sort()
				for id in ids {
					context.complete_update(id, workflow.pending[id])!
				}
				workflow.pending.clear()
			} else if event.name == 'finish' {
				context.complete('"workflow scope"')!
			}
		}
		.cancel_requested {
			context.cancel()!
		}
		else {}
	}
}

fn (workflow &UpdateContextWorkflow) update_names() []string {
	return ['hold', 'immediate', 'fail', 'reject']
}

fn (workflow &UpdateContextWorkflow) validate_update(name string, arguments []string, view sdk.WorkflowContextView) ! {
	if name == 'reject' {
		return sdk.ApplicationError{
			message: 'scoped validation rejected'
			details: ['"validation detail"']
		}
	}
}
