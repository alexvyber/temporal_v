module workflows

import sdk
import json2

struct SignalMetadataState {
	sender_identity      string
	originating_event_id i64
}

struct SignalMetadataWorkflow {
mut:
	state SignalMetadataState
}

fn new_signal_metadata_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return SignalMetadataWorkflow{}
}

fn (mut workflow SignalMetadataWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {}
		.signal_received {
			if event.name == 'finish' {
				context.complete(json2.encode(workflow.state))!
			} else if event.name == 'record' {
				workflow.state = SignalMetadataState{
					sender_identity:      event.sender_identity
					originating_event_id: event.originating_event_id
				}
			} else {
				return error('unexpected signal metadata event')
			}
		}
		.cancel_requested {
			context.cancel()!
		}
		else {
			return error('unexpected signal metadata workflow event')
		}
	}
}

fn (workflow &SignalMetadataWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name != 'state' { return error('unknown signal metadata query') }
	return json2.encode(workflow.state)
}
