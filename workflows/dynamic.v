module workflows

import sdk
import json2

struct DynamicInput {
	known bool
	fail  bool
	retry bool
}

struct DynamicState {
mut:
	workflow_type string
	remote        string
	local         string
	ready         bool
}

struct DynamicWorkflow {
mut:
	state  DynamicState
	remote u32
	local  u32
}

fn new_dynamic_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return DynamicWorkflow{}
}

fn (mut workflow DynamicWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			mut arguments_input := []string{}
			for value in event.raw_arguments {
				arguments_input << context.decode_raw_value(value)!.logical_data()
			}
			input := json2.decode[DynamicInput](sdk.single_argument(arguments_input)!)!
			workflow.state = DynamicState{
				workflow_type: context.info().workflow_type
			}
			arguments := sdk.Arguments([
				if input.fail {
					'fail'
				} else if input.retry {
					'retry'
				} else {
					'success'
				},
				'dynamic 世界',
				'third',
			])
			if input.known {
				workflow.remote = context.activity('v.echo', '"named"', sdk.ActivityOptions{})!
			} else {
				workflow.remote = context.activity(workflow.state.workflow_type + '.remote',
					arguments, sdk.ActivityOptions{})!
			}
			workflow.local = context.local_activity(workflow.state.workflow_type + '.local',
				arguments, sdk.LocalActivityOptions{})!
		}
		.activity_completed {
			if event.sequence == workflow.remote {
				workflow.state.remote = event.data
			} else if event.sequence == workflow.local {
				workflow.state.local = event.data
			} else {
				return error('dynamic workflow received unknown sequence')
			}
			workflow.state.ready = workflow.state.remote != '' && workflow.state.local != ''
		}
		.activity_failed, .activity_cancelled {
			return event.failure or { return error('missing dynamic failure') }
		}
		.signal_received {
			if event.name != 'finish' || !workflow.state.ready {
				return error('dynamic workflow finished before results')
			}
			context.complete(json2.encode(workflow.state))!
		}
		.cancel_requested {
			context.cancel()!
		}
		else {
			return error('unexpected dynamic workflow event')
		}
	}
}

fn (workflow &DynamicWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name != 'state' { return error('unknown dynamic query') }
	return json2.encode(workflow.state)
}
