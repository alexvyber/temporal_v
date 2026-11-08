module main

import temporal_v.sdk
import temporal_v.temporal
import x.json2
import time

struct EventGroupWireWorkflow {
mut:
	timer u32
}

fn event_group_wire_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return EventGroupWireWorkflow{}
}

fn (mut workflow EventGroupWireWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		context.timer(time.millisecond)!
		workflow.timer = context.timer(time.hour)!
	}
	if event.kind == .timer_fired {
		context.cancel_timer(workflow.timer)!
		context.complete('"group-wire-ok"')!
	}
}

struct EventGroupWireInterceptor {}

fn (interceptor EventGroupWireInterceptor) intercept(command temporal.CoresdkWorkflowCommands_WorkflowCommand, information sdk.WorkflowInfo, next sdk.CommandNext) !temporal.CoresdkWorkflowCommands_WorkflowCommand {
	if information.workflow_type != 'v.event-group-wire' { return next.execute(command)! }
	variant := command.variant or { return next.execute(command)! }
	id := if variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_StartTimer_Case {
		'timer-' + variant.value.seq.str()
	} else if variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_CompleteWorkflowExecution_Case {
		'complete'
	} else {
		return next.execute(command)!
	}
	marker := temporal.TemporalApiSdkV1_EventGroupMarker{
		variant: temporal.TemporalApiSdkV1_EventGroupMarker_Label_Case{
			value: temporal.TemporalApiSdkV1_EventGroupMarker_Label{
				id:    id
				label: temporal.TemporalApiCommonV1_Payload{
					metadata: {
						'encoding': 'json/plain'.bytes()
					}
					data:     json2.encode(id).bytes()
				}
			}
		}
	}
	return next.execute(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		...command
		event_group_markers: [
			marker,
		]
	})!
}
