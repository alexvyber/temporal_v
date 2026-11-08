module workflows

import x.json2
import temporal_v.sdk
import time

struct NexusInput {
	endpoint   string
	operation  string = 'echo'
	name       string
	delay_ms   int
	timeout_ms i64 = 30000
}

struct NexusWorkflow {
mut:
	result   string
	sequence u32
}

fn new_nexus_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return NexusWorkflow{}
}

fn (mut workflow NexusWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[NexusInput](event.data)!
			workflow.sequence = context.nexus_operation(json2.encode(input), sdk.NexusOptions{
				summary:           'Nexus summary 世界'
				endpoint:          input.endpoint
				service:           'v-service'
				operation:         input.operation
				schedule_to_close: input.timeout_ms * time.millisecond
				headers:           {
					'x-v-sdk-test': 'nexus'
				}
			})!
		}
		.signal_received {
			if event.name == 'cancel' {
				context.cancel_nexus(workflow.sequence)!
			} else {
				return error('unknown Nexus workflow signal')
			}
		}
		.nexus_started {}
		.nexus_completed {
			workflow.result = event.data
			context.timer(2 * time.second)!
		}
		.nexus_failed {
			if failure := event.failure { return error(failure.root_message()) }
			return error(event.message)
		}
		.nexus_cancelled {
			context.complete('"Nexus cancelled"')!
		}
		.nexus_timed_out {
			context.complete('"Nexus timed out"')!
		}
		.timer_fired {
			context.complete(workflow.result)!
		}
		else {
			return error('unexpected Nexus workflow event')
		}
	}
}

struct NexusTarget {
mut:
	input NexusInput
}

fn new_nexus_target(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return NexusTarget{}
}

fn (mut workflow NexusTarget) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		workflow.input = json2.decode[NexusInput](event.data)!
		context.timer(if workflow.input.delay_ms > 0 {
			workflow.input.delay_ms * time.millisecond
		} else {
			time.second
		})!
	} else if event.kind == .timer_fired {
		if workflow.input.operation == 'async-fail' {
			return sdk.ApplicationError{
				message:       'async Nexus workflow failed'
				non_retryable: true
			}
		}
		context.complete(json2.encode('Hello from async Nexus, ${workflow.input.name}!'))!
	} else {
		return error('unexpected Nexus target event')
	}
}
