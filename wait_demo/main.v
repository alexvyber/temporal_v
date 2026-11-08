module main

import sdk
import temporal
import example_converter
import json2
import os
import time

struct InjectWaitFailure {
	late        bool
	description bool
}

fn (failure InjectWaitFailure) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	if failure.description && request.method == 'PollNexusOperationExecution' {
		return temporal.TemporalApiWorkflowserviceV1_PollNexusOperationExecutionResponse{
			run_id:     'resolved'
			wait_stage: .nexus_operation_wait_stage_closed
			outcome:    temporal.TemporalApiWorkflowserviceV1_PollNexusOperationExecutionResponse_Result_Case{}
		}.encode()
	}
	if failure.late { time.sleep(5 * time.millisecond) }
	return sdk.RpcError{
		message: 'injected wait transport failure'
		status:  13
	}
}

fn require_wait_error(err IError, expected int) ! {
	if err.code() != expected {
		return error('wrong wait status: ' + err.code().str() + ' ' + err.msg())
	}
	if expected == 13 && err.msg() != 'injected wait transport failure' {
		return error('early wait diagnostics lost')
	}
}

fn run() ! {
	if os.args.len > 1 && os.args[1] in ['worker', 'replay'] {
		mut runtime := &sdk.Runtime{}
		runtime.register('v.wait', make_wait_workflow)!
		converter := example_converter.from_env()!
		if os.args[1] == 'worker' {
			sdk.run_workflows(sdk.WorkerOptions{
				...sdk.options_from_env()!
				data_converter: converter
			}, runtime)!
		} else {
			runtime.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
				workflow_id:    os.args[3]
				task_queue:     os.args[4]
				data_converter: converter
			})!
			println('OK')
		}
		return
	}
	if os.args.len > 1 && os.args[1] == 'live' {
		run_live()!
		return
	}
	for late in [false, true] {
		client := sdk.connect(sdk.ClientOptions{
			address:      os.getenv('TEMPORAL_ADDRESS')
			interceptors: [
				sdk.ClientInterceptor(InjectWaitFailure{
					late: late
				}),
			]
		})!
		defer { client.close() }
		budget := if late { u32(1) } else { u32(10000) }
		expected := if late { 4 } else { 13 }
		workflow := sdk.WorkflowHandle{
			workflow_id: 'injected'
		}
		if _ := client.get_result(workflow, budget) {
			return error('workflow wait succeeded')
		} else {
			require_wait_error(err, expected)!
		}
		if _ := client.update_result(sdk.UpdateHandle{ workflow: workflow, id: 'injected' }, budget) {
			return error('Update wait succeeded')
		} else {
			require_wait_error(err, expected)!
		}
		if _ := client.activity_result(sdk.StandaloneActivityHandle{ activity_id: 'injected' },
			time.Duration(budget) * time.millisecond)
		{
			return error('Activity wait succeeded')
		} else {
			require_wait_error(err, expected)!
		}
		if _ := client.nexus_operation_result(sdk.NexusExecutionHandle{ operation_id: 'injected' },
			time.Duration(budget) * time.millisecond)
		{
			return error('Nexus wait succeeded')
		} else {
			require_wait_error(err, expected)!
		}
	}
	for late in [false, true] {
		client := sdk.connect(sdk.ClientOptions{
			address:      os.getenv('TEMPORAL_ADDRESS')
			interceptors: [
				sdk.ClientInterceptor(InjectWaitFailure{
					late:        late
					description: true
				}),
			]
		})!
		defer { client.close() }
		budget := if late { time.millisecond } else { 10 * time.second }
		if _ := client.nexus_operation_result(sdk.NexusExecutionHandle{ operation_id: 'injected' },
			budget)
		{
			return error('Nexus Describe wait succeeded')
		} else {
			require_wait_error(err, if late { 4 } else { 13 })!
		}
	}
	cancellation := sdk.new_rpc_cancellation()!
	defer { cancellation.close() }
	cancellation.cancel()
	cancelled_client := sdk.connect(sdk.ClientOptions{
		address:      os.getenv('TEMPORAL_ADDRESS')
		cancellation: cancellation
	})!
	defer { cancelled_client.close() }
	workflow := sdk.WorkflowHandle{
		workflow_id: 'injected'
	}
	if _ := cancelled_client.get_result(workflow, 10000) {
		return error('cancelled workflow wait succeeded')
	} else {
		require_wait_error(err, 1)!
	}
	if _ := cancelled_client.update_result(sdk.UpdateHandle{ workflow: workflow, id: 'injected' },
		10000)
	{
		return error('cancelled Update wait succeeded')
	} else {
		require_wait_error(err, 1)!
	}
	if _ := cancelled_client.activity_result(sdk.StandaloneActivityHandle{ activity_id: 'injected' },
		10 * time.second)
	{
		return error('cancelled Activity wait succeeded')
	} else {
		require_wait_error(err, 1)!
	}
	if _ := cancelled_client.nexus_operation_result(sdk.NexusExecutionHandle{
		operation_id: 'injected'
	}, 10 * time.second)
	{
		return error('cancelled Nexus wait succeeded')
	} else {
		require_wait_error(err, 1)!
	}
	println('WAIT_DEADLINE_VALIDATION_OK')
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}

fn run_live() ! {
	client := sdk.connect(sdk.ClientOptions{
		address:        os.getenv('TEMPORAL_ADDRESS')
		data_converter: example_converter.from_env()!
	})!
	defer { client.close() }
	queue := os.getenv('TEMPORAL_TASK_QUEUE')
	workflow := client.start(sdk.WorkflowStart{
		workflow_id:   queue + '-wait'
		workflow_type: 'v.wait'
		task_queue:    queue
		arguments:     '{"initial":0}'
	})!
	update := client.start_update(workflow, sdk.UpdateRequest{
		name:                'delayed_add'
		arguments:           '{"amount":1}'
		id:                  'wait-update'
		wait_for:            .accepted
		wait_timeout_millis: 10000
	})!
	activity := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:       queue + '-wait'
		activity_type:     'v.never'
		task_queue:        queue + '-empty'
		schedule_to_close: time.minute
	})!
	nexus := client.start_nexus_operation(sdk.NexusExecutionStart{
		operation_id: queue + '-wait'
		endpoint:     os.args[2]
		service:      'wait'
		operation:    'never'
	})!
	for _ in 0 .. 20 {
		if _ := client.get_result(workflow, 1) {
			return error('running workflow wait succeeded')
		} else {
			require_wait_error(err, 4)!
		}
		if _ := client.update_result(update, 1) {
			return error('running Update wait succeeded')
		} else {
			require_wait_error(err, 4)!
		}
		if _ := client.activity_result(activity, time.millisecond) {
			return error('running Activity wait succeeded')
		} else {
			require_wait_error(err, 4)!
		}
		if _ := client.nexus_operation_result(nexus, time.millisecond) {
			return error('running Nexus wait succeeded')
		} else {
			require_wait_error(err, 4)!
		}
	}
	if _ := client.get_result(sdk.WorkflowHandle{ workflow_id: queue + '-missing' }, 10000) {
		return error('missing workflow succeeded')
	} else {
		require_wait_error(err, 5)!
	}
	if _ := client.update_result(sdk.UpdateHandle{
		workflow: sdk.WorkflowHandle{
			workflow_id: queue + '-missing'
		}
		id:       'missing'
	}, 10000)
	{
		return error('missing Update succeeded')
	} else {
		require_wait_error(err, 5)!
	}
	if _ := client.activity_result(sdk.StandaloneActivityHandle{ activity_id: queue + '-missing' },
		10 * time.second)
	{
		return error('missing Activity succeeded')
	} else {
		require_wait_error(err, 5)!
	}
	if _ := client.nexus_operation_result(sdk.NexusExecutionHandle{ operation_id: queue + '-missing' },
		10 * time.second)
	{
		return error('missing Nexus succeeded')
	} else {
		require_wait_error(err, 5)!
	}
	client.signal(workflow, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
	client.update_result(update, 10000)!
	if client.get_result(workflow, 10000)!.status != .completed {
		return error('short waits cancelled workflow')
	}
	client.terminate_activity(activity, sdk.ActivityManagementOptions{
		reason: 'wait fixture cleanup'
	})!
	client.terminate_nexus_operation(nexus, reason: 'wait fixture cleanup')!
	println(json2.encode(workflow))
}

struct WaitWorkflow {
mut:
	update_id string
}

fn make_wait_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return WaitWorkflow{}
}

fn (mut workflow WaitWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.update_received {
			workflow.update_id = event.id
		}
		.signal_received {
			if event.name == 'finish' {
				context.complete_update(workflow.update_id, 'true')!
				context.complete('true')!
			}
		}
		else {}
	}
}

fn (workflow &WaitWorkflow) update_names() []string {
	return ['delayed_add']
}

fn (workflow &WaitWorkflow) validate_update(name string, values []string, view sdk.WorkflowContextView) ! {}
