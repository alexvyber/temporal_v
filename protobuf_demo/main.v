module main

import sdk
import temporal
import os
import json2

struct FrameCodec {}

fn (codec FrameCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		mut data := [u8(0), 0xff, 0x5a]
		data << value.data
		result << temporal.TemporalApiCommonV1_Payload{
			...value
			data: data
		}
	}
	return result
}

fn (codec FrameCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		if value.data.len < 3 || value.data[..3] != [u8(0), 0xff, 0x5a] {
			return error('invalid protobuf fixture frame')
		}
		result << temporal.TemporalApiCommonV1_Payload{
			...value
			data: value.data[3..].clone()
		}
	}
	return result
}

fn converter() sdk.DataConverter {
	return sdk.DataConverter{
		payload_converter: sdk.ProtobufPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{
			message_type: os.getenv('V_SDK_PROTOBUF_MESSAGE_TYPE')
		}
		codecs:            if os.getenv('V_SDK_PROTOBUF_CODEC') == 'true' {
			[sdk.PayloadCodec(FrameCodec{})]
		} else {
			[]sdk.PayloadCodec{}
		}
	}
}

struct ProtoWorkflow {
mut:
	value string
	ready bool
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ProtoWorkflow{}
}

fn (mut workflow ProtoWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			sdk.single_argument(event.arguments)!
			workflow.value = event.arguments[0]
			context.activity('v.protobuf', event.arguments[0], sdk.ActivityOptions{})!
		}
		.activity_completed {
			workflow.value = event.data
			workflow.ready = true
		}
		.activity_failed {
			failure := event.failure or { return error('missing protobuf failure') }
			cause := failure.cause or { return error('missing protobuf application cause') }
			if cause.message != 'expected protobuf failure' || cause.details.len != 1 {
				return error('wrong protobuf failure')
			}
			workflow.value = cause.details[0]
			workflow.ready = true
		}
		.signal_received {
			if event.name == 'finish' && workflow.ready { context.complete(workflow.value)! }
		}
		.cancel_requested {
			context.cancel()!
		}
		else {}
	}
}

fn (workflow &ProtoWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if !workflow.ready { return error('protobuf Activity not complete') }
	return workflow.value
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.protobuf', make_workflow)!
	return runtime
}

fn activity(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	mut message :=
		temporal.TemporalApiCommonV1_WorkflowExecution.decode(sdk.single_argument(arguments)!.bytes())!
	if message.workflow_id == 'fail' {
		return sdk.ApplicationError{
			message:       'expected protobuf failure'
			non_retryable: true
			details:       [message.encode().bytestr()]
		}
	}
	message.run_id += '-activity'
	return message.encode().bytestr()
}

fn run() ! {
	mode := os.args[1]
	if mode == 'wire' {
		wire_oracle(os.args[2])!
		return
	}
	if mode == 'validation' {
		validate_options()!
		return
	}
	if mode == 'worker' {
		mut activities := sdk.Activities{}
		activities.register('v.protobuf', activity)!
		sdk.run_worker(sdk.WorkerOptions{ ...sdk.options_from_env()!, data_converter: converter() },
			definitions()!, activities)!
		return
	}
	if mode == 'replay' {
		definitions()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id:    os.args[3]
			task_queue:     os.args[4]
			data_converter: converter()
		})!
		println('REPLAY_OK')
		return
	}
	client := sdk.connect(sdk.ClientOptions{
		address:        os.getenv('TEMPORAL_ADDRESS')
		data_converter: converter()
	})!
	defer { client.close() }
	if mode == 'start' {
		message := temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: os.args[3]
			run_id:      'protobuf-世界'
			pb_unknown:  [
				u8(0xa0),
				0x06,
				0x07,
			]
		}
		println(json2.encode(client.start(sdk.WorkflowStart{
			workflow_id:   os.args[2]
			workflow_type: 'v.protobuf'
			task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:     message.encode().bytestr()
		})!))
		return
	}
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	match mode {
		'query' {
			println(json2.encode(temporal.TemporalApiCommonV1_WorkflowExecution.decode(client.query(handle,
				'state', sdk.Arguments(sdk.TypedArguments{}))!.bytes())!))
		}
		'finish' {
			client.signal(handle, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
			println('OK')
		}
		'result' {
			result := client.get_result(handle, 10000)!
			if result.status != .completed { return error('protobuf workflow did not complete') }
			println(json2.encode(temporal.TemporalApiCommonV1_WorkflowExecution.decode(result.data.bytes())!))
		}
		'metadata' {
			println(json2.encode(client.workflow_metadata(handle)!))
		}
		else {
			return error('unknown protobuf role')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
