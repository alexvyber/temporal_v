module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import x.json2
import os
import time

struct FrameCodec {}

fn (codec FrameCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		mut data := [u8(0), 255, 90]
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
		if value.data.len < 3 || value.data[..3] != [u8(0), 255, 90] {
			return error('invalid mixed frame')
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
		codecs: if os.getenv('V_SDK_MIXED_CODEC') == 'true' {
			[sdk.PayloadCodec(FrameCodec{})]
		} else {
			[]sdk.PayloadCodec{}
		}
	}
}

fn mixed_arguments() !sdk.TypedArguments {
	message := temporal.TemporalApiCommonV1_WorkflowExecution{
		workflow_id: 'mixed-世界'
		run_id:      'protobuf'
		pb_unknown:  [
			u8(0xa0),
			6,
			7,
		]
	}
	return sdk.TypedArguments{
		values: [sdk.PayloadValue('{"value":7}'),
			sdk.PayloadValue(sdk.BinaryValue{
				data: [u8(0), 255, 0, 128]
			}),
			sdk.PayloadValue(sdk.protobuf_value(message,
				'temporal.api.common.v1.WorkflowExecution')!),
			sdk.PayloadValue(sdk.NullValue{}), sdk.PayloadValue('"null"'),
			sdk.PayloadValue(sdk.BinaryValue{
				data: 'null'.bytes()
			})]
	}
}

fn validate_arguments(values []string) ! {
	if values.len != 6 { return error('expected six mixed values') }
	if values[0] != '{"value":7}' || values[1].bytes() != [u8(0), 255, 0, 128]
		|| values[3] != 'null' || values[4] != '"null"' || values[5] != 'null' {
		return error('mixed value changed')
	}
	message := temporal.TemporalApiCommonV1_WorkflowExecution.decode(values[2].bytes())!
	if message.workflow_id != 'mixed-世界' || message.pb_unknown != [u8(0xa0), 6, 7] {
		return error('protobuf mixed value changed')
	}
}

fn validate_raw(values []sdk.RawValue) ! {
	if values.len != 6 { return error('raw mixed values missing') }
	for index, kind in [sdk.PayloadKind.json, .binary, .protobuf, .null_value, .json, .binary] {
		if values[index].kind() != kind { return error('raw mixed kind changed') }
	}
	null_value := values[3].as_value()!
	if null_value !is sdk.NullValue { return error('null value lost its type') }
	last := values[5].as_value()!
	if last !is sdk.BinaryValue { return error('binary null bytes lost their type') }
	if last.data != 'null'.bytes() { return error('raw binary null bytes changed') }
}

struct InspectActivity {}

fn (interceptor InspectActivity) intercept(mut context sdk.ActivityContext, input []string, next sdk.ActivityNext) !sdk.PayloadValue {
	validate_raw(context.input_values())!
	return next.execute(mut context, input)!
}

struct InspectQuery {}

fn (interceptor InspectQuery) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.QueryNext) !sdk.PayloadValue {
	validate_raw(next.input_values())!
	return next.execute(name, input)!
}

struct InspectUpdate {}

fn (interceptor InspectUpdate) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.UpdateValidatorNext) ! {
	validate_raw(next.input_values())!
	next.execute(name, input)!
}

struct Snapshot {
pub:
	seen  int
	ready bool
}

struct MixedWorkflow {
	output   string
	deferred bool
	callback bool
	local    bool
mut:
	seen  int
	ready bool
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return MixedWorkflow{}
}

fn (workflow &MixedWorkflow) snapshot() string {
	return json2.encode(Snapshot{ seen: workflow.seen, ready: workflow.ready })
}

fn (mut workflow MixedWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			validate_arguments(event.arguments)!
			validate_raw(event.raw_arguments)!
			workflow.seen++
			if workflow.local {
				context.local_activity('v.mixed.typed.' + workflow.output, mixed_arguments()!, sdk.LocalActivityOptions{})!
			} else {
				context.activity(if workflow.deferred {
					'v.mixed.async'
				} else if workflow.callback {
					'v.mixed.typed.' + workflow.output
				} else {
					'v.mixed'
				}, mixed_arguments()!, sdk.ActivityOptions{
					start_to_close:    time.minute
					heartbeat_timeout: 0
				})!
			}
		}
		.activity_completed {
			raw := event.raw_result or { return error('Activity raw result missing') }
			expected_kind := if !workflow.deferred && !workflow.callback {
				sdk.PayloadKind.json
			} else if workflow.output == 'protobuf' {
				sdk.PayloadKind.protobuf
			} else if workflow.output == 'null' {
				sdk.PayloadKind.null_value
			} else {
				sdk.PayloadKind.binary
			}
			if raw.kind() != expected_kind { return error('Activity raw result kind changed') }
			if workflow.deferred || workflow.callback {
				match workflow.output {
					'binary-null' {
						if event.data != 'null' { return error('binary null result changed') }
					}
					'binary' {
						if event.data.bytes() != [u8(0), 255, 0, 128] {
							return error('typed async binary result changed')
						}
					}
					'protobuf' {
						message :=
							temporal.TemporalApiCommonV1_WorkflowExecution.decode(event.data.bytes())!
						if message.workflow_id != 'typed-result' {
							return error('typed async protobuf result changed')
						}
					}
					'null' {
						if event.data != 'null' { return error('typed async null result changed') }
					}
					else {
						return error('unknown typed async kind')
					}
				}
			} else if event.data != '"activity-mixed-ok"' {
				return error('wrong mixed Activity result')
			}
			workflow.ready = true
		}
		.signal_received {
			if event.name == 'mix' {
				validate_arguments(event.arguments)!
				validate_raw(event.raw_arguments)!
				workflow.seen++
			}
			if event.name == 'finish' {
				match workflow.output {
					'binary-null' {
						context.complete(sdk.BinaryValue{ data: 'null'.bytes() })!
					}
					'binary' {
						context.complete(sdk.BinaryValue{ data: [u8(0), 255, 0, 128] })!
					}
					'protobuf' {
						context.complete(sdk.protobuf_value(temporal.TemporalApiCommonV1_WorkflowExecution{
							workflow_id: 'typed-result'
							run_id:      '世界'
						}, 'temporal.api.common.v1.WorkflowExecution')!)!
					}
					'null' {
						context.complete(sdk.NullValue{})!
					}
					else {
						context.complete(workflow.snapshot())!
					}
				}
			}
		}
		.update_received {
			validate_arguments(event.arguments)!
			validate_raw(event.raw_arguments)!
			workflow.seen++
			context.complete_update(event.id, if event.name == 'wire' {
				typed_value(workflow.output)!
			} else {
				sdk.PayloadValue(workflow.snapshot())
			})!
		}
		.cancel_requested {
			context.cancel()!
		}
		else {}
	}
}

fn (workflow &MixedWorkflow) query(name string, values []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	validate_arguments(values)!
	if !workflow.ready { return error('mixed Activity pending') }
	if name == 'wire' { return typed_value(workflow.output)! }
	return workflow.snapshot()
}

fn (workflow &MixedWorkflow) update_names() []string {
	return ['mix', 'wire']
}

fn (workflow &MixedWorkflow) validate_update(name string, values []string, view sdk.WorkflowContextView) ! {
	validate_arguments(values)!
}

fn activity(mut context sdk.ActivityContext, values []string) !sdk.PayloadValue {
	validate_arguments(values)!
	validate_raw(context.input_values())!
	return '"activity-mixed-ok"'
}

fn deferred_activity(mut context sdk.ActivityContext, values []string) !sdk.PayloadValue {
	validate_arguments(values)!
	validate_raw(context.input_values())!
	token := context.defer_completion()!
	path := os.join_path(os.getenv('V_SDK_MIXED_TOKEN_DIR'), context.info().run_id + '.json')
	mut file := os.open_file(path, 'wx', 0o600)!
	defer { file.close() }
	file.write_string(json2.encode(token))!
	return 'null'
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.mixed', make_workflow)!
	for output in ['binary', 'protobuf', 'null', 'binary-null'] {
		runtime.register('v.mixed.' + output, fn [output] (view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
			return MixedWorkflow{
				output: output
			}
		})!
		runtime.register('v.mixed.callback.' + output, fn [output] (view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
			return MixedWorkflow{
				output:   output
				callback: true
			}
		})!
		runtime.register('v.mixed.local.' + output, fn [output] (view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
			return MixedWorkflow{
				output:   output
				callback: true
				local:    true
			}
		})!
		runtime.register_local_activity('v.mixed.typed.' + output, typed_activity)!
		runtime.register('v.mixed.async.' + output, fn [output] (view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
			return MixedWorkflow{
				output:   output
				deferred: true
			}
		})!
	}
	return runtime
}

fn run() ! {
	mode := os.args[1]
	if mode == 'validation' {
		validate_values()!
		return
	}
	if mode == 'worker' {
		mut activities := sdk.Activities{}
		activities.register('v.mixed', activity)!
		for kind in ['binary', 'protobuf', 'null', 'binary-null'] {
			activities.register('v.mixed.typed.' + kind, typed_activity)!
		}
		activities.register('v.mixed.async', deferred_activity)!
		sdk.run_worker(sdk.WorkerOptions{
			...sdk.options_from_env()!
			data_converter:                converter()
			query_interceptors:            [
				sdk.QueryInterceptor(InspectQuery{}),
				sdk.QueryInterceptor(InspectQuery{}),
			]
			update_validator_interceptors: [
				sdk.UpdateValidatorInterceptor(InspectUpdate{}),
				sdk.UpdateValidatorInterceptor(InspectUpdate{}),
			]
		}, definitions()!, activities)!
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
	if mode == 'complete-async' {
		identifier := json2.decode[sdk.ActivityTaskToken](os.read_file(os.args[2])!)!
		value := match os.args[3] {
			'binary-null' {
				sdk.PayloadValue(sdk.BinaryValue{
					data: 'null'.bytes()
				})
			}
			'binary' {
				sdk.PayloadValue(sdk.BinaryValue{
					data: [u8(0), 255, 0, 128]
				})
			}
			'protobuf' {
				sdk.PayloadValue(sdk.protobuf_value(temporal.TemporalApiCommonV1_WorkflowExecution{
					workflow_id: 'typed-result'
					run_id:      '世界'
				}, 'temporal.api.common.v1.WorkflowExecution')!)
			}
			'null' {
				sdk.PayloadValue(sdk.NullValue{})
			}
			else {
				return error('unknown async value kind')
			}
		}

		client.complete_activity(identifier, value)!
		println('OK')
		return
	}
	if mode == 'start' {
		println(json2.encode(client.start(sdk.WorkflowStart{
			workflow_id:   os.args[2]
			workflow_type: if os.args.len > 3 {
				'v.mixed.' + os.args[3]
			} else {
				'v.mixed'
			}
			task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:     mixed_arguments()!
		})!))
		return
	}
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	match mode {
		'query-wire' {
			response := client.query_response(handle, 'wire', mixed_arguments()!)!
			println(json2.encode({
				'raw_kind': response.raw_value.kind().str()
				'raw_hex':  response.raw_value.data().hex()
			}))
		}
		'update-wire' {
			update := client.start_update(handle, sdk.UpdateRequest{
				name:                'wire'
				id:                  'typed-once'
				arguments:           mixed_arguments()!
				wait_for:            .completed
				wait_timeout_millis: 10000
			})!
			result := client.update_result(update, 10000)!
			if result.failed { return error(result.message) }
			raw := result.raw_value or { return error('typed Update result missing') }
			println(json2.encode({
				'raw_kind': raw.kind().str()
				'raw_hex':  raw.data().hex()
			}))
		}
		'query' {
			response := client.query_response(handle, 'state', mixed_arguments()!)!
			raw := response.raw_value
			if raw.kind() != .json || raw.data().bytestr() != response.data {
				return error('query raw result changed')
			}
			println(response.data)
		}
		'signal' {
			client.signal(handle, 'mix', mixed_arguments()!)!
			println('OK')
		}
		'update' {
			update := client.start_update(handle, sdk.UpdateRequest{
				name:                'mix'
				id:                  'mixed-once'
				arguments:           mixed_arguments()!
				wait_for:            .completed
				wait_timeout_millis: 10000
			})!
			result := client.update_result(update, 10000)!
			if result.failed { return error(result.message) }
			raw := result.raw_value or { return error('Update raw result missing') }
			if raw.kind() != .json || raw.data().bytestr() != result.data {
				return error('Update raw result changed')
			}
			println(result.data)
		}
		'finish' {
			client.signal(handle, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
			println('OK')
		}
		'result-wire' {
			result := client.get_result(handle, 10000)!
			raw := result.raw_value or { return error('workflow raw result missing') }
			println(json2.encode({
				'raw_kind': raw.kind().str()
				'raw_hex':  raw.data().hex()
				'status':   result.status.str()
				'data_hex': result.data.bytes().hex()
			}))
		}
		'result' {
			result := client.get_result(handle, 10000)!
			if result.status != .completed { return error(result.message) }
			println(result.data)
		}
		else {
			return error('unknown mixed role')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}

fn typed_value(kind string) !sdk.PayloadValue {
	return match kind {
		'binary' {
			sdk.PayloadValue(sdk.BinaryValue{
				data: [u8(0), 255, 0, 128]
			})
		}
		'binary-null' {
			sdk.PayloadValue(sdk.BinaryValue{
				data: 'null'.bytes()
			})
		}
		'null' {
			sdk.PayloadValue(sdk.NullValue{})
		}
		'protobuf' {
			sdk.PayloadValue(sdk.protobuf_value(temporal.TemporalApiCommonV1_WorkflowExecution{
				workflow_id: 'typed-result'
				run_id:      '世界'
			}, 'temporal.api.common.v1.WorkflowExecution')!)
		}
		else {
			return error('unknown typed callback kind')
		}
	}
}

fn typed_activity(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	validate_arguments(input)!
	validate_raw(context.input_values())!
	return typed_value(context.info().activity_type.all_after('v.mixed.typed.'))!
}
