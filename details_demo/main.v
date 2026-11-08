module main

import sdk
import temporal
import example_converter
import os
import json2
import time

fn base_converter() !sdk.DataConverter {
	profile := os.getenv('V_SDK_DETAILS_PROTO')
	if profile != '' {
		payload_converter := if profile == 'binary' {
			sdk.PayloadConverter(sdk.ProtobufPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{})
		} else {
			sdk.PayloadConverter(sdk.ProtobufJsonPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{})
		}
		return sdk.DataConverter{
			payload_converter: payload_converter
			codecs:            [
				sdk.PayloadCodec(BoundCodec{
					tag: 65
				}),
				sdk.PayloadCodec(BoundCodec{
					tag: 66
				}),
			]
			failure_converter: sdk.DefaultFailureConverter{
				encode_common_attributes: true
			}
		}
	}
	if os.getenv('V_SDK_DETAILS_OPAQUE') == 'true' {
		return sdk.DataConverter{
			payload_converter: OpaqueConverter{}
			codecs:            [
				sdk.PayloadCodec(BoundCodec{
					tag: 65
				}),
				sdk.PayloadCodec(BoundCodec{
					tag: 66
				}),
			]
			failure_converter: sdk.DefaultFailureConverter{
				encode_common_attributes: true
			}
		}
	}
	return example_converter.from_env()!
}

fn run() ! {
	mode := os.args[1]
	if mode == 'nexus-failure-validation' {
		validate_nexus_failures()!
		return
	}
	if mode == 'cancellation-validation' {
		validate_cancellation_errors()!
		return
	}
	if mode == 'metadata-validation' {
		validate_metadata()!
		return
	}
	if mode == 'counts-validation' {
		validate_counts()!
		return
	}
	if mode == 'validation' {
		validate_details()!
		return
	}
	if mode == 'worker' {
		mut activities := sdk.Activities{}
		activities.register('v.details.error', fail_details)!
		activities.register('v.details.heartbeat', heartbeat_details)!
		activities.register('v.details.defer', defer_details)!
		activities.register('v.details.headers', headers_activity)!
		activities.register('v.details.cancel', cancel_details_activity)!
		mut services := sdk.NexusServices{}
		services.register('typed.details', 'echo', sdk.NexusOperation{ start: echo_nexus })!
		services.register('typed.details', 'captured', sdk.NexusOperation{
			start: captured_nexus_failure
		})!
		services.register('typed.details', 'cancel', sdk.NexusOperation{ start: cancel_details_nexus })!
		sdk.run_worker(sdk.WorkerOptions{
			...sdk.options_from_env()!
			data_converter:                converter()!
			query_interceptors:            [
				sdk.QueryInterceptor(InspectTypedQuery{}),
				sdk.QueryInterceptor(InspectTypedQuery{}),
			]
			update_validator_interceptors: [
				sdk.UpdateValidatorInterceptor(InspectTypedUpdate{}),
				sdk.UpdateValidatorInterceptor(InspectTypedUpdate{}),
			]
		}, definitions()!, activities, sdk.WorkerServices{
			nexus: services
		})!
		return
	}
	if mode == 'replay' {
		definitions()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id:    os.args[3]
			task_queue:     os.args[4]
			data_converter: converter()!
		})!
		println('REPLAY_OK')
		return
	}
	client := sdk.connect(sdk.ClientOptions{
		address:        os.getenv('TEMPORAL_ADDRESS')
		data_converter: converter()!
	})!
	defer { client.close() }
	if mode == 'start-metadata' {
		mut memo := detail_map()!
		memo['removed'] = sdk.NullValue{}
		println(json2.encode(client.start(sdk.WorkflowStart{
			workflow_id:   os.args[2]
			workflow_type: 'v.details'
			task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:     sdk.TypedArguments{
				values: [
					sdk.PayloadValue(sdk.json_value(DetailsInput{ mode: 'metadata' })!),
				]
			}
			headers:       detail_map()!
			memo:          memo
		})!))
		return
	}
	if mode == 'start-captured-nexus-workflow' {
		println(json2.encode(client.start(sdk.WorkflowStart{
			workflow_id:   os.args[2]
			workflow_type: 'v.details'
			task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:     sdk.TypedArguments{
				values: [
					sdk.PayloadValue(sdk.json_value(DetailsInput{
						mode:     'nexus-captured-error'
						endpoint: os.args[3]
						index:    os.args[4].int()
					})!),
				]
			}
		})!))
		return
	}
	if mode == 'start-cancel-nexus-workflow' {
		println(json2.encode(client.start(sdk.WorkflowStart{
			workflow_id:   os.args[2]
			workflow_type: 'v.details'
			task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:     sdk.TypedArguments{
				values: [
					sdk.PayloadValue(sdk.json_value(DetailsInput{
						mode:     'nexus-cancel-error'
						endpoint: os.args[3]
					})!),
				]
			}
		})!))
		return
	}
	if mode == 'start-cancel-nexus' {
		println(json2.encode(client.start_nexus_operation(sdk.NexusExecutionStart{
			endpoint:     os.args[3]
			service:      'typed.details'
			operation:    'cancel'
			operation_id: os.args[2]
			input:        detail_arguments()!.values[1]
		})!))
		return
	}
	if mode == 'cancel-nexus-result' {
		handle := json2.decode[sdk.NexusExecutionHandle](os.args[2])!
		result := client.nexus_operation_result(handle, 30 * time.second)!
		failure := result.failure or { return error('Nexus cancellation failure missing') }
		if result.status != .nexus_operation_execution_status_canceled
			|| failure.root_kind() != .canceled || validate_failure(failure)! != 1 {
			return error('Nexus cancellation status/details changed')
		}
		println('OK')
		return
	}
	if mode == 'start-nexus-workflow' {
		println(json2.encode(client.start(sdk.WorkflowStart{
			workflow_id:   os.args[2]
			workflow_type: 'v.details'
			task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:     sdk.TypedArguments{
				values: [
					sdk.PayloadValue(sdk.json_value(DetailsInput{
						mode:     'nexus'
						endpoint: os.args[3]
						index:    os.args[4].int()
					})!),
				]
			}
		})!))
		return
	}
	if mode == 'start-nexus' {
		println(json2.encode(client.start_nexus_operation(sdk.NexusExecutionStart{
			operation_id: os.args[2]
			endpoint:     os.args[3]
			service:      'typed.details'
			operation:    'echo'
			input:        detail_arguments()!.values[os.args[4].int()]
		})!))
		return
	}
	if mode == 'direct-nexus-result' {
		handle := json2.decode[sdk.NexusExecutionHandle](os.args[2])!
		result := client.nexus_operation_result(handle, 30 * time.second)!
		value := result.value or {
			return error('direct typed Nexus result missing: ' + result.status.str())
		}

		validate_one_value(value, os.args[3].int())!
		println('OK')
		return
	}
	if mode == 'start' {
		println(json2.encode(client.start(sdk.WorkflowStart{
			workflow_id:   os.args[2]
			workflow_type: 'v.details'
			task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:     sdk.TypedArguments{
				values: [
					sdk.PayloadValue(sdk.json_value(DetailsInput{ mode: os.args[3] })!),
				]
			}
		})!))
		return
	}
	if mode.starts_with('token-') {
		token := json2.decode[sdk.ActivityTaskToken](os.read_file(os.args[2])!)!
		match mode {
			'token-heartbeat' {
				println(json2.encode(client.heartbeat_activity(token, detail_arguments()!)!))
			}
			'token-complete' {
				client.complete_activity(token, sdk.BinaryValue{ data: 'null'.bytes() })!
				println('OK')
			}
			'token-fail' {
				client.fail_activity(token, sdk.ApplicationError{
					message:       'typed deferred failure'
					non_retryable: true
					details:       detail_arguments()!
				}, detail_arguments()!)!
				println('OK')
			}
			'token-cancel' {
				client.report_activity_cancelled(token, detail_arguments()!)!
				println('OK')
			}
			else {
				return error('unknown details token role')
			}
		}

		return
	}
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	match mode {
		'metadata-query' {
			println(client.query_response(handle, 'meta', detail_arguments()!, sdk.QueryOptions{
				headers: detail_map()!
			})!.data)
		}
		'metadata-signal' {
			client.signal(handle, 'meta', detail_arguments()!, sdk.MessageOptions{
				headers: detail_map()!
			})!
			println('OK')
		}
		'metadata-update' {
			update := client.start_update(handle, sdk.UpdateRequest{
				name:                'meta'
				arguments:           detail_arguments()!
				headers:             detail_map()!
				id:                  'typed-metadata-once'
				wait_for:            .completed
				wait_timeout_millis: 10000
			})!
			result := client.update_result(update, 10000)!
			value := result.value or { return error('metadata Update result missing') }
			validate_one_value(value, 1)!
			println('OK')
		}
		'cancel-all' {
			client.signal(handle, 'cancel-all', sdk.Arguments(sdk.TypedArguments{}))!
			println('OK')
		}
		'nexus-result' {
			result := client.get_result(handle, 30000)!
			if result.status != .completed {
				return error('typed Nexus workflow failed: ' + result.message)
			}
			value := result.value or { return error('typed Nexus workflow result missing') }
			validate_one_value(value, os.args[3].int())!
			println('OK')
		}
		'query' {
			println(client.query(handle, 'state', detail_arguments()!)!)
		}
		'query-failure' {
			client.query(handle, 'fail', detail_arguments()!) or {
				if err is sdk.QueryError {
					if validate_failure(err.failure)! < 1 { return error('query lost details') }
					println('OK')
					return
				}
				return err
			}
			return error('failure query succeeded')
		}
		'update-failure' {
			update := client.start_update(handle, sdk.UpdateRequest{
				name:                'fail'
				arguments:           detail_arguments()!
				id:                  'typed-failure-once'
				wait_for:            .completed
				wait_timeout_millis: 10000
			})!
			result := client.update_result(update, 10000)!
			if !result.failed { return error('Update failure succeeded') }
			failure := result.failure or { return error('Update failure missing') }
			if validate_failure(failure)! < 1 { return error('Update lost details') }
			println('OK')
		}
		'finish', 'cancel-activity' {
			client.signal(handle, if mode == 'finish' { 'finish' } else { 'cancel-activity' },
				sdk.Arguments(sdk.TypedArguments{}))!
			println('OK')
		}
		'cancel' {
			client.cancel(handle, 'typed cancel')!
			println('OK')
		}
		'terminate' {
			client.terminate(handle, reason: 'typed termination', details: detail_arguments()!)!
			println('OK')
		}
		'result' {
			result := client.get_result(handle, 30000)!
			match result.status {
				.completed {
					value := result.value or { return error('completed typed result missing') }
					if value !is sdk.JsonValue { return error('completion was not JSON') }
					if value.data != 'true' {
						return error('wrong typed completion')
					}
				}
				.failed {
					failure := result.failure or { return error('workflow failure missing') }
					if validate_failure(failure)! < 1 {
						return error('workflow failure lost details')
					}
				}
				.canceled, .terminated {
					validate_detail_values(result.detail_values)!
					if result.raw_details.len != 7 {
						return error('workflow close raw details missing')
					}
				}
				else {
					return error('unexpected typed workflow status')
				}
			}

			println(json2.encode({
				'status': result.status.str()
				'data':   result.data
			}))
		}
		else {
			return error('unknown details role')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}

fn echo_nexus(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	value := context.typed_input()!
	if value.logical_data() != input { return error('Nexus logical input changed') }
	if _ := context.input_payload() {
	} else {
		return error('Nexus raw input missing')
	}
	return sdk.NexusResult{
		value: value
	}
}
