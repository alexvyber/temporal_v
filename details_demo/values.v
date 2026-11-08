module main

import temporal_v.sdk
import temporal_v.temporal

fn detail_arguments() !sdk.TypedArguments {
	message := temporal.TemporalApiCommonV1_WorkflowExecution{
		workflow_id: 'details-世界'
		pb_unknown:  [
			u8(0xa0),
			6,
			7,
		]
	}
	return sdk.TypedArguments{
		values: [sdk.PayloadValue(sdk.JsonValue{
			data: '{"number":7}'
		}),
			sdk.PayloadValue(sdk.BinaryValue{
				data: [u8(0), 255, 128]
			}),
			sdk.PayloadValue(sdk.protobuf_value(message,
				'temporal.api.common.v1.WorkflowExecution')!),
			sdk.PayloadValue(sdk.protobuf_json_value(message,
				'temporal.api.common.v1.WorkflowExecution')!),
			sdk.PayloadValue(sdk.NullValue{}),
			sdk.PayloadValue(sdk.BinaryValue{
				data: 'null'.bytes()
			}),
			sdk.PayloadValue(sdk.JsonValue{
				data: '"null"'
			})]
	}
}

fn validate_one_value(value sdk.PayloadValue, index int) ! {
	expected := detail_arguments()!.values[index]
	if value.type_name() != expected.type_name() || value.logical_data() != expected.logical_data() {
		return error('typed Nexus value changed')
	}
	match value {
		sdk.ProtobufValue {
			if value.message_type != 'temporal.api.common.v1.WorkflowExecution' {
				return error('typed binary message name changed')
			}
		}
		sdk.ProtobufJsonValue {
			if value.message_type != 'temporal.api.common.v1.WorkflowExecution' {
				return error('typed JSON message name changed')
			}
		}
		else {}
	}
}

fn validate_detail_values(values []sdk.PayloadValue) ! {
	expected := detail_arguments()!.values
	if values.len != expected.len { return error('typed detail count changed') }
	for index, value in values {
		match value {
			sdk.ProtobufValue {
				if value.message_type != 'temporal.api.common.v1.WorkflowExecution' {
					return error('binary protobuf name changed')
				}
			}
			sdk.ProtobufJsonValue {
				if value.message_type != 'temporal.api.common.v1.WorkflowExecution' {
					return error('JSON protobuf name changed')
				}
			}
			else {}
		}

		if value.type_name() != expected[index].type_name()
			|| value.logical_data() != expected[index].logical_data() {
			return error('typed detail ${index} changed: ${value.type_name()}')
		}
	}
}

fn validate_failure(value sdk.Failure) !int {
	mut count := 0
	if value.details_error != '' || value.attributes_error != '' {
		return error('failure decoding: ' + value.details_error + value.attributes_error)
	}
	if value.details.len > 0 {
		validate_detail_values(value.detail_values)!
		if value.raw_details.len != 7 { return error('raw detail count changed') }
		for index, typed in value.detail_values {
			if typed.logical_data() != value.details[index] {
				return error('logical details changed')
			}
		}
		count++
	}
	if cause := value.cause { count += validate_failure(*cause)! }
	return count
}

struct InspectTypedQuery {}

fn (interceptor InspectTypedQuery) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.QueryNext) !sdk.PayloadValue {
	validate_detail_values(next.typed_input_values()!)!
	if name == 'meta' { validate_detail_map(next.headers()!)! }
	return next.execute(name, input)!
}

struct InspectTypedUpdate {}

fn (interceptor InspectTypedUpdate) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.UpdateValidatorNext) ! {
	validate_detail_values(next.typed_input_values()!)!
	if name == 'meta' { validate_detail_map(next.headers()!)! }
	next.execute(name, input)!
}
