module main

import sdk
import temporal

fn validate_json_values() ! {
	name := 'temporal.api.common.v1.WorkflowExecution'
	converter := sdk.DataConverter{
		payload_converter: sdk.ProtobufJsonPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{
			message_type: name
		}
	}
	input := '{"workflow_id":"世界","runId":"run"}'
	encoded := converter.encode_values([input]) or { panic(err) }
	assert encoded[0].metadata['encoding'].bytestr() == 'json/protobuf'
	decoded := converter.decode_arguments(encoded) or { panic(err) }
	message := temporal.TemporalApiCommonV1_WorkflowExecution.from_json(decoded.values[0]) or {
		panic(err)
	}
	assert message.workflow_id == '世界' && message.run_id == 'run'
	for invalid in ['{"workflowId":"x","workflow_id":"y"}', '{"unknown":1}', '{"workflowId":1}'] {
		if _ := converter.encode_values([invalid]) { panic('invalid message accepted') }
	}

	explicit := sdk.protobuf_json_value(temporal.TemporalApiCommonV1_WorkflowExecution{
		workflow_id: 'typed-json-世界'
	}, name) or { panic(err) }
	plain := sdk.DataConverter{}
	mixed := plain.encode_arguments(sdk.TypedArguments{
		values: [
			sdk.PayloadValue(explicit),
			sdk.PayloadValue(sdk.NullValue{}),
			sdk.PayloadValue(sdk.BinaryValue{
				data: [u8(0), 255]
			}),
		]
	}) or { panic(err) }
	values := plain.decode_arguments(mixed) or { panic(err) }
	assert values.raw_values[0].kind() == .protobuf_json
	value := values.raw_values[0].as_value() or { panic(err) }
	assert value is sdk.ProtobufJsonValue
	assert value.message_type == name
	message2 := temporal.TemporalApiCommonV1_WorkflowExecution.from_json(value.data) or {
		panic(err)
	}
	assert message2.workflow_id == 'typed-json-世界'
	invalid_name := sdk.ProtobufJsonPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{
		message_type: 'other.Type'
	}
	if _ := invalid_name.from_payload(encoded[0], sdk.SerializationContext{}) {
		panic('wrong JSON message type accepted')
	}
	invalid_utf8 := temporal.TemporalApiCommonV1_WorkflowExecution{
		workflow_id: [u8(255)].bytestr()
	}
	if _ := sdk.protobuf_json_value(invalid_utf8, name) { panic('invalid JSON UTF-8 accepted') }
	integer := sdk.ProtobufJsonPayloadConverter[temporal.GoogleProtobuf_Int64Value]{}
	for invalid in ['9223372036854775808', '1.0000000000000000001', '-9223372036854775809'] {
		if _ := integer.to_payload(invalid, sdk.SerializationContext{}) {
			panic('invalid exact integer accepted')
		}
	}
	assert integer.from_payload(integer.to_payload('9223372036854775807', sdk.SerializationContext{}) or {
		panic(err)
	}, sdk.SerializationContext{}) or { panic(err) }.logical_data() == '"9223372036854775807"'

	// Explicit JSON remains JSON even when ordinary strings use a binary converter.
	binary := sdk.DataConverter{
		payload_converter: sdk.BinaryPayloadConverter{}
	}
	explicit_json := sdk.json_value('literal-世界')!
	json_payload := binary.encode_payload_values([sdk.PayloadValue(explicit_json)])!
	assert json_payload[0].metadata['encoding'].bytestr() == 'json/plain'
	assert json_payload[0].data.bytestr() == '"literal-世界"'
	json_raw := sdk.DataConverter{}.decode_arguments(json_payload)!.raw_values[0]
	forwarded_json := json_raw.as_value()!
	assert forwarded_json is sdk.JsonValue
	assert binary.encode_payload_values([forwarded_json])![0].encode() == json_payload[0].encode()
	println('PROTOBUF_JSON_CONVERTER_OK')
}
