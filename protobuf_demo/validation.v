module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal

fn validate_options() ! {
	name := 'temporal.api.common.v1.WorkflowExecution'
	codec := sdk.DataConverter{
		payload_converter: sdk.ProtobufPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{
			message_type: name
		}
	}
	message := temporal.TemporalApiCommonV1_WorkflowExecution{
		workflow_id: 'protobuf-世界'
		run_id:      'run'
		pb_unknown:  [
			u8(0xa0),
			0x06,
			0x07,
		]
	}
	bytes := message.encode()
	payloads := codec.encode_values([bytes.bytestr()])!
	assert payloads[0].metadata['encoding'].bytestr() == 'binary/protobuf'
	assert payloads[0].metadata['messageType'].bytestr() == name
	assert codec.decode_values(payloads)![0].bytes() == bytes
	mut unnamed := payloads[0]
	unnamed.metadata = payloads[0].metadata.clone()
	unnamed.metadata.delete('messageType')
	assert codec.decode_values([unnamed])![0].bytes() == bytes
	mut wrong := payloads[0]
	wrong.metadata = payloads[0].metadata.clone()
	wrong.metadata['messageType'] = 'other.Message'.bytes()
	if _ := codec.decode_values([wrong]) { return error('wrong protobuf message type accepted') }
	wrong.metadata['messageType'] = [u8(255)]
	if _ := codec.decode_values([wrong]) { return error('invalid protobuf message name accepted') }
	for bytes_bad in [[u8(255)], [u8(0x0a), 5, 1], [u8(0x0a), 1, 255],
		[u8(0x08), 1, 65]] {
		if _ := codec.encode_values([bytes_bad.bytestr()]) {
			return error('invalid protobuf data accepted')
		}
	}
	for invalid in ['.Type', 'package..Type', '2bad.Type', 'pkg.Message\x00'] {
		invalid_codec := sdk.ProtobufPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{
			message_type: invalid
		}
		if _ := invalid_codec.to_payload(bytes.bytestr(), sdk.SerializationContext{}) {
			return error('invalid configured type name accepted')
		}
	}
	assert codec.encode_arguments(sdk.Arguments(sdk.TypedArguments{}))!.len == 0
	assert codec.encode_arguments('')!.len == 1
	timestamp_codec := sdk.DataConverter{
		payload_converter: sdk.ProtobufPayloadConverter[temporal.GoogleProtobuf_Timestamp]{
			message_type: 'google.protobuf.Timestamp'
		}
	}
	timestamp := temporal.GoogleProtobuf_Timestamp{
		seconds: -1
		nanos:   999999999
	}
	encoded := timestamp_codec.encode_values([timestamp.encode().bytestr()])!
	assert temporal.GoogleProtobuf_Timestamp.decode(timestamp_codec.decode_values(encoded)![0].bytes())! == timestamp
	println('VALIDATION_OK')
}
