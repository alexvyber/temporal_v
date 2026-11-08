module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal

fn validate_values() ! {
	converter := sdk.DataConverter{
		codecs: [sdk.PayloadCodec(FrameCodec{}), sdk.PayloadCodec(FrameCodec{})]
	}
	mut encoded := converter.encode_arguments(mixed_arguments()!)!
	decoded := converter.decode_arguments(encoded)!
	validate_arguments(decoded.values)!
	validate_raw(decoded.raw_values)!
	assert converter.decode_values(encoded)! == decoded.values
	// Retained views own metadata and data, independently of the input and of
	// every public copy returned to callers.
	encoded[1].data[3] = 17
	encoded[1].metadata['encoding'][0] = u8(`X`)
	assert decoded.raw_values[1].encoding() == 'binary/plain'
	assert decoded.raw_values[1].data() == [u8(0), 255, 0, 128]
	mut copy := decoded.raw_values[1].to_payload()!
	copy.data[0] = 19
	copy.metadata['encoding'][0] = u8(`Y`)
	mut bytes := decoded.raw_values[1].data()
	bytes[0] = 20
	assert decoded.raw_values[1].data() == [u8(0), 255, 0, 128]
	assert decoded.raw_values[1].encoding() == 'binary/plain'
	plain := sdk.DataConverter{}
	bad_null := temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'binary/null'.bytes()
		}
		data:     [u8(1)]
	}
	if _ := plain.decode_arguments([bad_null]) { return error('nonempty null accepted') }
	bad_name := temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding':    'binary/protobuf'.bytes()
			'messageType': 'invalid..Name'.bytes()
		}
	}
	if _ := plain.decode_arguments([bad_name]) { return error('invalid protobuf name accepted') }
	unnamed := plain.encode_payload_values([
		sdk.PayloadValue(sdk.protobuf_value(temporal.GoogleProtobuf_Timestamp{ seconds: -1 }, '')!),
	])!
	assert plain.decode_arguments(unnamed)!.raw_values[0].message_type() == ''
	custom := sdk.DataConverter{
		payload_converter: sdk.BinaryPayloadConverter{}
		codecs:            [sdk.PayloadCodec(FrameCodec{})]
	}
	custom_bytes := [u8(0), 255, 2].bytestr()
	custom_wire := custom.encode_arguments(sdk.TypedArguments{
		values: [
			sdk.PayloadValue(custom_bytes),
		]
	})!
	assert custom.decode_arguments(custom_wire)!.values[0] == custom_bytes
	assert plain.encode_arguments(sdk.TypedArguments{})!.len == 0
	println('VALIDATION_OK')
}
