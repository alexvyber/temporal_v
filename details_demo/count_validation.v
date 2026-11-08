module main

import temporal_v.sdk
import temporal_v.temporal

struct InvalidCountCodec {
	count int
}

fn (codec InvalidCountCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return []temporal.TemporalApiCommonV1_Payload{len: codec.count, init: temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'binary/null'.bytes()
		}
	}}
}

fn (codec InvalidCountCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return codec.encode(values, context)!
}

fn validate_counts() ! {
	converter := sdk.DataConverter{
		codecs: [sdk.PayloadCodec(PackCodec{}), sdk.PayloadCodec(SplitCodec{}),
			sdk.PayloadCodec(PackCodec{})]
	}
	for length in [0, 1, 7] {
		values := detail_arguments()!.values[..length].clone()
		encoded := converter.encode_payload_values(values)!
		if encoded.len != 1 { return error('packing must produce one payload') }
		decoded := converter.decode_arguments(encoded)!
		if decoded.typed_values.len != length || decoded.raw_values.len != length {
			return error('codec did not restore argument count')
		}
		for index, value in decoded.typed_values {
			validate_one_value(value, index)!
		}
	}
	for value in detail_arguments()!.values {
		encoded := converter.encode_value(value)!
		decoded := converter.decode_value(encoded)!
		if decoded.logical_data() != value.logical_data() {
			return error('singular codec roundtrip changed value')
		}
	}
	for count in [0, 2] {
		invalid := sdk.DataConverter{
			codecs: [sdk.PayloadCodec(InvalidCountCodec{
				count: count
			})]
		}
		if _ := invalid.encode_value(sdk.NullValue{}) {
			return error('invalid singular encode count accepted')
		}
		if _ := invalid.decode_value(temporal.TemporalApiCommonV1_Payload{}) {
			return error('invalid singular decode count accepted')
		}
	}
	split := SplitCodec{}
	pieces := split.encode([temporal.TemporalApiCommonV1_Payload{}], sdk.SerializationContext{})!
	if _ := split.decode(pieces[..1], sdk.SerializationContext{}) {
		return error('missing split piece accepted')
	}
	if _ := split.decode([pieces[1], pieces[0]], sdk.SerializationContext{}) {
		return error('reordered split pieces accepted')
	}
	pack := PackCodec{}
	if _ := pack.decode([temporal.TemporalApiCommonV1_Payload{}], sdk.SerializationContext{}) {
		return error('missing pack marker accepted')
	}
	println('CODEC_COUNTS_VALIDATION_OK')
}
