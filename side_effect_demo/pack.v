module main

import temporal_v.sdk
import temporal_v.temporal

// Exercise cardinality-changing codecs without losing inner metadata or fields.
struct PackCodec {}

fn (codec PackCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return [
		temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding': 'binary/test-payloads'.bytes()
			}
			data:     temporal.TemporalApiCommonV1_Payloads{
				payloads: values
			}.encode()
		},
	]
}

fn (codec PackCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	if values.len == 0 { return []temporal.TemporalApiCommonV1_Payload{} }
	if values.len != 1 || values[0].metadata['encoding'].bytestr() != 'binary/test-payloads' {
		return error('invalid packed payload collection')
	}
	return temporal.TemporalApiCommonV1_Payloads.decode(values[0].data)!.payloads
}

struct SplitCodec {}

fn (codec SplitCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		data := value.encode()
		middle := data.len / 2
		for part in 0 .. 2 {
			result << temporal.TemporalApiCommonV1_Payload{
				metadata: {
					'encoding': 'binary/test-split'.bytes()
					'part':     part.str().bytes()
				}
				data:     if part == 0 { data[..middle].clone() } else { data[middle..].clone() }
			}
		}
	}
	return result
}

fn (codec SplitCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	if values.len % 2 != 0 { return error('incomplete split payload pair') }
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for index := 0; index < values.len; index += 2 {
		mut data := []u8{}
		for part in 0 .. 2 {
			value := values[index + part]
			if value.metadata['encoding'].bytestr() != 'binary/test-split'
				|| value.metadata['part'].bytestr() != part.str() {
				return error('invalid split payload order or encoding')
			}
			data << value.data
		}
		result << temporal.TemporalApiCommonV1_Payload.decode(data)!
	}
	return result
}
