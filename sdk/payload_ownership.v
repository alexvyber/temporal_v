module sdk

import temporal

// Codec callbacks may mutate nested fields of their local payload structs.
// Deep snapshots preserve caller inputs and isolate retained callback buffers,
// including external payload details and forward-compatible unknown fields.
fn owned_payload_messages(values []temporal.TemporalApiCommonV1_Payload) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{cap: values.len}
	for value in values {
		result << temporal.TemporalApiCommonV1_Payload.decode(value.encode())!
	}
	return result
}

fn (converter DataConverter) encode_codec_payloads(values []temporal.TemporalApiCommonV1_Payload) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := owned_payload_messages(values)!
	for codec in converter.codecs {
		result = owned_payload_messages(codec.encode(result, converter.serialization_context)!)!
	}
	return result
}

// Protobuf metadata structs contain mutable nested arrays and unknown fields.
fn owned_optional_proto[M](value ?M) ?M {
	if message := value {
		return M.decode(message.encode()) or {
			panic('validated protobuf metadata snapshot failed: ' + err.msg())
		}
	}
	return none
}
