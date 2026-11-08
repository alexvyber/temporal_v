module main

import temporal_v.sdk
import temporal_v.temporal

// Framing and context binding for tests, not encryption.
fn scope_key(context sdk.SerializationContext) string {
	return [context.kind.str(), context.namespace, context.workflow_id, context.activity_id,
		context.is_local.str(), context.endpoint, context.service, context.operation].join('|')
}

struct BoundCodec {
	tag u8
}

fn (codec BoundCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		mut metadata := value.metadata.clone()
		metadata['codec-scope-${codec.tag}'] = scope_key(context).bytes()
		mut data := [u8(255), 0, 165]
		data << value.data
		result << temporal.TemporalApiCommonV1_Payload{
			...value
			metadata: metadata
			data:     data
		}
	}
	return result
}

fn (codec BoundCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		if value.metadata['codec-scope-${codec.tag}'].bytestr() != scope_key(context)
			|| value.data.len < 3 || value.data[..3] != [u8(255), 0, 165] {
			return error('codec scope/frame mismatch')
		}
		mut metadata := value.metadata.clone()
		metadata.delete('codec-scope-${codec.tag}')
		result << temporal.TemporalApiCommonV1_Payload{
			...value
			metadata: metadata
			data:     value.data[3..].clone()
		}
	}
	return result
}

struct OpaqueConverter {}

fn (converter OpaqueConverter) to_payload(value sdk.PayloadValue, context sdk.SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	inner := sdk.DefaultPayloadConverter{}.to_payload(value, context)!
	return temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding':      'test/opaque'.bytes()
			'payload-scope': scope_key(context).bytes()
		}
		data:     inner.encode()
	}
}

fn (converter OpaqueConverter) from_payload(value temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) !sdk.PayloadValue {
	if value.metadata['encoding'].bytestr() != 'test/opaque'
		|| value.metadata['payload-scope'].bytestr() != scope_key(context) {
		return error('opaque payload scope mismatch')
	}
	return sdk.DefaultPayloadConverter{}.from_payload(temporal.TemporalApiCommonV1_Payload.decode(value.data)!,
		context)!
}

fn validate_details() ! {
	arguments := detail_arguments()!
	source := sdk.SerializationContext{
		kind:        .activity
		namespace:   'default'
		workflow_id: 'parent'
		activity_id: 'one'
	}
	target := sdk.SerializationContext{
		kind:        .workflow
		namespace:   'default'
		workflow_id: 'parent'
	}
	payload_converters := [sdk.PayloadConverter(sdk.DefaultPayloadConverter{}),
		sdk.PayloadConverter(sdk.BinaryPayloadConverter{}), sdk.PayloadConverter(OpaqueConverter{}),
		sdk.PayloadConverter(sdk.ProtobufPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{}),
		sdk.PayloadConverter(sdk.ProtobufJsonPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{})]
	for payload_converter in payload_converters {
		before := sdk.DataConverter{
			payload_converter:     payload_converter
			serialization_context: source
			codecs:                [
				sdk.PayloadCodec(BoundCodec{
					tag: 65
				}),
				sdk.PayloadCodec(BoundCodec{
					tag: 66
				}),
			]
			failure_converter:     sdk.DefaultFailureConverter{
				encode_common_attributes: true
			}
		}
		after := before.with_context(target)
		nested := sdk.ApplicationError{
			message:       'nested-世界'
			non_retryable: true
			details:       arguments
		}
		failure := sdk.ApplicationError{
			message:       'outer'
			non_retryable: true
			details:       arguments
			cause:         nested
		}
		encoded := before.encode_failure(failure, 'test')!
		captured := before.decode_failure(encoded)
		assert validate_failure(captured)! == 2
		rebound := after.encode_failure(captured, 'test')!
		decoded := after.decode_failure(rebound)
		assert decoded.attributes_error == '' && decoded.details_error == ''
		assert validate_failure(decoded)! == 2
		assert after.encode_failure(decoded, 'test')!.encode() == rebound.encode()
		if payload_converter is OpaqueConverter {
			assert decoded.raw_details[0].kind() == .unknown
		} else {
			assert decoded.raw_details[4].kind() == .null_value
				&& decoded.raw_details[5].kind() == .binary
		}
		// A source-bound codec must reject the rebound bytes before interpreting data.
		invalid_scope := before.decode_failure(rebound)
		assert invalid_scope.attributes_error != ''
	}
	// Explicit raw forwarding retains unrecognized metadata and arbitrary bytes.
	original := temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'binary/plain'.bytes()
			'extra':    [u8(0), 255]
		}
		data:     [u8(0), 255]
	}
	plain := sdk.DataConverter{}
	raw := plain.decode_arguments([original])!.raw_values[0]
	forwarded := plain.encode_payload_values([sdk.PayloadValue(raw)])![0]
	assert forwarded.encode() == original.encode()
	println('TYPED_DETAILS_VALIDATION_OK')
}
