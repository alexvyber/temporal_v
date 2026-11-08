module example_converter

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import x.json2

const value_context_key = 'temporal-v/value-context'
const codec_context_key = 'temporal-v/codec-context'

fn verify_context(metadata map[string][]u8, key string, actual sdk.SerializationContext) ! {
	if key !in metadata { return } // Raw protobuf callers may supply ordinary payloads.
	encoded := json2.decode[sdk.SerializationContext](metadata[key].bytestr())!
	if actual.kind == .unspecified || encoded.kind == .unspecified { return }
	// Workflow-backed asynchronous Nexus callbacks retain workflow context. This
	// fixture remains self-describing for the same reason real contextual codecs
	// must support completion paths that do not carry Nexus serialization context.
	if actual.kind == .nexus && encoded.kind == .workflow { return }
	if actual.kind != encoded.kind || actual.namespace != encoded.namespace
		|| actual.workflow_id != encoded.workflow_id || actual.activity_id != encoded.activity_id
		|| actual.is_local != encoded.is_local || actual.endpoint != encoded.endpoint
		|| actual.service != encoded.service || actual.operation != encoded.operation {
		return error('serialization scope mismatch: encoded=${json2.encode(encoded)} actual=${json2.encode(actual)}')
	}
}

// Binary input keeps arbitrary byte strings intact. JSON input remains a
// supported wire encoding for independent raw service requests.
struct ContextPayloadConverter {}

fn (converter ContextPayloadConverter) to_payload(value sdk.PayloadValue, context sdk.SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	mut payload := sdk.BinaryPayloadConverter{}.to_payload(value, context)!
	payload.metadata[value_context_key] = json2.encode(context).bytes()
	return payload
}

fn (converter ContextPayloadConverter) from_payload(value temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) !sdk.PayloadValue {
	verify_context(value.metadata, value_context_key, context)!
	if value.metadata['encoding'].bytestr() == 'binary/plain' {
		return sdk.BinaryPayloadConverter{}.from_payload(value, context)!
	}
	return sdk.DefaultPayloadConverter{}.from_payload(value, context)!
}

struct ContextCodec {}

fn (codec ContextCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		mut metadata := value.metadata.clone()
		metadata[codec_context_key] = json2.encode(context).bytes()
		result << temporal.TemporalApiCommonV1_Payload{
			...value
			metadata: metadata
		}
	}
	return result
}

fn (codec ContextCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		verify_context(value.metadata, codec_context_key, context)!
		mut metadata := value.metadata.clone()
		metadata.delete(codec_context_key)
		result << temporal.TemporalApiCommonV1_Payload{
			...value
			metadata: metadata
		}
	}
	return result
}
