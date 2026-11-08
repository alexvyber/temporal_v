module sdk

import protobuf
import encoding.utf8
import temporal

pub interface PayloadConverter {
	to_payload(value PayloadValue, context SerializationContext) !temporal.TemporalApiCommonV1_Payload
	from_payload(value temporal.TemporalApiCommonV1_Payload, context SerializationContext) !PayloadValue
}

pub interface PayloadCodec {
	encode(values []temporal.TemporalApiCommonV1_Payload, context SerializationContext) ![]temporal.TemporalApiCommonV1_Payload
	decode(values []temporal.TemporalApiCommonV1_Payload, context SerializationContext) ![]temporal.TemporalApiCommonV1_Payload
}

pub struct JsonPayloadConverter {}

pub fn (converter JsonPayloadConverter) to_payload(value PayloadValue, context SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	data := match value {
		string { value }
		JsonValue { value.data }
		else { return DefaultPayloadConverter{}.to_payload(value, context)! }
	}

	if !utf8.validate_str(data) { return error('JSON payload must be valid UTF-8') }
	protobuf.json_parse(data) or { return error(protocol_text(err.msg())) }
	return temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'json/plain'.bytes()
		}
		data:     data.bytes()
	}
}

pub fn (converter JsonPayloadConverter) from_payload(value temporal.TemporalApiCommonV1_Payload, context SerializationContext) !PayloadValue {
	if value.metadata['encoding'].bytestr() == 'binary/null' {
		if value.data.len != 0 { return error('binary/null payload must be empty') }
		return NullValue{}
	}
	if value.metadata['encoding'].bytestr() != 'json/plain' {
		return error('unsupported payload encoding')
	}
	data := value.data.bytestr()
	if !utf8.validate_str(data) { return error('JSON payload must be valid UTF-8') }
	protobuf.json_parse(data) or { return error(protocol_text(err.msg())) }
	return JsonValue{
		data: data
	}
}

pub struct BinaryPayloadConverter {}

pub fn (converter BinaryPayloadConverter) to_payload(value PayloadValue, context SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	data := match value {
		string { value.bytes() }
		BinaryValue { value.data.clone() }
		else { return DefaultPayloadConverter{}.to_payload(value, context)! }
	}

	return temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'binary/plain'.bytes()
		}
		data:     data
	}
}

pub fn (converter BinaryPayloadConverter) from_payload(value temporal.TemporalApiCommonV1_Payload, context SerializationContext) !PayloadValue {
	if value.metadata['encoding'].bytestr() != 'binary/plain' {
		return DefaultPayloadConverter{}.from_payload(value, context)!
	}
	return BinaryValue{
		data: value.data.clone()
	}
}

pub struct DataConverter {
pub:
	external_storage      ?&ExternalStorage
	serialization_context SerializationContext
	failure_converter     FailureConverter = DefaultFailureConverter{}
	payload_converter     PayloadConverter = DefaultPayloadConverter{}
	codecs                []PayloadCodec
}

pub fn (converter DataConverter) encode_values(values []string) ![]temporal.TemporalApiCommonV1_Payload {
	mut typed := []PayloadValue{}
	for value in values {
		typed << PayloadValue(value)
	}
	return converter.encode_payload_values(typed)!
}

pub fn (converter DataConverter) decode_values(values []temporal.TemporalApiCommonV1_Payload) ![]string {
	mut result := []string{}
	for value in converter.decode_payloads(values)! {
		result << converter.payload_converter.from_payload(value, converter.serialization_context)!.logical_data()
	}
	return result
}

fn (converter DataConverter) decode_payloads(values []temporal.TemporalApiCommonV1_Payload) ![]temporal.TemporalApiCommonV1_Payload {
	mut decoded := owned_payload_messages(converter.retrieve_payloads(values)!)!
	for index := converter.codecs.len - 1; index >= 0; index-- {
		decoded = owned_payload_messages(converter.codecs[index].decode(decoded,
			converter.serialization_context)!)!
	}
	return decoded
}

pub fn (converter DataConverter) decode_arguments(values []temporal.TemporalApiCommonV1_Payload) !DecodedArguments {
	mut result := []string{}
	mut raw_values := []RawValue{}
	mut typed_values := []PayloadValue{}
	for value in converter.decode_payloads(values)! {
		raw_values << RawValue{
			payload: temporal.TemporalApiCommonV1_Payload.decode(value.encode())!
		}
		typed := clone_payload_value(converter.payload_converter.from_payload(value,
			converter.serialization_context)!)
		typed_values << typed
		result << typed.logical_data()
	}
	return DecodedArguments{
		values:       result
		raw_values:   raw_values
		typed_values: typed_values
	}
}

// Singular protocol fields must decode to one semantic value. Codec input and
// output counts may differ for argument collections.
fn (converter DataConverter) decode_single_argument(values []temporal.TemporalApiCommonV1_Payload) !DecodedArguments {
	decoded := converter.decode_arguments(values)!
	if decoded.values.len != 1 {
		return error('expected exactly one decoded payload value, received ${decoded.values.len}')
	}
	return decoded
}

pub fn (converter DataConverter) encode_value(value PayloadValue) !temporal.TemporalApiCommonV1_Payload {
	encoded := converter.encode_payload_values([value])!
	if encoded.len != 1 {
		return error('single-payload field requires exactly one encoded payload')
	}
	return encoded[0]
}

pub fn (converter DataConverter) decode_value(value temporal.TemporalApiCommonV1_Payload) !PayloadValue {
	return converter.decode_single_argument([value])!.typed_values[0]
}

fn (converter DataConverter) payload_data(value temporal.TemporalApiCommonV1_Payload) !string {
	return converter.decode_single_argument([value])!.values[0]
}

pub type Arguments = []string | string | TypedArguments

pub fn (converter DataConverter) encode_arguments(arguments Arguments) ![]temporal.TemporalApiCommonV1_Payload {
	match arguments {
		TypedArguments { return converter.encode_payload_values(arguments.values)! }
		string { return converter.encode_values([arguments])! }
		[]string { return converter.encode_values(arguments)! }
	}
}

// Use the explicit arguments field for multi-argument input. data is a convenience
// for single values and never repacks arbitrary bytes as JSON.
fn event_data(arguments []string) string {
	if arguments.len == 0 { return 'null' }
	if arguments.len == 1 { return arguments[0] }
	return ''
}

pub fn single_argument(arguments []string) !string {
	if arguments.len != 1 {
		return error('expected exactly one argument, received ${arguments.len}')
	}
	return arguments[0]
}

fn failure(message string, category string) temporal.TemporalApiFailureV1_Failure {
	return temporal.TemporalApiFailureV1_Failure{
		message:      protocol_text(message)
		source:       'V'
		failure_info: temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case{
			value: temporal.TemporalApiFailureV1_ApplicationFailureInfo{
				type_:         protocol_text(category)
				non_retryable: true
			}
		}
	}
}

// Header keys are sorted so stateful codecs see a deterministic order.
pub fn (converter DataConverter) decode_map(headers map[string]temporal.TemporalApiCommonV1_Payload) !map[string]PayloadValue {
	mut result := map[string]PayloadValue{}
	mut keys := headers.keys()
	keys.sort()
	for key in keys {
		result[key] = converter.decode_value(headers[key])!
	}
	return result
}
