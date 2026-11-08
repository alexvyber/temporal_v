module sdk

import temporal

pub enum PayloadKind {
	unknown
	json
	binary
	protobuf
	protobuf_json
	null_value
}

// A codec-decoded value before the configured payload converter. The payload
// stays private so inspection cannot mutate a retained activation or input.
pub struct RawValue {
	payload temporal.TemporalApiCommonV1_Payload @[json: '-']
}

// Construct an owned raw view from a codec-decoded payload. Payload converters
// can use this to preserve an unknown application encoding without interpreting it.
pub fn raw_value(payload temporal.TemporalApiCommonV1_Payload) !RawValue {
	return RawValue{
		payload: temporal.TemporalApiCommonV1_Payload.decode(payload.encode())!
	}
}

pub fn (value RawValue) encoding() string {
	return value.payload.metadata['encoding'].bytestr()
}

pub fn (value RawValue) message_type() string {
	return value.payload.metadata['messageType'].bytestr()
}

pub fn (value RawValue) data() []u8 {
	return value.payload.data.clone()
}

pub fn (value RawValue) kind() PayloadKind {
	return match value.encoding() {
		'json/plain' { .json }
		'binary/plain' { .binary }
		'binary/protobuf' { .protobuf }
		'json/protobuf' { .protobuf_json }
		'binary/null' { .null_value }
		else { .unknown }
	}
}

pub fn (value RawValue) to_payload() !temporal.TemporalApiCommonV1_Payload {
	return temporal.TemporalApiCommonV1_Payload.decode(value.payload.encode())!
}

pub fn (value RawValue) as_value() !PayloadValue {
	return DefaultPayloadConverter{}.from_payload(value.to_payload()!, SerializationContext{})!
}

pub struct DecodedArguments {
pub:
	values       []string
	typed_values []PayloadValue
	raw_values   []RawValue
}

// decode_raw_value applies this converter's scoped payload conversion to an
// already codec/storage-decoded raw value. Codecs and storage are not repeated.
pub fn (converter DataConverter) decode_raw_value(value RawValue) !PayloadValue {
	return converter.payload_converter.from_payload(value.to_payload()!,
		converter.serialization_context)!
}

// decode_raw_value explicitly converts a dynamic Activity argument in its input scope.
pub fn (context &ActivityContext) decode_raw_value(value RawValue) !PayloadValue {
	return context.converter.decode_raw_value(value)!
}

// decode_raw_value explicitly converts a raw workflow value in this run's scope.
pub fn (context &WorkflowContext) decode_raw_value(value RawValue) !PayloadValue {
	return context.converter.decode_raw_value(value)!
}
