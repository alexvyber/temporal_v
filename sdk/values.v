module sdk

import temporal
import json2
import protobuf

// Explicit JSON retains its standard representation through converter composition.
pub struct JsonValue {
pub:
	data string
}

pub fn json_value[T](value T) !JsonValue {
	data := json2.encode(value)
	JsonPayloadConverter{}.to_payload(data, SerializationContext{})!
	return JsonValue{
		data: data
	}
}

pub struct BinaryValue {
pub:
	data []u8
}

pub struct NullValue {}

pub struct ProtobufJsonValue {
pub:
	data         string
	message_type string
}

pub fn protobuf_json_value[T](message T, message_type string) !ProtobufJsonValue {
	validate_protobuf_name(message_type)!
	data := message.json()!
	T.from_json(data)!
	return ProtobufJsonValue{
		data:         data
		message_type: message_type
	}
}

pub struct ProtobufValue {
pub:
	data         []u8
	message_type string
}

// Strings retain the existing logical JSON/custom-converter representation.
pub type PayloadValue = string
	| JsonValue
	| BinaryValue
	| NullValue
	| ProtobufValue
	| ProtobufJsonValue
	| RawValue

pub struct TypedArguments {
pub:
	values []PayloadValue
}

pub fn protobuf_value[T](message T, message_type string) !ProtobufValue {
	validate_protobuf_name(message_type)!
	data := message.encode()
	T.decode(data)!
	return ProtobufValue{
		data:         data
		message_type: message_type
	}
}

pub struct DefaultPayloadConverter {}

pub fn (converter DefaultPayloadConverter) to_payload(value PayloadValue, context SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	return match value {
		string {
			JsonPayloadConverter{}.to_payload(value, context)!
		}
		JsonValue {
			JsonPayloadConverter{}.to_payload(value.data, context)!
		}
		BinaryValue {
			temporal.TemporalApiCommonV1_Payload{
				metadata: {
					'encoding': 'binary/plain'.bytes()
				}
				data:     value.data.clone()
			}
		}
		NullValue {
			temporal.TemporalApiCommonV1_Payload{
				metadata: {
					'encoding': 'binary/null'.bytes()
				}
			}
		}
		ProtobufJsonValue {
			validate_protobuf_name(value.message_type)!
			protobuf.json_parse(value.data)!
			mut metadata := {
				'encoding': 'json/protobuf'.bytes()
			}
			if value.message_type != '' { metadata['messageType'] = value.message_type.bytes() }
			temporal.TemporalApiCommonV1_Payload{
				metadata: metadata
				data:     value.data.bytes()
			}
		}
		ProtobufValue {
			validate_protobuf_name(value.message_type)!
			mut metadata := {
				'encoding': 'binary/protobuf'.bytes()
			}
			if value.message_type != '' { metadata['messageType'] = value.message_type.bytes() }
			temporal.TemporalApiCommonV1_Payload{
				metadata: metadata
				data:     value.data.clone()
			}
		}
		RawValue {
			value.to_payload()!
		}
	}
}

pub fn (converter DefaultPayloadConverter) from_payload(payload temporal.TemporalApiCommonV1_Payload, context SerializationContext) !PayloadValue {
	encoding := payload.metadata['encoding'].bytestr()
	match encoding {
		'binary/null' {
			if payload.data.len != 0 { return error('binary/null payload must be empty') }
			return NullValue{}
		}
		'binary/plain' {
			return BinaryValue{
				data: payload.data.clone()
			}
		}
		'binary/protobuf' {
			name := payload.metadata['messageType'].bytestr()
			validate_protobuf_name(name)!
			return ProtobufValue{
				data:         payload.data.clone()
				message_type: name
			}
		}
		'json/protobuf' {
			name := payload.metadata['messageType'].bytestr()
			validate_protobuf_name(name)!
			data := payload.data.bytestr()
			protobuf.json_parse(data)!
			return ProtobufJsonValue{
				data:         data
				message_type: name
			}
		}
		else {
			return JsonPayloadConverter{}.from_payload(payload, context)!
		}
	}
}

pub fn (converter DataConverter) encode_payload_values(values []PayloadValue) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		result << converter.payload_converter.to_payload(value, converter.serialization_context)!
	}
	return converter.encode_codec_payloads(result)!
}

pub fn (value PayloadValue) logical_data() string {
	return match value {
		string {
			value
		}
		JsonValue {
			value.data
		}
		BinaryValue {
			value.data.bytestr()
		}
		ProtobufValue {
			value.data.bytestr()
		}
		ProtobufJsonValue {
			value.data
		}
		NullValue {
			'null'
		}
		RawValue {
			if value.kind() == .null_value {
				'null'
			} else {
				value.data().bytestr()
			}
		}
	}
}

fn clone_payload_value(value PayloadValue) PayloadValue {
	return match value {
		BinaryValue {
			PayloadValue(BinaryValue{
				data: value.data.clone()
			})
		}
		ProtobufValue {
			PayloadValue(ProtobufValue{
				data:         value.data.clone()
				message_type: value.message_type
			})
		}
		// RawValue keeps its payload private and every mutable accessor copies.
		RawValue {
			value
		}
		else {
			value
		}
	}
}
