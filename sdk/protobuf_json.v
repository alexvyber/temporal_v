module sdk

import temporal_v.temporal
import encoding.utf8

// A concrete generated type validates canonical protobuf JSON in both directions.
pub struct ProtobufJsonPayloadConverter[T] {
pub:
	message_type string
}

pub fn (converter ProtobufJsonPayloadConverter[T]) to_payload(input PayloadValue, context SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	mut name := converter.message_type
	data := match input {
		string {
			input
		}
		ProtobufJsonValue {
			validate_protobuf_name(input.message_type)!
			if name == '' { name = input.message_type }
			if converter.message_type != '' && input.message_type != ''
				&& converter.message_type != input.message_type {
				return error('protobuf JSON message type mismatch')
			}
			input.data
		}
		else {
			return DefaultPayloadConverter{}.to_payload(input, context)!
		}
	}

	validate_protobuf_name(converter.message_type)!
	if !utf8.validate_str(data) { return error('protobuf JSON must be valid UTF-8') }
	value := T.from_json(data)!
	mut metadata := {
		'encoding': 'json/protobuf'.bytes()
	}
	if name != '' { metadata['messageType'] = name.bytes() }
	return temporal.TemporalApiCommonV1_Payload{
		metadata: metadata
		data:     value.json()!.bytes()
	}
}

pub fn (converter ProtobufJsonPayloadConverter[T]) from_payload(payload temporal.TemporalApiCommonV1_Payload, context SerializationContext) !PayloadValue {
	if payload.metadata['encoding'].bytestr() != 'json/protobuf' {
		return DefaultPayloadConverter{}.from_payload(payload, context)!
	}
	name := payload.metadata['messageType'].bytestr()
	validate_protobuf_name(converter.message_type)!
	validate_protobuf_name(name)!
	if name != '' && converter.message_type != '' && name != converter.message_type {
		return error('protobuf JSON message type mismatch')
	}
	if !utf8.validate_str(payload.data.bytestr()) {
		return error('protobuf JSON must be valid UTF-8')
	}
	value := T.from_json(payload.data.bytestr())!
	return ProtobufJsonValue{
		data:         value.json()!
		message_type: name
	}
}
