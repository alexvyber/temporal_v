module sdk

import temporal_v.temporal

// The concrete generated message type validates bytes in both directions.
// A name is optional because Rust's ProstSerializable omits messageType metadata.
pub struct ProtobufPayloadConverter[T] {
pub:
	message_type string
}

fn validate_protobuf_name(name string) ! {
	if name == '' { return }
	for segment in name.split('.') {
		if segment == '' { return error('protobuf message name contains an empty segment') }
		for index, character in segment.bytes() {
			letter := (character >= `a` && character <= `z`)
				|| (character >= `A` && character <= `Z`) || character == `_`
			if !letter && !(index > 0 && character >= `0` && character <= `9`) {
				return error('invalid protobuf message name')
			}
		}
	}
}

pub fn (converter ProtobufPayloadConverter[T]) to_payload(input PayloadValue, context SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	mut name := converter.message_type
	data := match input {
		string {
			input.bytes()
		}
		ProtobufValue {
			validate_protobuf_name(input.message_type)!
			if name == '' { name = input.message_type }
			if converter.message_type != '' && input.message_type != ''
				&& converter.message_type != input.message_type {
				return error('protobuf payload message type does not match configured type')
			}
			input.data.clone()
		}
		else {
			return DefaultPayloadConverter{}.to_payload(input, context)!
		}
	}

	validate_protobuf_name(converter.message_type)!
	T.decode(data)!
	mut metadata := {
		'encoding': 'binary/protobuf'.bytes()
	}
	if name != '' { metadata['messageType'] = name.bytes() }
	return temporal.TemporalApiCommonV1_Payload{
		metadata: metadata
		data:     data
	}
}

pub fn (converter ProtobufPayloadConverter[T]) from_payload(payload temporal.TemporalApiCommonV1_Payload, context SerializationContext) !PayloadValue {
	validate_protobuf_name(converter.message_type)!
	if payload.metadata['encoding'].bytestr() != 'binary/protobuf' {
		return DefaultPayloadConverter{}.from_payload(payload, context)!
	}
	declared := payload.metadata['messageType'].bytestr()
	validate_protobuf_name(declared)!
	if converter.message_type != '' && declared != '' && declared != converter.message_type {
		return error('protobuf payload message type does not match configured type')
	}
	T.decode(payload.data)!
	return ProtobufValue{
		data:         payload.data.clone()
		message_type: declared
	}
}
