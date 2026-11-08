module sdk

import temporal_v.temporal
import x.json2

// Conversion is separate from payload conversion so applications can control
// error types and protocol metadata while retaining their payload codec policy.
pub interface FailureConverter {
	to_failure(err IError, converter DataConverter, generic_type string) !temporal.TemporalApiFailureV1_Failure
	from_failure(value temporal.TemporalApiFailureV1_Failure, converter DataConverter) Failure
}

pub struct DefaultFailureConverter {
pub:
	encode_common_attributes bool
}

pub fn (encoder DefaultFailureConverter) to_failure(err IError, converter DataConverter, generic_type string) !temporal.TemporalApiFailureV1_Failure {
	result := encode_error(err, converter, generic_type, 0)!
	if encoder.encode_common_attributes && err !is Failure {
		return encode_common_failure_attributes(result, converter, 0)!
	}
	return result
}

pub fn (decoder DefaultFailureConverter) from_failure(value temporal.TemporalApiFailureV1_Failure, converter DataConverter) Failure {
	return decode_standard_failure(value, converter)
}

pub fn (converter DataConverter) encode_failure(err IError, generic_type string) !temporal.TemporalApiFailureV1_Failure {
	return converter.failure_converter.to_failure(err, converter, generic_type)!
}

pub fn (converter DataConverter) decode_failure(value temporal.TemporalApiFailureV1_Failure) Failure {
	return capture_failure_context(converter.failure_converter.from_failure(value, converter),
		converter.serialization_context)
}

fn failure_from_proto(value temporal.TemporalApiFailureV1_Failure, converter DataConverter) Failure {
	return converter.decode_failure(value)
}

struct CommonFailureAttributes {
	message     string
	stack_trace string
}

fn failure_attribute_converter(converter DataConverter) DataConverter {
	// Common attributes use the protocol's standard JSON object regardless of
	// the application value converter; user codecs still protect the payload.
	return DataConverter{
		codecs:                converter.codecs
		serialization_context: converter.serialization_context
	}
}

fn encode_common_failure_attributes(value temporal.TemporalApiFailureV1_Failure, converter DataConverter, depth int) !temporal.TemporalApiFailureV1_Failure {
	if depth >= 64 { return error('failure attributes exceed the supported cause depth') }
	if _ := value.encoded_attributes { return value }
	mut result := value
	if cause := value.cause {
		nested := encode_common_failure_attributes(*cause, converter, depth + 1)!
		result.cause = &nested
	}
	result.encoded_attributes = failure_attribute_converter(converter).encode_value(json2.encode(CommonFailureAttributes{
		message:     value.message
		stack_trace: value.stack_trace
	}))!
	result.message = 'Encoded failure'
	result.stack_trace = ''
	return result
}

fn decode_common_failure_attributes(value temporal.TemporalApiCommonV1_Payload, converter DataConverter) !CommonFailureAttributes {
	return json2.decode[CommonFailureAttributes](failure_attribute_converter(converter).payload_data(value)!)!
}

fn capture_failure_context(failure Failure, context SerializationContext) Failure {
	mut captured := Failure{
		...failure
		source_context: context
	}
	if cause := failure.cause {
		nested := capture_failure_context(*cause, context)
		captured = Failure{
			...captured
			cause: &nested
		}
	}
	return captured
}

// Retain protocol metadata and opaque fields. When a failure crosses an actor
// scope, re-encode known payloads using that scope's converter/codec context.
fn transcode_captured_failure(failure Failure, converter DataConverter) !temporal.TemporalApiFailureV1_Failure {
	raw := failure.as_proto()!
	if failure.source_context.same_scope(converter.serialization_context) { return raw }
	mut result := raw
	if failure.detail_values.len != failure.details.len {
		return error('captured failure has missing typed details')
	}
	if _ := raw.encoded_attributes {
		if failure.attributes_error != '' {
			return error('cannot transcode undecoded failure attributes: ${failure.attributes_error}')
		}
		result.encoded_attributes = failure_attribute_converter(converter).encode_value(json2.encode(CommonFailureAttributes{
			message:     failure.message
			stack_trace: failure.stack_trace
		}))!
	}
	if info := raw.failure_info {
		match info {
			temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case {
				mut value := info.value
				if _ := value.details {
					if failure.details_error != '' {
						return error('cannot transcode undecoded application details: ${failure.details_error}')
					}
					value.details = temporal.TemporalApiCommonV1_Payloads{
						payloads: converter.encode_payload_values(failure.detail_values)!
					}
				}
				result.failure_info = temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case{
					value: value
				}
			}
			temporal.TemporalApiFailureV1_Failure_CanceledFailureInfo_Case {
				mut value := info.value
				if _ := value.details {
					if failure.details_error != '' {
						return error('cannot transcode undecoded cancellation details: ${failure.details_error}')
					}
					value.details = temporal.TemporalApiCommonV1_Payloads{
						payloads: converter.encode_payload_values(failure.detail_values)!
					}
				}
				result.failure_info = temporal.TemporalApiFailureV1_Failure_CanceledFailureInfo_Case{
					value: value
				}
			}
			temporal.TemporalApiFailureV1_Failure_TimeoutFailureInfo_Case {
				mut value := info.value
				if _ := value.last_heartbeat_details {
					if failure.details_error != '' {
						return error('cannot transcode undecoded timeout details: ${failure.details_error}')
					}
					value.last_heartbeat_details = temporal.TemporalApiCommonV1_Payloads{
						payloads: converter.encode_payload_values(failure.detail_values)!
					}
				}
				result.failure_info = temporal.TemporalApiFailureV1_Failure_TimeoutFailureInfo_Case{
					value: value
				}
			}
			temporal.TemporalApiFailureV1_Failure_ResetWorkflowFailureInfo_Case {
				mut value := info.value
				if _ := value.last_heartbeat_details {
					if failure.details_error != '' {
						return error('cannot transcode undecoded reset details: ${failure.details_error}')
					}
					value.last_heartbeat_details = temporal.TemporalApiCommonV1_Payloads{
						payloads: converter.encode_payload_values(failure.detail_values)!
					}
				}
				result.failure_info = temporal.TemporalApiFailureV1_Failure_ResetWorkflowFailureInfo_Case{
					value: value
				}
			}
			else {}
		}
	}
	if _ := raw.cause {
		cause := failure.cause or {
			return error('captured failure is missing its decoded cause for a scope change')
		}

		nested := transcode_captured_failure(*cause, converter)!
		result.cause = &nested
	}
	return result
}
