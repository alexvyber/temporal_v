module example_converter

import example_storage
import os
import sdk
import temporal

// This deliberately simple binary framing exercises codec composition. It is not encryption.
struct PrefixCodec {
	tag u8
}

fn (codec PrefixCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		mut framed := [u8(0), codec.tag, 255]
		framed << value.data
		result << temporal.TemporalApiCommonV1_Payload{
			metadata: value.metadata.clone()
			data:     framed
		}
	}
	return result
}

fn (codec PrefixCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		if value.data.len < 3 || value.data[..3] != [u8(0), codec.tag, 255] {
			return error('invalid example codec frame')
		}
		result << temporal.TemporalApiCommonV1_Payload{
			metadata: value.metadata.clone()
			data:     value.data[3..].clone()
		}
	}
	return result
}

pub fn from_env() !sdk.DataConverter {
	return example_storage.from_env(owned_fixture_converter(converter_from_env()!))!
}

fn converter_from_env() !sdk.DataConverter {
	converter := match os.getenv('V_SDK_EXAMPLE_CODEC') {
		'' {
			sdk.DataConverter{}
		}
		'context' {
			sdk.DataConverter{
				payload_converter: ContextPayloadConverter{}
				codecs:            [
					sdk.PayloadCodec(PrefixCodec{
						tag: `A`
					}),
					sdk.PayloadCodec(ContextCodec{}),
					sdk.PayloadCodec(PrefixCodec{
						tag: `B`
					}),
				]
			}
		}
		'prefix' {
			sdk.DataConverter{
				codecs: [sdk.PayloadCodec(PrefixCodec{
					tag: `A`
				}),
					sdk.PayloadCodec(PrefixCodec{
						tag: `B`
					})]
			}
		}
		'binary' {
			sdk.DataConverter{
				payload_converter: sdk.BinaryPayloadConverter{}
			}
		}
		'binary-prefix' {
			sdk.DataConverter{
				payload_converter: sdk.BinaryPayloadConverter{}
				codecs:            [
					sdk.PayloadCodec(PrefixCodec{
						tag: `A`
					}),
					sdk.PayloadCodec(PrefixCodec{
						tag: `B`
					}),
				]
			}
		}
		else {
			return error('unknown example data converter')
		}
	}

	match os.getenv('V_SDK_EXAMPLE_FAILURE_CONVERTER') {
		'' {
			return converter
		}
		'failing' {
			return sdk.DataConverter{
				...converter
				failure_converter: FailingFailureConverter{}
			}
		}
		'encoded' {
			return sdk.DataConverter{
				...converter
				failure_converter: sdk.DefaultFailureConverter{
					encode_common_attributes: true
				}
			}
		}
		'tagged' {
			return sdk.DataConverter{
				...converter
				failure_converter: TaggedFailureConverter{}
			}
		}
		else {
			return error('unknown example failure converter')
		}
	}
}

struct TaggedFailureConverter {}

fn (custom TaggedFailureConverter) to_failure(err IError, converter sdk.DataConverter, generic_type string) !temporal.TemporalApiFailureV1_Failure {
	mut result := sdk.DefaultFailureConverter{}.to_failure(err, converter, generic_type)!
	if err !is sdk.Failure { result.source = 'custom-v-failure' }
	return result
}

fn (custom TaggedFailureConverter) from_failure(value temporal.TemporalApiFailureV1_Failure, converter sdk.DataConverter) sdk.Failure {
	decoded := sdk.DefaultFailureConverter{}.from_failure(value, converter)
	return sdk.Failure{
		...decoded
		source: 'decoded-custom-v-failure'
	}
}

struct FailingFailureConverter {}

fn (custom FailingFailureConverter) to_failure(err IError, converter sdk.DataConverter, generic_type string) !temporal.TemporalApiFailureV1_Failure {
	return error('temporary failure converter outage')
}

fn (custom FailingFailureConverter) from_failure(value temporal.TemporalApiFailureV1_Failure, converter sdk.DataConverter) sdk.Failure {
	return sdk.DefaultFailureConverter{}.from_failure(value, converter)
}
