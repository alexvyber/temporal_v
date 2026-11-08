module example_converter

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import os

// Deliberately mutate nested fields in local structs. The SDK must isolate
// callers and repeated raw header reads from these callback buffers.
struct OwnedRoundtripCodec {}

fn (codec OwnedRoundtripCodec) encode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := values.clone()
	for index in 0 .. result.len {
		if 'temporal-v/owned-source-encoding' in result[index].metadata {
			return error('owned fixture codec encoded twice')
		}
		result[index].metadata['temporal-v/owned-source-encoding'] = result[index].metadata['encoding']
		result[index].metadata['encoding'] = 'binary/owned-fixture'.bytes()
		for offset in 0 .. result[index].data.len {
			result[index].data[offset] ^= u8(170)
		}
	}
	return result
}

fn (codec OwnedRoundtripCodec) decode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := values.clone()
	for index in 0 .. result.len {
		if result[index].metadata['encoding'].bytestr() != 'binary/owned-fixture' {
			return error('owned fixture input was mutated or decoded twice')
		}
		result[index].metadata['encoding'] = result[index].metadata['temporal-v/owned-source-encoding']
		result[index].metadata.delete('temporal-v/owned-source-encoding')
		for offset in 0 .. result[index].data.len {
			result[index].data[offset] ^= u8(170)
		}
	}
	return result
}

fn owned_fixture_converter(converter sdk.DataConverter) sdk.DataConverter {
	if os.getenv('V_CODEC_OWNERSHIP') != 'true' { return converter }
	mut codecs := [sdk.PayloadCodec(OwnedRoundtripCodec{})]
	codecs << converter.codecs
	return sdk.DataConverter{
		...converter
		codecs: codecs
	}
}
