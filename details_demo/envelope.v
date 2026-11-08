module main

import sdk
import temporal
import os

// Preserve the whole payload when framing changes its original wire contents.
// Nexus interprets standard encodings such as binary/null at its HTTP boundary.
struct EnvelopeCodec {}

fn (codec EnvelopeCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		result << temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding': 'binary/test-envelope'.bytes()
			}
			data:     value.encode()
		}
	}
	return result
}

fn (codec EnvelopeCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		if value.metadata['encoding'].bytestr() != 'binary/test-envelope' {
			return error('expected whole-payload test envelope')
		}
		result << temporal.TemporalApiCommonV1_Payload.decode(value.data)!
	}
	return result
}

fn converter() !sdk.DataConverter {
	base := base_converter()!
	mut codecs := base.codecs.clone()
	if os.getenv('V_SDK_DETAILS_PACK') == 'true' {
		codecs << sdk.PayloadCodec(PackCodec{})
		codecs << sdk.PayloadCodec(SplitCodec{})
		codecs << sdk.PayloadCodec(PackCodec{})
	}
	if os.getenv('V_SDK_DETAILS_ENVELOPE') == 'true' { codecs << sdk.PayloadCodec(EnvelopeCodec{}) }
	return sdk.DataConverter{
		...base
		codecs: codecs
	}
}
