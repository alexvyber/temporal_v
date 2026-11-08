module main

import temporal_v.sdk
import temporal_v.temporal

struct MutatingCodec {}

fn mutate_payloads(values []temporal.TemporalApiCommonV1_Payload) []temporal.TemporalApiCommonV1_Payload {
	mut result := values.clone()
	for index in 0 .. result.len {
		result[index].data[0]++
		result[index].metadata['private'][0]++
		result[index].pb_unknown[2]++
		result[index].external_payloads[0].size_bytes++
		result[index].external_payloads[0].pb_unknown[2]++
	}
	return result
}

fn (codec MutatingCodec) encode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return mutate_payloads(values)
}

fn (codec MutatingCodec) decode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return mutate_payloads(values)
}

struct CachedConverter {
	value temporal.TemporalApiCommonV1_Payload
}

fn (converter CachedConverter) to_payload(value sdk.PayloadValue, scope sdk.SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	return converter.value
}

fn (converter CachedConverter) from_payload(value temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) !sdk.PayloadValue {
	return sdk.DefaultPayloadConverter{}.from_payload(value, scope)!
}

struct CachedCodec {
	value temporal.TemporalApiCommonV1_Payload
}

fn (codec CachedCodec) encode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return [codec.value]
}

fn (codec CachedCodec) decode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return [codec.value]
}

struct DuplicateCodec {}

fn (codec DuplicateCodec) encode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return [values[0], values[0]]
}

fn (codec DuplicateCodec) decode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return [values[0], values[0]]
}

fn fixture_payload() temporal.TemporalApiCommonV1_Payload {
	return temporal.TemporalApiCommonV1_Payload{
		metadata:          {
			'encoding': 'json/plain'.bytes()
			'private':  [u8(7), 0, 255]
		}
		data:              '1'.bytes()
		pb_unknown:        [u8(152), 6, 1]
		external_payloads: [
			temporal.TemporalApiCommonV1_Payload_ExternalPayloadDetails{
				size_bytes: 1
				pb_unknown: [
					u8(152),
					6,
					1,
				]
			},
		]
	}
}

fn assert_payload(value temporal.TemporalApiCommonV1_Payload, increments int) {
	assert value.data.bytestr() == (1 + increments).str()
	assert value.metadata['private'] == [u8(7 + increments), 0, 255]
	assert value.pb_unknown == [u8(152), 6, u8(1 + increments)]
	assert value.external_payloads.len == 1
		&& value.external_payloads[0].size_bytes == 1 + increments
	assert value.external_payloads[0].pb_unknown == [u8(152), 6, u8(1 + increments)]
}

fn check_ownership(wire temporal.TemporalApiCommonV1_Payload, rounds int) ! {
	decoder := sdk.DataConverter{
		codecs: [sdk.PayloadCodec(MutatingCodec{})]
	}
	encoder := sdk.DataConverter{
		payload_converter: CachedConverter{wire}
		codecs:            [
			sdk.PayloadCodec(MutatingCodec{}),
			sdk.PayloadCodec(MutatingCodec{}),
		]
	}
	middle := sdk.DataConverter{
		codecs: [sdk.PayloadCodec(CachedCodec{wire}), sdk.PayloadCodec(MutatingCodec{})]
	}
	reverse := sdk.DataConverter{
		codecs: [sdk.PayloadCodec(MutatingCodec{}), sdk.PayloadCodec(CachedCodec{wire})]
	}
	duplicate := sdk.DataConverter{
		payload_converter: CachedConverter{wire}
		codecs:            [sdk.PayloadCodec(DuplicateCodec{})]
	}
	for _ in 0 .. rounds {
		first := decoder.decode_arguments([wire])!
		second := decoder.decode_arguments([wire])!
		assert first.values == ['2'] && second.values == ['2']
		assert_payload(first.raw_values[0].to_payload()!, 1)
		assert_payload(second.raw_values[0].to_payload()!, 1)
		mut encoded := encoder.encode_values(['0'])!
		assert_payload(encoded[0], 2)
		encoded[0].data[0] = u8(57)
		encoded[0].metadata['private'][0] = 42
		assert_payload(encoder.encode_values(['0'])![0], 2)
		assert_payload(middle.encode_values(['0'])![0], 1)
		assert_payload(reverse.decode_arguments([wire])!.raw_values[0].to_payload()!, 1)
		mut twins := duplicate.encode_values(['0'])!
		twins[0].data[0] = u8(57)
		twins[0].metadata['private'][0] = 42
		assert_payload(twins[1], 0)
		assert_payload(wire, 0)
	}
}

fn thread_check(wire temporal.TemporalApiCommonV1_Payload, done chan bool) {
	check_ownership(wire, 200) or { panic(err) }
	done <- true
}

fn main() {
	wire := fixture_payload()
	check_ownership(wire, 200)!
	done := chan bool{cap: 8}
	for _ in 0 .. 8 {
		spawn thread_check(wire, done)
	}
	for _ in 0 .. 8 {
		assert <-done
	}
	assert_payload(wire, 0)
	println('CODEC_OWNERSHIP_OK rounds=1800 threads=8')
}
