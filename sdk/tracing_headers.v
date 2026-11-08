module sdk

import alexvyber.temporal_v.temporal

// Propagator-owned headers never pass through application payload converters or codecs.
// Memo maps use encode_map/decode_map and retain the full application key space.
pub fn (converter DataConverter) encode_headers(values map[string]PayloadValue) !map[string]temporal.TemporalApiCommonV1_Payload {
	if trace_header_key in values {
		return error('header key _tracer-data is reserved for trace propagation')
	}
	return converter.encode_map(values)!
}

pub fn (converter DataConverter) decode_headers(values map[string]temporal.TemporalApiCommonV1_Payload) !map[string]PayloadValue {
	mut application := values.clone()
	application.delete(trace_header_key)
	return converter.decode_map(application)!
}

fn (converter DataConverter) decode_raw_headers(values map[string]temporal.TemporalApiCommonV1_Payload) !map[string]RawValue {
	mut result := map[string]RawValue{}
	mut keys := values.keys()
	keys.sort()
	for key in keys {
		if key == trace_header_key {
			continue
		}
		decoded := converter.decode_payloads([values[key]])!
		if decoded.len != 1 { return error('header codec requires exactly one payload') }
		result[key] = raw_value(decoded[0])!
	}
	return result
}

// Explicit codec/storage decoding, without the application payload converter.
// Propagator-owned headers remain separate from application values.
pub fn (context &WorkflowContext) raw_headers() !map[string]RawValue {
	return context.converter.decode_raw_headers(context.initialization.headers)!
}

pub fn (context &WorkflowContext) raw_event_headers(event Event) !map[string]RawValue {
	return context.converter.decode_raw_headers(event.headers)!
}

pub fn (next QueryNext) raw_headers() !map[string]RawValue {
	return next.run.context.converter.decode_raw_headers(next.request_headers)!
}

pub fn (next UpdateValidatorNext) raw_headers() !map[string]RawValue {
	return next.run.context.converter.decode_raw_headers(next.request_headers)!
}

pub fn (next QueryNext) decode_raw_value(value RawValue) !PayloadValue {
	return next.run.context.converter.decode_raw_value(value)!
}

pub fn (next UpdateValidatorNext) decode_raw_value(value RawValue) !PayloadValue {
	return next.run.context.converter.decode_raw_value(value)!
}
