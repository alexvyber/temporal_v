module sdk

import temporal_v.temporal

pub struct PatchActivationInput {
	converter  DataConverter @[json: '-']
	headers    map[string]temporal.TemporalApiCommonV1_Payload @[json: '-']
	memo       map[string]temporal.TemporalApiCommonV1_Payload @[json: '-']
	attributes map[string]temporal.TemporalApiCommonV1_Payload @[json: '-']
pub:
	workflow_info WorkflowInfo
	patch_id      string
}

// Experimental rollout policy for first live calls to public patched().
// The callback must be safe for concurrent workflow executions.
pub type PatchActivationCallback = fn (PatchActivationInput) bool

pub fn (input PatchActivationInput) raw_headers() !map[string]RawValue {
	return input.converter.decode_raw_headers(input.headers)!
}

pub fn (input PatchActivationInput) raw_memo() !map[string]RawValue {
	mut result := map[string]RawValue{}
	mut keys := input.memo.keys()
	keys.sort()
	for key in keys {
		decoded := input.converter.decode_payloads([input.memo[key]])!
		if decoded.len != 1 {
			return error('patch policy memo requires exactly one decoded payload')
		}
		result[key] = raw_value(decoded[0])!
	}
	return result
}

// Search attribute payloads are indexed JSON and do not pass through codecs.
pub fn (input PatchActivationInput) search_attributes() !map[string]RawValue {
	mut result := map[string]RawValue{}
	for key, value in input.attributes {
		result[key] = raw_value(value)!
	}
	return result
}

pub fn (input PatchActivationInput) decode_raw_value(value RawValue) !PayloadValue {
	return input.converter.decode_raw_value(value)!
}

fn (mut context WorkflowContext) patch_activation_decision(id string) bool {
	if context.replaying { return context.known_patches[id] }
	if context.known_patches[id] { return true }
	callback := context.patch_activation_callback or { return true }
	mut guard := context.command_guard
	previous := guard.reason
	guard.reason = 'patch activation policy'
	defer { guard.reason = previous }
	return callback(PatchActivationInput{
		workflow_info: context.info()
		patch_id:      id
		converter:     context.converter
		headers:       context.initialization.headers.clone()
		memo:          context.memo_values.clone()
		attributes:    context.search_attribute_values.clone()
	})
}
