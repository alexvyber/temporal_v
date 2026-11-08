module sdk

import crypto.sha256

@[heap]
struct WorkflowRandomStreams {
mut:
	seed   u64
	values map[string]u64
}

// WorkflowRandomStream is a named deterministic generator. Copies and repeated
// lookup share progress. Different names and the default generator are isolated.
// Keep names stable across replay. Values are not cryptographically secure.
pub struct WorkflowRandomStream {
	state       &WorkflowRandomStreams
	stream_name string
}

// random_stream returns the existing stream or creates one from Temporal's seed.
// Seed updates reset all named generators, including previously returned handles.
pub fn (mut context WorkflowContext) random_stream(name string) !WorkflowRandomStream {
	context.ensure_open()!
	if context.random_streams == unsafe { nil } {
		context.random_streams = &WorkflowRandomStreams{
			seed: context.random_seed
		}
	}
	return WorkflowRandomStream{
		state:       context.random_streams
		stream_name: name
	}
}

pub fn (stream WorkflowRandomStream) name() string {
	return stream.stream_name
}

// random_u64 advances only this named stream using the fixed SplitMix64 algorithm.
pub fn (stream WorkflowRandomStream) random_u64() u64 {
	mut state := stream.state
	current := state.values[stream.stream_name] or {
		named_random_seed(state.seed, stream.stream_name)
	}
	next := current + u64(0x9e3779b97f4a7c15)
	state.values[stream.stream_name] = next
	return splitmix64_value(next)
}

pub fn (stream WorkflowRandomStream) random_uuid() string {
	upper := stream.random_u64()
	lower := stream.random_u64()
	return random_uuid_value(upper, lower)
}

// random_f64 returns a deterministic value in [0, 1) from the high 53 bits.
pub fn (stream WorkflowRandomStream) random_f64() f64 {
	return f64(stream.random_u64() >> 11) * (1.0 / 9007199254740992.0)
}

pub fn (stream WorkflowRandomStream) random_bool() bool {
	return stream.random_u64() & 1 != 0
}

// Domain, seed encoding, truncation and generator are part of V replay compatibility.
fn named_random_seed(seed u64, name string) u64 {
	mut input := 'temporal-v-workflow-random-stream/v1'.bytes()
	input << u8(0)
	for shift in [56, 48, 40, 32, 24, 16, 8, 0] {
		input << u8(seed >> shift)
	}
	input << name.bytes()
	bytes := sha256.sum(input)
	mut result := u64(0)
	for index in 0 .. 8 {
		result = (result << 8) | u64(bytes[index])
	}
	return result
}

fn (mut context WorkflowContext) reseed_random(seed u64) {
	context.random_seed = seed
	context.random_state = seed
	if context.random_streams != unsafe { nil } {
		mut state := context.random_streams
		state.seed = seed
		state.values.clear()
	}
}
