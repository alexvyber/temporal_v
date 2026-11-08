module main

import temporal_v.sdk
import x.json2

fn test_named_stream_handles_share_progress_without_advancing_default_randomness() ! {
	mut context := sdk.WorkflowContext{}
	mut reference := sdk.WorkflowContext{}
	alpha := context.random_stream('alpha')!
	another := context.random_stream('alpha')!
	beta := context.random_stream('beta')!
	expected := reference.random_stream('alpha')!
	assert alpha.name() == 'alpha'
	assert alpha.random_u64() == expected.random_u64()
	beta.random_u64()
	assert another.random_u64() == expected.random_u64()
	assert alpha.random_u64() == expected.random_u64()
	first := context.random_u64()!
	assert first == u64(0xe220a8397b1dcdaf)
	assert first == reference.random_u64()!
	assert context.random_u64()! == reference.random_u64()!
}

fn test_named_random_values_and_uuid_shape() ! {
	mut context := sdk.WorkflowContext{}
	stream := context.random_stream('')!
	for _ in 0 .. 1000 {
		value := stream.random_f64()
		assert value >= 0 && value < 1
		stream.random_bool()
		uuid := stream.random_uuid()
		assert uuid.len == 36 && uuid[14] == `4` && uuid[19] in [`8`, `9`, `a`, `b`]
	}
}

struct RandomStreamVector {
	name   string
	values []u64
}

fn test_named_stream_frozen_python_reference_vectors() ! {
	vectors :=
		json2.decode[[]RandomStreamVector]($embed_file('../locks/random-stream-vectors.json').to_string())!
	for vector in vectors {
		mut context := sdk.WorkflowContext{}
		stream := context.random_stream(vector.name)!
		for expected in vector.values {
			assert stream.random_u64() == expected
		}
	}
}
