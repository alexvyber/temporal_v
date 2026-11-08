module main

import alexvyber.temporal_v.temporal

fn test_temporal_nested_failure_matches_wire_bytes() ! {
	mut child := temporal.TemporalApiFailureV1_Failure{
		message: 'inner'
	}
	failure := temporal.TemporalApiFailureV1_Failure{
		message: 'outer'
		cause:   &child
	}
	expected := [u8(10), 5, 111, 117, 116, 101, 114, 34, 7, 10, 5, 105, 110, 110, 101, 114]
	assert failure.encode() == expected
	decoded := temporal.TemporalApiFailureV1_Failure.decode(expected)!
	cause := decoded.cause or { panic('missing nested cause') }
	assert cause.message == 'inner'
}

fn test_single_member_oneof_preserves_presence_and_unknown_fields() ! {
	selector := temporal.TemporalApiCommonV1_WorkerSelector{
		selector: temporal.TemporalApiCommonV1_WorkerSelector_WorkerInstanceKey_Case{
			value: 'v'
		}
	}
	assert selector.encode() == [u8(10), 1, 118]
	wire := [u8(10), 1, 118, 152, 6, 7]
	decoded := temporal.TemporalApiCommonV1_WorkerSelector.decode(wire)!
	selected := decoded.selector or { panic('missing worker selector') }
	assert selected.value == 'v'
	assert decoded.encode() == wire
}

fn test_nested_message_and_oneof_case_have_distinct_names() ! {
	callback := temporal.TemporalApiCommonV1_Callback{
		variant: temporal.TemporalApiCommonV1_Callback_Nexus_Case{
			value: temporal.TemporalApiCommonV1_Callback_Nexus{
				url: 'https://example.test'
			}
		}
	}
	decoded := temporal.TemporalApiCommonV1_Callback.decode(callback.encode())!
	variant := decoded.variant or { panic('missing callback variant') }
	assert variant is temporal.TemporalApiCommonV1_Callback_Nexus_Case
	if variant is temporal.TemporalApiCommonV1_Callback_Nexus_Case {
		assert variant.value.url == 'https://example.test'
	}
}
