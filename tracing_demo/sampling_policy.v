module main

import temporal_v.sdk
import os

struct FixturePolicySampler {
	policy string
}

fn (sampler FixturePolicySampler) should_sample(parameters sdk.TraceSamplingParameters) sdk.TraceSamplingResult {
	if sampler.policy == 'record-only' {
		return sdk.TraceSamplingResult{
			decision: .record_only
		}
	}
	if sampler.policy == 'commands-only' {
		producer := parameters.kind == .producer
		return sdk.TraceSamplingResult{
			decision:    if producer { .record_and_sample } else { .drop }
			trace_state: if producer {
				'sampler=producer'
			} else {
				'sampler=drop'
			}
			attributes:  {
				'sampler.policy': sdk.TraceAttribute('commands-only')
			}
		}
	}
	tenant := parameters.baggage.member('tenant') or {
		return sdk.TraceSamplingResult{
			decision:    .drop
			trace_state: 'sampler=tenant'
		}
	}
	return sdk.TraceSamplingResult{
		decision:    if tenant.value == 'sampled' {
			.record_and_sample
		} else {
			.drop
		}
		trace_state: 'sampler=tenant'
		attributes:  {
			'sampler.policy': sdk.TraceAttribute('tenant')
		}
	}
}

fn (sampler FixturePolicySampler) description() string {
	return 'FixturePolicy{' + sampler.policy + '}'
}

fn fixture_sampling_policy() !sdk.TraceSampler {
	policy := os.getenv('V_TRACE_SAMPLER')
	return match policy {
		'' {
			sdk.TraceSampler(sdk.ParentBasedTraceSampler{})
		}
		'on' {
			sdk.TraceSampler(sdk.AlwaysOnTraceSampler{})
		}
		'off' {
			sdk.TraceSampler(sdk.AlwaysOffTraceSampler{})
		}
		'ratio' {
			sdk.TraceSampler(sdk.new_trace_id_ratio_sampler(0.5)!)
		}
		'parent-off-root' {
			sdk.TraceSampler(sdk.ParentBasedTraceSampler{
				root: sdk.new_trace_id_ratio_sampler(0)!
			})
		}
		'parent-on-root' {
			sdk.TraceSampler(sdk.ParentBasedTraceSampler{
				root: sdk.new_trace_id_ratio_sampler(1)!
			})
		}
		'remote-only' {
			sdk.TraceSampler(sdk.ParentBasedTraceSampler{
				root:               sdk.AlwaysOffTraceSampler{}
				local_sampled:      sdk.AlwaysOffTraceSampler{}
				local_not_sampled:  sdk.AlwaysOffTraceSampler{}
				remote_sampled:     sdk.AlwaysOnTraceSampler{}
				remote_not_sampled: sdk.AlwaysOnTraceSampler{}
			})
		}
		'record-only', 'commands-only', 'tenant' {
			sdk.TraceSampler(FixturePolicySampler{
				policy: policy
			})
		}
		else {
			return error('unknown sampling policy fixture')
		}
	}
}
