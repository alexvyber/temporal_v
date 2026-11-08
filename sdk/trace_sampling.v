module sdk

import math
import strconv

pub enum TraceSamplingDecision {
	drop
	record_only
	record_and_sample
}

pub struct TraceSamplingParameters {
pub:
	parent     ?TraceContext
	baggage    Baggage
	trace_id   string
	span_id    string
	name       string
	kind       TraceSpanKind
	attributes map[string]TraceAttribute
	links      []TraceLink
}

pub struct TraceSamplingResult {
pub:
	decision    TraceSamplingDecision
	attributes  map[string]TraceAttribute
	trace_state ?string
}

// Samplers may run concurrently. Workflow sampling must be deterministic and fast.
pub interface TraceSampler {
	should_sample(parameters TraceSamplingParameters) TraceSamplingResult
	description() string
}

fn sampling_parent_state(parameters TraceSamplingParameters) string {
	return if parent := parameters.parent { parent.trace_state } else { '' }
}

pub struct AlwaysOnTraceSampler {}

pub fn (sampler AlwaysOnTraceSampler) should_sample(parameters TraceSamplingParameters) TraceSamplingResult {
	return TraceSamplingResult{
		decision:    .record_and_sample
		trace_state: sampling_parent_state(parameters)
	}
}

pub fn (sampler AlwaysOnTraceSampler) description() string {
	return 'AlwaysOnSampler'
}

pub struct AlwaysOffTraceSampler {}

pub fn (sampler AlwaysOffTraceSampler) should_sample(parameters TraceSamplingParameters) TraceSamplingResult {
	return TraceSamplingResult{
		decision:    .drop
		trace_state: sampling_parent_state(parameters)
	}
}

pub fn (sampler AlwaysOffTraceSampler) description() string {
	return 'AlwaysOffSampler'
}

pub struct TraceIdRatioSampler {
	fraction    f64
	upper_bound u64
}

pub fn new_trace_id_ratio_sampler(fraction f64) !TraceIdRatioSampler {
	if math.is_nan(fraction) || math.is_inf(fraction, 0) || fraction < 0 || fraction > 1 {
		return error('trace sampling fraction must be finite and between zero and one')
	}
	return TraceIdRatioSampler{
		fraction:    fraction
		upper_bound: u64(fraction * f64(u64(1) << 63))
	}
}

pub fn (sampler TraceIdRatioSampler) should_sample(parameters TraceSamplingParameters) TraceSamplingResult {
	if !lower_trace_hex(parameters.trace_id, 32) {
		return TraceSamplingResult{
			decision:    .drop
			trace_state: sampling_parent_state(parameters)
		}
	}
	prefix := strconv.parse_uint(parameters.trace_id[16..], 16, 64) or {
		return TraceSamplingResult{
			decision:    .drop
			trace_state: sampling_parent_state(parameters)
		}
	}
	return TraceSamplingResult{
		decision:    if (prefix >> 1) < sampler.upper_bound {
			.record_and_sample
		} else {
			.drop
		}
		trace_state: sampling_parent_state(parameters)
	}
}

pub fn (sampler TraceIdRatioSampler) description() string {
	return 'TraceIDRatioBased{${sampler.fraction}}'
}

pub struct ParentBasedTraceSampler {
pub:
	root               TraceSampler = AlwaysOnTraceSampler{}
	remote_sampled     TraceSampler = AlwaysOnTraceSampler{}
	remote_not_sampled TraceSampler = AlwaysOffTraceSampler{}
	local_sampled      TraceSampler = AlwaysOnTraceSampler{}
	local_not_sampled  TraceSampler = AlwaysOffTraceSampler{}
}

pub fn (sampler ParentBasedTraceSampler) should_sample(parameters TraceSamplingParameters) TraceSamplingResult {
	parent := parameters.parent or { return sampler.root.should_sample(parameters) }
	if parent.remote {
		if parent.sampled() { return sampler.remote_sampled.should_sample(parameters) }
		return sampler.remote_not_sampled.should_sample(parameters)
	}
	if parent.sampled() { return sampler.local_sampled.should_sample(parameters) }
	return sampler.local_not_sampled.should_sample(parameters)
}

pub fn (sampler ParentBasedTraceSampler) description() string {
	return 'ParentBased{root=${sampler.root.description()},remote_sampled=${sampler.remote_sampled.description()},remote_not_sampled=${sampler.remote_not_sampled.description()},local_sampled=${sampler.local_sampled.description()},local_not_sampled=${sampler.local_not_sampled.description()}}'
}

pub struct NoopTraceExporter {}

pub fn (exporter NoopTraceExporter) export(context &TraceExportContext, spans []TraceSpanData) ! {
	context.check_cancelled()!
}

fn clone_sampling_links(links []TraceLink) []TraceLink {
	mut result := []TraceLink{cap: links.len}
	for link in links {
		result << TraceLink{
			...link
			attributes: clone_trace_attributes(link.attributes)
		}
	}
	return result
}
