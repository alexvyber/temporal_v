module main

import sdk
import os
import json2
import sync
import time
import math

struct SamplingCase {
	method   string
	fraction string
	trace_id string
	parent   bool
	remote   bool
	flags    u8
	state    string
}

struct SamplingResult {
	decision int
	state    string
}

fn sampler_for_case(input SamplingCase) !sdk.TraceSampler {
	ratio := sdk.new_trace_id_ratio_sampler(input.fraction.f64())!
	return match input.method {
		'ratio' {
			sdk.TraceSampler(ratio)
		}
		'parent' {
			sdk.TraceSampler(sdk.ParentBasedTraceSampler{
				root: ratio
			})
		}
		'on' {
			sdk.TraceSampler(sdk.AlwaysOnTraceSampler{})
		}
		'off' {
			sdk.TraceSampler(sdk.AlwaysOffTraceSampler{})
		}
		else {
			return error('unknown sampler fixture strategy')
		}
	}
}

fn sampling_contexts() ! {
	cases := json2.decode[[]SamplingCase](os.read_file(os.args[2])!)!
	for input in cases {
		parent := if input.parent {
			?sdk.TraceContext(sdk.TraceContext{
				trace_id:    input.trace_id
				span_id:     'abcdef1234567890'
				trace_flags: input.flags
				remote:      input.remote
				trace_state: input.state
			})
		} else {
			?sdk.TraceContext(none)
		}
		result := sampler_for_case(input)!.should_sample(sdk.TraceSamplingParameters{
			parent:   parent
			trace_id: input.trace_id
			span_id:  '1234567890abcdef'
			name:     'fixture'
			kind:     .internal
		})
		println(json2.encode(SamplingResult{
			decision: int(result.decision)
			state:    result.trace_state or { '' }
		}))
	}
}

@[heap]
struct SamplingAudit {
	mutex &sync.RwMutex = sync.new_rwmutex()
mut:
	requests []sdk.TraceSamplingParameters
	returned []i64 = [i64(4), 5]
}

struct AuditTraceSampler {
	audit &SamplingAudit
}

fn (sampler AuditTraceSampler) should_sample(parameters sdk.TraceSamplingParameters) sdk.TraceSamplingResult {
	mut audit := sampler.audit
	audit.mutex.lock()
	audit.requests << parameters
	returned := audit.returned
	audit.mutex.unlock()
	return sdk.TraceSamplingResult{
		decision:    match parameters.name {
			'record-only' { .record_only }
			'drop' { .drop }
			else { .record_and_sample }
		}
		attributes:  {
			'value':    sdk.TraceAttribute('sampler')
			'returned': sdk.TraceAttribute(sdk.TraceIntegerArray{
				values: returned
			})
		}
		trace_state: 'sampler=chosen'
	}
}

fn (sampler AuditTraceSampler) description() string {
	return 'AuditTraceSampler'
}

fn (sampler AuditTraceSampler) mutate_result() {
	mut audit := sampler.audit
	audit.mutex.lock()
	defer { audit.mutex.unlock() }
	audit.returned[0] = 200
}

struct InvalidStateSampler {}

fn (sampler InvalidStateSampler) should_sample(parameters sdk.TraceSamplingParameters) sdk.TraceSamplingResult {
	return sdk.TraceSamplingResult{
		decision:    .record_and_sample
		trace_state: 'invalid'
	}
}

fn (sampler InvalidStateSampler) description() string {
	return 'InvalidState'
}

struct DelayedTraceSampler {}

fn (sampler DelayedTraceSampler) should_sample(parameters sdk.TraceSamplingParameters) sdk.TraceSamplingResult {
	time.sleep(20 * time.millisecond)
	return sdk.TraceSamplingResult{
		decision: .record_and_sample
	}
}

fn (sampler DelayedTraceSampler) description() string {
	return 'DelayedFixture'
}

fn trace_integer_values(value sdk.TraceAttribute) ![]i64 {
	if value is sdk.TraceIntegerArray { return value.values.clone() }
	return error('trace integer array expected')
}

fn sampling_validation() ! {
	for value in [f64(-0.01), 1.01, math.nan(), math.inf(1)] {
		if _ := sdk.new_trace_id_ratio_sampler(value) {
			return error('invalid ratio sampler admitted')
		}
	}
	exporter := RecordingTraceExporter{
		audit: &ExportAudit{}
	}
	sampler := AuditTraceSampler{
		audit: &SamplingAudit{}
	}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:       exporter
		sampler:        sampler
		flush_interval: time.millisecond
	})!
	defer { provider.shutdown(5 * time.second) or {} }
	parent := sdk.TraceContext{
		trace_id:    '1234567890abcdef1234567890abcdef'
		span_id:     'abcdef1234567890'
		trace_flags: 3
		trace_state: 'vendor=value'
		remote:      true
	}
	baggage := sdk.new_baggage([sdk.BaggageMember{ key: 'tenant', value: 'sampling' }])!
	mut integers := [i64(1), 2]
	span := provider.start_span('sampled', sdk.TraceSpanOptions{
		parent:     parent
		baggage:    baggage
		attributes: {
			'value':    sdk.TraceAttribute('caller')
			'integers': sdk.TraceAttribute(sdk.TraceIntegerArray{
				values: integers
			})
		}
		links:      [
			sdk.TraceLink{
				context:    parent
				attributes: {
					'linked': sdk.TraceAttribute(sdk.TraceIntegerArray{
						values: integers
					})
				}
			},
		]
	})!
	if !span.is_recording() || !span.context().sampled()
		|| span.context().trace_state != 'sampler=chosen' {
		return error('sample decision/state not applied')
	}
	integers[0] = 100
	sampler.mutate_result()
	span.finish()
	record := provider.start_span('record-only', sdk.TraceSpanOptions{ parent: parent })!
	if !record.is_recording() || record.context().sampled() {
		return error('record-only state changed')
	}
	record.set_attribute('recorded', sdk.TraceAttribute(true))!
	if _ := record.set_attribute('', sdk.TraceAttribute(true)) {
		return error('record-only span ignored attribute validation')
	}
	record.add_event(sdk.TraceEvent{ name: 'recorded', unix_nanos: 1 })!
	record.finish()
	dropped := provider.start_span('drop', sdk.TraceSpanOptions{ parent: parent })!
	if dropped.is_recording() || dropped.context().sampled() {
		return error('drop decision ignored')
	}
	dropped.set_attribute('', sdk.TraceAttribute(true))!
	dropped.finish()
	forced := provider.start_span('drop', sdk.TraceSpanOptions{ parent: parent, sampled: true })!
	if !forced.is_recording() || !forced.context().sampled() {
		return error('explicit sample override lost')
	}
	forced.finish()
	suppressed := provider.start_span('sampled', sdk.TraceSpanOptions{
		parent:  parent
		sampled: false
	})!
	if suppressed.is_recording() || suppressed.context().sampled() {
		return error('explicit suppression lost')
	}
	suppressed.finish()
	provider.force_flush(5 * time.second)!
	records := audit_records(exporter.audit)
	if records.len != 2 { return error('unsampled span entered exporter queue') }
	first := records[0]
	if first.attributes['value'] or { return error('sampler attribute missing') } != sdk.TraceAttribute('sampler') {
		return error('sampler did not override caller attribute')
	}
	if trace_integer_values(first.attributes['integers'] or {
		return error('integer snapshot missing')
	})! != [i64(1), 2] || trace_integer_values(first.attributes['returned'] or {
		return error('sampler snapshot missing')
	})! != [i64(4), 5] {
		return error('sampling result/attribute snapshot mutated')
	}
	sampler.audit.mutex.rlock()
	requests := sampler.audit.requests.clone()
	sampler.audit.mutex.runlock()
	if requests.len != 5 || requests[0].baggage.header() != 'tenant=sampling' || (requests[0].parent or {
		return error('sampling parent missing')
	}).remote != true {
		return error('sampler inputs changed')
	}
	if trace_integer_values(requests[0].attributes['integers'] or {
		return error('sampler input snapshot missing')
	})! != [i64(1), 2] || trace_integer_values(requests[0].links[0].attributes['linked'] or {
		return error('sampling link missing')
	})! != [i64(1), 2] {
		return error('sampler input snapshot mutated')
	}
	stats := provider.stats()
	if stats.accepted_spans != 2 || stats.suppressed_spans != 3 {
		return error('sampling counters changed')
	}
	invalid := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter: sdk.NoopTraceExporter{}
		sampler:  InvalidStateSampler{}
	})!
	if _ := invalid.start_span('invalid', sdk.TraceSpanOptions{}) {
		return error('invalid sampler tracestate accepted')
	}
	invalid.shutdown(5 * time.second)!
	timed_exporter := RecordingTraceExporter{
		audit: &ExportAudit{}
	}
	timed := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter: timed_exporter
		sampler:  DelayedTraceSampler{}
	})!
	delayed := timed.start_span('delayed', sdk.TraceSpanOptions{})!
	delayed.finish()
	timed.force_flush(5 * time.second)!
	timing := audit_records(timed_exporter.audit)
	if timing.len != 1
		|| timing[0].end_unix_nanos - timing[0].start_unix_nanos < u64(20 * time.millisecond) {
		return error('span duration omitted sampling callback time')
	}
	timed.shutdown(5 * time.second)!
	sampling_concurrency()!
	println('SAMPLING_OK')
}

@[heap]
struct SamplerCloseState {
	mutex &sync.RwMutex = sync.new_rwmutex()
mut:
	provider ?&sdk.TraceProvider
}

struct ClosingTraceSampler {
	state &SamplerCloseState
}

fn (sampler ClosingTraceSampler) attach(provider &sdk.TraceProvider) {
	mut state := sampler.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	state.provider = provider
}

fn (sampler ClosingTraceSampler) should_sample(parameters sdk.TraceSamplingParameters) sdk.TraceSamplingResult {
	sampler.state.mutex.rlock()
	provider := sampler.state.provider
	sampler.state.mutex.runlock()
	if owner := provider { owner.shutdown(0) or {} }
	return sdk.TraceSamplingResult{
		decision: .record_and_sample
	}
}

fn (sampler ClosingTraceSampler) description() string {
	return 'ClosingFixture'
}

fn sampling_concurrency() ! {
	sampler := AuditTraceSampler{
		audit: &SamplingAudit{}
	}
	exporter := RecordingTraceExporter{
		audit: &ExportAudit{}
	}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:          exporter
		sampler:           sampler
		max_pending_spans: 512
		max_batch:         16
		flush_interval:    time.millisecond
	})!
	done := chan string{cap: 8}
	for index in 0 .. 8 {
		spawn emit_trace_spans(provider, index, done)
	}
	for _ in 0 .. 8 {
		message := <-done
		if message != '' { return error(message) }
	}
	provider.force_flush(5 * time.second)!
	provider.shutdown(5 * time.second)!
	sampler.audit.mutex.rlock()
	requests := sampler.audit.requests.clone()
	sampler.audit.mutex.runlock()
	if requests.len != 64 || audit_records(exporter.audit).len != 64 {
		return error('concurrent sampler lost callbacks or spans')
	}
	mut identities := map[string]bool{}
	for request in requests {
		if request.span_id in identities { return error('concurrent sampling identity collision') }
		identities[request.span_id] = true
	}
	closing := ClosingTraceSampler{
		state: &SamplerCloseState{}
	}
	close_exporter := RecordingTraceExporter{
		audit: &ExportAudit{}
	}
	owner := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter: close_exporter
		sampler:  closing
	})!
	closing.attach(owner)
	late := owner.start_span('sampler-closing')!
	if late.is_recording() { return error('sampler-closed provider still records') }
	late.finish()
	owner.shutdown(5 * time.second)!
	if owner.stats().accepted_spans != 0 || owner.stats().dropped_spans != 1
		|| close_exporter.audit.shutdowns != 1 {
		return error('sampler-initiated close leaked ownership')
	}
}
