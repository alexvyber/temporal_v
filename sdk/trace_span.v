module sdk

import sync
import time

@[params]
pub struct TraceSpanOptions {
pub:
	parent           ?TraceContext
	baggage          Baggage
	context          ?TraceContext
	sampled          ?bool
	suppress_export  bool
	kind             TraceSpanKind
	scope_name       string = 'temporal-v'
	scope_version    string = '0.0.1'
	start_unix_nanos ?u64
	attributes       map[string]TraceAttribute
	links            []TraceLink
}

@[params]
pub struct TraceEndOptions {
pub:
	unix_nanos ?u64
}

@[heap]
struct TraceSpanState {
	mutex           &sync.RwMutex = sync.new_rwmutex()
	provider        &TraceProvider
	data            TraceSpanData
	limits          TraceLimits
	started_mono    i64
	automatic_start bool
	suppress_export bool
	recording       bool
mut:
	finished           bool
	attributes         map[string]TraceAttribute
	events             []TraceEvent
	links              []TraceLink
	dropped_attributes u32
	dropped_events     u32
	dropped_links      u32
	status             TraceStatus
	status_message     string
}

@[heap]
pub struct TraceSpan {
	state &TraceSpanState
}

fn trace_drop_count(current u32, extra u64) u32 {
	sum := u64(current) + extra
	return if sum > u64(4294967295) { u32(4294967295) } else { u32(sum) }
}

fn limited_trace_attributes(values map[string]TraceAttribute, maximum int) !(map[string]TraceAttribute, u32) {
	trace_attributes_size(values)!
	mut names := values.keys()
	names.sort()
	mut selected := map[string]TraceAttribute{}
	for index, name in names {
		if index >= maximum {
			break
		}
		selected[name] = values[name] or {
			return error('trace attributes changed during snapshot')
		}
	}
	return clone_trace_attributes(selected), u32(values.len - selected.len)
}

fn trace_wall_nanos() u64 {
	now := time.now().unix_nano()
	return if now > 0 { u64(now) } else { u64(0) }
}

pub fn (provider &TraceProvider) start_span(name string, options TraceSpanOptions) !&TraceSpan {
	started_mono := time.sys_mono_now()
	started := options.start_unix_nanos or { trace_wall_nanos() }
	mut context := if supplied := options.context {
		supplied
	} else if parent := options.parent {
		parent.child()!
	} else {
		new_trace_context(options.sampled or { true })!
	}
	context.validate()!
	context = TraceContext{
		...context
		trace_state: normalize_trace_state(context.trace_state)!
		remote:      false
	}
	parent_id := if parent := options.parent {
		parent.validate()!
		if parent.trace_id != context.trace_id {
			return error('trace span and parent have different trace IDs')
		}
		parent.span_id
	} else {
		''
	}
	trace_data_size(TraceSpanData{
		context:          context
		parent_span_id:   parent_id
		name:             name
		kind:             options.kind
		scope_name:       options.scope_name
		scope_version:    options.scope_version
		start_unix_nanos: started
		end_unix_nanos:   started
		attributes:       options.attributes
		links:            options.links
	})!
	if options.links.len > 1024 { return error('too many trace span links') }
	sampling := provider.state.options.sampler.should_sample(TraceSamplingParameters{
		parent:     options.parent
		baggage:    options.baggage
		trace_id:   context.trace_id
		span_id:    context.span_id
		name:       name
		kind:       options.kind
		attributes: clone_trace_attributes(options.attributes)
		links:      clone_sampling_links(options.links)
	})
	trace_attributes_size(sampling.attributes)!
	decision := if sampled := options.sampled {
		if sampled { TraceSamplingDecision.record_and_sample } else { TraceSamplingDecision.drop }
	} else {
		sampling.decision
	}
	context = TraceContext{
		...context
		trace_flags: if decision == .record_and_sample {
			context.trace_flags | u8(1)
		} else {
			context.trace_flags & u8(254)
		}
	}
	if state := sampling.trace_state {
		context = TraceContext{
			...context
			trace_state: normalize_trace_state(state)!
		}
	}
	remote := if parent := options.parent { parent.remote } else { false }
	data := TraceSpanData{
		context:          context
		parent_span_id:   parent_id
		parent_remote:    remote
		name:             name
		kind:             options.kind
		scope_name:       options.scope_name
		scope_version:    options.scope_version
		start_unix_nanos: started
		end_unix_nanos:   started
	}
	trace_data_size(data)!
	mut supplied := clone_trace_attributes(options.attributes)
	for key, value in sampling.attributes {
		supplied[key] = value
	}
	attributes, dropped := limited_trace_attributes(supplied,
		provider.state.options.limits.attributes)!
	mut links := []TraceLink{}
	if options.links.len > 1024 { return error('too many trace span links') }
	for index, link in options.links {
		if index >= provider.state.options.limits.links {
			break
		}
		link.context.validate()!
		values, lost := limited_trace_attributes(link.attributes,
			provider.state.options.limits.attributes)!
		links << TraceLink{
			...link
			attributes:         values
			dropped_attributes: trace_drop_count(link.dropped_attributes, u64(lost))
		}
	}
	return &TraceSpan{
		state: &TraceSpanState{
			provider:           provider
			data:               data
			limits:             provider.state.options.limits
			started_mono:       started_mono
			automatic_start:    options.start_unix_nanos == none
			suppress_export:    options.suppress_export
			recording:          decision != .drop
			attributes:         attributes
			links:              links
			dropped_attributes: dropped
			dropped_links:      u32(options.links.len - links.len)
		}
	}
}

pub fn (span &TraceSpan) context() TraceContext {
	return span.state.data.context
}

pub fn (span &TraceSpan) is_recording() bool {
	span.state.mutex.rlock()
	defer { span.state.mutex.runlock() }
	if span.state.finished || span.state.suppress_export || !span.state.recording {
		return false
	}
	return !span.state.provider.stats().closing
}

pub fn (span &TraceSpan) set_attribute(name string, value TraceAttribute) ! {
	mut state := span.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.finished || state.suppress_export || !state.recording { return }
	trace_attributes_size({
		name: value
	})!
	if name !in state.attributes && state.attributes.len >= state.limits.attributes {
		state.dropped_attributes = trace_drop_count(state.dropped_attributes, 1)
		return
	}
	copied := clone_trace_attributes({
		name: value
	})
	state.attributes[name] = copied[name] or { return error('trace attribute snapshot missing') }
}

pub fn (span &TraceSpan) add_event(event TraceEvent) ! {
	mut state := span.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.finished || state.suppress_export || !state.recording { return }
	if event.name == '' { return error('trace event name is empty') }
	validate_trace_string(event.name, 'event name', 4096)!
	if state.events.len >= state.limits.events {
		state.dropped_events = trace_drop_count(state.dropped_events, 1)
		return
	}
	values, lost := limited_trace_attributes(event.attributes, state.limits.attributes)!
	state.events << TraceEvent{
		...event
		attributes:         values
		dropped_attributes: trace_drop_count(event.dropped_attributes, u64(lost))
	}
}

pub fn (span &TraceSpan) add_link(link TraceLink) ! {
	mut state := span.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.finished || state.suppress_export || !state.recording { return }
	link.context.validate()!
	if state.links.len >= state.limits.links {
		state.dropped_links = trace_drop_count(state.dropped_links, 1)
		return
	}
	values, lost := limited_trace_attributes(link.attributes, state.limits.attributes)!
	state.links << TraceLink{
		...link
		attributes:         values
		dropped_attributes: trace_drop_count(link.dropped_attributes, u64(lost))
	}
}

pub fn (span &TraceSpan) set_status(status TraceStatus, message string) ! {
	mut state := span.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.finished || state.suppress_export || !state.recording || state.status == .ok
		|| status == .unset {
		return
	}
	validate_trace_string(message, 'status message', 65536)!
	state.status = status
	state.status_message = if status == .error { message } else { '' }
}

pub fn (span &TraceSpan) record_exception(exception_type string, failure IError) ! {
	mut attributes := {
		'exception.type':    TraceAttribute(exception_type)
		'exception.message': TraceAttribute(failure.msg())
	}
	if failure is PanicError {
		if failure.stack_trace != '' {
			attributes['exception.stacktrace'] = TraceAttribute(failure.stack_trace)
		}
	}
	span.add_event(TraceEvent{
		name:       'exception'
		unix_nanos: trace_wall_nanos()
		attributes: attributes
	})!
}

pub fn (span &TraceSpan) finish(options TraceEndOptions) {
	mut state := span.state
	state.mutex.lock()
	if state.finished {
		state.mutex.unlock()
		return
	}
	state.finished = true
	if state.suppress_export || !state.data.context.sampled() {
		provider := state.provider
		state.mutex.unlock()
		mut owner := provider.state
		owner.mutex.lock()
		owner.stats.suppressed_spans++
		owner.mutex.unlock()
		return
	}
	finished := if timestamp := options.unix_nanos {
		timestamp
	} else if state.automatic_start {
		elapsed := time.sys_mono_now() - state.started_mono
		duration := if elapsed > 0 { u64(elapsed) } else { u64(0) }
		if u64(18446744073709551615) - state.data.start_unix_nanos < duration {
			u64(18446744073709551615)
		} else {
			state.data.start_unix_nanos + duration
		}
	} else {
		trace_wall_nanos()
	}
	data := TraceSpanData{
		...state.data
		end_unix_nanos:     finished
		attributes:         clone_trace_attributes(state.attributes)
		events:             state.events.clone()
		links:              state.links.clone()
		dropped_attributes: state.dropped_attributes
		dropped_events:     state.dropped_events
		dropped_links:      state.dropped_links
		status:             state.status
		status_message:     state.status_message
	}
	provider := state.provider
	state.mutex.unlock()
	provider.record(data) or {
		mut owner := provider.state
		owner.mutex.lock()
		owner.stats.dropped_spans++
		owner.stats.last_error = trace_export_error_message(err.msg())
		owner.mutex.unlock()
	}
}
