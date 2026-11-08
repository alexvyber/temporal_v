module sdk

import strconv
import crypto.rand as crypto_rand
import alexvyber.temporal_v.temporal

pub const trace_header_key = '_tracer-data'

pub struct TraceContext {
pub:
	trace_id    string
	span_id     string
	trace_flags u8
	trace_state string
	remote      bool
}

fn lower_trace_hex(value string, length int) bool {
	if value.len != length { return false }
	for character in value.bytes() {
		if !(character >= `0` && character <= `9`) && !(character >= `a` && character <= `f`) {
			return false
		}
	}
	return true
}

fn (context TraceContext) validate() ! {
	if !lower_trace_hex(context.trace_id, 32) || context.trace_id == '0'.repeat(32)
		|| !lower_trace_hex(context.span_id, 16) || context.span_id == '0'.repeat(16) {
		return error('invalid trace or span ID')
	}
	normalize_trace_state(context.trace_state)!
}

fn trace_state_key_part(value string, maximum int, tenant bool) bool {
	if value.len < 1 || value.len > maximum { return false }
	if !(value[0] >= `a` && value[0] <= `z`) && !(tenant && value[0] >= `0` && value[0] <= `9`) {
		return false
	}
	for character in value.bytes() {
		if (character >= `a` && character <= `z`)
			|| (character >= `0` && character <= `9`)
			|| character in [`_`, `-`, `*`, `/`] {
			continue
		}
		return false
	}
	return true
}

pub fn normalize_trace_state(value string) !string {
	mut entries := []string{}
	mut seen := map[string]bool{}
	for member in value.split(',') {
		if member == '' {
			continue
		}
		separator := member.index('=') or { return error('invalid trace state member') }
		key := member[..separator].trim_left(' \t')
		data := member[separator + 1..].trim_right(' \t')
		parts := key.split('@')
		valid_key := if parts.len == 1 {
			trace_state_key_part(parts[0], 256, false)
		} else if parts.len == 2 {
			trace_state_key_part(parts[0], 241, true) && trace_state_key_part(parts[1], 14, false)
		} else {
			false
		}
		if !valid_key || data.len < 1 || data.len > 256 || key in seen || entries.len == 32 {
			return error('invalid trace state key, value or member count')
		}
		for character in data.bytes() {
			if character < 32 || character > 126 || character in [`,`, `=`] {
				return error('invalid trace state value')
			}
		}
		if data[data.len - 1] == ` ` { return error('trace state value ends with a space') }
		seen[key] = true
		entries << key + '=' + data
	}
	return entries.join(',')
}

// A malformed tracestate must not discard a valid traceparent.
pub fn parse_trace_parent(parent string, state string) !TraceContext {
	parts := parent.split('-')
	if parts.len < 4 || !lower_trace_hex(parts[0], 2) || parts[0] == 'ff'
		|| !lower_trace_hex(parts[3], 2) {
		return error('invalid traceparent version or flags')
	}
	flags := u8(strconv.parse_uint(parts[3], 16, 8)!)
	if parts[0] == '00' && (parts.len != 4 || flags > 3) {
		return error('invalid version-zero traceparent')
	}
	context := TraceContext{
		trace_id:    parts[1]
		span_id:     parts[2]
		trace_flags: flags & u8(3)
		trace_state: normalize_trace_state(state) or { '' }
		remote:      true
	}
	context.validate()!
	return context
}

pub fn (context TraceContext) sampled() bool {
	return context.trace_flags & u8(1) != 0
}

pub fn (context TraceContext) trace_parent() !string {
	context.validate()!
	flags := [context.trace_flags & u8(3)].hex()
	return '00-${context.trace_id}-${context.span_id}-${flags}'
}

pub fn (context TraceContext) propagation_headers() !map[string]string {
	mut result := {
		'traceparent': context.trace_parent()!
	}
	state := normalize_trace_state(context.trace_state)!
	if state != '' { result['tracestate'] = state }
	return result
}

pub fn extract_trace_context(headers map[string]string) ?TraceContext {
	mut parent := ''
	mut state := []string{}
	mut found := false
	for key, value in headers {
		match key.to_lower() {
			'traceparent' {
				if found { return none }
				parent = value
				found = true
			}
			'tracestate' {
				state << value
			}
			else {}
		}
	}
	if !found { return none }
	return parse_trace_parent(parent, state.join(',')) or { return none }
}

pub fn new_trace_context(sampled bool) !TraceContext {
	context := TraceContext{
		trace_id:    crypto_rand.bytes(16)!.hex()
		span_id:     crypto_rand.bytes(8)!.hex()
		trace_flags: if sampled {
			u8(3)
		} else {
			u8(2)
		}
	}
	context.validate()!
	return context
}

pub fn (parent TraceContext) child() !TraceContext {
	parent.validate()!
	context := TraceContext{
		...parent
		span_id: crypto_rand.bytes(8)!.hex()
		remote:  false
	}
	context.validate()!
	return context
}

// Temporal's tracer carrier is an SDK header, separate from application codecs.
pub fn (context TraceContext) temporal_trace_header() !temporal.TemporalApiCommonV1_Payload {
	return PropagationContext{
		span: context
	}.temporal_header()!
}

pub fn extract_temporal_trace_context(headers map[string]temporal.TemporalApiCommonV1_Payload) ?TraceContext {
	return extract_temporal_propagation_context(headers).span
}
