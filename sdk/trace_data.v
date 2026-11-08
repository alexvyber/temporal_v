module sdk

import x.json2
import encoding.utf8

pub struct TraceStringArray {
pub:
	values []string
}

pub struct TraceBooleanArray {
pub:
	values []bool
}

pub struct TraceIntegerArray {
pub:
	values []i64
}

pub struct TraceDoubleArray {
pub:
	values []f64
}

pub type TraceAttribute = TraceBooleanArray
	| TraceDoubleArray
	| TraceIntegerArray
	| TraceStringArray
	| bool
	| f64
	| i64
	| string

pub enum TraceSpanKind {
	internal
	server
	client
	producer
	consumer
}

pub enum TraceStatus {
	unset
	ok
	error
}

pub struct TraceEvent {
pub:
	name               string
	unix_nanos         u64
	attributes         map[string]TraceAttribute
	dropped_attributes u32
}

pub struct TraceLink {
pub:
	context            TraceContext
	attributes         map[string]TraceAttribute
	dropped_attributes u32
}

pub struct TraceSpanData {
pub:
	context            TraceContext
	parent_span_id     string
	parent_remote      bool
	name               string
	kind               TraceSpanKind
	scope_name         string = 'temporal-v'
	scope_version      string = '0.0.1'
	start_unix_nanos   u64
	end_unix_nanos     u64
	attributes         map[string]TraceAttribute
	events             []TraceEvent
	links              []TraceLink
	dropped_attributes u32
	dropped_events     u32
	dropped_links      u32
	status             TraceStatus
	status_message     string
}

struct TraceWireEvent {
	name               string
	unix_nanos         string
	attributes         map[string]json2.Any
	dropped_attributes u32
}

struct TraceWireLink {
	context            TraceContext
	attributes         map[string]json2.Any
	dropped_attributes u32
}

struct TraceWireSpan {
	context            TraceContext
	parent_span_id     string
	parent_remote      bool
	name               string
	kind               int
	scope_name         string
	scope_version      string
	start_unix_nanos   string
	end_unix_nanos     string
	attributes         map[string]json2.Any
	events             []TraceWireEvent
	links              []TraceWireLink
	dropped_attributes u32
	dropped_events     u32
	dropped_links      u32
	status             int
	status_message     string
}

fn validate_trace_string(value string, label string, maximum int) ! {
	if value.len > maximum || !utf8.validate_str(value) {
		return error('invalid trace ${label} UTF-8 or byte length')
	}
}

fn trace_attribute_size(value TraceAttribute) !u64 {
	match value {
		string {
			validate_trace_string(value, 'attribute', 65536)!
			return u64(value.len) * 6 + 64
		}
		bool, i64, f64 {
			return 128
		}
		TraceStringArray {
			if value.values.len > 4096 { return error('trace attribute array is too long') }
			mut total := u64(128)
			for item in value.values {
				validate_trace_string(item, 'array value', 65536)!
				total += u64(item.len) * 6 + 32
			}
			return total
		}
		TraceBooleanArray {
			if value.values.len > 4096 { return error('trace attribute array is too long') }
			return u64(value.values.len) * 32 + 128
		}
		TraceIntegerArray {
			if value.values.len > 4096 { return error('trace attribute array is too long') }
			return u64(value.values.len) * 64 + 128
		}
		TraceDoubleArray {
			if value.values.len > 4096 { return error('trace attribute array is too long') }
			return u64(value.values.len) * 64 + 128
		}
	}
}

fn trace_attributes_size(values map[string]TraceAttribute) !u64 {
	if values.len > 1024 { return error('too many trace attributes') }
	mut total := u64(128)
	for name, value in values {
		if name == '' { return error('trace attribute name is empty') }
		validate_trace_string(name, 'attribute name', 1024)!
		total += u64(name.len) * 6 + 64 + trace_attribute_size(value)!
		if total > 8 * 1024 * 1024 { return error('trace attributes exceed the export byte limit') }
	}
	return total
}

fn trace_data_size(data TraceSpanData) !u64 {
	data.context.validate()!
	if data.parent_span_id != '' && !lower_trace_hex(data.parent_span_id, 16) {
		return error('invalid trace parent span ID')
	}
	if data.name == '' || data.scope_name == '' || data.end_unix_nanos < data.start_unix_nanos {
		return error('invalid trace span name, scope or timestamp order')
	}
	validate_trace_string(data.name, 'span name', 4096)!
	validate_trace_string(data.scope_name, 'scope name', 1024)!
	validate_trace_string(data.scope_version, 'scope version', 1024)!
	validate_trace_string(data.status_message, 'status message', 65536)!
	if data.events.len > 1024 || data.links.len > 1024 {
		return error('too many trace events or links')
	}
	mut total := u64(2048) + u64(data.name.len + data.scope_name.len + data.scope_version.len +
		data.status_message.len + data.context.trace_state.len) * 6 +
		trace_attributes_size(data.attributes)!
	for event in data.events {
		if event.name == '' { return error('trace event name is empty') }
		validate_trace_string(event.name, 'event name', 4096)!
		total += u64(256) + u64(event.name.len) * 6 + trace_attributes_size(event.attributes)!
		if total > 8 * 1024 * 1024 { return error('trace span exceeds the export byte limit') }
	}
	for link in data.links {
		link.context.validate()!
		total += u64(512) + u64(link.context.trace_state.len) * 6 +
			trace_attributes_size(link.attributes)!
		if total > 8 * 1024 * 1024 { return error('trace span exceeds the export byte limit') }
	}
	if total > 8 * 1024 * 1024 { return error('trace span exceeds the export byte limit') }
	return total
}

fn trace_wire_attribute(value TraceAttribute) json2.Any {
	mut kind := ''
	mut result := json2.Any('')
	match value {
		string {
			kind = 'string'
			result = json2.Any(value)
		}
		bool {
			kind = 'boolean'
			result = json2.Any(value)
		}
		i64 {
			kind = 'integer'
			result = json2.Any(value.str())
		}
		f64 {
			kind = 'double'
			result = json2.Any(value.str())
		}
		TraceStringArray {
			kind = 'string_array'
			result = json2.Any(value.values.map(json2.Any(it)))
		}
		TraceBooleanArray {
			kind = 'boolean_array'
			result = json2.Any(value.values.map(json2.Any(it)))
		}
		TraceIntegerArray {
			kind = 'integer_array'
			result = json2.Any(value.values.map(json2.Any(it.str())))
		}
		TraceDoubleArray {
			kind = 'double_array'
			result = json2.Any(value.values.map(json2.Any(it.str())))
		}
	}

	return json2.Any({
		'kind':  json2.Any(kind)
		'value': result
	})
}

fn trace_wire_attributes(values map[string]TraceAttribute) map[string]json2.Any {
	mut result := map[string]json2.Any{}
	for name, value in values {
		result[name] = trace_wire_attribute(value)
	}
	return result
}

fn clone_trace_attributes(values map[string]TraceAttribute) map[string]TraceAttribute {
	mut result := map[string]TraceAttribute{}
	for name, value in values {
		result[name] = match value {
			TraceStringArray {
				TraceAttribute(TraceStringArray{
					values: value.values.clone()
				})
			}
			TraceBooleanArray {
				TraceAttribute(TraceBooleanArray{
					values: value.values.clone()
				})
			}
			TraceIntegerArray {
				TraceAttribute(TraceIntegerArray{
					values: value.values.clone()
				})
			}
			TraceDoubleArray {
				TraceAttribute(TraceDoubleArray{
					values: value.values.clone()
				})
			}
			else {
				value
			}
		}
	}
	return result
}

fn clone_trace_span(data TraceSpanData) TraceSpanData {
	mut events := []TraceEvent{}
	mut links := []TraceLink{}
	for event in data.events {
		events << TraceEvent{
			...event
			attributes: clone_trace_attributes(event.attributes)
		}
	}
	for link in data.links {
		links << TraceLink{
			...link
			attributes: clone_trace_attributes(link.attributes)
		}
	}
	return TraceSpanData{
		...data
		attributes: clone_trace_attributes(data.attributes)
		events:     events
		links:      links
	}
}

fn trace_wire_span(data TraceSpanData) !TraceWireSpan {
	mut events := []TraceWireEvent{}
	mut links := []TraceWireLink{}
	for event in data.events {
		events << TraceWireEvent{
			name:               event.name
			unix_nanos:         event.unix_nanos.str()
			attributes:         trace_wire_attributes(event.attributes)
			dropped_attributes: event.dropped_attributes
		}
	}
	for link in data.links {
		links << TraceWireLink{
			context:            TraceContext{
				...link.context
				trace_state: normalize_trace_state(link.context.trace_state)!
			}
			attributes:         trace_wire_attributes(link.attributes)
			dropped_attributes: link.dropped_attributes
		}
	}
	return TraceWireSpan{
		context:            TraceContext{
			...data.context
			trace_state: normalize_trace_state(data.context.trace_state)!
		}
		parent_span_id:     data.parent_span_id
		parent_remote:      data.parent_remote
		name:               data.name
		kind:               int(data.kind)
		scope_name:         data.scope_name
		scope_version:      data.scope_version
		start_unix_nanos:   data.start_unix_nanos.str()
		end_unix_nanos:     data.end_unix_nanos.str()
		attributes:         trace_wire_attributes(data.attributes)
		events:             events
		links:              links
		dropped_attributes: data.dropped_attributes
		dropped_events:     data.dropped_events
		dropped_links:      data.dropped_links
		status:             int(data.status)
		status_message:     data.status_message
	}
}
