module sdk

import alexvyber.temporal_v.temporal
import x.json2
import strconv

pub type SideEffect = fn () !PayloadValue

pub type SideEffectEqual = fn (previous PayloadValue, current PayloadValue) bool

pub struct NondeterminismError {
pub:
	message string
}

pub fn (err NondeterminismError) msg() string {
	return 'Nondeterminism: ' + err.message
}

pub fn (err NondeterminismError) code() int {
	return 1
}

struct RecordedWorkflowMarker {
	details    map[string]temporal.TemporalApiCommonV1_Payloads
	result     []temporal.TemporalApiCommonV1_Payload
	call_count i64
}

fn (mut context WorkflowContext) retain_recorded_value_error(value IError) IError {
	context.recorded_value_error = value
	return value
}

fn marker_integer(value string) !i64 {
	number := strconv.parse_int(value, 10, 64)!
	if number < 1 { return error('workflow marker integer must be positive') }
	return number
}

fn (context &WorkflowContext) marker_argument(values []temporal.TemporalApiCommonV1_Payload) !string {
	return context.converter.decode_single_argument(values)!.values[0]
}

fn (context &WorkflowContext) marker_values(values []PayloadValue) !temporal.TemporalApiCommonV1_Payloads {
	encoded := context.converter.encode_payload_values(values)!
	if encoded.len == 0 { return error('workflow marker codec produced no payloads') }
	return temporal.TemporalApiCommonV1_Payloads{
		payloads: encoded
	}
}

fn (mut context WorkflowContext) emit_workflow_marker(name string, details map[string]temporal.TemporalApiCommonV1_Payloads) {
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_RecordWorkflowMarker_Case{
			value: temporal.CoresdkWorkflowCommands_RecordWorkflowMarker{
				marker_name: name
				details:     details.clone()
			}
		}
	})
}

// Values are looked up before user callbacks so replay never invokes the
// nondurable computation, including after replacement of a worker.
pub fn (mut context WorkflowContext) side_effect(operation SideEffect) !PayloadValue {
	context.ensure_open()!
	if previous := context.recorded_value_error { return previous }
	if context.next_side_effect_id == i64(9223372036854775807) {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'side effect counter exhausted'
		})
	}
	context.next_side_effect_id++
	id := context.next_side_effect_id
	if context.replaying {
		recorded := context.side_effect_markers[id] or {
			return context.retain_recorded_value_error(NondeterminismError{
				message: 'no recorded SideEffect value for ID ${id}'
			})
		}
		value := context.converter.decode_single_argument(recorded.result) or {
			return context.retain_recorded_value_error(WorkflowTaskError{
				message: 'side effect value decoding failed: ${err.msg()}'
			})
		}
		context.emit_workflow_marker('SideEffect', recorded.details)
		context.side_effect_markers.delete(id)
		return value.typed_values[0]
	}
	value := operation() or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'side effect computation failed: ${err.msg()}'
		})
	}
	data := context.marker_values([value]) or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'side effect encoding failed: ${err.msg()}'
		})
	}
	canonical := context.converter.decode_single_argument(data.payloads) or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'side effect value decoding failed: ${err.msg()}'
		})
	}
	identity := context.marker_values([PayloadValue(JsonValue{ data: id.str() })]) or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'side effect identity encoding failed: ${err.msg()}'
		})
	}
	context.emit_workflow_marker('SideEffect', {
		'side-effect-id': identity
		'data':           data
	})
	return canonical.typed_values[0]
}

pub fn (mut context WorkflowContext) mutable_side_effect(id string, operation SideEffect, equal SideEffectEqual) !PayloadValue {
	context.ensure_open()!
	if previous := context.recorded_value_error { return previous }
	validate_version_name(id, 'mutable side effect ID', true) or {
		return context.retain_recorded_value_error(WorkflowTaskError{ message: err.msg() })
	}
	previous_count := context.mutable_side_effect_calls[id]
	if previous_count == i64(9223372036854775807) {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'mutable side effect counter exhausted'
		})
	}
	count := previous_count + 1
	context.mutable_side_effect_calls[id] = count
	if context.replaying {
		mut markers := (context.mutable_side_effect_markers[id] or {
			map[i64]RecordedWorkflowMarker{}
		}).clone()
		mut selected := i64(0)
		for hint, _ in markers {
			if hint <= count && hint > selected { selected = hint }
		}
		if selected > 0 {
			marker := markers[selected]
			context.mutable_side_effect_values[id] = marker.result.clone()
			if selected == count {
				context.emit_workflow_marker('MutableSideEffect', marker.details)
			}
			for hint in markers.keys() {
				if hint <= count { markers.delete(hint) }
			}
			if markers.len == 0 {
				context.mutable_side_effect_markers.delete(id)
			} else {
				context.mutable_side_effect_markers[id] = markers.clone()
			}
		}
		encoded := context.mutable_side_effect_values[id] or {
			return context.retain_recorded_value_error(NondeterminismError{
				message: 'no recorded MutableSideEffect value for ${id} at call ${count}'
			})
		}
		decoded := context.converter.decode_single_argument(encoded) or {
			return context.retain_recorded_value_error(WorkflowTaskError{
				message: 'mutable side effect decoding failed: ${err.msg()}'
			})
		}
		return decoded.typed_values[0]
	}
	computed := operation() or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'mutable side effect computation failed: ${err.msg()}'
		})
	}
	data := context.marker_values([computed]) or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'mutable side effect encoding failed: ${err.msg()}'
		})
	}
	canonical := context.converter.decode_single_argument(data.payloads) or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'mutable side effect value decoding failed: ${err.msg()}'
		})
	}
	if previous := context.mutable_side_effect_values[id] {
		old := context.converter.decode_single_argument(previous) or {
			return context.retain_recorded_value_error(WorkflowTaskError{
				message: 'mutable side effect cached value decoding failed: ${err.msg()}'
			})
		}
		if equal(clone_payload_value(old.typed_values[0]),
			clone_payload_value(canonical.typed_values[0]))
		{
			return old.typed_values[0]
		}
	}
	identity := context.marker_values([
		PayloadValue(JsonValue{ data: json2.encode(id + '_' + count.str()) }),
	]) or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'mutable side effect identity encoding failed: ${err.msg()}'
		})
	}
	hint := context.marker_values([PayloadValue(JsonValue{ data: count.str() })]) or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'mutable side effect counter encoding failed: ${err.msg()}'
		})
	}
	wrapper := protobuf_value(data, 'temporal.api.common.v1.Payloads') or {
		return context.retain_recorded_value_error(WorkflowTaskError{ message: err.msg() })
	}
	body := context.marker_values([PayloadValue(JsonValue{ data: json2.encode(id) }),
		PayloadValue(wrapper)]) or {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'mutable side effect wrapper encoding failed: ${err.msg()}'
		})
	}
	context.emit_workflow_marker('MutableSideEffect', {
		'side-effect-id':                   identity
		'data':                             body
		'mutable-side-effect-call-counter': hint
	})
	context.mutable_side_effect_values[id] = data.payloads.clone()
	return canonical.typed_values[0]
}

fn (mut context WorkflowContext) remember_workflow_markers(input []temporal.CoresdkWorkflowActivation_NotifyWorkflowMarker) ! {
	mut markers := input.clone()
	markers.sort(a.event_id < b.event_id)
	for marker in markers {
		if marker.event_id < 1 { return error('recorded workflow marker has no event ID') }
		if marker.event_id <= context.last_workflow_marker_event_id {
			continue
		}
		identity := marker.details['side-effect-id'] or {
			return error('workflow marker has no identity payloads')
		}
		body := marker.details['data'] or { return error('workflow marker has no data payloads') }
		if marker.marker_name == 'SideEffect' {
			id := marker_integer(context.marker_argument(identity.payloads)!)!
			if id > context.next_side_effect_id {
				context.side_effect_markers[id] = RecordedWorkflowMarker{
					details: marker.details.clone()
					result:  body.payloads.clone()
				}
			}
		} else if marker.marker_name == 'MutableSideEffect' {
			hint := marker.details['mutable-side-effect-call-counter'] or {
				return error('mutable side effect marker has no call counter')
			}
			count := marker_integer(context.marker_argument(hint.payloads)!)!
			decoded := context.converter.decode_arguments(body.payloads)!
			if decoded.values.len != 2 {
				return error('mutable side effect wrapper must contain ID and payloads')
			}
			id := json2.decode[string](decoded.values[0])!
			validate_version_name(id, 'recorded mutable side effect ID', true)!
			marker_id := json2.decode[string](context.marker_argument(identity.payloads)!)!
			prefix := id + '_'
			if !marker_id.starts_with(prefix) {
				return error('mutable side effect marker identity does not match its value')
			}
			marker_integer(marker_id[prefix.len..])!
			mut values := temporal.TemporalApiCommonV1_Payloads{}
			wrapper := decoded.typed_values[1]
			if wrapper is ProtobufValue {
				if wrapper.message_type != 'temporal.api.common.v1.Payloads' {
					return error('mutable side effect wrapper has a different protobuf type')
				}
				values = temporal.TemporalApiCommonV1_Payloads.decode(wrapper.data)!
			} else if wrapper is ProtobufJsonValue {
				if wrapper.message_type != 'temporal.api.common.v1.Payloads' {
					return error('mutable side effect wrapper has a different protobuf JSON type')
				}
				values = temporal.TemporalApiCommonV1_Payloads.from_json(wrapper.data)!
			} else {
				return error('mutable side effect value has no protobuf payload wrapper')
			}
			if values.payloads.len == 0 {
				return error('mutable side effect wrapper contains no value')
			}
			if count > context.mutable_side_effect_calls[id] {
				mut pending := (context.mutable_side_effect_markers[id] or {
					map[i64]RecordedWorkflowMarker{}
				}).clone()
				if count in pending {
					return error('duplicate mutable side effect call counter in history')
				}
				pending[count] = RecordedWorkflowMarker{
					details:    marker.details.clone()
					result:     values.payloads.clone()
					call_count: count
				}
				context.mutable_side_effect_markers[id] = pending.clone()
			}
		} else {
			return error('unsupported workflow marker: ${marker.marker_name}')
		}
		context.last_workflow_marker_event_id = marker.event_id
	}
}
