module sdk

import temporal
import json2
import encoding.utf8

enum EventGroupKind {
	label
	inbound_event
	inbound_update
}

// Experimental. IDs are plaintext history metadata; labels pass through codecs.
pub struct EventGroup {
	kind       EventGroupKind
	identifier string
	label      ?string
	event_id   i64
}

pub fn new_event_group(id string) !EventGroup {
	if id == '' || !utf8.validate_str(id) { return error('Event Group ID must be nonempty UTF-8') }
	return EventGroup{
		identifier: id
	}
}

pub fn (group EventGroup) with_label(label string) !EventGroup {
	if group.kind != .label || group.identifier == '' {
		return error('only explicit Event Groups can have labels')
	}
	if label == '' || !utf8.validate_str(label) {
		return error('Event Group label must be nonempty UTF-8')
	}
	return EventGroup{
		...group
		label: label
	}
}

pub fn (group EventGroup) id() string {
	return group.identifier
}

pub type WorkflowGroupCallback = fn (mut WorkflowContext) !

@[params]
pub struct EventGroupOptions {
pub:
	event_groups []EventGroup
}

struct EventGroupState {
	groups  []EventGroup
	markers []temporal.TemporalApiSdkV1_EventGroupMarker
}

fn (group EventGroup) key() string {
	return match group.kind {
		.label { 'label\x00' + group.identifier }
		.inbound_event { 'event\x00' + group.event_id.str() }
		.inbound_update { 'update\x00' + group.identifier }
	}
}

fn merge_group_values(enclosing []EventGroup, direct []EventGroup) []EventGroup {
	mut result := enclosing.clone()
	mut indices := map[string]int{}
	for index, group in result {
		indices[group.key()] = index
	}
	for group in direct {
		if index := indices[group.key()] {
			result[index] = group
		} else {
			indices[group.key()] = result.len
			result << group
		}
	}
	return result
}

fn group_marker_key(marker temporal.TemporalApiSdkV1_EventGroupMarker) string {
	variant := marker.variant or { return '' }
	return match variant {
		temporal.TemporalApiSdkV1_EventGroupMarker_Label_Case {
			'label\x00' + variant.value.id
		}
		temporal.TemporalApiSdkV1_EventGroupMarker_InboundEvent_Case {
			'event\x00' + variant.value.inbound_event_id.str()
		}
		temporal.TemporalApiSdkV1_EventGroupMarker_InboundUpdate_Case {
			'update\x00' + variant.value.inbound_update_id
		}
	}
}

fn merge_group_markers(enclosing []temporal.TemporalApiSdkV1_EventGroupMarker, direct []temporal.TemporalApiSdkV1_EventGroupMarker) []temporal.TemporalApiSdkV1_EventGroupMarker {
	mut result := []temporal.TemporalApiSdkV1_EventGroupMarker{}
	mut indices := map[string]int{}
	mut combined := enclosing.clone()
	combined << direct
	for marker in combined {
		key := group_marker_key(marker)
		if key == '' {
			continue
		}
		if index := indices[key] {
			result[index] = marker
		} else {
			indices[key] = result.len
			result << marker
		}
	}
	return result
}

fn (context &WorkflowContext) encode_event_group(group EventGroup) !temporal.TemporalApiSdkV1_EventGroupMarker {
	return match group.kind {
		.label {
			if group.identifier == '' || !utf8.validate_str(group.identifier) {
				return error('invalid Event Group ID')
			}
			mut payload := ?temporal.TemporalApiCommonV1_Payload(none)
			if label := group.label {
				if label == '' || !utf8.validate_str(label) {
					return error('invalid Event Group label')
				}
				// UI labels always start as json/plain, regardless of the configured
				// application payload converter. Only the codec pipeline applies.
				mut encoded := [
					temporal.TemporalApiCommonV1_Payload{
						metadata: {
							'encoding': 'json/plain'.bytes()
						}
						data:     json2.encode(label).bytes()
					},
				]
				encoded = context.converter.encode_codec_payloads(encoded)!
				if encoded.len != 1 {
					return error('Event Group label requires exactly one codec payload')
				}
				payload = temporal.TemporalApiCommonV1_Payload.decode(encoded[0].encode())!
			}
			temporal.TemporalApiSdkV1_EventGroupMarker{
				variant: temporal.TemporalApiSdkV1_EventGroupMarker_Label_Case{
					value: temporal.TemporalApiSdkV1_EventGroupMarker_Label{
						id:    group.identifier
						label: payload
					}
				}
			}
		}
		.inbound_event {
			if group.event_id <= 0 { return error('invalid inbound Event Group event ID') }
			temporal.TemporalApiSdkV1_EventGroupMarker{
				variant: temporal.TemporalApiSdkV1_EventGroupMarker_InboundEvent_Case{
					value: temporal.TemporalApiSdkV1_EventGroupMarker_InboundEvent{
						inbound_event_id: group.event_id
					}
				}
			}
		}
		.inbound_update {
			if group.identifier == '' { return error('invalid inbound Event Group Update ID') }
			temporal.TemporalApiSdkV1_EventGroupMarker{
				variant: temporal.TemporalApiSdkV1_EventGroupMarker_InboundUpdate_Case{
					value: temporal.TemporalApiSdkV1_EventGroupMarker_InboundUpdate{
						inbound_update_id: group.identifier
					}
				}
			}
		}
	}
}

fn (context &WorkflowContext) command_event_groups(direct []EventGroup) !EventGroupState {
	if !context.event_groups_enabled {
		if direct.len > 0 {
			return WorkflowTaskError{
				message: 'Event Groups require experimental_event_groups'
			}
		}
		return EventGroupState{}
	}
	mut markers := []temporal.TemporalApiSdkV1_EventGroupMarker{}
	for group in direct {
		markers << context.encode_event_group(group)!
	}
	return EventGroupState{
		groups:  merge_group_values(context.event_group_state.groups, direct)
		markers: merge_group_markers(context.event_group_state.markers, markers)
	}
}

pub fn (context &WorkflowContext) current_event_groups() []EventGroup {
	return context.event_group_state.groups.clone()
}

// Scopes are synchronous. Capture current_event_groups() and re-enter the scope
// when an event-driven handler resumes in a later callback.
pub fn (mut context WorkflowContext) with_event_groups(groups []EventGroup, callback WorkflowGroupCallback) ! {
	context.ensure_open()!
	if !context.event_groups_enabled {
		return WorkflowTaskError{
			message: 'Event Groups require experimental_event_groups'
		}
	}
	state := context.command_event_groups(groups)!
	previous := context.event_group_state
	context.event_group_state = state
	defer { context.event_group_state = previous }
	callback(mut context)!
}

pub fn (mut context WorkflowContext) with_event_group(group EventGroup, callback WorkflowGroupCallback) ! {
	context.with_event_groups([group], callback)!
}

fn (mut context WorkflowContext) enter_handler_event_group(event Event) {
	if !context.event_groups_enabled { return }
	mut groups := []EventGroup{}
	if event.kind == .signal_received && event.originating_event_id > 0 {
		groups << EventGroup{
			kind:     .inbound_event
			event_id: event.originating_event_id
		}
	}
	if event.kind == .update_received {
		if pending := context.updates[event.id] {
			groups << EventGroup{
				kind:       .inbound_update
				identifier: pending.id
			}
		}
	}
	if event.kind !in [.signal_received, .update_received] { return }
	mut markers := []temporal.TemporalApiSdkV1_EventGroupMarker{}
	for group in groups {
		markers << context.encode_event_group(group) or {
			panic('invalid Core inbound Event Group: ' + err.msg())
		}
	}
	context.event_group_state = EventGroupState{
		groups:  groups
		markers: markers
	}
}

fn groupable_command(command temporal.CoresdkWorkflowCommands_WorkflowCommand) bool {
	variant := command.variant or { return false }
	return variant !is temporal.CoresdkWorkflowCommands_WorkflowCommand_RespondToQuery_Case
		&& variant !is temporal.CoresdkWorkflowCommands_WorkflowCommand_UpdateResponse_Case
		&& variant !is temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelTimer_Case
		&& variant !is temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelActivity_Case
		&& variant !is temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelLocalActivity_Case
		&& variant !is temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelChildWorkflowExecution_Case
		&& variant !is temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelSignalWorkflow_Case
		&& variant !is temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelNexusOperation_Case
}

fn (mut context WorkflowContext) queue_command(command temporal.CoresdkWorkflowCommands_WorkflowCommand) {
	context.ensure_writable() or { panic(err.msg()) }
	context.command_context_values << context.capture_context_values()
	if !context.event_groups_enabled {
		context.commands << command
		return
	}
	markers := if groupable_command(command) {
		merge_group_markers(context.event_group_state.markers, command.event_group_markers)
	} else {
		command.event_group_markers
	}
	mut owned := []temporal.TemporalApiSdkV1_EventGroupMarker{}
	for marker in markers {
		owned << temporal.TemporalApiSdkV1_EventGroupMarker.decode(marker.encode()) or {
			panic('validated Event Group marker clone failed: ' + err.msg())
		}
	}
	context.commands << temporal.CoresdkWorkflowCommands_WorkflowCommand{
		...command
		event_group_markers: owned
	}
}
