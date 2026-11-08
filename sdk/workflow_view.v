module sdk

import alexvyber.temporal_v.temporal
import time

// An owned, read-only snapshot; it has no command or mutable-runtime access.
pub struct WorkflowContextView {
	information       WorkflowInfo @[json: '-']
	initialization    temporal.CoresdkWorkflowActivation_InitializeWorkflow @[json: '-']
	converter         DataConverter @[json: '-']
	memo_values       map[string]temporal.TemporalApiCommonV1_Payload @[json: '-']
	attributes        map[string]temporal.TemporalApiCommonV1_Payload @[json: '-']
	request           map[string]temporal.TemporalApiCommonV1_Payload @[json: '-']
	arguments         []RawValue @[json: '-']
	clock             time.Time  @[json: '-']
	details           string     @[json: '-']
	replaying         bool       @[json: '-']
	replaying_history bool       @[json: '-']
}

fn owned_payload_map(values map[string]temporal.TemporalApiCommonV1_Payload) !map[string]temporal.TemporalApiCommonV1_Payload {
	mut keys := values.keys()
	keys.sort()
	mut inputs := []temporal.TemporalApiCommonV1_Payload{cap: keys.len}
	for key in keys {
		inputs << values[key]
	}
	owned := owned_payload_messages(inputs)!
	mut result := map[string]temporal.TemporalApiCommonV1_Payload{}
	for index, key in keys {
		result[key] = owned[index]
	}
	return result
}

pub fn (context &WorkflowContext) view() !WorkflowContextView {
	return context.interaction_view(map[string]temporal.TemporalApiCommonV1_Payload{},
		[]RawValue{}, context.capture_context_values())
}

fn (context &WorkflowContext) interaction_view(headers map[string]temporal.TemporalApiCommonV1_Payload, arguments []RawValue, values WorkflowContextValues) !WorkflowContextView {
	return WorkflowContextView{
		information:       WorkflowInfo{
			...context.info()
			context_values: WorkflowContextValues{
				entries: values.entries.clone()
			}
		}
		initialization:    temporal.CoresdkWorkflowActivation_InitializeWorkflow.decode(context.initialization.encode())!
		converter:         context.converter
		memo_values:       owned_payload_map(context.memo_values)!
		attributes:        owned_payload_map(context.search_attribute_values)!
		request:           owned_payload_map(headers)!
		arguments:         arguments.clone()
		clock:             context.now()
		details:           context.current_details
		replaying:         context.replaying
		replaying_history: context.replaying_history
	}
}

pub fn (view WorkflowContextView) info() WorkflowInfo {
	return owned_workflow_info(view.information)
}

pub fn (view WorkflowContextView) now() time.Time {
	return view.clock
}

pub fn (view WorkflowContextView) current_details() string {
	return view.details
}

pub fn (view WorkflowContextView) is_replaying() bool {
	return view.replaying
}

pub fn (view WorkflowContextView) is_replaying_history_events() bool {
	return view.replaying_history
}

pub fn (view WorkflowContextView) context_value[T](key string) ?T {
	return view.information.context_values.get[T](key)
}

pub fn (view WorkflowContextView) headers() !map[string]PayloadValue {
	return view.converter.decode_headers(view.initialization.headers)!
}

pub fn (view WorkflowContextView) raw_headers() !map[string]RawValue {
	return view.converter.decode_raw_headers(view.initialization.headers)!
}

pub fn (view WorkflowContextView) request_headers() !map[string]PayloadValue {
	return view.converter.decode_headers(view.request)!
}

pub fn (view WorkflowContextView) raw_request_headers() !map[string]RawValue {
	return view.converter.decode_raw_headers(view.request)!
}

pub fn (view WorkflowContextView) memo() !map[string]PayloadValue {
	return view.converter.decode_map(view.memo_values)!
}

pub fn (view WorkflowContextView) raw_memo() !map[string]RawValue {
	mut result := map[string]RawValue{}
	mut keys := view.memo_values.keys()
	keys.sort()
	for key in keys {
		decoded := view.converter.decode_payloads([view.memo_values[key]])!
		if decoded.len != 1 {
			return error('workflow view memo codec requires exactly one payload')
		}
		result[key] = raw_value(decoded[0])!
	}
	return result
}

pub fn (view WorkflowContextView) search_attributes() ![]SearchAttribute {
	return decode_search_attributes(view.attributes)!
}

pub fn (view WorkflowContextView) raw_search_attributes() !map[string]RawValue {
	mut result := map[string]RawValue{}
	for key, value in view.attributes {
		result[key] = raw_value(value)!
	}
	return result
}

pub fn (view WorkflowContextView) raw_initialization() !temporal.CoresdkWorkflowActivation_InitializeWorkflow {
	return temporal.CoresdkWorkflowActivation_InitializeWorkflow.decode(view.initialization.encode())!
}

pub fn (view WorkflowContextView) input_values() []RawValue {
	return view.arguments.clone()
}

pub fn (view WorkflowContextView) decode_raw_value(value RawValue) !PayloadValue {
	return view.converter.decode_raw_value(value)!
}

pub fn (view WorkflowContextView) start_values() ![]RawValue {
	return view.converter.decode_raw_arguments(view.initialization.arguments)!.raw_values
}
