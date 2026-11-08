module sdk

import temporal_v.temporal

// NamedQueriesWorkflow declares named query dispatch alongside QueryableWorkflow.
// Names are nonempty and unique. Dynamic query fallback never handles these names.
pub interface NamedQueriesWorkflow {
	query_names() []string
}

pub interface DynamicQueriesWorkflow {
	query_dynamic(name string, arguments []RawValue, view WorkflowContextView) !PayloadValue
}

// NamedSignalsWorkflow declares the signals handled through on_event when the
// workflow also has a dynamic signal handler.
pub interface NamedSignalsWorkflow {
	signal_names() []string
}

pub interface DynamicSignalsWorkflow {
mut:
	signal_dynamic(event Event, mut context WorkflowContext) !
}

fn interaction_names(names []string, kind string) ![]string {
	mut seen := map[string]bool{}
	for name in names {
		if name == '' || name in seen {
			return error('invalid or duplicate named ' + kind + ' registration')
		}
		seen[name] = true
	}
	return names.clone()
}

fn named_query(workflow Workflow, name string) bool {
	if workflow is NamedQueriesWorkflow { return name in workflow.query_names() }
	return workflow is QueryableWorkflow && workflow !is DynamicQueriesWorkflow
}

fn dynamic_signal(workflow Workflow, name string) bool {
	if workflow !is DynamicSignalsWorkflow { return false }
	if workflow is NamedSignalsWorkflow { return name !in workflow.signal_names() }
	return true
}

fn validate_dynamic_interactions(workflow Workflow) ! {
	if workflow is NamedQueriesWorkflow {
		interaction_names(workflow.query_names(), 'query')!
		if workflow !is QueryableWorkflow {
			return error('named query declarations require a query handler')
		}
	}
	if workflow is DynamicQueriesWorkflow && workflow is QueryableWorkflow
		&& workflow !is NamedQueriesWorkflow {
		return error('combined named and dynamic queries require query_names')
	}
	if workflow is NamedSignalsWorkflow { interaction_names(workflow.signal_names(), 'signal')! }
}

fn (converter DataConverter) decode_raw_arguments(input []temporal.TemporalApiCommonV1_Payload) !DecodedArguments {
	mut raw_values := []RawValue{}
	mut typed_values := []PayloadValue{}
	mut legacy_values := []string{}
	for payload in converter.decode_payloads(input)! {
		raw := raw_value(payload)!
		value := PayloadValue(raw)
		raw_values << raw
		typed_values << value
		legacy_values << value.logical_data()
	}
	return DecodedArguments{
		values:       legacy_values
		typed_values: typed_values
		raw_values:   raw_values
	}
}

fn (context &WorkflowContext) decode_signal_arguments(workflow Workflow, name string, input []temporal.TemporalApiCommonV1_Payload) !DecodedArguments {
	if dynamic_signal(workflow, name) { return context.converter.decode_raw_arguments(input)! }
	return context.converter.decode_arguments(input)!
}

fn (context &WorkflowContext) decode_query_arguments(workflow Workflow, name string, input []temporal.TemporalApiCommonV1_Payload) !DecodedArguments {
	if named_query(workflow, name) { return context.converter.decode_arguments(input)! }
	if workflow is DynamicQueriesWorkflow { return context.converter.decode_raw_arguments(input)! }
	return context.converter.decode_arguments(input)!
}

fn described_interaction_names(definitions []WorkflowInteractionDefinition, names []string, kind string) ![]WorkflowInteractionDefinition {
	mut descriptions := map[string]string{}
	for definition in definitions {
		if definition.name !in names {
			return error('description for an unregistered ' + kind + ': ' + definition.name)
		}
		if definition.name in descriptions {
			return error('duplicate ' + kind + ' description: ' + definition.name)
		}
		descriptions[definition.name] = definition.description
	}
	mut result := []WorkflowInteractionDefinition{}
	for name in names {
		result << WorkflowInteractionDefinition{
			name:        name
			description: descriptions[name]
		}
	}
	return result
}

fn decode_activity_arguments(handler ActivityDispatch, converter DataConverter, input []temporal.TemporalApiCommonV1_Payload) !DecodedArguments {
	if _ := handler.dynamic { return converter.decode_raw_arguments(input)! }
	return converter.decode_arguments(input)!
}
