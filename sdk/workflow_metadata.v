module sdk

pub struct WorkflowInteractionDefinition {
pub:
	name        string
	description string
}

pub struct WorkflowInteractions {
pub:
	queries []WorkflowInteractionDefinition
	signals []WorkflowInteractionDefinition
	updates []WorkflowInteractionDefinition
}

// Descriptions are read-only metadata. Event handlers still own dispatch.
pub interface DescribedWorkflow {
	interaction_definitions() WorkflowInteractions
}

pub struct WorkflowDefinition {
pub:
	type_              string @[json: 'type']
	query_definitions  []WorkflowInteractionDefinition @[json: 'queryDefinitions']
	signal_definitions []WorkflowInteractionDefinition @[json: 'signalDefinitions']
	update_definitions []WorkflowInteractionDefinition @[json: 'updateDefinitions']
}

fn sorted_interactions(values []WorkflowInteractionDefinition) ![]WorkflowInteractionDefinition {
	mut sorted := values.clone()
	sorted.sort(a.name < b.name)
	mut seen := map[string]bool{}
	for value in sorted {
		if value.name in seen {
			return error('duplicate workflow interaction definition: ${value.name}')
		}
		seen[value.name] = true
	}
	return sorted
}

fn workflow_definition(run &Run) !WorkflowDefinition {
	mut interactions := WorkflowInteractions{}
	if run.workflow is DescribedWorkflow {
		interactions = run.workflow.interaction_definitions()
	} else {
		// Event-based workflows receive signals dynamically. A query interface
		// without declarations similarly represents a dynamic query handler.
		interactions = WorkflowInteractions{
			signals: [WorkflowInteractionDefinition{}]
		}
		if run.workflow is QueryableWorkflow {
			interactions = WorkflowInteractions{
				...interactions
				queries: [WorkflowInteractionDefinition{}]
			}
		}
	}
	mut query_names := []string{}
	mut declared_queries := false
	if run.workflow is NamedQueriesWorkflow {
		query_names = interaction_names(run.workflow.query_names(), 'query')!
		declared_queries = true
	}
	if run.workflow is DynamicQueriesWorkflow {
		query_names << ''
		declared_queries = true
	}
	if declared_queries {
		interactions = WorkflowInteractions{
			...interactions
			queries: described_interaction_names(if run.workflow is DescribedWorkflow {
				interactions.queries
			} else {
				[]WorkflowInteractionDefinition{}
			}, query_names, 'query')!
		}
	}
	mut signal_names := []string{}
	mut declared_signals := false
	if run.workflow is NamedSignalsWorkflow {
		signal_names = interaction_names(run.workflow.signal_names(), 'signal')!
		declared_signals = true
	}
	if run.workflow is DynamicSignalsWorkflow {
		signal_names << ''
		declared_signals = true
	}
	if declared_signals {
		interactions = WorkflowInteractions{
			...interactions
			signals: described_interaction_names(if run.workflow is DescribedWorkflow {
				interactions.signals
			} else {
				[]WorkflowInteractionDefinition{}
			}, signal_names, 'signal')!
		}
	}
	mut updates := []WorkflowInteractionDefinition{}
	names := registered_update_names(run.workflow)!
	if names.len > 0 {
		mut descriptions := map[string]string{}
		for definition in interactions.updates {
			if definition.name !in names {
				return error('description for an unregistered Update: ${definition.name}')
			}
			if definition.name in descriptions {
				return error('duplicate Update description: ${definition.name}')
			}
			descriptions[definition.name] = definition.description
		}
		for name in names {
			updates << WorkflowInteractionDefinition{
				name:        name
				description: descriptions[name]
			}
		}
	} else if interactions.updates.len > 0 {
		return error('workflow declares Update metadata without Update handlers')
	}
	return WorkflowDefinition{
		type_:              run.context.workflow_type
		query_definitions:  sorted_interactions(interactions.queries)!
		signal_definitions: sorted_interactions(interactions.signals)!
		update_definitions: sorted_interactions(updates)!
	}
}
