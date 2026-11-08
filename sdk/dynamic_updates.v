module sdk

import temporal_v.temporal

// Dynamic validation receives owned, codec-decoded raw arguments. A named
// handler always takes priority, including its configured payload conversion.
pub interface DynamicUpdatesWorkflow {
	validate_dynamic_update(name string, arguments []RawValue, view WorkflowContextView) !
}

fn registered_update_names(workflow Workflow) ![]string {
	mut names := []string{}
	if workflow is UpdatableWorkflow {
		for name in workflow.update_names() {
			if name == '' || name in names {
				return error('invalid or duplicate named Update registration')
			}
			names << name
		}
	}
	if workflow is DynamicUpdatesWorkflow { names << '' }
	return names
}

fn named_update(workflow Workflow, name string) bool {
	if workflow is UpdatableWorkflow { return name in workflow.update_names() }
	return false
}

fn (context &WorkflowContext) decode_update_arguments(workflow Workflow, name string, input []temporal.TemporalApiCommonV1_Payload) !DecodedArguments {
	if named_update(workflow, name) { return context.converter.decode_arguments(input)! }
	if workflow is DynamicUpdatesWorkflow {
		return context.converter.decode_raw_arguments(input)!
	}
	return context.converter.decode_arguments(input)!
}
