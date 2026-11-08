module main

import temporal_v.sdk

struct VersioningValidationWorkflow {}

fn new_versioning_validation(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return VersioningValidationWorkflow{}
}

fn (mut workflow VersioningValidationWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {}

fn versioning_validation() ![]string {
	for deployment in [?sdk.WorkerDeployment(none),
		?sdk.WorkerDeployment(sdk.WorkerDeployment{
			name:           'disabled'
			use_versioning: false
		})] {
		mut runtime := sdk.Runtime{}
		runtime.register('versioned', new_versioning_validation, versioning_behavior: .pinned)!
		if _ := sdk.run_workflows(sdk.WorkerOptions{
			address:    'http://127.0.0.1:1'
			deployment: deployment
		}, runtime)
		{
			return error('invalid versioning configuration started a worker')
		} else {
			if !err.msg().contains('worker deployment versioning is disabled') {
				return error('versioning validation reached native connection setup: ${err.msg()}')
			}
		}
	}
	return [
		'Per-workflow versioning rejects absent/disabled deployment versioning before opening a native worker',
	]
}
