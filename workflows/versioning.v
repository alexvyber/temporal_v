module workflows

import temporal_v.sdk
import x.json2

struct VersionedWorkflow {
mut:
	counter     Counter
	information sdk.WorkflowInfo
}

fn new_versioned_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return VersionedWorkflow{}
}

fn (mut workflow VersionedWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	workflow.information = context.info()
	workflow.counter.on_event(event, mut context)!
}

fn (workflow &VersionedWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name == 'versioning' { return json2.encode(workflow.information) }
	return workflow.counter.query(name, arguments, view)!
}
