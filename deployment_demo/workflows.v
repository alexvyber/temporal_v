module main

import sdk
import json2

struct ControlInput {
	mode            string
	phase           int
	deployment_name string
}

struct ControlWorkflow {}

fn new_control_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ControlWorkflow{}
}

fn (mut workflow ControlWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			input := json2.decode[ControlInput](event.data)!
			if input.mode == 'child' {
				context.child('v.version-child', context.info().workflow_id + '-child', 'null', sdk.ChildOptions{
					versioning_override: sdk.PinnedVersioningOverride{
						version: sdk.WorkerDeploymentVersion{
							deployment_name: input.deployment_name
							build_id:        'build-b'
						}
					}
				})!
			} else if input.mode == 'continue' && input.phase == 0 {
				context.continue_as_new(json2.encode(ControlInput{ ...input, phase: 1 }), sdk.ContinueOptions{})!
			} else {
				context.complete('"continued"')!
			}
		}
		.child_completed {
			context.complete(event.data)!
		}
		.child_failed {
			return error('versioned child failed: ' + event.data)
		}
		else {}
	}
}

fn (workflow &ControlWorkflow) query(name string, args []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return 'null'
}

struct ImmediateWorkflow {}

fn new_immediate_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ImmediateWorkflow{}
}

fn (mut workflow ImmediateWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started { context.complete('"child done"')! }
}

fn (workflow &ImmediateWorkflow) query(name string, args []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return 'null'
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.version-control', new_control_workflow, versioning_behavior: .pinned)!
	runtime.register('v.version-child', new_immediate_workflow, versioning_behavior: .pinned)!
	return runtime
}

fn serve_control() ! {
	sdk.run_workflows(sdk.options_from_env()!, definitions()!)!
}
