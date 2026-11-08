module workflows

import sdk
import json2
import crypto.sha256
import example_activities

struct FailureChainWorkflow {
	local bool
}

fn new_remote_failure_chain(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return FailureChainWorkflow{}
}

fn new_local_failure_chain(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return FailureChainWorkflow{
		local: true
	}
}

fn (mut workflow FailureChainWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		if workflow.local {
			context.local_activity('v.nested-error', sdk.Arguments(sdk.TypedArguments{}), sdk.LocalActivityOptions{
				retry: sdk.RetryPolicy{
					maximum_attempts: 1
				}
			})!
		} else {
			context.activity('v.nested-error', sdk.Arguments(sdk.TypedArguments{}), sdk.ActivityOptions{
				retry: sdk.RetryPolicy{
					maximum_attempts: 1
				}
			})!
		}
	} else if event.kind == .activity_failed {
		failure := event.failure or { return error('missing structured Activity failure') }
		context.upsert_memo({
			'captured_failure_hash': json2.encode(sha256.hexhash(failure.protobuf.bytestr()))
		})!
		return failure
	}
}

struct FailureUpdateWorkflow {}

fn new_failure_update_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return FailureUpdateWorkflow{}
}

fn (workflow &FailureUpdateWorkflow) update_names() []string {
	return ['nested', 'rejected']
}

fn (workflow &FailureUpdateWorkflow) validate_update(name string, arguments []string, view sdk.WorkflowContextView) ! {
	if name == 'rejected' { return example_activities.nested_application_error() }
}

fn (mut workflow FailureUpdateWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .update_received { return example_activities.nested_application_error() }
	if event.kind == .signal_received { context.complete('null')! }
}
