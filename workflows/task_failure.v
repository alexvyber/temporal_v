module workflows

import alexvyber.temporal_v.sdk

struct BlockedWorkflow {}

struct RepairedWorkflow {}

fn new_blocked_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return BlockedWorkflow{}
}

fn new_repaired_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RepairedWorkflow{}
}

fn (mut workflow BlockedWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	return sdk.WorkflowTaskError{
		message: 'fixture requires a repaired workflow deployment'
	}
}

fn (mut workflow RepairedWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started { context.complete('"repaired"')! }
}

struct BinaryDiagnosticWorkflow {
	application bool
}

fn new_binary_error(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return BinaryDiagnosticWorkflow{}
}

fn new_binary_application_error(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return BinaryDiagnosticWorkflow{
		application: true
	}
}

fn (mut workflow BinaryDiagnosticWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		bytes := [u8(255), 128].bytestr()
		context.log(.error, 'binary diagnostic ' + bytes, {
			'bytes': bytes
		})
		if workflow.application {
			return sdk.ApplicationError{
				message:       'binary diagnostic ' + bytes
				category:      'BinaryError' + bytes
				non_retryable: true
			}
		}
		return error('binary diagnostic ' + bytes)
	}
}
