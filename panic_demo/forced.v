module main

import sdk
import os
import json2
import time

struct ForcedWorkflow {
mut:
	mode string
}

fn forced_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ForcedWorkflow{}
}

fn request_forced_failure(mode string, mut context sdk.WorkflowContext) ! {
	if os.getenv('V_FORCED_FIXED') == 'true' { return }
	context.timer(time.second)!
	if mode == 'after-close' { context.complete(sdk.PayloadValue('"uncommitted"'))! }
	context.force_task_fail(sdk.ApplicationError{
		message:       'forced task failure'
		category:      'ForceTaskOnly'
		non_retryable: true
	})
	context.force_task_fail(error('second force request must not replace the first'))
	if mode == 'panic' { panic('forced callback panic') }
	if mode == 'error' {
		return sdk.ApplicationError{
			message:  'terminal callback error'
			category: 'MustNotFailExecution'
		}
	}
	if mode != 'after-close' { context.complete(sdk.PayloadValue('"uncommitted"'))! }
}

fn (mut workflow ForcedWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		input := json2.decode[map[string]string](event.arguments[0])!
		workflow.mode = input['mode']
		if workflow.mode !in ['signal', 'update'] {
			request_forced_failure(workflow.mode, mut context)!
			if os.getenv('V_FORCED_FIXED') == 'true' {
				context.complete(sdk.PayloadValue('"repaired"'))!
			}
		}
	}
	if event.kind == .signal_received {
		request_forced_failure('signal', mut context)!
		if os.getenv('V_FORCED_FIXED') == 'true' {
			context.complete(sdk.PayloadValue('"repaired"'))!
		}
	}
	if event.kind == .update_received {
		request_forced_failure('update', mut context)!
		if os.getenv('V_FORCED_FIXED') == 'true' {
			context.complete_update(event.id, sdk.PayloadValue('"repaired"'))!
			context.complete(sdk.PayloadValue('"repaired"'))!
		}
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (workflow ForcedWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.mode))
}

fn (workflow ForcedWorkflow) update_names() []string {
	return ['force']
}

fn (workflow ForcedWorkflow) validate_update(name string, input []string, view sdk.WorkflowContextView) ! {}
