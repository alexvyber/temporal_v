module workflows

import os
import alexvyber.temporal_v.sdk
import time

struct PatchWorkflow {
	mode string
}

fn new_patch_legacy(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return PatchWorkflow{
		mode: 'legacy'
	}
}

fn new_patch_original(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return PatchWorkflow{
		mode: 'patched'
	}
}

fn new_patch_deprecated(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return PatchWorkflow{
		mode: 'deprecated'
	}
}

fn new_patch_removed(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return PatchWorkflow{
		mode: 'removed'
	}
}

fn register_patch(mut runtime sdk.Runtime) ! {
	match os.getenv('V_SDK_EXAMPLE_PATCH_MODE') {
		'', 'patched' { runtime.register('v.patch', new_patch_original)! }
		'legacy' { runtime.register('v.patch', new_patch_legacy)! }
		'deprecated' { runtime.register('v.patch', new_patch_deprecated)! }
		'removed' { runtime.register('v.patch', new_patch_removed)! }
		else { return error('invalid example patch mode') }
	}
}

fn (mut workflow PatchWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			mut changed := workflow.mode == 'removed'
			if workflow.mode == 'patched' { changed = context.patched('example-version-1')! }
			if workflow.mode == 'deprecated' {
				context.deprecate_patch('example-version-1')!
				changed = true
			}
			if changed {
				context.activity('v.echo', '"patch"', sdk.ActivityOptions{})!
			} else {
				context.timer(time.second)!
			}
		}
		.activity_completed {
			context.timer(time.second)!
		}
		.timer_fired {
			context.complete('"compatible"')!
		}
		else {
			return error('unexpected patch event')
		}
	}
}
