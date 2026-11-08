module main

import sdk
import os

struct StorageParent {}

fn new_storage_parent(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return StorageParent{}
}

fn (mut workflow StorageParent) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		context.child('storage.child', context.info().workflow_id + '-child', event.data, sdk.ChildOptions{})!
	}
	if event.kind == .child_completed { context.complete(event.data)! }
}

struct StorageChild {}

fn new_storage_child(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return StorageChild{}
}

fn (mut workflow StorageChild) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started { context.complete(event.data)! }
}

struct StorageContinue {}

fn new_storage_continue(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return StorageContinue{}
}

fn (mut workflow StorageContinue) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind != .started { return }
	if context.info().continued_from_run_id == '' {
		context.continue_as_new(event.data, sdk.ContinueOptions{})!
	} else {
		context.complete(event.data)!
	}
}

struct StorageSender {
mut:
	data string
}

fn new_storage_sender(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return StorageSender{}
}

fn (mut workflow StorageSender) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		workflow.data = event.data
		context.signal_external(sdk.ExternalWorkflow{
			workflow_id: context.info().workflow_id + '-receiver'
		}, 'deliver', event.data)!
	}
	if event.kind == .external_signal_completed { context.complete(workflow.data)! }
}

struct StorageReceiver {}

fn new_storage_receiver(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return StorageReceiver{}
}

fn (mut workflow StorageReceiver) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .signal_received { context.complete(event.data)! }
}

fn storage_workflow_type() string {
	return match os.getenv('V_SDK_EXAMPLE_STORAGE_TARGETS') {
		'child' {
			'storage.parent'
		}
		'continue' {
			'storage.continue'
		}
		'signal' {
			'storage.sender'
		}
		else {
			if os.getenv('V_SDK_EXAMPLE_STORAGE_LOCAL') == 'true' {
				'storage.local'
			} else {
				'storage'
			}
		}
	}
}
