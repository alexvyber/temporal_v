module workflows

import temporal_v.sdk
import x.json2

struct ContextParentInput {
	namespace string
}

struct ContextFailureParent {}

fn new_context_failure_parent(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ContextFailureParent{}
}

fn (mut workflow ContextFailureParent) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[ContextParentInput](event.data)!
			context.child('v.failure-chain.remote', context.info().workflow_id + '-child',
				sdk.Arguments(sdk.TypedArguments{}), sdk.ChildOptions{ namespace: input.namespace })!
		}
		.child_failed {
			return event.failure or { return error('missing child failure') }
		}
		.child_started {}
		else {
			return error('unexpected contextual parent event')
		}
	}
}
