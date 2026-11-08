module workflows

import json2
import sdk
import time

struct Parent {}

fn new_parent(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return Parent{}
}

fn (mut parent Parent) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			context.child('v.restart', context.info().workflow_id + '-child', event.data, sdk.ChildOptions{})!
		}
		.child_completed {
			context.complete(event.data)!
		}
		.child_failed {
			return error('child failed: ${event.message}')
		}
		.child_cancelled {
			return error('child cancelled')
		}
		.child_started {}
		else {
			return error('unexpected parent event')
		}
	}
}

struct ChainInput {
	remaining int
	total     int
}

struct Chain {
mut:
	input ChainInput
}

fn new_chain(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return Chain{}
}

fn (mut chain Chain) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		chain.input = json2.decode[ChainInput](event.data)!
		if chain.input.remaining < 0 || chain.input.remaining > 10 {
			return error('remaining must be 0–10')
		}
		if !context.patched('chain-v1')! { return error('expected recorded patch') }
		context.timer(2 * time.second)!
		return
	}
	if event.kind != .timer_fired { return error('unexpected chain event') }
	input := chain.input
	if input.remaining > 0 {
		context.continue_as_new(json2.encode(ChainInput{
			remaining: input.remaining - 1
			total:     input.total + 1
		}), sdk.ContinueOptions{})!
	} else {
		context.complete(json2.encode(input.total))!
	}
}
