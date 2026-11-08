module workflows

import temporal_v.sdk

struct ArgumentsWorkflow {
	local bool
mut:
	initial string
	value   string
	ready   bool
}

fn new_remote_arguments(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ArgumentsWorkflow{}
}

fn new_local_arguments(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ArgumentsWorkflow{
		local: true
	}
}

fn require_three(arguments []string) ! {
	if arguments.len != 3 { return error('expected three independent payloads') }
}

fn (mut workflow ArgumentsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			require_three(event.arguments)!
			if event.data != '' {
				return error('multiple inputs were repacked into a single value')
			}
			workflow.initial = event.arguments[1]
			workflow.value = event.arguments[1]
			if workflow.local {
				context.local_activity('v.arguments', sdk.Arguments(event.arguments), sdk.LocalActivityOptions{})!
			} else {
				context.activity('v.arguments', sdk.Arguments(event.arguments), sdk.ActivityOptions{})!
			}
		}
		.activity_completed {
			if event.data != workflow.initial {
				return error('Activity changed binary argument bytes')
			}
			workflow.ready = true
		}
		.signal_received {
			if event.name == 'finish' {
				if event.arguments.len != 0 || !workflow.ready {
					return error('finish requires no arguments and a completed Activity')
				}
				context.complete(workflow.value)!
			} else {
				require_three(event.arguments)!
				workflow.value = event.arguments[1]
			}
		}
		.update_received {
			require_three(event.arguments)!
			workflow.value = event.arguments[1]
			context.complete_update(event.id, workflow.value)!
		}
		.activity_failed {
			return error(event.message)
		}
		else {
			return error('unexpected arguments event')
		}
	}
}

fn (workflow &ArgumentsWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name == 'echo' {
		require_three(arguments)!
		return arguments[1]
	}
	if arguments.len != 0 { return error('state/ready query requires no arguments') }
	if name == 'ready' {
		return if workflow.ready { 'yes' } else { 'no' }
	}
	if name == 'state' { return workflow.value }
	return error('unknown arguments query')
}

fn (workflow &ArgumentsWorkflow) update_names() []string {
	return ['replace']
}

fn (workflow &ArgumentsWorkflow) validate_update(name string, arguments []string, view sdk.WorkflowContextView) ! {
	require_three(arguments)!
}

struct ArgumentsEcho {}

fn new_arguments_echo(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ArgumentsEcho{}
}

fn (mut workflow ArgumentsEcho) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		require_three(event.arguments)!
		context.complete(event.arguments[1])!
	}
}

struct ArgumentsZero {}

fn new_arguments_zero(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ArgumentsZero{}
}

fn (mut workflow ArgumentsZero) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		if event.arguments.len != 0 { return error('default start synthesized an argument') }
		context.complete('zero')!
	}
}

struct ArgumentsParent {}

fn new_arguments_parent(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ArgumentsParent{}
}

fn (mut workflow ArgumentsParent) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		context.child('v.arguments.echo', context.info().workflow_id + '-child',
			sdk.Arguments(event.arguments), sdk.ChildOptions{})!
	} else if event.kind == .child_completed {
		context.complete(event.data)!
	} else if event.kind == .child_failed {
		return error(event.message)
	}
}

struct ArgumentsContinue {}

fn new_arguments_continue(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ArgumentsContinue{}
}

fn (mut workflow ArgumentsContinue) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		require_three(event.arguments)!
		if event.arguments[0] == 'continue' {
			context.continue_as_new(sdk.Arguments(['done', event.arguments[1], event.arguments[2]]), sdk.ContinueOptions{})!
		} else {
			context.complete(event.arguments[1])!
		}
	}
}
