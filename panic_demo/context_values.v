module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import x.json2
import os
import time

struct ContextRequest {
	workflow_id string
	label       string
}

struct ContextCapture {
mut:
	values sdk.WorkflowContextValues
}

fn context_expect(context &sdk.WorkflowContext, label string) ! {
	request := context.context_value[ContextRequest]('request') or {
		return error('missing request context: ' + label)
	}
	if request.label != label || request.workflow_id != context.info().workflow_id {
		return error('context value crossed workflow or scope')
	}
}

fn context_wrong_type(mut context sdk.WorkflowContext) ! {
	if context.context_value[ContextRequest]('request') != none {
		return error('wrong type downcast succeeded')
	}
	value := context.context_value[string]('request') or {
		return error('shadowed context missing')
	}
	if value != 'wrong-type' { return error('wrong shadow value') }
	context.timer(time.hour, sdk.TimerOptions{ summary: 'wrong-type' })!
	return error('expected scope error')
}

fn context_inner(mut context sdk.WorkflowContext) ! {
	context_expect(context, 'inner')!
	if (context.context_value[int]('count') or { -1 }) != 7 {
		return error('nested scope lost another key')
	}
	context.timer(time.hour, sdk.TimerOptions{ summary: 'inner' })!
}

fn context_count(mut context sdk.WorkflowContext) ! {
	context_expect(context, 'outer')!
	context.timer(time.hour, sdk.TimerOptions{ summary: 'outer-count' })!
	context.with_context_value('request', ContextRequest{context.info().workflow_id, 'inner'},
		context_inner)!
	context_expect(context, 'outer')!
	context.timer(time.hour, sdk.TimerOptions{ summary: 'restored-inner' })!
	context.with_context_value('request', 'wrong-type', context_wrong_type) or {
		if err.msg() != 'expected scope error' { return err }
	}
	context_expect(context, 'outer')!
	context.timer(time.hour, sdk.TimerOptions{ summary: 'restored-error' })!
	mut capture := context.context_value[&ContextCapture]('capture') or {
		return error('missing capture box')
	}
	capture.values = context.capture_context_values()
	context.activity('v.event-group.echo', '"context-remote"', sdk.ActivityOptions{})!
	context.local_activity('v.event-group.echo', '"retry"', sdk.LocalActivityOptions{
		local_retry_threshold: time.millisecond
		retry:                 sdk.RetryPolicy{
			maximum_attempts: 2
			initial_interval: 5 * time.second
		}
	})!
}

fn context_outer(mut context sdk.WorkflowContext) ! {
	context.with_context_value('count', 7, context_count)!
}

fn context_bootstrap(mut context sdk.WorkflowContext) ! {
	context.with_context_value('request', ContextRequest{context.info().workflow_id, 'outer'},
		context_outer)!
}

fn context_resume(mut context sdk.WorkflowContext) ! {
	context_expect(context, 'outer')!
	context.timer(time.hour, sdk.TimerOptions{ summary: 'captured-resume' })!
}

fn context_never(mut context sdk.WorkflowContext) ! {
	panic('empty context key callback ran')
}

fn context_panic(mut context sdk.WorkflowContext) ! {
	context_expect(context, 'panic')!
	if os.getenv('V_CONTEXT_PANIC_FIXED') != 'true' { panic('scoped context panic') }
}

struct ContextValuesWorkflow {
mut:
	captured  sdk.WorkflowContextValues
	completed int
}

fn context_values_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ContextValuesWorkflow{}
}

fn (mut workflow ContextValuesWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if context.context_value[ContextRequest]('request') != none {
		return error('context escaped callback')
	}
	if event.kind == .started {
		input := json2.decode[map[string]string](event.data)!
		if input['mode'] == 'panic' {
			context.with_context_value('request', ContextRequest{context.info().workflow_id, 'panic'},
				context_panic)!
			if context.context_value[ContextRequest]('request') != none {
				return error('panic scope leaked')
			}
			context.complete('"panic-repaired"')!
			return
		}
		mut rejected := false
		context.with_context_value('', 'invalid', context_never) or { rejected = true }
		if !rejected { return error('empty context key accepted') }
		mut capture := &ContextCapture{}
		context.with_context_value('capture', capture, context_bootstrap)!
		workflow.captured = capture.values
		if context.context_value[int]('count') != none
			|| context.context_value[&ContextCapture]('capture') != none {
			return error('context bindings leaked')
		}
		context.timer(time.hour, sdk.TimerOptions{ summary: 'outside' })!
	}
	if event.kind == .activity_completed { workflow.completed++ }
	if event.kind in [.activity_failed, .activity_cancelled] {
		return error('context fixture Activity failed')
	}
	if event.kind == .signal_received {
		context.with_context_values(workflow.captured, context_resume)!
		if context.context_value[ContextRequest]('request') != none {
			return error('captured context leaked')
		}
		context.timer(time.hour, sdk.TimerOptions{ summary: 'after-resume' })!
		context.complete('"context-ok"')!
	}
}

fn (workflow &ContextValuesWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode({
		'ready': (workflow.completed == 2).str()
	}))
}

struct ContextValuesCommandInterceptor {}

fn (interceptor ContextValuesCommandInterceptor) intercept(command temporal.CoresdkWorkflowCommands_WorkflowCommand, info sdk.WorkflowInfo, next sdk.CommandNext) !temporal.CoresdkWorkflowCommands_WorkflowCommand {
	if info.workflow_type != 'v.context-values' { return next.execute(command)! }
	variant := command.variant or { return next.execute(command)! }
	if variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_StartTimer_Case {
		sequence := variant.value.seq
		if sequence in [1, 2, 3, 5, 7, 8] {
			request := info.context_value[ContextRequest]('request') or {
				return error('queued timer lost context: ' + sequence.str())
			}
			expected := if sequence == 2 { 'inner' } else { 'outer' }
			if request.label != expected || request.workflow_id != info.workflow_id || (info.context_value[int]('count') or {
				-1
			}) != 7 {
				return error('wrong queued timer context: ' + sequence.str())
			}
		} else if sequence == 4 {
			if info.context_value[ContextRequest]('request') != none || (info.context_value[string]('request') or {
				''
			}) != 'wrong-type' {
				return error('wrong-type queued context lost')
			}
		} else if info.context_value[ContextRequest]('request') != none {
			return error('outside command inherited a context: ' + sequence.str())
		}
	}
	if variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleActivity_Case
		|| variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleLocalActivity_Case {
		request := info.context_value[ContextRequest]('request') or {
			return error('queued Activity lost context')
		}
		if request.label != 'outer' || request.workflow_id != info.workflow_id {
			return error('wrong queued Activity context')
		}
	}
	// Runtime values must never become serialized WorkflowInfo fields.
	if json2.encode(info).contains('ContextCapture')
		|| json2.encode(info).contains('context_values') {
		return error('runtime context serialized')
	}
	return next.execute(command)!
}
