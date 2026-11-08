module main

import alexvyber.temporal_v.sdk
import x.json2
import os
import time

fn condition_true(workflow sdk.Workflow, info sdk.WorkflowInfo) bool {
	return true
}

fn condition_false(workflow sdk.Workflow, info sdk.WorkflowInfo) bool {
	return false
}

fn condition_counter(workflow sdk.Workflow, info sdk.WorkflowInfo) bool {
	if workflow is ConditionsWorkflow { return workflow.counter >= 2 }
	return false
}

fn condition_cascade(workflow sdk.Workflow, info sdk.WorkflowInfo) bool {
	if workflow is ConditionsWorkflow { return 'immediate' in workflow.results }
	return false
}

fn condition_shield(workflow sdk.Workflow, info sdk.WorkflowInfo) bool {
	if workflow is ConditionsWorkflow { return workflow.shield_ready }
	return false
}

fn condition_captured(workflow sdk.Workflow, info sdk.WorkflowInfo) bool {
	request := info.context_value[ContextRequest]('request') or {
		panic('condition predicate lost its context value')
	}
	if request.label != 'condition' || request.workflow_id != info.workflow_id {
		panic('condition predicate crossed context')
	}
	return condition_counter(workflow, info)
}

fn condition_panic(workflow sdk.Workflow, info sdk.WorkflowInfo) bool {
	if os.getenv('V_CONDITION_PANIC_FIXED') != 'true' {
		panic('workflow condition predicate panic')
	}
	return true
}

struct ConditionCapture {
mut:
	sequence u32
}

fn condition_group_scope(mut context sdk.WorkflowContext) ! {
	mut capture := context.context_value[&ConditionCapture]('condition-capture') or {
		return error('missing condition capture')
	}
	capture.sequence = context.wait_condition(condition_captured, sdk.WaitConditionOptions{})!
}

fn condition_value_scope(mut context sdk.WorkflowContext) ! {
	context.with_event_group(labeled_group('condition')!, condition_group_scope)!
}

fn condition_capture_scope(mut context sdk.WorkflowContext) ! {
	context.with_context_value('request', ContextRequest{context.info().workflow_id, 'condition'},
		condition_value_scope)!
}

struct ConditionsWorkflow {
mut:
	mode         string
	counter      int
	shield_ready bool
	cancelled    bool
	scope        u32
	late         u32
	ids          map[u32]string
	results      map[string]string
}

fn conditions_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ConditionsWorkflow{}
}

fn (mut workflow ConditionsWorkflow) remember(mut context sdk.WorkflowContext, name string, predicate sdk.WorkflowCondition, options sdk.WaitConditionOptions) !u32 {
	sequence := context.wait_condition(predicate, options)!
	workflow.ids[sequence] = name
	return sequence
}

fn (mut workflow ConditionsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		workflow.mode = json2.decode[map[string]string](event.data)!['mode']
		if workflow.mode == 'panic' {
			workflow.remember(mut context, 'panic', condition_panic, sdk.WaitConditionOptions{})!
			return
		}
		if workflow.mode == 'abandon' {
			workflow.remember(mut context, 'abandoned', condition_false, sdk.WaitConditionOptions{
				timeout: time.hour
			})!
			context.complete('"abandoned-ok"')!
			return
		}
		if workflow.mode == 'plain' {
			workflow.remember(mut context, 'plain', condition_true, sdk.WaitConditionOptions{})!
			return
		}
		if workflow.mode == 'failure' {
			workflow.remember(mut context, 'failure', condition_true, sdk.WaitConditionOptions{})!
			return
		}
		mut invalid := false
		context.wait_condition(condition_false, sdk.WaitConditionOptions{ timeout: -time.second }) or {
			invalid = true
		}
		if !invalid { return error('negative condition timeout accepted') }
		invalid = false
		context.wait_condition(condition_false, sdk.WaitConditionOptions{ scope: 4294967294 }) or {
			invalid = true
		}
		if !invalid { return error('unknown condition scope accepted') }
		workflow.remember(mut context, 'immediate', condition_true, sdk.WaitConditionOptions{})!
		workflow.remember(mut context, 'counter', condition_counter, sdk.WaitConditionOptions{})!
		workflow.remember(mut context, 'timeout', condition_false, sdk.WaitConditionOptions{
			timeout:      5 * time.second
			summary:      'condition-timeout'
			event_groups: [labeled_group('condition-timeout')!]
		})!
		workflow.scope = context.cancellation_scope(sdk.ScopeOptions{})!
		workflow.remember(mut context, 'scope', condition_false, sdk.WaitConditionOptions{
			scope: workflow.scope
		})!
		invalid = false
		context.release_scope(workflow.scope) or { invalid = true }
		if !invalid { return error('released scope with pending condition') }
		shield := context.cancellation_scope(sdk.ScopeOptions{ shielded: true })!
		workflow.remember(mut context, 'shield', condition_shield, sdk.WaitConditionOptions{
			scope: shield
		})!
		explicit :=
			workflow.remember(mut context, 'explicit', condition_false, sdk.WaitConditionOptions{})!
		if !context.cancel_condition(explicit)! || context.cancel_condition(explicit)! {
			return error('condition cancellation is not idempotent')
		}
		cancelled := context.cancellation_scope(sdk.ScopeOptions{})!
		context.cancel_scope(cancelled)!
		workflow.remember(mut context, 'ready-over-cancel', condition_true, sdk.WaitConditionOptions{
			scope: cancelled
		})!
		mut capture := &ConditionCapture{}
		context.with_context_value('condition-capture', capture, condition_capture_scope)!
		workflow.ids[capture.sequence] = 'captured'
		workflow.remember(mut context, 'root', condition_false, sdk.WaitConditionOptions{})!
		workflow.late = workflow.remember(mut context, 'late', condition_false, sdk.WaitConditionOptions{
			timeout:      time.hour
			summary:      'late-condition'
			event_groups: [labeled_group('late-condition')!]
		})!
	}
	if event.kind == .signal_received {
		match event.name {
			'counter' {
				workflow.counter++
			}
			'cancel-scope' {
				context.cancel_scope(workflow.scope, reason: 'condition scope cause')!
				if context.cancel_scope(workflow.scope, reason: 'replacement cause')! {
					return error('scope cancelled twice')
				}
				if context.scope_cancel_reason(workflow.scope)! != 'condition scope cause' {
					return error('first cancellation cause overwritten')
				}
				descendant :=
					context.cancellation_scope(sdk.ScopeOptions{ parent: workflow.scope })!
				if !context.scope_cancelled(descendant)!
					|| context.scope_cancel_reason(descendant)! != 'condition scope cause' {
					return error('descendant lost cancellation cause')
				}
				context.release_scope(descendant)!
				context.cancel_condition(workflow.late)!
			}
			'shield' {
				workflow.shield_ready = true
			}
			else {
				return error('unknown condition signal')
			}
		}
	}
	if event.kind == .cancel_requested { workflow.cancelled = true }
	if event.kind == .timer_fired { return error('internal condition timer reached user handler') }
	if event.kind in [.condition_satisfied, .condition_cancelled, .condition_timed_out] {
		name := workflow.ids[event.sequence] or { return error('unknown condition resolution') }
		if name in workflow.results { return error('condition resolved twice') }
		workflow.results[name] = event.kind.str()
		if name == 'scope' && event.message != 'condition scope cause' {
			return error('scope cancellation reason lost')
		}
		if name == 'root' && event.message != 'requested by V client' {
			return error('root cancellation reason lost')
		}
		if name == 'plain' {
			context.complete('"plain-ok"')!
			return
		}
		if name == 'failure' {
			return sdk.ApplicationError{
				message:       'condition handler failed'
				category:      'ConditionFailure'
				non_retryable: true
			}
		}
		if name == 'panic' {
			context.complete('"predicate-repaired"')!
			return
		}
		if name == 'immediate' {
			workflow.remember(mut context, 'cascade', condition_cascade, sdk.WaitConditionOptions{})!
		}
		if name == 'scope' { context.release_scope(workflow.scope)! }
		if name == 'captured' {
			context_expect(context, 'condition')!
			if context.current_event_groups().map(it.id()) != ['condition'] {
				return error('condition continuation lost Event Group scope')
			}
			context.timer(time.hour, sdk.TimerOptions{ summary: 'condition-continuation' })!
		}
	}
	if workflow.results.len == 11 && workflow.cancelled {
		context.complete(json2.encode(workflow.results))!
	}
}

fn (workflow &ConditionsWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.results))
}
