module sdk

import time

// Predicates must be deterministic and read-only. They run during replay.
pub type WorkflowCondition = fn (workflow Workflow, information WorkflowInfo) bool

@[params]
pub struct WaitConditionOptions {
pub:
	scope        u32
	timeout      time.Duration
	summary      string
	event_groups []EventGroup
}

struct PendingCondition {
	predicate WorkflowCondition
	options   WaitConditionOptions
	values    WorkflowContextValues
	groups    EventGroupState
mut:
	timer     u32
	timed_out bool
}

// Returns an event handle. The workflow receives condition_satisfied,
// condition_cancelled or condition_timed_out with this sequence.
pub fn (mut context WorkflowContext) wait_condition(predicate WorkflowCondition, options WaitConditionOptions) !u32 {
	context.ensure_open()!
	if options.timeout < 0 { return error('condition timeout must be nonnegative') }
	if options.scope != 0 && options.scope !in context.scopes {
		return error('unknown condition cancellation scope')
	}
	if context.next_condition == u32(0xffffffff) { return error('condition sequence exhausted') }
	groups := context.command_event_groups(options.event_groups)!
	context.next_condition++
	sequence := context.next_condition
	context.conditions[sequence] = PendingCondition{
		predicate: predicate
		options:   options
		values:    context.capture_context_values()
		groups:    groups
	}
	return sequence
}

// Explicit cancellation removes the wait even in a shielded scope.
pub fn (mut context WorkflowContext) cancel_condition(sequence u32) !bool {
	context.ensure_open()!
	condition := context.conditions[sequence] or { return false }
	context.conditions.delete(sequence)
	if condition.timer != 0 {
		context.condition_timers.delete(condition.timer)
		context.cancel_timer(condition.timer)!
	}
	context.deferred_condition_events << ConditionResolution{
		condition: condition
		event:     Event{
			kind:     .condition_cancelled
			sequence: sequence
			message:  'condition cancelled'
		}
	}
	return true
}

struct ConditionResolution {
	condition PendingCondition
	event     Event
}

fn (mut run Run) condition_ready(condition PendingCondition) bool {
	values := run.context.context_values
	groups := run.context.event_group_state
	run.context.context_values = condition.values
	run.context.event_group_state = condition.groups
	defer {
		run.context.context_values = values
		run.context.event_group_state = groups
	}
	return condition.predicate(run.workflow, run.context.info())
}

fn (mut run Run) condition_timer(condition PendingCondition) !u32 {
	values := run.context.context_values
	groups := run.context.event_group_state
	run.context.context_values = condition.values
	run.context.event_group_state = condition.groups
	defer {
		run.context.context_values = values
		run.context.event_group_state = groups
	}
	return run.context.timer(condition.options.timeout, TimerOptions{
		scope:   condition.options.scope
		summary: condition.options.summary
	})!
}

fn (mut run Run) resolve_condition(resolution ConditionResolution) ! {
	values := run.context.context_values
	groups := run.context.event_group_state
	run.context.context_values = resolution.condition.values
	run.context.event_group_state = resolution.condition.groups
	defer {
		run.context.context_values = values
		run.context.event_group_state = groups
	}
	if resolution.condition.timer != 0 {
		run.context.condition_timers.delete(resolution.condition.timer)
		run.context.cancel_timer(resolution.condition.timer)!
	}
	run.deliver(resolution.event)!
}

fn (mut run Run) poll_conditions() ! {
	if run.context.condition_polling || run.context.closed { return }
	run.context.condition_polling = true
	defer { run.context.condition_polling = false }
	for !run.context.closed {
		mut progress := false
		pending := run.context.deferred_condition_events.clone()
		run.context.deferred_condition_events.clear()
		for resolution in pending {
			if run.context.closed { return }
			run.resolve_condition(resolution)!
			progress = true
		}
		mut sequences := run.context.conditions.keys()
		sequences.sort()
		for sequence in sequences {
			if run.context.closed { return }
			mut condition := run.context.conditions[sequence] or { continue }
			ready := run.condition_ready(condition)
			cancelled := run.context.scope_cancelled(condition.options.scope)!
			if ready || cancelled || condition.timed_out {
				mut cancellation_message := ''
				if cancelled && !ready {
					cancellation_message = run.context.scope_cancel_reason(condition.options.scope)!
				}
				run.context.conditions.delete(sequence)
				kind := if ready {
					EventKind.condition_satisfied
				} else if cancelled {
					EventKind.condition_cancelled
				} else {
					EventKind.condition_timed_out
				}
				run.resolve_condition(ConditionResolution{
					condition: condition
					event:     Event{
						kind:     kind
						sequence: sequence
						message:  cancellation_message
					}
				})!
				progress = true
			} else if condition.timer == 0 && condition.options.timeout > 0 {
				condition.timer = run.condition_timer(condition)!
				run.context.condition_timers[condition.timer] = sequence
				run.context.conditions[sequence] = condition
			}
		}
		if !progress { break
		 }
	}
}
