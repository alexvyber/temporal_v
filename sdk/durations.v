module sdk

import temporal_v.temporal
import time

fn duration_proto(duration time.Duration) !temporal.GoogleProtobuf_Duration {
	if duration < 0 { return error('duration cannot be negative') }
	return temporal.GoogleProtobuf_Duration{
		seconds: i64(duration) / i64(time.second)
		nanos:   int(i64(duration) % i64(time.second))
	}
}

@[params]
pub struct TimerOptions {
pub:
	event_groups []EventGroup
	scope        u32
	summary      string
}

pub fn (mut context WorkflowContext) timer(duration time.Duration, options TimerOptions) !u32 {
	context.ensure_open()!
	if duration <= 0 { return error('timer must be positive') }
	context.ensure_scope(options.scope)!
	encoded := duration_proto(duration)!
	metadata := context.converter.command_metadata(options.summary, '')!
	groups := context.command_event_groups(options.event_groups)!
	context.next_timer++
	sequence := context.next_timer
	context.timers[sequence] = true
	context.timer_scopes[sequence] = options.scope
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		user_metadata:       metadata.value
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_StartTimer_Case{
			value: temporal.CoresdkWorkflowCommands_StartTimer{
				seq:                   sequence
				start_to_fire_timeout: encoded
			}
		}
	})
	return sequence
}

// Returns false if the timer has already fired or was already cancelled.
pub fn (mut context WorkflowContext) cancel_timer(sequence u32) !bool {
	context.ensure_open()!
	if sequence !in context.timers { return false }
	context.timers.delete(sequence)
	context.timer_scopes.delete(sequence)
	context.cancelled_timers[sequence] = true
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelTimer_Case{
			value: temporal.CoresdkWorkflowCommands_CancelTimer{
				seq: sequence
			}
		}
	})
	return true
}
