module sdk

import alexvyber.temporal_v.temporal
import time

pub struct LocalActivityOptions {
pub:
	event_groups                []EventGroup
	activity_id                 string
	summary                     string
	headers                     map[string]PayloadValue
	start_to_close              time.Duration = 10 * time.second
	schedule_to_close           time.Duration
	schedule_to_start           time.Duration
	local_retry_threshold       time.Duration = time.minute
	retry                       RetryPolicy
	cancellation                ActivityCancellation = .wait_completed
	include_arguments_in_marker bool
	scope                       u32
}

pub fn (mut context WorkflowContext) local_activity(name string, arguments Arguments, options LocalActivityOptions) !u32 {
	context.ensure_open()!
	if name == '' || options.start_to_close < 0 || options.schedule_to_close < 0
		|| options.schedule_to_start < 0 || options.local_retry_threshold <= 0
		|| (options.start_to_close == 0 && options.schedule_to_close == 0) {
		return error('invalid local Activity options')
	}
	context.ensure_scope(options.scope)!
	sequence := context.next_activity + 1
	activity_id := if options.activity_id == '' {
		'local-activity-${sequence}'
	} else {
		options.activity_id
	}
	converter := context.activity_converter(name, activity_id, context.queue, true)
	encoded := converter.encode_arguments(arguments)!
	metadata := converter.command_metadata(options.summary, '')!
	groups := context.command_event_groups(options.event_groups)!
	mut command := temporal.CoresdkWorkflowCommands_ScheduleLocalActivity{
		activity_type:               name
		arguments:                   encoded
		headers:                     converter.encode_headers(options.headers)!
		attempt:                     1
		retry_policy:                options.retry.encode()!
		local_retry_threshold:       duration_proto(options.local_retry_threshold)!
		cancellation_type:           options.cancellation.encode()
		include_arguments_in_marker: options.include_arguments_in_marker
	}
	if options.start_to_close > 0 {
		command.start_to_close_timeout = duration_proto(options.start_to_close)!
	}
	if options.schedule_to_close > 0 {
		command.schedule_to_close_timeout = duration_proto(options.schedule_to_close)!
	}
	if options.schedule_to_start > 0 {
		command.schedule_to_start_timeout = duration_proto(options.schedule_to_start)!
	}
	context.next_activity = sequence
	context.activity_contexts[sequence] = converter.serialization_context
	command.seq = sequence
	command.activity_id = activity_id
	context.activities[sequence] = true
	context.activity_scopes[sequence] = options.scope
	context.local_activities[sequence] = command
	context.local_activity_event_groups[sequence] = groups
	context.local_activity_context_values[sequence] = context.capture_context_values()
	context.local_activity_sequences[sequence] = sequence
	if metadata_value := metadata.value {
		context.local_activity_metadata[sequence] = metadata_value
	}
	context.schedule_local(command)
	return sequence
}

fn (mut context WorkflowContext) schedule_local(command temporal.CoresdkWorkflowCommands_ScheduleLocalActivity) {
	sequence := context.local_activity_sequences[command.seq] or { command.seq }
	previous := context.context_values
	context.context_values = context.local_activity_context_values[sequence]
	defer { context.context_values = previous }
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: context.local_activity_event_groups[sequence].markers
		user_metadata:       context.local_command_metadata(sequence)
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleLocalActivity_Case{
			value: command
		}
	})
}

fn (mut context WorkflowContext) local_backoff(sequence u32, backoff temporal.CoresdkActivityResult_DoBackoff) ! {
	if context.activity_cancellations[sequence] {
		context.activities.delete(sequence)
		context.activity_contexts.delete(sequence)
		context.activity_scopes.delete(sequence)
		context.activity_cancellations.delete(sequence)
		context.local_activities.delete(sequence)
		context.local_activity_metadata.delete(sequence)
		context.local_activity_event_groups.delete(sequence)
		context.local_activity_context_values.delete(sequence)
		context.deferred_events << Event{
			kind:     .activity_cancelled
			sequence: sequence
		}
		return
	}
	mut command := context.local_activities[sequence] or {
		return error('backoff for unknown local Activity')
	}
	duration := backoff.backoff_duration or {
		return error('missing local Activity backoff duration')
	}

	if duration.seconds < 0 || duration.nanos < 0 || duration.nanos >= 1000000000
		|| duration.seconds > 9223372035 {
		return error('invalid local Activity backoff duration')
	}
	command.attempt = backoff.attempt
	command.original_schedule_time = backoff.original_schedule_time
	context.local_activities[sequence] = command
	previous := context.context_values
	context.context_values = context.local_activity_context_values[sequence]
	defer { context.context_values = previous }
	timer := context.timer(duration.seconds * time.second + duration.nanos * time.nanosecond, TimerOptions{
		scope:        context.activity_scopes[sequence]
		event_groups: context.local_activity_event_groups[sequence].groups
	})!
	context.local_backoffs[timer] = sequence
}

pub fn (mut context WorkflowContext) cancel_activity(sequence u32) !bool {
	context.ensure_open()!
	if sequence !in context.activities || context.activity_cancellations[sequence] { return false }
	context.activity_cancellations[sequence] = true
	if sequence in context.local_activities {
		for timer, activity in context.local_backoffs {
			if activity != sequence {
				continue
			}
			context.cancel_timer(timer)!
			context.local_backoffs.delete(timer)
			context.local_activities.delete(sequence)
			context.local_activity_metadata.delete(sequence)
			context.local_activity_event_groups.delete(sequence)
			context.local_activity_context_values.delete(sequence)
			context.activities.delete(sequence)
			context.activity_contexts.delete(sequence)
			context.activity_scopes.delete(sequence)
			context.activity_cancellations.delete(sequence)
			context.deferred_events << Event{
				kind:     .activity_cancelled
				sequence: sequence
			}
			return true
		}
		context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
			variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelLocalActivity_Case{
				value: temporal.CoresdkWorkflowCommands_RequestCancelLocalActivity{
					seq: context.local_activities[sequence].seq
				}
			}
		})
	} else {
		context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
			variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelActivity_Case{
				value: temporal.CoresdkWorkflowCommands_RequestCancelActivity{
					seq: sequence
				}
			}
		})
	}
	return true
}
