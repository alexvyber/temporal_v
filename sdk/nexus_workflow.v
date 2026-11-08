module sdk

import temporal
import time

pub enum NexusCancellation {
	wait_completed
	abandon
	try_cancel
	wait_requested
}

pub struct NexusOptions {
pub:
	event_groups      []EventGroup
	summary           string
	endpoint          string
	service           string
	operation         string
	schedule_to_close time.Duration = time.minute
	schedule_to_start time.Duration
	start_to_close    time.Duration
	headers           map[string]string
	scope             u32
	cancellation      NexusCancellation = .wait_completed
}

pub fn (mut context WorkflowContext) nexus_operation(input_value PayloadValue, options NexusOptions) !u32 {
	context.ensure_open()!
	if options.endpoint == '' || options.service == '' || options.operation == ''
		|| options.schedule_to_close <= 0 || options.schedule_to_start < 0
		|| options.start_to_close < 0 {
		return error('invalid Nexus operation options')
	}
	context.ensure_scope(options.scope)!
	converter := context.converter.with_context(nexus_serialization_context(options.endpoint,
		options.service, options.operation))
	argument := converter.encode_value(input_value)!
	metadata := converter.command_metadata(options.summary, '')!
	groups := context.command_event_groups(options.event_groups)!
	mut command := temporal.CoresdkWorkflowCommands_ScheduleNexusOperation{
		endpoint:                  options.endpoint
		service:                   options.service
		operation:                 options.operation
		input:                     argument
		schedule_to_close_timeout: duration_proto(options.schedule_to_close)!
		nexus_header:              options.headers
		cancellation_type:         match options.cancellation {
			.wait_completed { .wait_cancellation_completed }
			.abandon { .abandon }
			.try_cancel { .try_cancel }
			.wait_requested { .wait_cancellation_requested }
		}
	}
	if options.schedule_to_start > 0 {
		command.schedule_to_start_timeout = duration_proto(options.schedule_to_start)!
	}
	if options.start_to_close > 0 {
		command.start_to_close_timeout = duration_proto(options.start_to_close)!
	}
	context.next_nexus++
	sequence := context.next_nexus
	command.seq = sequence
	context.nexus_operations[sequence] = true
	context.nexus_contexts[sequence] = converter.serialization_context
	context.nexus_scopes[sequence] = options.scope
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		user_metadata:       metadata.value
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleNexusOperation_Case{
			value: command
		}
	})
	return sequence
}

pub fn (mut context WorkflowContext) cancel_nexus(sequence u32) !bool {
	context.ensure_open()!
	if sequence !in context.nexus_operations || context.nexus_cancellations[sequence] {
		return false
	}
	context.nexus_cancellations[sequence] = true
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelNexusOperation_Case{
			value: temporal.CoresdkWorkflowCommands_RequestCancelNexusOperation{
				seq: sequence
			}
		}
	})
	return true
}
