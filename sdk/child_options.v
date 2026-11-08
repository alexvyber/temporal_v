module sdk

import temporal_v.temporal
import time

pub enum ParentClosePolicy {
	terminate
	abandon
	request_cancel
}

pub enum ChildCancellation {
	abandon
	try_cancel
	wait_completed
	wait_requested
}

pub enum WorkflowIdReuse {
	allow_duplicate
	allow_duplicate_failed_only
	reject_duplicate
}

pub struct ChildOptions {
pub:
	event_groups        []EventGroup
	static_summary      string
	static_details      string
	namespace           string
	task_queue          string
	execution_timeout   time.Duration
	run_timeout         time.Duration
	task_timeout        time.Duration     = 10 * time.second
	parent_close_policy ParentClosePolicy = .request_cancel
	cancellation        ChildCancellation = .try_cancel
	scope               u32
	id_reuse            WorkflowIdReuse = .allow_duplicate
	retry               ?RetryPolicy
	cron_schedule       string
	headers             map[string]PayloadValue
	memo                map[string]PayloadValue
	search_attributes   ?temporal.TemporalApiCommonV1_SearchAttributes
	versioning_intent   temporal.CoresdkCommon_VersioningIntent
	versioning_override ?VersioningOverride
	priority            ?temporal.TemporalApiCommonV1_Priority
}

pub fn (converter DataConverter) encode_map(values map[string]PayloadValue) !map[string]temporal.TemporalApiCommonV1_Payload {
	mut result := map[string]temporal.TemporalApiCommonV1_Payload{}
	mut keys := values.keys()
	keys.sort()
	for key in keys {
		value := values[key] or { return error('payload map key missing') }
		result[key] = converter.encode_value(value)!
	}
	return result
}

fn (policy ParentClosePolicy) encode() temporal.CoresdkChildWorkflow_ParentClosePolicy {
	return match policy {
		.terminate { .parent_close_policy_terminate }
		.abandon { .parent_close_policy_abandon }
		.request_cancel { .parent_close_policy_request_cancel }
	}
}

fn (policy ChildCancellation) encode() temporal.CoresdkChildWorkflow_ChildWorkflowCancellationType {
	return match policy {
		.abandon { .abandon }
		.try_cancel { .try_cancel }
		.wait_completed { .wait_cancellation_completed }
		.wait_requested { .wait_cancellation_requested }
	}
}

fn (policy WorkflowIdReuse) encode() temporal.TemporalApiEnumsV1_WorkflowIdReusePolicy {
	return match policy {
		.allow_duplicate { .workflow_id_reuse_policy_allow_duplicate }
		.allow_duplicate_failed_only { .workflow_id_reuse_policy_allow_duplicate_failed_only }
		.reject_duplicate { .workflow_id_reuse_policy_reject_duplicate }
	}
}

pub fn (mut context WorkflowContext) child(workflow_type string, workflow_id string, arguments Arguments, options ChildOptions) !u32 {
	context.ensure_open()!
	if workflow_type == '' || workflow_id == '' || options.task_timeout <= 0
		|| options.run_timeout < 0 || options.execution_timeout < 0 {
		return error('invalid child workflow options')
	}
	context.ensure_scope(options.scope)!
	mut override := ?temporal.TemporalApiWorkflowV1_VersioningOverride(none)
	if value := options.versioning_override { override = versioning_override_proto(value)! }
	namespace := if options.namespace == '' { context.namespace } else { options.namespace }
	converter := context.converter.with_context(workflow_serialization_context(namespace,
		workflow_id))
	encoded := converter.encode_arguments(arguments)!
	metadata := converter.command_metadata(options.static_summary, options.static_details)!
	groups := context.command_event_groups(options.event_groups)!
	mut command := temporal.CoresdkWorkflowCommands_StartChildWorkflowExecution{
		namespace:                if options.namespace == '' {
			context.namespace
		} else {
			options.namespace
		}
		workflow_id:              workflow_id
		workflow_type:            workflow_type
		task_queue:               if options.task_queue == '' {
			context.queue
		} else {
			options.task_queue
		}
		input:                    encoded
		workflow_task_timeout:    duration_proto(options.task_timeout)!
		parent_close_policy:      options.parent_close_policy.encode()
		cancellation_type:        options.cancellation.encode()
		workflow_id_reuse_policy: options.id_reuse.encode()
		cron_schedule:            options.cron_schedule
		headers:                  converter.encode_headers(options.headers)!
		memo:                     converter.encode_map(options.memo)!
		search_attributes:        options.search_attributes
		versioning_intent:        options.versioning_intent
		versioning_override:      override
		priority:                 options.priority
	}
	if options.execution_timeout > 0 {
		command.workflow_execution_timeout = duration_proto(options.execution_timeout)!
	}
	if options.run_timeout > 0 {
		command.workflow_run_timeout = duration_proto(options.run_timeout)!
	}
	if retry := options.retry { command.retry_policy = retry.encode()! }
	context.next_child++
	sequence := context.next_child
	command.seq = sequence
	context.children[sequence] = true
	context.child_contexts[sequence] = converter.serialization_context
	context.child_scopes[sequence] = options.scope
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		user_metadata:       metadata.value
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_StartChildWorkflowExecution_Case{
			value: command
		}
	})
	return sequence
}

pub fn (mut context WorkflowContext) cancel_child(sequence u32, reason string, options EventGroupOptions) !bool {
	context.ensure_open()!
	if sequence !in context.children || context.child_cancellations[sequence] { return false }
	groups := if options.event_groups.len > 0 {
		context.command_event_groups(options.event_groups)!.markers
	} else {
		[]temporal.TemporalApiSdkV1_EventGroupMarker{}
	}
	context.child_cancellations[sequence] = true
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelChildWorkflowExecution_Case{
			value: temporal.CoresdkWorkflowCommands_CancelChildWorkflowExecution{
				child_workflow_seq: sequence
				reason:             reason
			}
		}
	})
	return true
}

pub struct ContinueOptions {
pub:
	event_groups        []EventGroup
	workflow_type       string
	task_queue          string
	run_timeout         ?time.Duration
	task_timeout        ?time.Duration
	retry               ?RetryPolicy
	headers             map[string]PayloadValue
	memo                map[string]PayloadValue
	search_attributes   ?temporal.TemporalApiCommonV1_SearchAttributes
	versioning_intent   temporal.CoresdkCommon_VersioningIntent
	versioning_behavior temporal.TemporalApiEnumsV1_ContinueAsNewVersioningBehavior
	backoff             time.Duration
}

pub fn (mut context WorkflowContext) continue_as_new(arguments Arguments, options ContinueOptions) ! {
	context.ensure_open()!
	if options.backoff < 0 { return error('negative continuation backoff') }
	groups := context.command_event_groups(options.event_groups)!
	encoded := context.converter.encode_arguments(arguments)!
	mut command := temporal.CoresdkWorkflowCommands_ContinueAsNewWorkflowExecution{
		workflow_type:               if options.workflow_type == '' {
			context.workflow_type
		} else {
			options.workflow_type
		}
		task_queue:                  if options.task_queue == '' {
			context.queue
		} else {
			options.task_queue
		}
		arguments:                   encoded
		workflow_run_timeout:        context.initialization.workflow_run_timeout
		workflow_task_timeout:       context.initialization.workflow_task_timeout
		headers:                     context.converter.encode_headers(options.headers)!
		memo:                        context.converter.encode_map(options.memo)!
		search_attributes:           options.search_attributes
		versioning_intent:           options.versioning_intent
		initial_versioning_behavior: options.versioning_behavior
	}
	if timeout := options.run_timeout { command.workflow_run_timeout = duration_proto(timeout)! }
	if timeout := options.task_timeout {
		if timeout <= 0 { return error('workflow task timeout must be positive') }
		command.workflow_task_timeout = duration_proto(timeout)!
	}
	if retry := options.retry { command.retry_policy = retry.encode()! }
	if options.backoff > 0 { command.backoff_start_interval = duration_proto(options.backoff)! }
	context.stop_pending(false, true)
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_ContinueAsNewWorkflowExecution_Case{
			value: command
		}
	})
	context.closed = true
}
