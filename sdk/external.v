module sdk

import alexvyber.temporal_v.temporal

pub struct ExternalWorkflow {
pub:
	workflow_id string
	run_id      string
	namespace   string
	scope       u32
}

fn (context &WorkflowContext) external_target(target ExternalWorkflow) !temporal.CoresdkCommon_NamespacedWorkflowExecution {
	if target.workflow_id == '' { return error('external workflow ID is required') }
	return temporal.CoresdkCommon_NamespacedWorkflowExecution{
		namespace:   if target.namespace == '' { context.namespace } else { target.namespace }
		workflow_id: target.workflow_id
		run_id:      target.run_id
	}
}

pub fn (mut context WorkflowContext) signal_external(target ExternalWorkflow, name string, arguments Arguments, options WorkflowSignalOptions) !u32 {
	context.ensure_open()!
	if name == '' { return error('signal name is required') }
	context.ensure_scope(target.scope)!
	execution := context.external_target(target)!
	converter := context.converter.with_context(workflow_serialization_context(execution.namespace,
		execution.workflow_id))
	encoded := converter.encode_arguments(arguments)!
	groups := context.command_event_groups(options.event_groups)!
	metadata := converter.command_metadata(options.summary, '')!
	context.next_external++
	sequence := context.next_external
	context.external_operations[sequence] = 'signal'
	context.external_scopes[sequence] = target.scope
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		user_metadata:       metadata.value
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_SignalExternalWorkflowExecution_Case{
			value: temporal.CoresdkWorkflowCommands_SignalExternalWorkflowExecution{
				seq:         sequence
				target:      temporal.CoresdkWorkflowCommands_SignalExternalWorkflowExecution_WorkflowExecution_Case{
					value: execution
				}
				signal_name: name
				args:        encoded
			}
		}
	})
	return sequence
}

pub fn (mut context WorkflowContext) cancel_external(target ExternalWorkflow, reason string, options EventGroupOptions) !u32 {
	context.ensure_open()!
	groups := context.command_event_groups(options.event_groups)!
	context.ensure_scope(target.scope)!
	execution := context.external_target(target)!
	context.next_external++
	sequence := context.next_external
	context.external_operations[sequence] = 'cancel'
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelExternalWorkflowExecution_Case{
			value: temporal.CoresdkWorkflowCommands_RequestCancelExternalWorkflowExecution{
				seq:                sequence
				workflow_execution: execution
				reason:             reason
			}
		}
	})
	return sequence
}

pub fn (mut context WorkflowContext) cancel_signal(sequence u32) !bool {
	context.ensure_open()!
	if context.external_operations[sequence] != 'signal' { return false }
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelSignalWorkflow_Case{
			value: temporal.CoresdkWorkflowCommands_CancelSignalWorkflow{
				seq: sequence
			}
		}
	})
	return true
}

@[params]
pub struct WorkflowSignalOptions {
pub:
	summary      string
	event_groups []EventGroup
}
