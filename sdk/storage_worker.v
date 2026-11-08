module sdk

import temporal_v.temporal
import temporal_v.corebridge
import time

struct WorkflowStorageTargets {
	current StorageContext
	parent  ?StorageContext
}

fn workflow_storage_context(runtime &Runtime, activation temporal.CoresdkWorkflowActivation_WorkflowActivation, config WorkerOptions) WorkflowStorageTargets {
	mut initialization := temporal.CoresdkWorkflowActivation_InitializeWorkflow{}
	if run := runtime.runs[activation.run_id] { initialization = run.context.initialization }
	for job in activation.jobs {
		variant := job.variant or { continue }
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_InitializeWorkflow_Case {
			initialization = variant.value
		}
	}
	current := StorageContext{
		target: SerializationContext{
			kind:          .workflow
			namespace:     config.namespace
			workflow_id:   initialization.workflow_id
			workflow_type: initialization.workflow_type
		}
		run_id: activation.run_id
	}
	mut parent := ?StorageContext(none)
	if initialization.continued_from_execution_run_id == '' {
		if execution := initialization.parent_workflow_info {
			parent = storage_workflow_target(current, execution.namespace, execution.workflow_id,
				execution.run_id, '')
		}
	}
	return WorkflowStorageTargets{
		current: current
		parent:  parent
	}
}

fn storage_workflow_target(current StorageContext, namespace string, id string, run_id string, workflow_type string) StorageContext {
	return StorageContext{
		...current
		target: SerializationContext{
			kind:          .workflow
			namespace:     if namespace == '' {
				current.target.namespace
			} else {
				namespace
			}
			workflow_id:   id
			workflow_type: workflow_type
		}
		run_id: run_id
	}
}

fn (targets WorkflowStorageTargets) for_command(command temporal.CoresdkWorkflowCommands_WorkflowCommand) !StorageContext {
	variant := command.variant or { return error('storage command has no variant') }
	current := targets.current
	match variant {
		temporal.CoresdkWorkflowCommands_WorkflowCommand_StartChildWorkflowExecution_Case {
			child := variant.value
			return storage_workflow_target(current, child.namespace, child.workflow_id, '',
				child.workflow_type)
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_SignalExternalWorkflowExecution_Case {
			target := variant.value.target or { return error('signal command has no target') }
			match target {
				temporal.CoresdkWorkflowCommands_SignalExternalWorkflowExecution_WorkflowExecution_Case {
					return storage_workflow_target(current, target.value.namespace,
						target.value.workflow_id, target.value.run_id, '')
				}
				temporal.CoresdkWorkflowCommands_SignalExternalWorkflowExecution_ChildWorkflowId_Case {
					return storage_workflow_target(current, '', target.value, '', '')
				}
			}
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ContinueAsNewWorkflowExecution_Case {
			return storage_workflow_target(current, '', current.target.workflow_id, '', if variant.value.workflow_type == '' {
				current.target.workflow_type
			} else {
				variant.value.workflow_type
			})
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_CompleteWorkflowExecution_Case {
			if parent := targets.parent { return parent }
			return current
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_StartTimer_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleActivity_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_RespondToQuery_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelActivity_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelTimer_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_FailWorkflowExecution_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelWorkflowExecution_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_SetPatchMarker_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_RecordWorkflowMarker_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelChildWorkflowExecution_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelExternalWorkflowExecution_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelSignalWorkflow_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleLocalActivity_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelLocalActivity_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_UpsertWorkflowSearchAttributes_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ModifyWorkflowProperties_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_UpdateResponse_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleNexusOperation_Case,
		temporal.CoresdkWorkflowCommands_WorkflowCommand_RequestCancelNexusOperation_Case {
			return current
		}
	}
}

fn storage_activation_failure(run_id string, err IError) []u8 {
	return temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
		run_id: run_id
		status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Failed_Case{
			value: temporal.CoresdkWorkflowCompletion_Failure{
				failure:     failure(err.msg(), 'VExternalStorageError')
				force_cause: .workflow_task_failed_cause_external_storage_failure
			}
		}
	}.encode()
}

fn store_workflow_commands(commands []temporal.CoresdkWorkflowCommands_WorkflowCommand, activation temporal.CoresdkWorkflowActivation_WorkflowActivation, converter DataConverter, targets WorkflowStorageTargets) ![]temporal.CoresdkWorkflowCommands_WorkflowCommand {
	if converter.external_storage == none { return commands }
	context := targets.current
	mut metadata_queries := map[string]bool{}
	for job in activation.jobs {
		variant := job.variant or { continue }
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_QueryWorkflow_Case {
			if variant.value.query_type == '__temporal_workflow_metadata' {
				metadata_queries[variant.value.query_id] = true
			}
		}
	}
	mut result := []temporal.CoresdkWorkflowCommands_WorkflowCommand{}
	for command in commands {
		variant := command.variant or {
			result << command
			continue
		}

		if variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_RespondToQuery_Case {
			if variant.value.query_id in metadata_queries {
				result << command
				continue
			}
			// Legacy queries carry is_replaying=true, but their replies are sent.
			bytes := converter.storage_message(.workflow_command, command.encode(), true, context, corebridge.PayloadMessageOptions{}) or {
				result << failed_query(variant.value.query_id, err, DataConverter{})!
				continue
			}
			result << temporal.CoresdkWorkflowCommands_WorkflowCommand.decode(bytes)!
			continue
		}
		if activation.is_replaying
			|| variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleLocalActivity_Case {
			result << command
			continue
		}
		bytes := converter.storage_message(.workflow_command, command.encode(), true,
			targets.for_command(command)!, corebridge.PayloadMessageOptions{})!
		result << temporal.CoresdkWorkflowCommands_WorkflowCommand.decode(bytes)!
	}
	return result
}

fn storage_activity_failure(err IError) temporal.CoresdkActivityResult_ActivityExecutionResult {
	return temporal.CoresdkActivityResult_ActivityExecutionResult{
		status: temporal.CoresdkActivityResult_ActivityExecutionResult_Failed_Case{
			value: temporal.CoresdkActivityResult_Failure{
				failure: temporal.TemporalApiFailureV1_Failure{
					message:      err.msg()
					source:       'V'
					failure_info: temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case{
						value: temporal.TemporalApiFailureV1_ApplicationFailureInfo{
							type_:         'VExternalStorageError'
							non_retryable: false
						}
					}
				}
				cause:   .activity_task_failed_cause_external_storage_failure
			}
		}
	}
}

fn activity_storage_context(information ActivityInfo) StorageContext {
	mut deadline := time.Time{}
	if started := information.started_at {
		if information.start_to_close > 0 { deadline = started.add(information.start_to_close) }
	}
	return StorageContext{
		deadline: deadline
		target:   SerializationContext{
			kind:          if information.workflow_id == '' { .activity } else { .workflow }
			namespace:     information.workflow_namespace
			workflow_id:   information.workflow_id
			workflow_type: information.workflow_type
			activity_id:   information.activity_id
			activity_type: information.activity_type
		}
		run_id:   information.run_id
	}
}

fn (converter DataConverter) store_activity_result(token []u8, result temporal.CoresdkActivityResult_ActivityExecutionResult, context StorageContext) temporal.CoresdkActivityResult_ActivityExecutionResult {
	bytes := converter.storage_message(.activity_completion, temporal.Coresdk_ActivityTaskCompletion{
		task_token: token
		result:     result
	}.encode(), true, context, corebridge.PayloadMessageOptions{}) or {
		return storage_activity_failure(err)
	}
	completion := temporal.Coresdk_ActivityTaskCompletion.decode(bytes) or {
		return storage_activity_failure(err)
	}
	return completion.result or {
		return storage_activity_failure(error('Activity storage completion has no result'))
	}
}

fn execute_activity_handler(handler ActivityDispatch, mut context ActivityContext, arguments []string) !PayloadValue {
	return execute_traced_activity(handler, mut context, arguments)!
}

fn prepare_activity(handler ActivityDispatch, context ActivityContext, received_start temporal.CoresdkActivityTask_Start, results chan ActivityResult) {
	mut outcome := ActivityCompletionOutcome{
		result: ActivityResult{
			token: context.task_token
		}
	}
	prepare_activity_recovered(handler, context, received_start, mut outcome)
	results <- outcome.result
}

struct ActivityCompletionOutcome {
mut:
	result ActivityResult
}

fn prepare_activity_recovered(handler ActivityDispatch, context ActivityContext, received_start temporal.CoresdkActivityTask_Start, mut outcome ActivityCompletionOutcome) {
	defer { context.metrics.close() }
	$if v3_backend ? {
		defer {
			if message := recover() {
				// A failed application converter/storage callback cannot safely encode its own panic.
				outcome.result = ActivityResult{
					token:  context.task_token
					result: activity_panic_failure(message, recovery_stack_trace())
				}
			}
		}
	}
	outcome.result = prepare_activity_result(handler, context, received_start)
}

fn prepare_activity_result(handler ActivityDispatch, context ActivityContext, received_start temporal.CoresdkActivityTask_Start) ActivityResult {
	start := retrieve_activity_start(context, received_start) or {
		return rejected_prepared_activity(context, err)
	}
	input := decode_activity_arguments(handler, context.converter, start.input) or {
		return rejected_prepared_activity(context, err)
	}
	details := context.result_converter().decode_raw_arguments(start.heartbeat_details) or {
		return rejected_prepared_activity(context, activity_field_error(err, 'heartbeat details'))
	}
	headers := context.converter.decode_raw_headers(start.header_fields) or {
		return rejected_prepared_activity(context, activity_field_error(err, 'headers'))
	}
	prepared := ActivityContext{
		...context
		trace_headers:   start.header_fields.clone()
		argument_values: input.raw_values
		typed_arguments: input.typed_values
		information:     ActivityInfo{
			...context.information
			raw_heartbeat_details: details.raw_values
			raw_headers:           headers
		}
	}
	return execute_activity_result(handler, prepared, input.values)
}

fn rejected_prepared_activity(context ActivityContext, err IError) ActivityResult {
	result := if err is ExternalStorageError {
		storage_activity_failure(err)
	} else {
		activity_failure(err.msg(), context.result_converter())
	}
	return ActivityResult{
		token:  context.task_token
		result: context.converter.store_activity_result(context.task_token, result,
			activity_storage_context(context.information))
	}
}

fn retrieve_activity_start(context ActivityContext, start temporal.CoresdkActivityTask_Start) !temporal.CoresdkActivityTask_Start {
	task := temporal.CoresdkActivityTask_ActivityTask{
		task_token: context.task_token
		variant:    temporal.CoresdkActivityTask_ActivityTask_Start_Case{
			value: start
		}
	}
	bytes := context.converter.storage_message(.activity_task, task.encode(), false,
		activity_storage_context(context.information), corebridge.PayloadMessageOptions{})!
	decoded := temporal.CoresdkActivityTask_ActivityTask.decode(bytes)!
	variant := decoded.variant or { return error('missing prepared Activity task variant') }
	if variant is temporal.CoresdkActivityTask_ActivityTask_Start_Case { return variant.value }
	return error('prepared Activity task is not a start')
}

fn activity_field_error(err IError, field string) IError {
	if err is ExternalStorageError { return err }
	return error('${field} decode failed: ${err.msg()}')
}
