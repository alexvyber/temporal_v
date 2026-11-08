module sdk

import temporal

pub struct WorkflowInfo {
pub:
	context_values                           WorkflowContextValues @[json: '-']
	current_task_deployment_version          ?temporal.CoresdkCommon_WorkerDeploymentVersion
	last_sdk_version                         string
	target_worker_deployment_version_changed bool

	execution_timeout               ?temporal.GoogleProtobuf_Duration
	run_timeout                     ?temporal.GoogleProtobuf_Duration
	task_timeout                    ?temporal.GoogleProtobuf_Duration
	start_time                      ?temporal.GoogleProtobuf_Timestamp
	execution_expiration_time       ?temporal.GoogleProtobuf_Timestamp
	parent_workflow                 ?temporal.CoresdkCommon_NamespacedWorkflowExecution
	root_workflow                   ?temporal.TemporalApiCommonV1_WorkflowExecution
	retry_policy                    ?temporal.TemporalApiCommonV1_RetryPolicy
	priority                        ?temporal.TemporalApiCommonV1_Priority
	original_execution_run_id       string
	continued_initiator             temporal.TemporalApiEnumsV1_ContinueAsNewInitiator
	workflow_id                     string
	run_id                          string
	workflow_type                   string
	namespace                       string
	task_queue                      string
	time_millis                     i64
	attempt                         int
	continued_from_run_id           string
	first_execution_run_id          string
	cron_schedule                   string
	cancellation_requested          bool
	history_length                  u32
	history_size_bytes              u64
	continue_as_new_suggested       bool
	suggest_continue_as_new_reasons []temporal.TemporalApiEnumsV1_SuggestContinueAsNewReason
}

pub fn (context &WorkflowContext) info() WorkflowInfo {
	return WorkflowInfo{
		context_values:                           context.capture_context_values()
		current_task_deployment_version:          owned_optional_proto[temporal.CoresdkCommon_WorkerDeploymentVersion](context.current_task_deployment_version)
		last_sdk_version:                         context.last_sdk_version
		target_worker_deployment_version_changed: context.target_worker_deployment_version_changed
		execution_timeout:                        owned_optional_proto[temporal.GoogleProtobuf_Duration](context.initialization.workflow_execution_timeout)
		run_timeout:                              owned_optional_proto[temporal.GoogleProtobuf_Duration](context.initialization.workflow_run_timeout)
		task_timeout:                             owned_optional_proto[temporal.GoogleProtobuf_Duration](context.initialization.workflow_task_timeout)
		start_time:                               owned_optional_proto[temporal.GoogleProtobuf_Timestamp](context.initialization.start_time)
		execution_expiration_time:                owned_optional_proto[temporal.GoogleProtobuf_Timestamp](context.initialization.workflow_execution_expiration_time)
		parent_workflow:                          owned_optional_proto[temporal.CoresdkCommon_NamespacedWorkflowExecution](context.initialization.parent_workflow_info)
		root_workflow:                            owned_optional_proto[temporal.TemporalApiCommonV1_WorkflowExecution](context.initialization.root_workflow)
		retry_policy:                             owned_optional_proto[temporal.TemporalApiCommonV1_RetryPolicy](context.initialization.retry_policy)
		priority:                                 owned_optional_proto[temporal.TemporalApiCommonV1_Priority](context.initialization.priority)
		original_execution_run_id:                if context.initialization.original_execution_run_id == '' {
			context.run_id
		} else {
			context.initialization.original_execution_run_id
		}
		continued_initiator:                      context.initialization.continued_initiator
		workflow_id:                              context.workflow_id
		run_id:                                   context.run_id
		workflow_type:                            context.workflow_type
		namespace:                                context.namespace
		task_queue:                               context.queue
		time_millis:                              context.now().unix_milli()
		attempt:                                  context.initialization.attempt
		continued_from_run_id:                    context.initialization.continued_from_execution_run_id
		first_execution_run_id:                   context.initialization.first_execution_run_id
		cron_schedule:                            context.initialization.cron_schedule
		cancellation_requested:                   context.cancellation_requested
		history_length:                           context.history_length
		history_size_bytes:                       context.history_size_bytes
		continue_as_new_suggested:                context.continue_as_new_suggested
		suggest_continue_as_new_reasons:          context.suggest_continue_as_new_reasons.clone()
	}
}

pub fn (mut context WorkflowContext) patched(id string, options EventGroupOptions) !bool {
	if id.starts_with(numeric_version_patch_prefix) {
		return error('patch ID uses the reserved numeric version namespace')
	}
	return context.patch_decision(id, true, options)!
}

fn (mut context WorkflowContext) patch_decision(id string, consult_policy bool, options EventGroupOptions) !bool {
	context.ensure_open()!
	if id == '' { return error('patch ID is required') }
	if decision := context.patch_decisions[id] { return decision }
	decision := if consult_policy {
		context.patch_activation_decision(id)
	} else {
		!context.replaying || context.known_patches[id]
	}
	groups := if decision {
		context.command_event_groups(options.event_groups)!.markers
	} else {
		[]temporal.TemporalApiSdkV1_EventGroupMarker{}
	}
	context.patch_decisions[id] = decision
	if decision {
		context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
			event_group_markers: groups
			variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_SetPatchMarker_Case{
				value: temporal.CoresdkWorkflowCommands_SetPatchMarker{
					patch_id: id
				}
			}
		})
	}
	return decision
}

pub fn (mut context WorkflowContext) deprecate_patch(id string, options EventGroupOptions) ! {
	if id.starts_with(numeric_version_patch_prefix) {
		return error('patch ID uses the reserved numeric version namespace')
	}
	context.ensure_open()!
	if id == '' { return error('patch ID is required') }
	if id in context.patch_decisions { return }
	if context.replaying && !context.known_patches[id] {
		context.patch_decisions[id] = false
		return
	}
	context.deprecate_patch_marker(id, options)!
}

fn (mut context WorkflowContext) deprecate_patch_marker(id string, options EventGroupOptions) ! {
	context.ensure_open()!
	if id == '' { return error('patch ID is required') }
	if context.deprecated_patches[id] { return }
	groups := context.command_event_groups(options.event_groups)!
	context.deprecated_patches[id] = true
	context.patch_decisions[id] = true
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_SetPatchMarker_Case{
			value: temporal.CoresdkWorkflowCommands_SetPatchMarker{
				patch_id:   id
				deprecated: true
			}
		}
	})
}

fn update_response(id string, response temporal.CoresdkWorkflowCommands_UpdateResponse_Response) temporal.CoresdkWorkflowCommands_WorkflowCommand {
	return temporal.CoresdkWorkflowCommands_WorkflowCommand{
		variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_UpdateResponse_Case{
			value: temporal.CoresdkWorkflowCommands_UpdateResponse{
				protocol_instance_id: id
				response:             response
			}
		}
	}
}

pub fn (mut context WorkflowContext) complete_update(id string, result_value PayloadValue) ! {
	context.ensure_writable()!
	if id !in context.updates { return error('unknown pending update') }
	result := context.update_converter(id).encode_value(result_value)!
	context.queue_command(update_response(id, temporal.CoresdkWorkflowCommands_UpdateResponse_Completed_Case{
		value: result
	}))
	context.updates.delete(id)
}

pub fn (mut context WorkflowContext) fail_update(id string, err IError) ! {
	context.ensure_writable()!
	if id !in context.updates { return error('unknown pending update') }
	context.queue_command(update_response(id, temporal.CoresdkWorkflowCommands_UpdateResponse_Rejected_Case{
		value: context.update_converter(id).encode_failure(err, 'VUpdateError')!
	}))
	context.updates.delete(id)
}

fn validate_update(initial_run &Run, request temporal.CoresdkWorkflowActivation_DoUpdate, data []string, raw_values []RawValue, typed_values []PayloadValue) ! {
	mut run := initial_run
	mut guard := run.context.command_guard
	previous := guard.reason
	guard.reason = 'Update validator'
	defer { guard.reason = previous }
	span := run.context.inbound_trace_span('ValidateUpdate:' + request.name, request.headers)
	defer {
		if value := span { value.finish() }
	}
	validate_update_handler(run, request, data, raw_values, typed_values, span) or {
		if failed_span := span {
			if tracing := run.context.tracing {
				if tracing.record_errors {
					failed_span.record_exception('UpdateValidationError', err) or {}
					failed_span.set_status(.error, err.msg()) or {}
				}
			}
		}
		return err
	}
}

fn validate_update_handler(run &Run, request temporal.CoresdkWorkflowActivation_DoUpdate, data []string, raw_values []RawValue, typed_values []PayloadValue, span ?&TraceSpan) ! {
	names := registered_update_names(run.workflow)!
	if request.name !in names && '' !in names { return error('unknown update: ' + request.name) }
	if request.run_validator {
		UpdateValidatorNext{
			context_values:  run.context.capture_context_values()
			propagation:     run.context.inbound_propagation(request.headers, span)
			run:             run
			request_headers: request.headers
			argument_values: raw_values
			typed_arguments: typed_values
		}.execute(request.name, data)!
	}
}

fn (mut run Run) update(request temporal.CoresdkWorkflowActivation_DoUpdate) ! {
	result_converter :=
		run.context.converter.with_result_context(request.propagated_nexus_serialization_context)
	decoded := run.context.decode_update_arguments(run.workflow, request.name, request.input) or {
		run.context.queue_command(update_response(request.protocol_instance_id, temporal.CoresdkWorkflowCommands_UpdateResponse_Rejected_Case{
			value: result_converter.encode_failure(err, 'VUpdateError')!
		}))
		return
	}
	data := decoded.values
	validate_update(run, request, data, decoded.raw_values, decoded.typed_values) or {
		if err is WorkflowTaskError { return err }
		run.context.queue_command(update_response(request.protocol_instance_id, temporal.CoresdkWorkflowCommands_UpdateResponse_Rejected_Case{
			value: result_converter.encode_failure(err, 'VUpdateError')!
		}))
		return
	}
	policy := update_unfinished_policy(run.workflow, request.name)
	run.context.queue_command(update_response(request.protocol_instance_id, temporal.CoresdkWorkflowCommands_UpdateResponse_Accepted_Case{
		value: temporal.GoogleProtobuf_Empty{}
	}))
	run.context.updates[request.protocol_instance_id] = PendingWorkflowUpdate{
		result_context: result_converter.serialization_context
		name:           request.name
		id:             request.id
		policy:         policy
	}
	run.invoke(Event{
		kind:            .update_received
		raw_arguments:   decoded.raw_values
		typed_arguments: decoded.typed_values
		id:              request.protocol_instance_id
		name:            request.name
		data:            event_data(data)
		arguments:       data
		headers:         request.headers.clone()
	}) or {
		if err is WorkflowTaskError || err is PanicError { return err }
		run.context.fail_update(request.protocol_instance_id, err)!
	}
}

fn (context &WorkflowContext) update_converter(id string) DataConverter {
	if pending := context.updates[id] {
		return context.converter.with_context(pending.result_context)
	}
	return context.converter
}

pub fn (context &WorkflowContext) update_result_serialization_context(id string) !SerializationContext {
	if id !in context.updates { return error('unknown pending update') }
	return context.update_converter(id).serialization_context
}
