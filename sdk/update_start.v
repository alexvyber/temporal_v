module sdk

import alexvyber.temporal_v.temporal
import time

pub enum UpdateStage {
	unspecified
	admitted
	accepted
	completed
}

pub struct UpdateRpcError {
pub:
	message     string
	status      int
	workflow    WorkflowHandle
	update_id   string
	rpc_failure ?RpcError
}

pub fn (err UpdateRpcError) msg() string {
	return err.message
}

pub fn (err UpdateRpcError) code() int {
	return err.status
}

fn (client &Client) update_request(handle WorkflowHandle, options UpdateRequest, id string) !temporal.TemporalApiWorkflowserviceV1_UpdateWorkflowExecutionRequest {
	if options.name == '' { return error('update name is required') }
	if options.wait_for !in [.accepted, .completed] {
		return error('Updates must wait for accepted or completed stage')
	}
	converter := client.workflow_converter(handle.workflow_id)
	encoded := converter.encode_arguments(options.arguments)!
	return temporal.TemporalApiWorkflowserviceV1_UpdateWorkflowExecutionRequest{
		namespace:              client.options.namespace
		first_execution_run_id: options.first_execution_run_id
		workflow_execution:     execution(handle)!
		wait_policy:            temporal.TemporalApiUpdateV1_WaitPolicy{
			lifecycle_stage: if options.wait_for == .completed {
				.update_workflow_execution_lifecycle_stage_completed
			} else {
				.update_workflow_execution_lifecycle_stage_accepted
			}
		}
		request:                temporal.TemporalApiUpdateV1_Request{
			request_id:                             options.request_id
			completion_callbacks:                   options.completion_callbacks
			links:                                  options.links
			propagated_nexus_serialization_context: options.propagated_nexus_serialization_context
			meta:                                   temporal.TemporalApiUpdateV1_Meta{
				update_id: id
				identity:  client.options.transport.identity
			}
			input:                                  temporal.TemporalApiUpdateV1_Input{
				name:   options.name
				header: temporal.TemporalApiCommonV1_Header{
					fields: converter.encode_headers(options.headers)!
				}
				args:   temporal.TemporalApiCommonV1_Payloads{
					payloads: encoded
				}
			}
		}
	}
}

fn (client &Client) update_handle(response temporal.TemporalApiWorkflowserviceV1_UpdateWorkflowExecutionResponse) !UpdateHandle {
	ref := response.update_ref or { return error('update response has no reference') }
	run := ref.workflow_execution or { return error('update reference has no execution') }
	converter :=
		client.workflow_converter(run.workflow_id).with_result_context(response.propagated_nexus_serialization_context)
	mut outcome := ?UpdateResult(none)
	if immediate := response.outcome {
		outcome = update_outcome(immediate, converter)!
	}
	stage := match response.stage {
		.update_workflow_execution_lifecycle_stage_unspecified { UpdateStage.unspecified }
		.update_workflow_execution_lifecycle_stage_admitted { UpdateStage.admitted }
		.update_workflow_execution_lifecycle_stage_accepted { UpdateStage.accepted }
		.update_workflow_execution_lifecycle_stage_completed { UpdateStage.completed }
	}

	return UpdateHandle{
		link:                         response.link
		result_serialization_context: converter.serialization_context
		workflow:                     WorkflowHandle{
			workflow_id: run.workflow_id
			run_id:      run.run_id
		}
		id:                           ref.update_id
		outcome:                      outcome
		stage:                        stage
	}
}

pub fn (client &Client) start_update(handle WorkflowHandle, options UpdateRequest) !UpdateHandle {
	id := if options.id == '' { random_request_id()! } else { options.id }
	request := client.update_request(handle, options, id)!
	timeout := if options.wait_timeout_millis > 0 {
		options.wait_timeout_millis
	} else {
		client.options.rpc_timeout_millis
	}
	watch := time.new_stopwatch()
	for {
		remaining := i64(timeout) - watch.elapsed().milliseconds()
		if remaining <= 0 {
			return UpdateRpcError{
				message:   'Update deadline exceeded'
				status:    4
				workflow:  handle
				update_id: id
			}
		}
		bytes := client.call_with_timeout(.workflow, 'UpdateWorkflowExecution', request.encode(),
			u32(remaining)) or {
			return UpdateRpcError{
				message:     err.msg()
				status:      err.code()
				workflow:    handle
				update_id:   id
				rpc_failure: if err is RpcError { ?RpcError(RpcError{
						...err
					}) } else { none }
			}
		}
		response :=
			client.update_handle(temporal.TemporalApiWorkflowserviceV1_UpdateWorkflowExecutionResponse.decode(bytes)!)!
		if int(response.stage) >= int(options.wait_for) || response.outcome != none {
			return response
		}
	}
	return error('unreachable Update state')
}

pub fn (client &Client) update_with_start(start WorkflowStart, options UpdateRequest) !UpdateHandle {
	if start.request_eager_execution {
		return error('Update-with-start does not support eager execution')
	}
	if start.cron_schedule != '' || start.start_delay > 0 {
		return error('Update-with-start does not support cron or start delay')
	}
	id := if options.id == '' { random_request_id()! } else { options.id }
	handle := WorkflowHandle{
		workflow_id: start.workflow_id
	}
	request := temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest{
		namespace:   client.options.namespace
		resource_id: start.workflow_id
		operations:  [
			temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation{
				operation: temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_StartWorkflow_Case{
					value: client.start_request(start)!
				}
			},
			temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation{
				operation: temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_UpdateWorkflow_Case{
					value: client.update_request(handle, options, id)!
				}
			},
		]
	}
	timeout := if options.wait_timeout_millis > 0 {
		options.wait_timeout_millis
	} else {
		client.options.rpc_timeout_millis
	}
	watch := time.new_stopwatch()
	for {
		remaining := i64(timeout) - watch.elapsed().milliseconds()
		if remaining <= 0 {
			return UpdateRpcError{
				message:   'Update-with-start deadline exceeded'
				status:    4
				workflow:  handle
				update_id: id
			}
		}
		bytes := client.call_with_timeout(.workflow, 'ExecuteMultiOperation', request.encode(),
			u32(remaining)) or {
			return UpdateRpcError{
				message:     err.msg()
				status:      err.code()
				workflow:    handle
				update_id:   id
				rpc_failure: if err is RpcError { ?RpcError(RpcError{
						...err
					}) } else { none }
			}
		}
		response :=
			temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationResponse.decode(bytes)!
		if response.responses.len != 2 {
			return error('unexpected Update-with-start response count')
		}
		operation := response.responses[1].response or {
			return error('missing Update-with-start outcome')
		}

		if operation is temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationResponse_Response_UpdateWorkflow_Case {
			result := client.update_handle(operation.value)!
			if int(result.stage) >= int(options.wait_for) || result.outcome != none {
				return result
			}
		} else {
			return error('wrong Update-with-start response operation')
		}
	}
	return error('unreachable Update-with-start state')
}
