module sdk

import alexvyber.temporal_v.temporal

pub struct ActivityTaskToken {
pub:
	result_serialization_context ?SerializationContext
	serialization_context        ?SerializationContext
	value                        []u8
}

pub struct ActivityExecutionIdentifier {
pub:
	result_serialization_context ?SerializationContext
	serialization_context        ?SerializationContext
	workflow                     WorkflowHandle
	activity_id                  string
}

pub type ActivityIdentifier = ActivityTaskToken
	| ActivityExecutionIdentifier
	| StandaloneActivityHandle

fn validate_activity_identifier(identifier ActivityIdentifier) ! {
	match identifier {
		ActivityTaskToken {
			if identifier.value.len == 0 { return error('Activity task token is empty') }
		}
		StandaloneActivityHandle {
			validate_standalone_activity(identifier)!
		}
		ActivityExecutionIdentifier {
			execution(identifier.workflow)!
			if identifier.activity_id == '' { return error('Activity ID is required') }
		}
	}
}

// The application must persist the token before returning from its handler.
// Local Activities cannot be completed asynchronously.
pub fn (mut context ActivityContext) defer_completion() !ActivityTaskToken {
	context.check_cancelled()!
	if context.information.is_local {
		return error('local Activities cannot complete asynchronously')
	}
	if context.deferred { return error('Activity completion already deferred') }
	context.deferred = true
	return ActivityTaskToken{
		result_serialization_context: context.result_serialization_context()
		value:                        context.task_token.clone()
		serialization_context:        context.converter.serialization_context
	}
}

pub struct ActivityHeartbeatStatus {
pub:
	cancel_requested bool
	paused           bool
	reset            bool
}

pub fn (client &Client) heartbeat_activity(identifier ActivityIdentifier, details Arguments) !ActivityHeartbeatStatus {
	validate_activity_identifier(identifier)!
	converter := client.activity_completion_converter(identifier)!
	encoded := temporal.TemporalApiCommonV1_Payloads{
		payloads: converter.encode_arguments(details)!
	}
	match identifier {
		ActivityTaskToken {
			response := client.rpc_record_activity_task_heartbeat(temporal.TemporalApiWorkflowserviceV1_RecordActivityTaskHeartbeatRequest{
				namespace:  client.options.namespace
				task_token: identifier.value
				details:    encoded
				identity:   client.options.transport.identity
			})!
			return ActivityHeartbeatStatus{
				cancel_requested: response.cancel_requested
				paused:           response.activity_paused
				reset:            response.activity_reset
			}
		}
		else {
			address := activity_execution_address(identifier)!
			response := client.rpc_record_activity_task_heartbeat_by_id(temporal.TemporalApiWorkflowserviceV1_RecordActivityTaskHeartbeatByIdRequest{
				namespace:   client.options.namespace
				resource_id: address.resource_id
				workflow_id: address.workflow_id
				run_id:      address.run_id
				activity_id: address.activity_id
				details:     encoded
				identity:    client.options.transport.identity
			})!
			return ActivityHeartbeatStatus{
				cancel_requested: response.cancel_requested
				paused:           response.activity_paused
				reset:            response.activity_reset
			}
		}
	}
}

pub fn (client &Client) complete_activity(identifier ActivityIdentifier, result PayloadValue) ! {
	validate_activity_identifier(identifier)!
	converter := client.activity_completion_converter(identifier)!
	encoded := temporal.TemporalApiCommonV1_Payloads{
		payloads: converter.encode_payload_values([result])!
	}
	match identifier {
		ActivityTaskToken {
			client.rpc_respond_activity_task_completed(temporal.TemporalApiWorkflowserviceV1_RespondActivityTaskCompletedRequest{
				namespace:  client.options.namespace
				task_token: identifier.value
				result:     encoded
				identity:   client.options.transport.identity
			})!
		}
		else {
			address := activity_execution_address(identifier)!
			client.rpc_respond_activity_task_completed_by_id(temporal.TemporalApiWorkflowserviceV1_RespondActivityTaskCompletedByIdRequest{
				namespace:   client.options.namespace
				resource_id: address.resource_id
				workflow_id: address.workflow_id
				run_id:      address.run_id
				activity_id: address.activity_id
				result:      encoded
				identity:    client.options.transport.identity
			})!
		}
	}
}

pub fn (client &Client) fail_activity(identifier ActivityIdentifier, failure IError, last_heartbeat_details Arguments) ! {
	validate_activity_identifier(identifier)!
	converter := client.activity_completion_converter(identifier)!
	encoded := converter.encode_failure(failure, 'VActivityError')!
	details := temporal.TemporalApiCommonV1_Payloads{
		payloads: converter.encode_arguments(last_heartbeat_details)!
	}
	match identifier {
		ActivityTaskToken {
			client.rpc_respond_activity_task_failed(temporal.TemporalApiWorkflowserviceV1_RespondActivityTaskFailedRequest{
				namespace:              client.options.namespace
				task_token:             identifier.value
				failure:                encoded
				last_heartbeat_details: details
				identity:               client.options.transport.identity
			})!
		}
		else {
			address := activity_execution_address(identifier)!
			client.rpc_respond_activity_task_failed_by_id(temporal.TemporalApiWorkflowserviceV1_RespondActivityTaskFailedByIdRequest{
				namespace:              client.options.namespace
				resource_id:            address.resource_id
				workflow_id:            address.workflow_id
				run_id:                 address.run_id
				activity_id:            address.activity_id
				failure:                encoded
				last_heartbeat_details: details
				identity:               client.options.transport.identity
			})!
		}
	}
}

pub fn (client &Client) report_activity_cancelled(identifier ActivityIdentifier, details Arguments) ! {
	validate_activity_identifier(identifier)!
	converter := client.activity_completion_converter(identifier)!
	encoded := temporal.TemporalApiCommonV1_Payloads{
		payloads: converter.encode_arguments(details)!
	}
	match identifier {
		ActivityTaskToken {
			client.rpc_respond_activity_task_canceled(temporal.TemporalApiWorkflowserviceV1_RespondActivityTaskCanceledRequest{
				namespace:  client.options.namespace
				task_token: identifier.value
				details:    encoded
				identity:   client.options.transport.identity
			})!
		}
		else {
			address := activity_execution_address(identifier)!
			client.rpc_respond_activity_task_canceled_by_id(temporal.TemporalApiWorkflowserviceV1_RespondActivityTaskCanceledByIdRequest{
				namespace:   client.options.namespace
				resource_id: address.resource_id
				workflow_id: address.workflow_id
				run_id:      address.run_id
				activity_id: address.activity_id
				details:     encoded
				identity:    client.options.transport.identity
			})!
		}
	}
}

fn (client &Client) activity_completion_converter(identifier ActivityIdentifier) !DataConverter {
	match identifier {
		StandaloneActivityHandle {
			raw := client.rpc_describe_activity_execution(temporal.TemporalApiWorkflowserviceV1_DescribeActivityExecutionRequest{
				namespace:   client.options.namespace
				activity_id: identifier.activity_id
				run_id:      identifier.run_id
			})!
			info := raw.info or { return error('Activity completion has no execution information') }
			return client.standalone_converter(identifier.activity_id).with_result_context(info.propagated_nexus_serialization_context)
		}
		ActivityTaskToken {
			if scope := identifier.result_serialization_context {
				if scope.kind !in [.activity, .nexus] {
					return error('Activity result context must be Activity or Nexus scope')
				}
				return client.options.data_converter.with_context(scope)
			}
			if context := identifier.serialization_context {
				if context.kind != .activity {
					return error('Activity token has a non-Activity serialization context')
				}
				return client.options.data_converter.with_context(context)
			}
			return client.options.data_converter
		}
		ActivityExecutionIdentifier {
			if scope := identifier.result_serialization_context {
				if scope.kind !in [.activity, .nexus] {
					return error('Activity result context must be Activity or Nexus scope')
				}
				return client.options.data_converter.with_context(scope)
			}
			if context := identifier.serialization_context {
				if context.kind != .activity {
					return error('Activity identifier has a non-Activity serialization context')
				}
				return client.options.data_converter.with_context(context)
			}
			return client.options.data_converter.with_context(SerializationContext{
				kind:        .activity
				namespace:   client.options.namespace
				workflow_id: identifier.workflow.workflow_id
				activity_id: identifier.activity_id
			})
		}
	}
}

struct ActivityExecutionAddress {
	workflow_id string
	run_id      string
	activity_id string
	resource_id string
}

fn activity_execution_address(identifier ActivityIdentifier) !ActivityExecutionAddress {
	match identifier {
		ActivityExecutionIdentifier {
			return ActivityExecutionAddress{
				workflow_id: identifier.workflow.workflow_id
				run_id:      identifier.workflow.run_id
				activity_id: identifier.activity_id
				resource_id: 'workflow:' + identifier.workflow.workflow_id
			}
		}
		StandaloneActivityHandle {
			return ActivityExecutionAddress{
				run_id:      identifier.run_id
				activity_id: identifier.activity_id
				resource_id: 'activity:' + identifier.activity_id
			}
		}
		ActivityTaskToken {
			return error('task token has no execution address')
		}
	}
}
