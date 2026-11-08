module sdk

import encoding.base64
import x.json2
import temporal_v.temporal

struct WorkflowOperationToken {
	version      int = 1
	namespace    string
	workflow_id  string
	first_run_id string
}

// Attach a durable Temporal completion callback before acknowledging the async
// Nexus start. Handler process loss does not lose the eventual operation result.
pub fn (mut context NexusContext) start_workflow(original &Client, options WorkflowStart) !NexusResult {
	client := context.trace_client(original)!
	defer { client.close() }
	context.check_cancelled()!
	if context.async_started {
		return error('only one async operation can be started per Nexus invocation')
	}
	info := context.info()
	if info.request_id == '' || info.callback_url == '' {
		return error('workflow-backed Nexus operation requires request ID and completion callback')
	}
	mut links := options.links.clone()
	for link in info.links {
		if link.type_.starts_with(nexus_link_prefix) { links << common_link_from_nexus(link)! }
	}
	mut callbacks := options.completion_callbacks.clone()
	callbacks << temporal.TemporalApiCommonV1_Callback{
		links:   links
		variant: temporal.TemporalApiCommonV1_Callback_Nexus_Case{
			value: temporal.TemporalApiCommonV1_Callback_Nexus{
				url:    info.callback_url
				header: info.callback_headers
			}
		}
	}
	started := client.start(WorkflowStart{
		...options
		propagated_nexus_serialization_context: temporal.TemporalApiNexusV1_PropagatedSerializationContext{
			endpoint:  info.endpoint
			service:   info.service
			operation: info.operation
		}
		workflow_id:                            if options.workflow_id == '' {
			'nexus-' + info.request_id
		} else {
			options.workflow_id
		}
		request_id:                             info.request_id
		conflict_policy:                        .workflow_id_conflict_policy_use_existing
		completion_callbacks:                   callbacks
		links:                                  links
		on_conflict_options:                    temporal.TemporalApiWorkflowV1_OnConflictOptions{
			attach_completion_callbacks: true
			attach_request_id:           true
			attach_links:                true
		}
	})!
	context.async_started = true
	link := nexus_link_from_common(temporal.TemporalApiCommonV1_Link{
		variant: temporal.TemporalApiCommonV1_Link_WorkflowEvent_Case{
			value: temporal.TemporalApiCommonV1_Link_WorkflowEvent{
				namespace:   client.options.namespace
				workflow_id: started.workflow_id
				run_id:      started.run_id
				reference:   temporal.TemporalApiCommonV1_Link_WorkflowEvent_EventRef_Case{
					value: temporal.TemporalApiCommonV1_Link_WorkflowEvent_EventReference{
						event_id:   1
						event_type: .event_type_workflow_execution_started
					}
				}
			}
		}
	})!
	return NexusResult{
		links:           [link]
		operation_token: base64.url_encode_str(json2.encode(WorkflowOperationToken{
			namespace:    client.options.namespace
			workflow_id:  started.workflow_id
			first_run_id: started.run_id
		}))
	}
}

pub fn (mut context NexusContext) cancel_workflow(original &Client, operation_token string) ! {
	client := context.trace_client(original)!
	defer { client.close() }
	context.check_cancelled()!
	token := json2.decode[WorkflowOperationToken](base64.url_decode_str(operation_token))!
	if token.version != 1 || token.namespace != client.options.namespace || token.workflow_id == ''
		|| token.first_run_id == '' {
		return error('invalid workflow operation token')
	}
	client.rpc_request_cancel_workflow_execution(temporal.TemporalApiWorkflowserviceV1_RequestCancelWorkflowExecutionRequest{
		namespace:              token.namespace
		workflow_execution:     temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: token.workflow_id
		}
		first_execution_run_id: token.first_run_id
		reason:                 'Nexus operation cancellation'
		identity:               client.options.transport.identity
	})!
}
