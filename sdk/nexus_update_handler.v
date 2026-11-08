module sdk

import temporal
import json2
import encoding.base64

struct UpdateOperationToken {
	version     int    = 1
	kind        string = 'update'
	namespace   string
	workflow_id string
	run_id      string
	update_id   string
}

// Attach callbacks before acknowledging acceptance. The server delivers the
// eventual Update result independently of the Nexus handler's process lifetime.
pub fn (mut context NexusContext) start_update(original &Client, workflow WorkflowHandle, options UpdateRequest) !NexusResult {
	client := context.trace_client(original)!
	defer { client.close() }
	context.check_cancelled()!
	if context.async_started {
		return error('only one async operation can be started per Nexus invocation')
	}
	info := context.info()
	if info.request_id == '' || info.callback_url == '' || workflow.workflow_id == '' {
		return error('update-backed Nexus operation requires workflow ID, request ID and completion callback')
	}
	if options.wait_for != .accepted {
		return error('update-backed Nexus operation must wait for acceptance')
	}
	id := if options.id == '' { info.request_id } else { options.id }
	mut links := options.links.clone()
	for link in info.links {
		if link.type_.starts_with(nexus_link_prefix) { links << common_link_from_nexus(link)! }
	}
	mut headers := info.callback_headers.clone()
	headers['Nexus-Operation-Token'] = base64.url_encode_str(json2.encode(UpdateOperationToken{
		namespace:   client.options.namespace
		workflow_id: workflow.workflow_id
		run_id:      workflow.run_id
		update_id:   id
	}))
	mut callbacks := options.completion_callbacks.clone()
	callbacks << temporal.TemporalApiCommonV1_Callback{
		links:   links
		variant: temporal.TemporalApiCommonV1_Callback_Nexus_Case{
			value: temporal.TemporalApiCommonV1_Callback_Nexus{
				url:    info.callback_url
				header: headers
			}
		}
	}
	started := client.start_update(workflow, UpdateRequest{
		...options
		id:                                     id
		request_id:                             info.request_id
		links:                                  links
		completion_callbacks:                   callbacks
		propagated_nexus_serialization_context: temporal.TemporalApiNexusV1_PropagatedSerializationContext{
			endpoint:  info.endpoint
			service:   info.service
			operation: info.operation
		}
	})!
	context.async_started = true
	mut response_links := []temporal.TemporalApiNexusV1_Link{}
	if link := started.link { response_links << nexus_link_from_common(link)! }
	if result := started.outcome {
		if failure := result.failure { return failure }
		if result.failed {
			return ApplicationError{
				message:       result.message
				non_retryable: true
			}
		}
		return NexusResult{
			value: result.value or { return error('Update outcome has no typed result') }
			links: response_links
		}
	}
	return NexusResult{
		links:           response_links
		operation_token: base64.url_encode_str(json2.encode(UpdateOperationToken{
			namespace:   client.options.namespace
			workflow_id: started.workflow.workflow_id
			run_id:      started.workflow.run_id
			update_id:   started.id
		}))
	}
}
