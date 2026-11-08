module sdk

import temporal

@[params]
pub struct WorkflowTerminationOptions {
pub:
	reason                 string
	details                Arguments = Arguments([]string{})
	first_execution_run_id string
	links                  []temporal.TemporalApiCommonV1_Link
}

pub fn (client &Client) terminate(handle WorkflowHandle, options WorkflowTerminationOptions) ! {
	target := execution(handle)!
	converter := client.workflow_converter(handle.workflow_id)
	encoded := converter.encode_arguments(options.details)!
	mut request := temporal.TemporalApiWorkflowserviceV1_TerminateWorkflowExecutionRequest{
		namespace:              client.options.namespace
		workflow_execution:     target
		reason:                 options.reason
		identity:               client.options.transport.identity
		first_execution_run_id: options.first_execution_run_id
		links:                  options.links
	}
	if encoded.len > 0 {
		request.details = temporal.TemporalApiCommonV1_Payloads{
			payloads: encoded
		}
	}
	client.rpc_terminate_workflow_execution(request)!
}
