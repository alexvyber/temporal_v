module sdk

import temporal

@[params]
pub struct WorkflowPauseOptions {
pub:
	reason     string
	request_id string
}

fn (options WorkflowPauseOptions) identity() !string {
	validate_version_name(options.reason, 'workflow pause reason', false)!
	validate_version_name(options.request_id, 'workflow pause request ID', false)!
	return if options.request_id != '' { options.request_id } else { random_request_id()! }
}

// Workflow pausing must be enabled by the server namespace configuration.
pub fn (client &Client) pause_workflow(handle WorkflowHandle, options WorkflowPauseOptions) ! {
	target := execution(handle)!
	client.rpc_pause_workflow_execution(temporal.TemporalApiWorkflowserviceV1_PauseWorkflowExecutionRequest{
		namespace:   client.options.namespace
		workflow_id: target.workflow_id
		run_id:      target.run_id
		reason:      options.reason
		request_id:  options.identity()!
		identity:    client.options.transport.identity
	})!
}

pub fn (client &Client) unpause_workflow(handle WorkflowHandle, options WorkflowPauseOptions) ! {
	target := execution(handle)!
	client.rpc_unpause_workflow_execution(temporal.TemporalApiWorkflowserviceV1_UnpauseWorkflowExecutionRequest{
		namespace:   client.options.namespace
		workflow_id: target.workflow_id
		run_id:      target.run_id
		reason:      options.reason
		request_id:  options.identity()!
		identity:    client.options.transport.identity
	})!
}
