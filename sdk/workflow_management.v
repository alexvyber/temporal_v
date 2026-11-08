module sdk

import temporal

pub struct WorkflowDescription {
pub:
	execution WorkflowListing
	raw       temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionResponse
}

@[params]
pub struct WorkflowDescribeOptions {
pub:
	decode_memo bool = true
}

pub fn (client &Client) describe_workflow(handle WorkflowHandle, options WorkflowDescribeOptions) !WorkflowDescription {
	target := execution(handle)!
	body := temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionRequest{
		namespace: client.options.namespace
		execution: target
	}.encode()
	bytes := client.call_rpc(ClientRpcRequest{
		service:        .workflow
		method:         'DescribeWorkflowExecution'
		body:           body
		raw_payloads:   true
		timeout_millis: client.options.rpc_timeout_millis
		retry:          client.options.rpc_retry
		cancellation:   client.options.cancellation
	})!
	raw := temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionResponse.decode(bytes)!
	info := raw.workflow_execution_info or {
		return error('workflow description has no execution information')
	}

	page := client.workflow_listing_page([info], []u8{}, options.decode_memo)!
	return WorkflowDescription{
		execution: page.executions[0]
		raw:       raw
	}
}

@[params]
pub struct WorkflowResetOptions {
pub:
	workflow_task_finish_event_id i64
	reason                        string
	request_id                    string
	exclude_types                 []temporal.TemporalApiEnumsV1_ResetReapplyExcludeType
	post_reset_operations         []temporal.TemporalApiWorkflowV1_PostResetOperation
}

pub fn (client &Client) reset_workflow(handle WorkflowHandle, options WorkflowResetOptions) !WorkflowHandle {
	target := execution(handle)!
	if options.workflow_task_finish_event_id < 2 {
		return error('workflow reset requires a workflow task event ID greater than one')
	}
	validate_version_name(options.reason, 'workflow reset reason', false)!
	validate_version_name(options.request_id, 'workflow reset request ID', false)!
	request_id := if options.request_id != '' { options.request_id } else { random_request_id()! }
	request := temporal.TemporalApiWorkflowserviceV1_ResetWorkflowExecutionRequest{
		namespace:                     client.options.namespace
		workflow_execution:            target
		reason:                        options.reason
		request_id:                    request_id
		workflow_task_finish_event_id: options.workflow_task_finish_event_id
		reset_reapply_exclude_types:   options.exclude_types
		post_reset_operations:         options.post_reset_operations
		identity:                      client.options.transport.identity
	}
	// Some server versions preserve the original start request ID on reset,
	// so repeated reset IDs can create different runs. Never automatically retry
	// this mutation, even when the client enables retries for other calls.
	bytes := client.call_rpc(ClientRpcRequest{
		service:        .workflow
		method:         'ResetWorkflowExecution'
		body:           request.encode()
		retry:          false
		timeout_millis: client.options.rpc_timeout_millis
		cancellation:   client.options.cancellation
	})!
	result := temporal.TemporalApiWorkflowserviceV1_ResetWorkflowExecutionResponse.decode(bytes)!
	if result.run_id == '' { return error('workflow reset response has no run ID') }
	return WorkflowHandle{
		workflow_id: handle.workflow_id
		run_id:      result.run_id
	}
}

@[params]
pub struct WorkflowResetSignalOptions {
pub:
	headers map[string]PayloadValue
	links   []temporal.TemporalApiCommonV1_Link
}

// Encode post-reset signals in the target workflow's scope before the reset RPC.
pub fn (client &Client) reset_signal_operation(handle WorkflowHandle, name string, arguments Arguments, options WorkflowResetSignalOptions) !temporal.TemporalApiWorkflowV1_PostResetOperation {
	execution(handle)!
	validate_version_name(name, 'post-reset signal name', true)!
	converter := client.workflow_converter(handle.workflow_id)
	values := converter.encode_arguments(arguments)!
	headers := converter.encode_headers(options.headers)!
	return temporal.TemporalApiWorkflowV1_PostResetOperation{
		variant: temporal.TemporalApiWorkflowV1_PostResetOperation_SignalWorkflow_Case{
			value: temporal.TemporalApiWorkflowV1_PostResetOperation_SignalWorkflow{
				signal_name: name
				input:       temporal.TemporalApiCommonV1_Payloads{
					payloads: values
				}
				header:      temporal.TemporalApiCommonV1_Header{
					fields: headers
				}
				links:       options.links
			}
		}
	}
}

pub fn (client &Client) delete_workflow(handle WorkflowHandle) ! {
	client.rpc_delete_workflow_execution(temporal.TemporalApiWorkflowserviceV1_DeleteWorkflowExecutionRequest{
		namespace:          client.options.namespace
		workflow_execution: execution(handle)!
	})!
}
