module sdk

import alexvyber.temporal_v.temporal

@[params]
pub struct QueryOptions {
pub:
	headers          map[string]PayloadValue
	reject_condition temporal.TemporalApiEnumsV1_QueryRejectCondition = .query_reject_condition_none
}

pub struct QueryRejectedError {
pub:
	workflow WorkflowHandle
	status   temporal.TemporalApiEnumsV1_WorkflowExecutionStatus
}

pub fn (err QueryRejectedError) msg() string {
	return 'query rejected: ${err.status}'
}

pub fn (err QueryRejectedError) code() int {
	return 9
}

pub struct SignalWithStartResult {
pub:
	workflow               WorkflowHandle
	started                bool
	first_execution_run_id string
	signal_link            ?temporal.TemporalApiCommonV1_Link
}

pub fn (client &Client) signal_with_start(options WorkflowStart, name string, arguments Arguments) !SignalWithStartResult {
	if options.request_eager_execution {
		return error('signal-with-start does not support eager execution')
	}
	if name == '' { return error('signal name is required') }
	if options.conflict_policy == .workflow_id_conflict_policy_fail {
		return error('signal-with-start cannot use the fail conflict policy')
	}
	if options.completion_callbacks.len > 0 || options.on_conflict_options != none
		|| options.propagated_nexus_serialization_context != none {
		return error('signal-with-start does not support completion callbacks, on-conflict options or propagated Nexus context')
	}
	base := client.start_request(options)!
	converter := client.workflow_converter(options.workflow_id)
	request := temporal.TemporalApiWorkflowserviceV1_SignalWithStartWorkflowExecutionRequest{
		namespace:                   base.namespace
		workflow_id:                 base.workflow_id
		workflow_type:               base.workflow_type
		task_queue:                  base.task_queue
		input:                       base.input
		workflow_execution_timeout:  base.workflow_execution_timeout
		workflow_run_timeout:        base.workflow_run_timeout
		workflow_task_timeout:       base.workflow_task_timeout
		identity:                    base.identity
		request_id:                  base.request_id
		workflow_id_reuse_policy:    base.workflow_id_reuse_policy
		workflow_id_conflict_policy: base.workflow_id_conflict_policy
		signal_name:                 name
		signal_input:                temporal.TemporalApiCommonV1_Payloads{
			payloads: converter.encode_arguments(arguments)!
		}
		retry_policy:                base.retry_policy
		cron_schedule:               base.cron_schedule
		memo:                        base.memo
		search_attributes:           base.search_attributes
		header:                      base.header
		workflow_start_delay:        base.workflow_start_delay
		user_metadata:               base.user_metadata
		links:                       base.links
		versioning_override:         base.versioning_override
		priority:                    base.priority
		time_skipping_config:        base.time_skipping_config
	}
	response := client.rpc_signal_with_start_workflow_execution(request)!
	return SignalWithStartResult{
		workflow:               WorkflowHandle{
			workflow_id: options.workflow_id
			run_id:      response.run_id
		}
		started:                response.started
		first_execution_run_id: response.first_execution_run_id
		signal_link:            response.signal_link
	}
}

pub struct QueryResponse {
pub:
	value     PayloadValue @[json: '-']
	data      string
	raw_value RawValue @[json: '-']
	link      ?temporal.TemporalApiCommonV1_Link
}
