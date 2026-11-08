module sdk

import temporal_v.temporal
import time

pub struct NexusExecutionHandle {
pub:
	operation_id string
	run_id       string
	started      bool
}

pub struct NexusExecutionStart {
pub:
	operation_id         string
	endpoint             string
	service              string
	operation            string
	input                PayloadValue = PayloadValue('null')
	request_id           string
	schedule_to_close    time.Duration = time.minute
	schedule_to_start    time.Duration
	start_to_close       time.Duration
	headers              map[string]string
	summary              string
	details              string
	search_attributes    []SearchAttribute
	id_reuse             temporal.TemporalApiEnumsV1_NexusOperationIdReusePolicy    = .nexus_operation_id_reuse_policy_allow_duplicate
	conflict_policy      temporal.TemporalApiEnumsV1_NexusOperationIdConflictPolicy = .nexus_operation_id_conflict_policy_fail
	on_conflict_options  ?temporal.TemporalApiNexusoperationV1_OnConflictOptions
	completion_callbacks []temporal.TemporalApiCommonV1_Callback
	links                []temporal.TemporalApiCommonV1_Link
}

pub struct NexusExecutionResult {
pub:
	value           ?PayloadValue @[json: '-']
	raw_value       ?RawValue     @[json: '-']
	run_id          string
	operation_token string
	status          temporal.TemporalApiEnumsV1_NexusOperationExecutionStatus
	data            string
	failure         ?Failure
}

pub struct NexusExecutionStarted {
pub:
	run_id          string
	operation_token string
	stage           temporal.TemporalApiEnumsV1_NexusOperationWaitStage
}

pub struct NexusExecutionDescription {
pub:
	raw                  temporal.TemporalApiWorkflowserviceV1_DescribeNexusOperationExecutionResponse
	input                string
	outcome              ?NexusExecutionResult
	last_attempt_failure ?Failure
}

@[params]
pub struct NexusManagementOptions {
pub:
	reason     string
	request_id string
}

fn validate_nexus_execution(handle NexusExecutionHandle) ! {
	if handle.operation_id == '' { return error('Nexus operation ID is required') }
}

pub fn (client &Client) start_nexus_operation(options NexusExecutionStart) !NexusExecutionHandle {
	if options.operation_id == '' || options.endpoint == '' || options.service == ''
		|| options.operation == '' {
		return error('Nexus operation ID, endpoint, service and operation are required')
	}
	if options.schedule_to_close <= 0 || options.schedule_to_start < 0 || options.start_to_close < 0 {
		return error('invalid Nexus execution timeouts')
	}
	converter := client.options.data_converter.with_context(nexus_serialization_context(options.endpoint,
		options.service, options.operation))
	metadata := converter.command_metadata(options.summary, options.details)!
	mut request := temporal.TemporalApiWorkflowserviceV1_StartNexusOperationExecutionRequest{
		namespace:                 client.options.namespace
		identity:                  client.options.transport.identity
		request_id:                if options.request_id != '' {
			options.request_id
		} else {
			random_request_id()!
		}
		operation_id:              options.operation_id
		endpoint:                  options.endpoint
		service:                   options.service
		operation:                 options.operation
		input:                     converter.encode_value(options.input)!
		schedule_to_close_timeout: duration_proto(options.schedule_to_close)!
		id_reuse_policy:           options.id_reuse
		id_conflict_policy:        options.conflict_policy
		on_conflict_options:       options.on_conflict_options
		nexus_header:              options.headers
		user_metadata:             metadata.value
		search_attributes:         search_attributes_proto(options.search_attributes)!
		completion_callbacks:      options.completion_callbacks
		links:                     options.links
	}
	if options.schedule_to_start > 0 {
		request.schedule_to_start_timeout = duration_proto(options.schedule_to_start)!
	}
	if options.start_to_close > 0 {
		request.start_to_close_timeout = duration_proto(options.start_to_close)!
	}
	response := client.rpc_start_nexus_operation_execution(request)!
	return NexusExecutionHandle{
		operation_id: options.operation_id
		run_id:       response.run_id
		started:      response.started
	}
}

fn (client &Client) poll_nexus_execution(handle NexusExecutionHandle, stage temporal.TemporalApiEnumsV1_NexusOperationWaitStage, timeout time.Duration) !temporal.TemporalApiWorkflowserviceV1_PollNexusOperationExecutionResponse {
	validate_nexus_execution(handle)!
	if timeout <= 0 { return error('Nexus result wait timeout must be positive') }
	request := temporal.TemporalApiWorkflowserviceV1_PollNexusOperationExecutionRequest{
		namespace:    client.options.namespace
		operation_id: handle.operation_id
		run_id:       handle.run_id
		wait_stage:   stage
	}
	watch := time.new_stopwatch()
	for {
		remaining := timeout - watch.elapsed()
		if remaining <= 0 { return error_with_code('Nexus result deadline exceeded', 4) }
		millis := remaining.milliseconds()
		wait := u32(if millis < 1 {
			i64(1)
		} else if millis < client.options.rpc_timeout_millis {
			millis
		} else {
			i64(client.options.rpc_timeout_millis)
		})
		bytes := client.call_with_timeout(.workflow, 'PollNexusOperationExecution',
			request.encode(), wait) or {
			if watch.elapsed() >= timeout {
				return error_with_code('Nexus result deadline exceeded', 4)
			}
			if err.code() == 4 {
				continue
			}
			return err
		}
		response :=
			temporal.TemporalApiWorkflowserviceV1_PollNexusOperationExecutionResponse.decode(bytes)!
		if int(response.wait_stage) >= int(stage) { return response }
	}
	return error('unreachable Nexus wait')
}

pub fn (client &Client) nexus_operation_started(handle NexusExecutionHandle, timeout time.Duration) !NexusExecutionStarted {
	response := client.poll_nexus_execution(handle, .nexus_operation_wait_stage_started, timeout)!
	return NexusExecutionStarted{
		run_id:          response.run_id
		operation_token: response.operation_token
		stage:           response.wait_stage
	}
}

fn (client &Client) nexus_execution_description(handle NexusExecutionHandle, input bool, outcome bool, timeout u32) !temporal.TemporalApiWorkflowserviceV1_DescribeNexusOperationExecutionResponse {
	validate_nexus_execution(handle)!
	request := temporal.TemporalApiWorkflowserviceV1_DescribeNexusOperationExecutionRequest{
		namespace:       client.options.namespace
		operation_id:    handle.operation_id
		run_id:          handle.run_id
		include_input:   input
		include_outcome: outcome
	}
	return temporal.TemporalApiWorkflowserviceV1_DescribeNexusOperationExecutionResponse.decode(client.call_with_timeout(.workflow,
		'DescribeNexusOperationExecution', request.encode(), timeout)!)!
}

fn (client &Client) nexus_execution_converter(info temporal.TemporalApiNexusV1_NexusOperationExecutionInfo) DataConverter {
	return client.options.data_converter.with_context(nexus_serialization_context(info.endpoint,
		info.service, info.operation))
}

fn nexus_outcome(info temporal.TemporalApiNexusV1_NexusOperationExecutionInfo, raw_result ?temporal.TemporalApiCommonV1_Payload, raw_failure ?temporal.TemporalApiFailureV1_Failure, converter DataConverter) !NexusExecutionResult {
	if payload := raw_result {
		decoded := converter.decode_single_argument([payload])!
		return NexusExecutionResult{
			run_id:          info.run_id
			operation_token: info.operation_token
			status:          info.status
			data:            decoded.values[0]
			value:           decoded.typed_values[0]
			raw_value:       decoded.raw_values[0]
		}
	}
	if failure := raw_failure {
		return NexusExecutionResult{
			run_id:          info.run_id
			operation_token: info.operation_token
			status:          info.status
			failure:         converter.decode_failure(failure)
		}
	}
	return error('Nexus execution has no outcome')
}

pub fn (client &Client) nexus_operation_result(handle NexusExecutionHandle, timeout time.Duration) !NexusExecutionResult {
	watch := time.new_stopwatch()
	response := client.poll_nexus_execution(handle, .nexus_operation_wait_stage_closed, timeout)!
	// Describe the resolved run to retain its precise terminal status and original
	// target. A use-existing start may have requested a different target entirely.
	remaining := timeout - watch.elapsed()
	if remaining <= 0 { return error_with_code('Nexus result deadline exceeded', 4) }
	millis := remaining.milliseconds()
	wait := u32(if millis < 1 {
		i64(1)
	} else if millis < client.options.rpc_timeout_millis {
		millis
	} else {
		i64(client.options.rpc_timeout_millis)
	})
	raw := client.nexus_execution_description(NexusExecutionHandle{
		operation_id: handle.operation_id
		run_id:       response.run_id
	}, false, false, wait) or {
		if watch.elapsed() >= timeout {
			return error_with_code('Nexus result deadline exceeded', 4)
		}
		return err
	}
	info := raw.info or { return error('Nexus execution has no information') }
	converter := client.nexus_execution_converter(info)
	variant := response.outcome or { return error('closed Nexus operation has no outcome') }
	match variant {
		temporal.TemporalApiWorkflowserviceV1_PollNexusOperationExecutionResponse_Result_Case {
			return nexus_outcome(info, variant.value, none, converter)!
		}
		temporal.TemporalApiWorkflowserviceV1_PollNexusOperationExecutionResponse_Failure_Case {
			return nexus_outcome(info, none, variant.value, converter)!
		}
	}
}

pub fn (client &Client) describe_nexus_operation(handle NexusExecutionHandle) !NexusExecutionDescription {
	raw :=
		client.nexus_execution_description(handle, true, true, client.options.rpc_timeout_millis)!
	info := raw.info or { return error('Nexus execution has no information') }
	converter := client.nexus_execution_converter(info)
	mut input := ''
	if payload := raw.input { input = converter.payload_data(payload)! }
	mut outcome := ?NexusExecutionResult(none)
	if variant := raw.outcome {
		match variant {
			temporal.TemporalApiWorkflowserviceV1_DescribeNexusOperationExecutionResponse_Result_Case {
				outcome = nexus_outcome(info, variant.value, none, converter)!
			}
			temporal.TemporalApiWorkflowserviceV1_DescribeNexusOperationExecutionResponse_Failure_Case {
				outcome = nexus_outcome(info, none, variant.value, converter)!
			}
		}
	}
	mut last_attempt := ?Failure(none)
	if failure := info.last_attempt_failure { last_attempt = converter.decode_failure(failure) }
	return NexusExecutionDescription{
		raw:                  raw
		input:                input
		outcome:              outcome
		last_attempt_failure: last_attempt
	}
}

pub fn (client &Client) cancel_nexus_operation(handle NexusExecutionHandle, options NexusManagementOptions) ! {
	validate_nexus_execution(handle)!
	client.rpc_request_cancel_nexus_operation_execution(temporal.TemporalApiWorkflowserviceV1_RequestCancelNexusOperationExecutionRequest{
		namespace:    client.options.namespace
		operation_id: handle.operation_id
		run_id:       handle.run_id
		identity:     client.options.transport.identity
		request_id:   if options.request_id != '' {
			options.request_id
		} else {
			random_request_id()!
		}
		reason:       options.reason
	})!
}

pub fn (client &Client) terminate_nexus_operation(handle NexusExecutionHandle, options NexusManagementOptions) ! {
	validate_nexus_execution(handle)!
	client.rpc_terminate_nexus_operation_execution(temporal.TemporalApiWorkflowserviceV1_TerminateNexusOperationExecutionRequest{
		namespace:    client.options.namespace
		operation_id: handle.operation_id
		run_id:       handle.run_id
		identity:     client.options.transport.identity
		request_id:   if options.request_id != '' {
			options.request_id
		} else {
			random_request_id()!
		}
		reason:       options.reason
	})!
}
