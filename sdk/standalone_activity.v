module sdk

import json2
import temporal
import time

pub struct StandaloneActivityHandle {
pub:
	activity_id string
	run_id      string
	started     bool
}

pub struct StandaloneActivityStart {
pub:
	request_eager_execution                bool
	propagated_nexus_serialization_context ?temporal.TemporalApiNexusV1_PropagatedSerializationContext
	activity_id                            string
	activity_type                          string
	task_queue                             string
	arguments                              Arguments = Arguments([]string{})
	request_id                             string
	schedule_to_close                      time.Duration
	schedule_to_start                      time.Duration
	start_to_close                         time.Duration = 30 * time.second
	heartbeat_timeout                      time.Duration
	start_delay                            time.Duration
	retry                                  ?RetryPolicy
	headers                                map[string]PayloadValue
	search_attributes                      []SearchAttribute
	summary                                string
	details                                string
	priority                               ?temporal.TemporalApiCommonV1_Priority
	id_reuse                               temporal.TemporalApiEnumsV1_ActivityIdReusePolicy    = .activity_id_reuse_policy_allow_duplicate
	conflict_policy                        temporal.TemporalApiEnumsV1_ActivityIdConflictPolicy = .activity_id_conflict_policy_fail
	on_conflict_options                    ?temporal.TemporalApiCommonV1_OnConflictOptions
	completion_callbacks                   []temporal.TemporalApiCommonV1_Callback
	links                                  []temporal.TemporalApiCommonV1_Link
}

pub enum StandaloneActivityStatus {
	completed
	failed
	canceled
	timed_out
	terminated
}

pub struct StandaloneActivityResult {
pub:
	typed_values []PayloadValue @[json: '-']
	raw_values   []RawValue     @[json: '-']
	status       StandaloneActivityStatus
	values       []string
	failure      ?Failure
	retry_state  temporal.TemporalApiEnumsV1_RetryState
}

pub struct StandaloneActivityDescription {
pub:
	typed_arguments       []PayloadValue @[json: '-']
	raw_arguments         []RawValue     @[json: '-']
	heartbeat_values      []PayloadValue @[json: '-']
	raw_heartbeat_details []RawValue     @[json: '-']
	raw                   temporal.TemporalApiWorkflowserviceV1_DescribeActivityExecutionResponse
	arguments             []string
	heartbeat_details     []string
	headers               map[string]PayloadValue
	summary               string
	details               string
	last_failure          ?Failure
	outcome               ?StandaloneActivityResult
}

pub struct ActivityManagementOptions {
pub:
	reason                   string
	request_id               string
	jitter                   time.Duration
	keep_paused              bool
	reset_heartbeat          bool
	restore_original_options bool
}

fn validate_standalone_activity(handle StandaloneActivityHandle) ! {
	if handle.activity_id == '' { return error('standalone Activity ID is required') }
}

fn (client &Client) standalone_converter(activity_id string) DataConverter {
	return client.options.data_converter.with_context(SerializationContext{
		kind:        .activity
		namespace:   client.options.namespace
		activity_id: activity_id
	})
}

pub fn (client &Client) start_activity(options StandaloneActivityStart) !StandaloneActivityHandle {
	if options.activity_id == '' || options.activity_type == '' || options.task_queue == '' {
		return error('standalone Activity ID, type and task queue are required')
	}
	for duration in [options.schedule_to_close, options.schedule_to_start, options.start_to_close,
		options.heartbeat_timeout, options.start_delay] {
		if duration < 0 { return error('Activity timeouts and delay cannot be negative') }
	}
	if options.schedule_to_close == 0 && options.start_to_close == 0 {
		return error('Activity requires schedule-to-close or start-to-close timeout')
	}
	converter := client.standalone_converter(options.activity_id)
	mut request := temporal.TemporalApiWorkflowserviceV1_StartActivityExecutionRequest{
		request_eager_execution:                options.request_eager_execution
		propagated_nexus_serialization_context: options.propagated_nexus_serialization_context
		namespace:                              client.options.namespace
		identity:                               client.options.transport.identity
		request_id:                             if options.request_id != '' {
			options.request_id
		} else {
			random_request_id()!
		}
		activity_id:                            options.activity_id
		activity_type:                          temporal.TemporalApiCommonV1_ActivityType{
			name: options.activity_type
		}
		task_queue:                             temporal.TemporalApiTaskqueueV1_TaskQueue{
			name: options.task_queue
		}
		input:                                  temporal.TemporalApiCommonV1_Payloads{
			payloads: converter.encode_arguments(options.arguments)!
		}
		header:                                 temporal.TemporalApiCommonV1_Header{
			fields: converter.encode_headers(options.headers)!
		}
		search_attributes:                      search_attributes_proto(options.search_attributes)!
		id_reuse_policy:                        options.id_reuse
		id_conflict_policy:                     options.conflict_policy
		priority:                               options.priority
		on_conflict_options:                    options.on_conflict_options
		completion_callbacks:                   options.completion_callbacks
		links:                                  options.links
	}
	if options.schedule_to_close > 0 {
		request.schedule_to_close_timeout = duration_proto(options.schedule_to_close)!
	}
	if options.schedule_to_start > 0 {
		request.schedule_to_start_timeout = duration_proto(options.schedule_to_start)!
	}
	if options.start_to_close > 0 {
		request.start_to_close_timeout = duration_proto(options.start_to_close)!
	}
	if options.heartbeat_timeout > 0 {
		request.heartbeat_timeout = duration_proto(options.heartbeat_timeout)!
	}
	if options.start_delay > 0 { request.start_delay = duration_proto(options.start_delay)! }
	if retry := options.retry { request.retry_policy = retry.encode()! }
	if options.summary != '' || options.details != '' {
		metadata := converter.command_metadata(options.summary, options.details)!
		request.user_metadata = metadata.value
	}
	response := client.rpc_start_activity_execution(request)!
	return StandaloneActivityHandle{
		activity_id: options.activity_id
		run_id:      response.run_id
		started:     response.started
	}
}

fn standalone_outcome(value temporal.TemporalApiActivityV1_ActivityExecutionOutcome, converter DataConverter) !StandaloneActivityResult {
	variant := value.value or { return error('standalone Activity outcome is empty') }
	match variant {
		temporal.TemporalApiActivityV1_ActivityExecutionOutcome_Result_Case {
			decoded := converter.decode_arguments(variant.value.payloads)!
			return StandaloneActivityResult{
				status:       .completed
				values:       decoded.values
				typed_values: decoded.typed_values
				raw_values:   decoded.raw_values
			}
		}
		temporal.TemporalApiActivityV1_ActivityExecutionOutcome_Failure_Case {
			failure := converter.decode_failure(variant.value)
			status := match failure.kind {
				.canceled { StandaloneActivityStatus.canceled }
				.timeout { StandaloneActivityStatus.timed_out }
				.terminated { StandaloneActivityStatus.terminated }
				else { StandaloneActivityStatus.failed }
			}

			return StandaloneActivityResult{
				status:      status
				failure:     failure
				retry_state: value.retry_state
			}
		}
	}
}

// A result deadline cancels only the caller's wait, never the execution.
pub fn (client &Client) activity_result(handle StandaloneActivityHandle, timeout time.Duration) !StandaloneActivityResult {
	validate_standalone_activity(handle)!
	if timeout <= 0 { return error('Activity result wait timeout must be positive') }
	watch := time.new_stopwatch()
	request := temporal.TemporalApiWorkflowserviceV1_PollActivityExecutionRequest{
		namespace:   client.options.namespace
		activity_id: handle.activity_id
		run_id:      handle.run_id
	}
	for {
		remaining := timeout - watch.elapsed()
		if remaining <= 0 { return error_with_code('Activity result deadline exceeded', 4) }
		millis := remaining.milliseconds()
		wait := u32(if millis < 1 {
			i64(1)
		} else if millis < client.options.rpc_timeout_millis {
			millis
		} else {
			i64(client.options.rpc_timeout_millis)
		})
		bytes := client.call_with_timeout(.workflow, 'PollActivityExecution', request.encode(), wait) or {
			if watch.elapsed() >= timeout {
				return error_with_code('Activity result deadline exceeded', 4)
			}
			if err.code() == 4 {
				continue
			}
			return err
		}
		response :=
			temporal.TemporalApiWorkflowserviceV1_PollActivityExecutionResponse.decode(bytes)!
		if outcome := response.outcome {
			return standalone_outcome(outcome,
				client.standalone_converter(handle.activity_id).with_result_context(response.propagated_nexus_serialization_context))!
		}
	}
	return error('unreachable Activity result wait')
}

pub fn (client &Client) describe_activity(handle StandaloneActivityHandle) !StandaloneActivityDescription {
	validate_standalone_activity(handle)!
	raw := client.rpc_describe_activity_execution(temporal.TemporalApiWorkflowserviceV1_DescribeActivityExecutionRequest{
		namespace:                 client.options.namespace
		activity_id:               handle.activity_id
		run_id:                    handle.run_id
		include_input:             true
		include_outcome:           true
		include_heartbeat_details: true
		include_last_failure:      true
	})!
	converter := client.standalone_converter(handle.activity_id)
	mut result_converter := converter
	if information := raw.info {
		result_converter =
			converter.with_result_context(information.propagated_nexus_serialization_context)
	}
	mut arguments := DecodedArguments{}
	mut heartbeat := DecodedArguments{}
	mut headers := map[string]PayloadValue{}
	mut summary := ''
	mut details := ''
	mut last_failure := ?Failure(none)
	mut outcome := ?StandaloneActivityResult(none)
	if input := raw.input { arguments = converter.decode_arguments(input.payloads)! }
	if info := raw.info {
		if header := info.header { headers = converter.decode_headers(header.fields)! }
		if metadata := info.user_metadata {
			if payload := metadata.summary {
				summary = json2.decode[string](converter.payload_data(payload)!)!
			}
			if payload := metadata.details {
				details = json2.decode[string](converter.payload_data(payload)!)!
			}
		}
		if checkpoint := info.heartbeat_details {
			heartbeat = result_converter.decode_arguments(checkpoint.payloads)!
		}
		if failure := info.last_failure { last_failure = result_converter.decode_failure(failure) }
	}
	if result := raw.outcome { outcome = standalone_outcome(result, result_converter)! }
	return StandaloneActivityDescription{
		raw:                   raw
		arguments:             arguments.values
		typed_arguments:       arguments.typed_values
		raw_arguments:         arguments.raw_values
		heartbeat_details:     heartbeat.values
		heartbeat_values:      heartbeat.typed_values
		raw_heartbeat_details: heartbeat.raw_values
		headers:               headers
		summary:               summary
		details:               details
		last_failure:          last_failure
		outcome:               outcome
	}
}

fn management_request_id(options ActivityManagementOptions) !string {
	return if options.request_id != '' { options.request_id } else { random_request_id()! }
}

pub fn (client &Client) cancel_activity(handle StandaloneActivityHandle, options ActivityManagementOptions) ! {
	validate_standalone_activity(handle)!
	client.rpc_request_cancel_activity_execution(temporal.TemporalApiWorkflowserviceV1_RequestCancelActivityExecutionRequest{
		namespace:   client.options.namespace
		activity_id: handle.activity_id
		run_id:      handle.run_id
		identity:    client.options.transport.identity
		request_id:  management_request_id(options)!
		reason:      options.reason
	})!
}

pub fn (client &Client) terminate_activity(handle StandaloneActivityHandle, options ActivityManagementOptions) ! {
	validate_standalone_activity(handle)!
	client.rpc_terminate_activity_execution(temporal.TemporalApiWorkflowserviceV1_TerminateActivityExecutionRequest{
		namespace:   client.options.namespace
		activity_id: handle.activity_id
		run_id:      handle.run_id
		identity:    client.options.transport.identity
		request_id:  management_request_id(options)!
		reason:      options.reason
	})!
}

pub fn (client &Client) pause_activity(handle StandaloneActivityHandle, options ActivityManagementOptions) ! {
	validate_standalone_activity(handle)!
	client.rpc_pause_activity_execution(temporal.TemporalApiWorkflowserviceV1_PauseActivityExecutionRequest{
		namespace:   client.options.namespace
		activity_id: handle.activity_id
		run_id:      handle.run_id
		identity:    client.options.transport.identity
		request_id:  management_request_id(options)!
		reason:      options.reason
		resource_id: 'activity:' + handle.activity_id
	})!
}

pub fn (client &Client) unpause_activity(handle StandaloneActivityHandle, options ActivityManagementOptions) ! {
	validate_standalone_activity(handle)!
	if options.jitter < 0 { return error('Activity jitter cannot be negative') }
	client.rpc_unpause_activity_execution(temporal.TemporalApiWorkflowserviceV1_UnpauseActivityExecutionRequest{
		namespace:   client.options.namespace
		activity_id: handle.activity_id
		run_id:      handle.run_id
		identity:    client.options.transport.identity
		request_id:  management_request_id(options)!
		reason:      options.reason
		jitter:      duration_proto(options.jitter)!
		resource_id: 'activity:' + handle.activity_id
	})!
}

pub fn (client &Client) reset_activity(handle StandaloneActivityHandle, options ActivityManagementOptions) ! {
	validate_standalone_activity(handle)!
	if options.jitter < 0 { return error('Activity jitter cannot be negative') }
	client.rpc_reset_activity_execution(temporal.TemporalApiWorkflowserviceV1_ResetActivityExecutionRequest{
		namespace:                client.options.namespace
		activity_id:              handle.activity_id
		run_id:                   handle.run_id
		identity:                 client.options.transport.identity
		request_id:               management_request_id(options)!
		jitter:                   duration_proto(options.jitter)!
		resource_id:              'activity:' + handle.activity_id
		keep_paused:              options.keep_paused
		reset_heartbeat:          options.reset_heartbeat
		restore_original_options: options.restore_original_options
	})!
}
