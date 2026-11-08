module sdk

import corebridge
import json2
import temporal
import time

pub struct ClientOptions {
pub:
	plugins            []Plugin
	tracing            ?TraceOptions
	address            string = 'http://127.0.0.1:7233'
	namespace          string = 'default'
	rpc_timeout_millis u32    = 5000
	transport          TransportOptions
	data_converter     DataConverter
	telemetry          TelemetryOptions
	interceptors       []ClientInterceptor
	rpc_retry          bool
	cancellation       ?&RpcCancellation
}

pub struct WorkflowHandle {
pub:
	workflow_id string
	run_id      string
}

pub struct WorkflowStart {
pub:
	request_eager_execution                bool
	propagated_nexus_serialization_context ?temporal.TemporalApiNexusV1_PropagatedSerializationContext
	workflow_id                            string
	workflow_type                          string
	task_queue                             string
	arguments                              Arguments = Arguments([]string{})
	request_id                             string
	execution_timeout_seconds              i64
	task_timeout_seconds                   i64 = 10
	run_timeout_seconds                    i64
	retry                                  ?RetryPolicy
	cron_schedule                          string
	headers                                map[string]PayloadValue
	memo                                   map[string]PayloadValue
	id_reuse                               WorkflowIdReuse = .reject_duplicate
	conflict_policy                        temporal.TemporalApiEnumsV1_WorkflowIdConflictPolicy = .workflow_id_conflict_policy_unspecified
	start_delay                            time.Duration
	links                                  []temporal.TemporalApiCommonV1_Link
	search_attributes                      []SearchAttribute
	versioning_override                    ?VersioningOverride
	priority                               ?temporal.TemporalApiCommonV1_Priority
	static_summary                         string
	static_details                         string
	completion_callbacks                   []temporal.TemporalApiCommonV1_Callback
	on_conflict_options                    ?temporal.TemporalApiWorkflowV1_OnConflictOptions
}

pub enum WorkflowStatus {
	completed
	failed
	canceled
	terminated
	timed_out
	continued_as_new
}

pub struct WorkflowResult {
pub:
	value         ?PayloadValue  @[json: '-']
	detail_values []PayloadValue @[json: '-']
	raw_details   []RawValue     @[json: '-']
	raw_value     ?RawValue      @[json: '-']
	details       []string
	retry_state   temporal.TemporalApiEnumsV1_RetryState
	status        WorkflowStatus
	data          string
	message       string
	next_run_id   string
	failure       ?Failure
}

@[heap]
pub struct Client {
	plugins    []PluginRegistration
	connection &corebridge.Connection
	options    ClientOptions
}

pub fn connect(original ClientOptions) !&Client {
	options, plugins := configure_client(original)!
	if tracing := options.tracing { tracing.validate()! }
	if options.namespace == '' || options.rpc_timeout_millis == 0 {
		return error('namespace and positive RPC timeout are required')
	}
	connection := corebridge.connect(options.address, options.transport.core_transport()!,
		options.telemetry.core_telemetry()!)!
	return &Client{
		connection: connection
		options:    options
		plugins:    plugins
	}
}

pub fn (client &Client) close() {
	client.connection.close()
}

fn (client &Client) call(service corebridge.RpcService, method string, request []u8) ![]u8 {
	return client.call_with_timeout(service, method, request, client.options.rpc_timeout_millis)!
}

fn (client &Client) call_with_timeout(service corebridge.RpcService, method string, request []u8, timeout u32) ![]u8 {
	if client.connection.is_closed() { return error('client is closed') }
	return client.call_rpc(ClientRpcRequest{
		service:        service
		method:         method
		body:           request
		timeout_millis: timeout
		retry:          client.options.rpc_retry
		cancellation:   client.options.cancellation
	})!
}

fn execution(handle WorkflowHandle) !temporal.TemporalApiCommonV1_WorkflowExecution {
	if handle.workflow_id == '' { return error('workflow_id is required') }
	return temporal.TemporalApiCommonV1_WorkflowExecution{
		workflow_id: handle.workflow_id
		run_id:      handle.run_id
	}
}

fn (client &Client) start_request(options WorkflowStart) !temporal.TemporalApiWorkflowserviceV1_StartWorkflowExecutionRequest {
	if options.workflow_id == '' || options.workflow_type == '' || options.task_queue == '' {
		return error('workflow_id, workflow_type and task_queue are required')
	}
	if options.execution_timeout_seconds < 0 || options.task_timeout_seconds < 1
		|| options.run_timeout_seconds < 0 || options.start_delay < 0 {
		return error('invalid workflow timeouts')
	}
	mut override := ?temporal.TemporalApiWorkflowV1_VersioningOverride(none)
	if value := options.versioning_override { override = versioning_override_proto(value)! }
	converter := client.workflow_converter(options.workflow_id)
	encoded := converter.encode_arguments(options.arguments)!
	mut request := temporal.TemporalApiWorkflowserviceV1_StartWorkflowExecutionRequest{
		request_eager_execution:                options.request_eager_execution
		propagated_nexus_serialization_context: options.propagated_nexus_serialization_context
		namespace:                              client.options.namespace
		workflow_id:                            options.workflow_id
		workflow_type:                          temporal.TemporalApiCommonV1_WorkflowType{
			name: options.workflow_type
		}
		task_queue:                             temporal.TemporalApiTaskqueueV1_TaskQueue{
			name: options.task_queue
		}
		input:                                  temporal.TemporalApiCommonV1_Payloads{
			payloads: encoded
		}
		workflow_task_timeout:                  temporal.GoogleProtobuf_Duration{
			seconds: options.task_timeout_seconds
		}
		identity:                               client.options.transport.identity
		request_id:                             if options.request_id != '' {
			options.request_id
		} else {
			random_request_id()!
		}
		workflow_id_reuse_policy:               options.id_reuse.encode()
		workflow_id_conflict_policy:            options.conflict_policy
		cron_schedule:                          options.cron_schedule
		links:                                  options.links
		versioning_override:                    override
		priority:                               options.priority
		search_attributes:                      search_attributes_proto(options.search_attributes)!
		completion_callbacks:                   options.completion_callbacks
		on_conflict_options:                    options.on_conflict_options
		header:                                 temporal.TemporalApiCommonV1_Header{
			fields: converter.encode_headers(options.headers)!
		}
		memo:                                   temporal.TemporalApiCommonV1_Memo{
			fields: converter.encode_map(options.memo)!
		}
	}
	if options.execution_timeout_seconds > 0 {
		request.workflow_execution_timeout = temporal.GoogleProtobuf_Duration{
			seconds: options.execution_timeout_seconds
		}
	}
	if options.run_timeout_seconds > 0 {
		request.workflow_run_timeout = temporal.GoogleProtobuf_Duration{
			seconds: options.run_timeout_seconds
		}
	}
	if retry := options.retry { request.retry_policy = retry.encode()! }
	if options.start_delay > 0 {
		request.workflow_start_delay = duration_proto(options.start_delay)!
	}
	if options.static_summary != '' || options.static_details != '' {
		mut metadata := temporal.TemporalApiSdkV1_UserMetadata{}
		if options.static_summary != '' {
			metadata.summary = converter.encode_value(json2.encode(options.static_summary))!
		}
		if options.static_details != '' {
			metadata.details = converter.encode_value(json2.encode(options.static_details))!
		}
		request.user_metadata = metadata
	}
	return request
}

pub fn (client &Client) start(options WorkflowStart) !WorkflowHandle {
	request := client.start_request(options)!
	bytes := client.call(.workflow, 'StartWorkflowExecution', request.encode())!
	response := temporal.TemporalApiWorkflowserviceV1_StartWorkflowExecutionResponse.decode(bytes)!
	return WorkflowHandle{
		workflow_id: options.workflow_id
		run_id:      response.run_id
	}
}

@[params]
pub struct MessageOptions {
pub:
	request_id string
	links      []temporal.TemporalApiCommonV1_Link
	headers    map[string]PayloadValue
}

pub fn (client &Client) signal(handle WorkflowHandle, name string, arguments Arguments, options MessageOptions) ! {
	if name == '' { return error('signal name is required') }
	converter := client.workflow_converter(handle.workflow_id)
	encoded := converter.encode_arguments(arguments)!
	request := temporal.TemporalApiWorkflowserviceV1_SignalWorkflowExecutionRequest{
		namespace:          client.options.namespace
		workflow_execution: execution(handle)!
		header:             temporal.TemporalApiCommonV1_Header{
			fields: converter.encode_headers(options.headers)!
		}
		signal_name:        name
		input:              temporal.TemporalApiCommonV1_Payloads{
			payloads: encoded
		}
		identity:           client.options.transport.identity
		links:              options.links
		request_id:         if options.request_id != '' {
			options.request_id
		} else {
			random_request_id()!
		}
	}
	client.call(.workflow, 'SignalWorkflowExecution', request.encode())!
}

pub fn (client &Client) query(handle WorkflowHandle, name string, arguments Arguments, options QueryOptions) !string {
	return client.query_response(handle, name, arguments, options)!.data
}

pub fn (client &Client) query_response(handle WorkflowHandle, name string, arguments Arguments, options QueryOptions) !QueryResponse {
	if name == '' { return error('query name is required') }
	converter := client.workflow_converter(handle.workflow_id)
	encoded := converter.encode_arguments(arguments)!
	request := temporal.TemporalApiWorkflowserviceV1_QueryWorkflowRequest{
		query_reject_condition: options.reject_condition
		namespace:              client.options.namespace
		execution:              execution(handle)!
		query:                  temporal.TemporalApiQueryV1_WorkflowQuery{
			header:     temporal.TemporalApiCommonV1_Header{
				fields: converter.encode_headers(options.headers)!
			}
			query_type: name
			query_args: temporal.TemporalApiCommonV1_Payloads{
				payloads: encoded
			}
		}
	}
	bytes := client.call(.workflow, 'QueryWorkflow', request.encode()) or {
		if err is RpcError { return query_failure_error(err, converter) }
		return err
	}
	response := temporal.TemporalApiWorkflowserviceV1_QueryWorkflowResponse.decode(bytes)!
	if rejected := response.query_rejected {
		return QueryRejectedError{
			workflow: handle
			status:   rejected.status
		}
	}
	result := response.query_result or { return error('query response has no result') }
	decoded := converter.decode_single_argument(result.payloads)!
	return QueryResponse{
		value:     decoded.typed_values[0]
		data:      decoded.values[0]
		raw_value: decoded.raw_values[0]
		link:      response.link
	}
}

pub fn (client &Client) cancel(handle WorkflowHandle, reason string) ! {
	request := temporal.TemporalApiWorkflowserviceV1_RequestCancelWorkflowExecutionRequest{
		namespace:          client.options.namespace
		workflow_execution: execution(handle)!
		identity:           client.options.transport.identity
		request_id:         random_request_id()!
		reason:             reason
	}
	client.call(.workflow, 'RequestCancelWorkflowExecution', request.encode())!
}

pub fn (client &Client) get_result(handle WorkflowHandle, wait_timeout_millis u32) !WorkflowResult {
	if client.connection.is_closed() { return error('client is closed') }
	if wait_timeout_millis == 0 { return error('result wait timeout must be positive') }
	run := execution(handle)!
	mut result_converter := client.workflow_converter(handle.workflow_id)
	watch := time.new_stopwatch()
	mut page_token := []u8{}
	for {
		remaining := i64(wait_timeout_millis) - watch.elapsed().milliseconds()
		if remaining <= 0 { return error_with_code('workflow result deadline exceeded', 4) }
		request := temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryRequest{
			namespace:                 client.options.namespace
			execution:                 run
			wait_new_event:            true
			history_event_filter_type: .history_event_filter_type_close_event
			next_page_token:           page_token
		}
		timeout := u32(if remaining < i64(client.options.rpc_timeout_millis) {
			remaining
		} else {
			i64(client.options.rpc_timeout_millis)
		})
		bytes := client.call_with_timeout(.workflow, 'GetWorkflowExecutionHistory',
			request.encode(), timeout) or {
			if watch.elapsed() >= time.Duration(wait_timeout_millis) * time.millisecond {
				return error_with_code('workflow result deadline exceeded', 4)
			}
			if err.code() == 4 {
				continue
			}
			return err
		}
		response :=
			temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse.decode(bytes)!
		if propagated := response.propagated_nexus_serialization_context {
			result_converter = result_converter.with_result_context(propagated)
		}
		page_token = response.next_page_token
		if history := response.history {
			for event in history.events {
				if event.event_type !in [.event_type_workflow_execution_completed,
					.event_type_workflow_execution_failed, .event_type_workflow_execution_canceled,
					.event_type_workflow_execution_terminated,
					.event_type_workflow_execution_timed_out,
					.event_type_workflow_execution_continued_as_new] {
					continue
				}
				return closed_result(event, result_converter)!
			}
		}
	}
}

fn closed_result(event temporal.TemporalApiHistoryV1_HistoryEvent, converter DataConverter) !WorkflowResult {
	attributes := event.attributes or { return error('close event has no attributes') }
	match attributes {
		temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionCompletedEventAttributes_Case {
			result := attributes.value.result or {
				return WorkflowResult{
					status: .completed
					value:  PayloadValue(NullValue{})
					data:   'null'
				}
			}

			if result.payloads.len == 0 {
				return WorkflowResult{
					status: .completed
					value:  PayloadValue(NullValue{})
					data:   'null'
				}
			}
			decoded := converter.decode_single_argument(result.payloads)!
			return WorkflowResult{
				status:    .completed
				value:     decoded.typed_values[0]
				data:      decoded.values[0]
				raw_value: decoded.raw_values[0]
			}
		}
		temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionFailedEventAttributes_Case {
			info := attributes.value.failure or { return error('missing workflow failure') }
			decoded := failure_from_proto(info, converter)
			return WorkflowResult{
				status:      .failed
				message:     decoded.message
				failure:     decoded
				retry_state: attributes.value.retry_state
				next_run_id: attributes.value.new_execution_run_id
			}
		}
		temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionCanceledEventAttributes_Case {
			mut details := DecodedArguments{}
			if encoded := attributes.value.details {
				details = converter.decode_arguments(encoded.payloads)!
			}
			return WorkflowResult{
				status:        .canceled
				details:       details.values
				detail_values: details.typed_values
				raw_details:   details.raw_values
			}
		}
		temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionTerminatedEventAttributes_Case {
			mut details := DecodedArguments{}
			if encoded := attributes.value.details {
				details = converter.decode_arguments(encoded.payloads)!
			}
			return WorkflowResult{
				status:        .terminated
				message:       attributes.value.reason
				details:       details.values
				detail_values: details.typed_values
				raw_details:   details.raw_values
			}
		}
		temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionTimedOutEventAttributes_Case {
			return WorkflowResult{
				status:      .timed_out
				retry_state: attributes.value.retry_state
				next_run_id: attributes.value.new_execution_run_id
			}
		}
		temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes_Case {
			return WorkflowResult{
				status:      .continued_as_new
				next_run_id: attributes.value.new_execution_run_id
			}
		}
		else {
			return error('unexpected workflow close event')
		}
	}
}
