module sdk

import corebridge
import temporal
import time

pub struct NexusRequestInfo {
pub:
	endpoint         string
	service          string
	operation        string
	request_id       string
	headers          map[string]string
	callback_url     string
	callback_headers map[string]string
	links            []temporal.TemporalApiNexusV1_Link
	deadline_millis  i64
}

pub struct NexusContext {
	connection           &corebridge.Connection
	client_configuration ClientOptions
	client_plugins       []PluginRegistration
	tracing              ?TraceOptions
	trace_current        ?TraceContext
	trace_baggage        Baggage
	input_value          PayloadValue = PayloadValue(NullValue{})
	raw_input            ?RawValue
	metrics              &MetricMeter = &MetricMeter{}
	information          NexusRequestInfo
	logger               Logger = ConsoleLogger{}
	cancellation         chan bool
mut:
	cancelled     bool
	async_started bool
	signaled      bool
}

pub fn (context &NexusContext) info() NexusRequestInfo {
	return context.information
}

pub fn (mut context NexusContext) check_cancelled() ! {
	select {
		_ := <-context.cancellation {
			context.cancelled = true
		}
		else {}
	}
	if context.cancelled || (context.information.deadline_millis > 0
		&& time.now().unix_milli() >= context.information.deadline_millis) {
		return error_with_code('Nexus task cancelled', 499)
	}
}

pub fn (mut context NexusContext) sleep(duration time.Duration) ! {
	watch := time.new_stopwatch()
	for watch.elapsed() < duration {
		context.check_cancelled()!
		time.sleep(10 * time.millisecond)
	}
	context.check_cancelled()!
}

pub struct NexusResult {
pub:
	value           PayloadValue = PayloadValue('null')
	operation_token string
	links           []temporal.TemporalApiNexusV1_Link
}

pub type NexusHandler = fn (mut context NexusContext, input string) !NexusResult

pub type NexusCancelHandler = fn (mut context NexusContext, operation_token string) !

pub struct NexusOperation {
pub:
	start  NexusHandler @[required]
	cancel ?NexusCancelHandler
}

pub struct NexusServices {
mut:
	operations map[string]NexusOperation
}

pub fn (mut services NexusServices) register(service string, operation string, handler NexusOperation) ! {
	if service == '' || operation == '' || service.contains('\n') || operation.contains('\n') {
		return error('invalid Nexus service/operation')
	}
	key := service + '\n' + operation
	if key in services.operations { return error('duplicate Nexus operation registration') }
	services.operations[key] = handler
}

struct NexusCompletion {
	token  []u8
	status temporal.CoresdkNexus_NexusTaskCompletion_Status
}

fn nexus_handler_error(message string, kind string) temporal.CoresdkNexus_NexusTaskCompletion_Status {
	return temporal.CoresdkNexus_NexusTaskCompletion_Error_Case{
		value: temporal.TemporalApiNexusV1_HandlerError{
			error_type:     kind
			failure:        temporal.TemporalApiNexusV1_Failure{
				message: protocol_text(message)
			}
			retry_behavior: .nexus_handler_error_retry_behavior_non_retryable
		}
	}
}

fn nexus_response(response temporal.TemporalApiNexusV1_Response) temporal.CoresdkNexus_NexusTaskCompletion_Status {
	return temporal.CoresdkNexus_NexusTaskCompletion_Completed_Case{
		value: response
	}
}

fn execute_nexus(connection &corebridge.Connection, config WorkerOptions, services NexusServices, metrics &MetricMeter, task temporal.CoresdkNexus_NexusTask, cancellation chan bool) !temporal.CoresdkNexus_NexusTaskCompletion_Status {
	base_converter := config.data_converter
	tracing := config.tracing
	logger := config.logger
	client_configuration, client_plugins := worker_client_configuration(config)
	request_metrics := metrics.scope()!
	defer { request_metrics.close() }
	variant := task.variant or { return error('Nexus task missing variant') }
	if variant !is temporal.CoresdkNexus_NexusTask_Task_Case {
		return error('expected Nexus request task')
	}
	request := variant.value.request or { return error('Nexus task missing request') }
	operation := request.variant or { return error('Nexus request missing operation') }
	trace_span := start_nexus_trace(tracing, task.endpoint, request)
	defer {
		if value := trace_span { value.finish() }
	}
	current := if value := trace_span {
		?TraceContext(value.context())
	} else {
		?TraceContext(none)
	}
	mut information := NexusRequestInfo{
		endpoint: task.endpoint
		headers:  request.header
	}
	if deadline := task.request_deadline {
		information = NexusRequestInfo{
			...information
			deadline_millis: deadline.seconds * 1000 + deadline.nanos / 1000000
		}
	}
	if operation is temporal.TemporalApiNexusV1_Request_StartOperation_Case {
		start := operation.value
		converter := base_converter.with_context(nexus_serialization_context(task.endpoint,
			start.service, start.operation))
		emit_log(logger, .info,
			'NEXUS_START service=${start.service} operation=${start.operation} id=${start.request_id}')
		registration := services.operations[start.service + '\n' + start.operation] or {
			return nexus_handler_error('unknown Nexus operation', 'not_found')
		}
		decoded := if value := start.payload {
			converter.decode_single_argument([value])!
		} else {
			DecodedArguments{
				values:       ['null']
				typed_values: [PayloadValue(NullValue{})]
			}
		}
		input := decoded.values[0]
		mut context := NexusContext{
			connection:           connection
			client_configuration: client_configuration
			client_plugins:       client_plugins
			tracing:              tracing
			trace_current:        current
			trace_baggage:        nexus_trace_baggage(tracing, request.header)
			input_value:          decoded.typed_values[0]
			raw_input:            if decoded.raw_values.len == 1 {
				?RawValue(decoded.raw_values[0])
			} else {
				none
			}
			metrics:              request_metrics
			logger:               logger
			information:          NexusRequestInfo{
				...information
				service:          start.service
				operation:        start.operation
				request_id:       start.request_id
				callback_url:     start.callback
				callback_headers: start.callback_header
				links:            start.links.clone()
			}
			cancellation:         cancellation
		}
		context.check_cancelled()!
		result := invoke_nexus_start(registration.start, mut context, input) or {
			finish_nexus_trace_error(trace_span, tracing, err)
			if err is ApplicationError || err is Failure || err is CancelledError {
				return nexus_response(temporal.TemporalApiNexusV1_Response{
					variant: temporal.TemporalApiNexusV1_Response_StartOperation_Case{
						value: temporal.TemporalApiNexusV1_StartOperationResponse{
							variant: temporal.TemporalApiNexusV1_StartOperationResponse_Failure_Case{
								value: converter.encode_nexus_failure(err)!
							}
						}
					}
				})
			}
			if err.code() == 499 {
				return temporal.CoresdkNexus_NexusTaskCompletion_AckCancel_Case{
					value: true
				}
			}
			return nexus_error_status(err)
		}
		if result.operation_token != '' {
			return nexus_response(temporal.TemporalApiNexusV1_Response{
				variant: temporal.TemporalApiNexusV1_Response_StartOperation_Case{
					value: temporal.TemporalApiNexusV1_StartOperationResponse{
						variant: temporal.TemporalApiNexusV1_StartOperationResponse_AsyncSuccess_Case{
							value: temporal.TemporalApiNexusV1_StartOperationResponse_Async{
								operation_token: result.operation_token
								links:           result.links
							}
						}
					}
				}
			})
		}
		return nexus_response(temporal.TemporalApiNexusV1_Response{
			variant: temporal.TemporalApiNexusV1_Response_StartOperation_Case{
				value: temporal.TemporalApiNexusV1_StartOperationResponse{
					variant: temporal.TemporalApiNexusV1_StartOperationResponse_SyncSuccess_Case{
						value: temporal.TemporalApiNexusV1_StartOperationResponse_Sync{
							payload: converter.encode_value(result.value)!
							links:   result.links
						}
					}
				}
			}
		})
	}
	if operation is temporal.TemporalApiNexusV1_Request_CancelOperation_Case {
		cancel := operation.value
		registration := services.operations[cancel.service + '\n' + cancel.operation] or {
			return nexus_handler_error('unknown Nexus operation', 'not_found')
		}
		handler := registration.cancel or {
			return nexus_handler_error('operation does not support cancellation', 'not_implemented')
		}

		mut context := NexusContext{
			connection:           connection
			client_configuration: client_configuration
			client_plugins:       client_plugins
			tracing:              tracing
			trace_current:        current
			trace_baggage:        nexus_trace_baggage(tracing, request.header)
			metrics:              request_metrics
			logger:               logger
			information:          NexusRequestInfo{
				...information
				service:   cancel.service
				operation: cancel.operation
			}
			cancellation:         cancellation
		}
		invoke_nexus_cancel(handler, mut context, cancel.operation_token) or {
			finish_nexus_trace_error(trace_span, tracing, err)
			return err
		}
		return nexus_response(temporal.TemporalApiNexusV1_Response{
			variant: temporal.TemporalApiNexusV1_Response_CancelOperation_Case{
				value: temporal.TemporalApiNexusV1_CancelOperationResponse{}
			}
		})
	}
	return error('unsupported Nexus request')
}

fn dispatch_nexus(connection &corebridge.Connection, config WorkerOptions, services NexusServices, metrics &MetricMeter, received_task temporal.CoresdkNexus_NexusTask, token []u8, cancellation chan bool, results chan NexusCompletion) {
	converter := config.data_converter
	deadline := if timestamp := received_task.request_deadline {
		time.unix(timestamp.seconds).add(timestamp.nanos * time.nanosecond)
	} else {
		time.Time{}
	}
	storage_context := StorageContext{
		deadline: deadline
	}
	bytes := converter.storage_message(.nexus_task, received_task.encode(), false, storage_context, corebridge.PayloadMessageOptions{}) or {
		results <- NexusCompletion{
			token:  token
			status: nexus_error_status(err)
		}
		return
	}
	task := temporal.CoresdkNexus_NexusTask.decode(bytes) or {
		results <- NexusCompletion{
			token:  token
			status: nexus_error_status(err)
		}
		return
	}
	status := execute_nexus(connection, config, services, metrics, task, cancellation) or {
		if err.code() == 499 {
			results <- NexusCompletion{
				token:  token
				status: temporal.CoresdkNexus_NexusTaskCompletion_AckCancel_Case{
					value: true
				}
			}
		} else {
			results <- NexusCompletion{
				token:  token
				status: nexus_error_status(err)
			}
		}
		return
	}
	encoded := temporal.CoresdkNexus_NexusTaskCompletion{
		task_token: token
		status:     status
	}.encode()
	stored := converter.storage_message(.nexus_completion, encoded, true, storage_context, corebridge.PayloadMessageOptions{}) or {
		results <- NexusCompletion{
			token:  token
			status: nexus_error_status(NexusHandlerError{ message: err.msg(), retry: .retryable })
		}
		return
	}
	completion := temporal.CoresdkNexus_NexusTaskCompletion.decode(stored) or {
		results <- NexusCompletion{
			token:  token
			status: nexus_error_status(err)
		}
		return
	}
	if stored_status := completion.status {
		results <- NexusCompletion{
			token:  token
			status: stored_status
		}
	} else {
		results <- NexusCompletion{
			token:  token
			status: nexus_error_status(error('stored Nexus completion has no status'))
		}
	}
}

fn poll_nexus(connection &corebridge.Connection, tasks chan PollResult) {
	for {
		bytes := connection.poll_nexus() or {
			tasks <- PollResult{
				message: err.msg()
			}
			return
		}
		tasks <- PollResult{
			bytes: bytes
		}
		if bytes.len == 0 { return }
	}
}

pub fn run_nexus_worker(config WorkerOptions, services NexusServices) ! {
	setup := prepare_worker_plugins(config, &Runtime{}, Activities{}, services, .nexus)!
	run_worker_interceptors(setup)!
}

fn run_nexus_worker_configured(config WorkerOptions, services NexusServices) ! {
	owner := open_worker(config, .nexus)!
	connection := owner.connection
	control_id := config.control.attach(connection)
	defer {
		notify_worker_shutdown(config.worker_interceptors, WorkerShutdownInfo{
			role:       .nexus
			namespace:  config.namespace
			task_queue: config.task_queue
		}, config.logger)
		config.control.detach(control_id)
		owner.close()
	}
	serve_nexus(connection, config, services)!
}

fn serve_nexus(connection &corebridge.Connection, config WorkerOptions, services NexusServices) ! {
	metrics := (&MetricMeter{
		bridge: connection.metric_meter()!
	}).with_attributes({
		'namespace':  MetricAttribute(config.namespace)
		'task_queue': MetricAttribute(config.task_queue)
	})
	defer { metrics.close() }
	tasks := chan PollResult{cap: config.nexus_concurrency + 1}
	results := chan NexusCompletion{cap: config.nexus_concurrency}
	mut active := map[string]chan bool{}
	mut stopped := false
	poller := spawn poll_nexus(connection, tasks)
	defer {
		connection.shutdown()
		for _, cancellation in active {
			select {
				cancellation <- true {}
				else {}
			}
		}
		for !stopped || active.len > 0 {
			select {
				task := <-tasks {
					if task.message != '' || task.bytes.len == 0 {
						stopped = true
						continue
					}
					pending := temporal.CoresdkNexus_NexusTask.decode(task.bytes) or { continue }
					pending_variant := pending.variant or { continue }
					if pending_variant is temporal.CoresdkNexus_NexusTask_Task_Case {
						connection.complete_nexus(temporal.CoresdkNexus_NexusTaskCompletion{
							task_token: pending_variant.value.task_token
							status:     temporal.CoresdkNexus_NexusTaskCompletion_AckCancel_Case{
								value: true
							}
						}.encode()) or {
							emit_log(config.logger, .error,
								'Nexus drain acknowledgement failed: ${err.msg()}')
						}
					}
				}
				result := <-results {
					active.delete(result.token.hex())
					connection.complete_nexus(temporal.CoresdkNexus_NexusTaskCompletion{
						task_token: result.token
						status:     result.status
					}.encode()) or {
						emit_log(config.logger, .error,
							'Nexus drain completion failed: ${err.msg()}')
					}
				}
			}
		}
		poller.wait()
	}
	emit_log(config.logger, .info, 'NEXUS_READY queue=${config.task_queue}')
	for !stopped || active.len > 0 {
		select {
			task_result := <-tasks {
				if task_result.message != '' {
					stopped = true
					return error(task_result.message)
				}
				if task_result.bytes.len == 0 {
					stopped = true
					continue
				}
				task := temporal.CoresdkNexus_NexusTask.decode(task_result.bytes)!
				variant := task.variant or { return error('Nexus task missing variant') }
				if variant is temporal.CoresdkNexus_NexusTask_Task_Case {
					cancellation := chan bool{cap: 1}
					active[variant.value.task_token.hex()] = cancellation
					spawn dispatch_nexus(connection, config, services, metrics, task,
						variant.value.task_token, cancellation, results)
				} else if variant is temporal.CoresdkNexus_NexusTask_CancelTask_Case {
					if cancellation := active[variant.value.task_token.hex()] {
						select {
							cancellation <- true {}
							else {}
						}
					}
				}
			}
			result := <-results {
				active.delete(result.token.hex())
				connection.complete_nexus(temporal.CoresdkNexus_NexusTaskCompletion{
					task_token: result.token
					status:     result.status
				}.encode())!
			}
		}
	}
	emit_log(config.logger, .info, 'NEXUS_SHUTDOWN complete')
}

pub fn (context &NexusContext) typed_input() !PayloadValue {
	return clone_payload_value(context.input_value)
}

pub fn (context &NexusContext) input_payload() ?RawValue {
	return context.raw_input
}
