module sdk

import alexvyber.temporal_v.corebridge
import alexvyber.temporal_v.temporal
import time

pub type ActivityHandler = fn (mut context ActivityContext, arguments []string) !PayloadValue

// Dynamic Activities receive owned payloads after codec/storage decoding.
pub type DynamicActivityHandler = fn (mut context ActivityContext, arguments []RawValue) !PayloadValue

struct ActivityDispatch {
	named   ?ActivityHandler
	dynamic ?DynamicActivityHandler
}

pub struct Activities {
mut:
	dynamic_handler ?DynamicActivityHandler
	handlers        map[string]ActivityHandler
}

pub fn (mut activities Activities) register(name string, handler ActivityHandler) ! {
	if name == '' || name in activities.handlers {
		return error('invalid or duplicate Activity registration: ${name}')
	}
	activities.handlers[name] = handler
}

pub struct ActivityContext {
	worker_control       &WorkerControl = &WorkerControl{}
	client_configuration ClientOptions
	client_plugins       []PluginRegistration
	tracing              ?TraceOptions
	trace_headers        map[string]temporal.TemporalApiCommonV1_Payload
	typed_arguments      []PayloadValue
	argument_values      []RawValue
	metrics              &MetricMeter = &MetricMeter{}
	result_context       ?SerializationContext
	connection           &corebridge.Connection
	information          ActivityInfo
	converter            DataConverter
	logger               Logger = ConsoleLogger{}
	interceptors         []ActivityInterceptor
	task_token           []u8
	cancellation         chan ActivityCancelInfo
mut:
	trace_current      ?TraceContext
	trace_baggage      Baggage
	heartbeat_payloads []temporal.TemporalApiCommonV1_Payload
	cancel_information ActivityCancelInfo
	cancelled          bool
	deferred           bool
}

// Poll without throwing. The first observed cancellation remains authoritative.
pub fn (mut context ActivityContext) is_cancelled() bool {
	if context.cancelled { return true }
	select {
		information := <-context.cancellation {
			context.cancel_information = information
			context.cancelled = true
		}
		else {}
	}
	if !context.cancelled && context.information.is_local
		&& context.connection.shutdown_grace_expired() {
		context.cancel_information = ActivityCancelInfo{
			reason:  .worker_shutdown
			details: temporal.CoresdkActivityTask_ActivityCancellationDetails{
				is_worker_shutdown: true
			}
		}
		context.cancelled = true
	}
	return context.cancelled
}

pub fn (mut context ActivityContext) check_cancelled() ! {
	if context.is_cancelled() {
		return ActivityCancelledError{
			information: context.cancellation_snapshot()
		}
	}
}

// Wait cooperatively, preserving the last heartbeat details. Remote Activities
// continue heartbeating so server-requested cancellation can be delivered.
// Local Activities wait without sending heartbeats. Transport errors propagate.
pub fn (mut context ActivityContext) wait_cancelled() !ActivityCancelInfo {
	for {
		context.sleep(time.minute) or {
			if err is ActivityCancelledError { return context.cancellation_snapshot() }
			return err
		}
	}
	return error('unreachable Activity cancellation wait')
}

pub fn (mut context ActivityContext) heartbeat(details Arguments) ! {
	context.check_cancelled()!
	if context.information.is_local { return }
	encoded := context.result_converter().encode_arguments(details)!
	payloads := if storage := context.converter.external_storage {
		storage.store_payloads(encoded, activity_storage_context(context.information))!
	} else {
		encoded
	}
	context.connection.heartbeat(temporal.Coresdk_ActivityHeartbeat{
		task_token: context.task_token
		details:    payloads
	}.encode())!
	context.heartbeat_payloads = payloads
}

// An Activity wait reports liveness without overwriting application progress.
pub fn (mut context ActivityContext) sleep(duration time.Duration) ! {
	if duration < 0 { return error('Activity sleep duration cannot be negative') }
	watch := time.new_stopwatch()
	for watch.elapsed() < duration {
		context.check_cancelled()!
		if !context.information.is_local {
			context.connection.heartbeat(temporal.Coresdk_ActivityHeartbeat{
				task_token: context.task_token
				details:    context.heartbeat_payloads
			}.encode())!
		}
		remaining := duration - watch.elapsed()
		if remaining > 0 {
			time.sleep(if remaining < 50 * time.millisecond {
				remaining
			} else {
				50 * time.millisecond
			})
		}
	}
	context.check_cancelled()!
}

struct PollResult {
	bytes         []u8
	message       string
	storage_error string
}

struct ActivityResult {
	token  []u8
	result temporal.CoresdkActivityResult_ActivityExecutionResult
}

fn poll_activities(connection &corebridge.Connection, tasks chan PollResult) {
	for {
		bytes := connection.poll(false) or {
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

fn execute_activity_result(handler ActivityDispatch, initial_context ActivityContext, arguments []string) ActivityResult {
	mut context := initial_context
	value := execute_activity_handler(handler, mut context, arguments) or {
		mut result := temporal.CoresdkActivityResult_ActivityExecutionResult{}
		mut management_interruption := false
		if err is ActivityCancelledError {
			if context.information.is_local && err.information.reason == .worker_shutdown {
				context.worker_control.mark_local_shutdown(context.connection,
					context.information.namespace, context.information.run_id)
			}
			management_interruption = err.information.reason in [.paused, .reset]
		}
		// Pause/reset interrupt this attempt, while keeping the Activity pending.
		// Like sdk-go's separate ErrActivityPaused/ErrActivityReset causes, they
		// report retryable failures instead of workflow-requested cancellations.
		if management_interruption {
			result = activity_error(err, context.result_converter())
		} else if err.code() == 499 {
			canceled_error := if err is CancelledError || err is ActivityCancelledError {
				err
			} else {
				IError(CancelledError{})
			}
			encoded := context.result_converter().encode_failure(canceled_error,
				'VActivityCanceled') or {
				return ActivityResult{
					token:  context.task_token
					result: context.converter.store_activity_result(context.task_token, activity_conversion_failure(
						'cancellation encoding failed: ' + err.msg()),
						activity_storage_context(context.information))
				}
			}
			result.status = temporal.CoresdkActivityResult_ActivityExecutionResult_Cancelled_Case{
				value: temporal.CoresdkActivityResult_Cancellation{
					failure: encoded
				}
			}
		} else {
			result = activity_error(err, context.result_converter())
		}
		return ActivityResult{
			token:  context.task_token
			result: context.converter.store_activity_result(context.task_token, result,
				activity_storage_context(context.information))
		}
	}
	if context.deferred {
		return ActivityResult{
			token:  context.task_token
			result: temporal.CoresdkActivityResult_ActivityExecutionResult{
				status: temporal.CoresdkActivityResult_ActivityExecutionResult_WillCompleteAsync_Case{
					value: temporal.CoresdkActivityResult_WillCompleteAsync{}
				}
			}
		}
	}
	encoded := context.result_converter().encode_value(value) or {
		return ActivityResult{
			token:  context.task_token
			result: context.converter.store_activity_result(context.task_token, activity_failure(err.msg(),
				context.result_converter()), activity_storage_context(context.information))
		}
	}
	return ActivityResult{
		token:  context.task_token
		result: context.converter.store_activity_result(context.task_token, temporal.CoresdkActivityResult_ActivityExecutionResult{
			status: temporal.CoresdkActivityResult_ActivityExecutionResult_Completed_Case{
				value: temporal.CoresdkActivityResult_Success{
					result: encoded
				}
			}
		}, activity_storage_context(context.information))
	}
}

fn activity_failure(message string, converter DataConverter) temporal.CoresdkActivityResult_ActivityExecutionResult {
	encoded := converter.encode_failure(ApplicationError{
		message:       message
		category:      'VActivityError'
		non_retryable: true
	}, 'VActivityError') or {
		return activity_conversion_failure('failure encoding failed: ${err.msg()}')
	}
	return temporal.CoresdkActivityResult_ActivityExecutionResult{
		status: temporal.CoresdkActivityResult_ActivityExecutionResult_Failed_Case{
			value: temporal.CoresdkActivityResult_Failure{
				failure: encoded
			}
		}
	}
}

fn reject_activity(connection &corebridge.Connection, token []u8, message string, converter DataConverter) ! {
	connection.complete(temporal.Coresdk_ActivityTaskCompletion{
		task_token: token
		result:     activity_failure(message, converter)
	}.encode(), false)!
}

pub fn run_activities(config WorkerOptions, activities Activities) ! {
	setup := prepare_worker_plugins(config, &Runtime{}, activities, NexusServices{}, .activity)!
	run_worker_interceptors(setup)!
}

fn run_activities_configured(config WorkerOptions, activities Activities) ! {
	owner := open_worker(config, .activity)!
	connection := owner.connection
	control_id := config.control.attach(connection)
	defer {
		notify_worker_shutdown(config.worker_interceptors, WorkerShutdownInfo{
			role:       .activity
			namespace:  config.namespace
			task_queue: config.task_queue
		}, config.logger)
		config.control.detach(control_id)
		owner.close()
	}
	serve_activities(connection, config, activities, Activities{}, config.activity_concurrency)!
}

fn serve_activities(connection &corebridge.Connection, config WorkerOptions, activities Activities, local_activities Activities, concurrency int) ! {
	client_configuration, client_plugins := worker_client_configuration(config)
	metrics := (&MetricMeter{
		bridge: connection.metric_meter()!
	}).with_attributes({
		'namespace': MetricAttribute(config.namespace)
	})
	defer { metrics.close() }
	tasks := chan PollResult{cap: concurrency + 1}
	results := chan ActivityResult{cap: concurrency}
	mut active := map[string]chan ActivityCancelInfo{}
	mut stopped := false
	poller := spawn poll_activities(connection, tasks)
	// Drain every V handler and the poller before releasing their shared Core connection,
	// including protocol and completion errors. A handler must cooperate with cancellation.
	defer {
		connection.shutdown()
		shutdown_cancellation := ActivityCancelInfo{
			reason:  .worker_shutdown
			details: temporal.CoresdkActivityTask_ActivityCancellationDetails{
				is_worker_shutdown: true
			}
		}
		for _, cancellation in active {
			select {
				cancellation <- shutdown_cancellation {}
				else {}
			}
		}
		drain_activity_tasks(connection, config, tasks, results, stopped, mut active)
		poller.wait()
	}
	emit_log(config.logger, .info,
		'ACTIVITY_READY concurrency=${concurrency} queue=${config.task_queue}')
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
				task := temporal.CoresdkActivityTask_ActivityTask.decode(task_result.bytes)!
				variant := task.variant or { return error('Activity task missing variant') }
				key := task.task_token.hex()
				if variant is temporal.CoresdkActivityTask_ActivityTask_Start_Case {
					start := variant.value
					activity_converter := config.data_converter.with_context(SerializationContext{
						kind:          .activity
						namespace:     if start.workflow_namespace == '' {
							config.namespace
						} else {
							start.workflow_namespace
						}
						workflow_id:   if execution := start.workflow_execution {
							execution.workflow_id
						} else {
							''
						}
						workflow_type: start.workflow_type
						activity_id:   start.activity_id
						activity_type: start.activity_type
						task_queue:    config.task_queue
						is_local:      start.is_local
					})
					result_converter :=
						activity_converter.with_result_context(start.propagated_nexus_serialization_context)
					registry := if start.is_local { local_activities } else { activities }
					handler := registry.resolve(start.activity_type) or {
						reject_activity(connection, task.task_token,
							'unsupported activity: ${start.activity_type}', result_converter)!
						continue
					}

					cancellation := chan ActivityCancelInfo{cap: 1}
					active[key] = cancellation
					context := ActivityContext{
						worker_control:       config.control
						client_configuration: client_configuration
						client_plugins:       client_plugins
						tracing:              config.tracing
						metrics:              metrics.scope()!
						result_context:       result_converter.serialization_context
						heartbeat_payloads:   start.heartbeat_details.clone()
						connection:           connection
						task_token:           task.task_token
						cancellation:         cancellation
						converter:            activity_converter
						logger:               config.logger
						interceptors:         config.activity_interceptors
						information:          ActivityInfo{
							namespace:                    config.namespace
							deadline:                     activity_deadline(start)
							workflow_type:                start.workflow_type
							task_queue:                   config.task_queue
							scheduled_at:                 activity_timestamp(start.scheduled_time)
							current_attempt_scheduled_at: activity_timestamp(start.current_attempt_scheduled_time)
							started_at:                   activity_timestamp(start.started_time)
							schedule_to_close:            activity_duration(start.schedule_to_close_timeout)
							start_to_close:               activity_duration(start.start_to_close_timeout)
							heartbeat_timeout:            activity_duration(start.heartbeat_timeout)
							retry_policy:                 start.retry_policy
							priority:                     start.priority
							workflow_id:                  if execution := start.workflow_execution {
								execution.workflow_id
							} else {
								''
							}
							run_id:                       activity_run_id(start)
							is_standalone:                start.run_id != ''
							activity_id:                  start.activity_id
							activity_type:                start.activity_type
							attempt:                      start.attempt
							is_local:                     start.is_local
							workflow_namespace:           start.workflow_namespace
						}
					}
					run_id := activity_run_id(start)
					emit_log(config.logger, .info,
						'ACTIVITY_START type=${start.activity_type} id=${start.activity_id} run=${run_id} active=${active.len}')
					spawn prepare_activity(handler, context, start, results)
				} else if variant is temporal.CoresdkActivityTask_ActivityTask_Cancel_Case {
					if cancellation := active[key] {
						select {
							cancellation <- ActivityCancelInfo{
								reason:  variant.value.reason
								details: variant.value.details
							} {}
							else {}
						}
						emit_log(config.logger, .info, 'ACTIVITY_CANCEL active=${active.len}')
					}
				}
			}
			result := <-results {
				active.delete(result.token.hex())
				connection.complete(temporal.Coresdk_ActivityTaskCompletion{
					task_token: result.token
					result:     result.result
				}.encode(), false)!
				emit_log(config.logger, .info, 'ACTIVITY_REPORTED active=${active.len}')
			}
		}
	}
	emit_log(config.logger, .info, 'ACTIVITY_SHUTDOWN complete')
}

fn activity_run_id(start temporal.CoresdkActivityTask_Start) string {
	if start.run_id != '' { return start.run_id }
	if execution := start.workflow_execution { return execution.run_id }
	return ''
}

pub fn (mut activities Activities) register_dynamic(handler DynamicActivityHandler) ! {
	if _ := activities.dynamic_handler { return error('dynamic Activity already registered') }
	activities.dynamic_handler = handler
}

fn (activities Activities) resolve(name string) !ActivityDispatch {
	if named := activities.handlers[name] { return ActivityDispatch{
			named: named
		} }
	if dynamic := activities.dynamic_handler { return ActivityDispatch{
			dynamic: dynamic
		} }
	return error('unsupported activity: ' + name)
}

fn (context &ActivityContext) result_converter() DataConverter {
	if scope := context.result_context { return context.converter.with_context(scope) }
	return context.converter
}

pub fn (context &ActivityContext) result_serialization_context() SerializationContext {
	return context.result_converter().serialization_context
}

pub fn (context &ActivityContext) input_values() []RawValue {
	return context.argument_values.clone()
}

pub fn (context &ActivityContext) typed_input_values() ![]PayloadValue {
	mut result := []PayloadValue{}
	for value in context.typed_arguments {
		result << clone_payload_value(value)
	}
	return result
}

fn drain_activity_tasks(connection &corebridge.Connection, config WorkerOptions, tasks chan PollResult, results chan ActivityResult, initial_stopped bool, mut active map[string]chan ActivityCancelInfo) {
	mut stopped := initial_stopped
	for !stopped || active.len > 0 {
		select {
			task := <-tasks {
				if task.bytes.len == 0 || task.message != '' {
					stopped = true
					continue
				}
				pending := temporal.CoresdkActivityTask_ActivityTask.decode(task.bytes) or {
					continue
				}
				pending_variant := pending.variant or { continue }
				if pending_variant is temporal.CoresdkActivityTask_ActivityTask_Start_Case {
					connection.complete(temporal.Coresdk_ActivityTaskCompletion{
						task_token: pending.task_token
						result:     activity_error(error('worker stopped before Activity dispatch'),
							config.data_converter)
					}.encode(), false) or {
						emit_log(config.logger, .error,
							'Activity drain rejection failed: ${err.msg()}')
					}
				}
			}
			result := <-results {
				active.delete(result.token.hex())
				connection.complete(temporal.Coresdk_ActivityTaskCompletion{
					task_token: result.token
					result:     result.result
				}.encode(), false) or {
					emit_log(config.logger, .error,
						'Activity drain completion failed: ${err.msg()}')
				}
			}
		}
	}
}
