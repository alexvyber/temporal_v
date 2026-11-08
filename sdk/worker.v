module sdk

import corebridge
import os
import temporal
import sync

pub fn run_workflows(config WorkerOptions, runtime &Runtime) ! {
	setup := prepare_worker_plugins(config, runtime, Activities{}, NexusServices{}, .workflow)!
	run_worker_interceptors(setup)!
}

// One Core worker polls workflows, local/remote Activities and registered Nexus
// services. Core eager delivery uses the same slots as ordinary polling.
@[params]
pub struct WorkerServices {
pub:
	nexus NexusServices
}

pub fn run_worker(config WorkerOptions, runtime &Runtime, activities Activities, services WorkerServices) ! {
	setup := prepare_worker_plugins(config, runtime, activities, services.nexus, .combined)!
	run_worker_interceptors(setup)!
}

fn run_workflow_worker(config WorkerOptions, mut runtime Runtime, activities Activities, combined bool, services NexusServices) ! {
	runtime.validate_versioning(config)!
	nexus_enabled := combined && services.operations.len > 0
	owner := open_worker(config, if nexus_enabled {
		.all
	} else if combined {
		.combined
	} else {
		.workflow
	})!
	connection := owner.connection
	metrics := &MetricMeter{
		bridge: connection.metric_meter() or {
			owner.close()
			return err
		}
	}
	runtime.metrics = metrics
	control_id := config.control.attach(connection)
	local_done := chan string{cap: 1}
	nexus_done := chan string{cap: 1}
	if nexus_enabled {
		spawn run_nexus_handlers(connection, config, services, nexus_done)
	} else {
		nexus_done <- ''
	}
	spawn run_activity_handlers(connection, config, activities, runtime.local_handlers, combined,
		local_done)
	mut handlers_joined := false
	defer {
		connection.shutdown()
		if !handlers_joined {
			message := <-local_done
			if message != '' {
				emit_log(config.logger, .error, 'LOCAL_ACTIVITY_WORKER_FAILED message=${message}')
			}
			nexus_message := <-nexus_done
			if nexus_message != '' {
				emit_log(config.logger, .error, 'NEXUS_WORKER_FAILED message=${nexus_message}')
			}
		}
		notify_worker_shutdown(config.worker_interceptors, WorkerShutdownInfo{
			role:       if combined {
				.combined
			} else {
				.workflow
			}
			namespace:  config.namespace
			task_queue: config.task_queue
		}, config.logger)
		// Slot dispatchers must remain alive until metric leases release Core's worker.
		metrics.close()
		config.control.detach(control_id)
		owner.close()
	}
	emit_log(config.logger, .info,
		'WORKFLOW_READY pid=${os.getpid()} queue=${config.task_queue} cached_runs=${runtime.cached_runs()}')
	serve_workflows(connection, config, mut runtime)!
	connection.shutdown()
	activity_message := <-local_done
	nexus_message := <-nexus_done
	handlers_joined = true
	if activity_message != '' { return error('Activity handler loop failed: ${activity_message}') }
	if nexus_message != '' { return error('Nexus handler loop failed: ${nexus_message}') }
	emit_log(config.logger, .info,
		'WORKFLOW_SHUTDOWN complete cached_runs=${runtime.cached_runs()}')
}

fn poll_workflows(connection &corebridge.Connection, config WorkerOptions, tasks chan PollResult) {
	mut preparations := sync.new_waitgroup()
	mut poll_error := ''
	// Later activations omit workflow identity. Keep it in the poller instead
	// of reading mutable workflow state from asynchronous retrieval threads.
	mut storage_targets := map[string]StorageContext{}
	for {
		bytes := connection.poll(true) or {
			poll_error = err.msg()
			break
		}
		if bytes.len == 0 {
			break
		}
		activation := temporal.CoresdkWorkflowActivation_WorkflowActivation.decode(bytes) or {
			tasks <- PollResult{
				bytes: bytes
			}
			continue
		}
		mut target := storage_targets[activation.run_id] or {
			StorageContext{
				target: SerializationContext{
					kind:      .workflow
					namespace: config.namespace
				}
				run_id: activation.run_id
			}
		}
		mut evicted := false
		for job in activation.jobs {
			variant := job.variant or { continue }
			if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_InitializeWorkflow_Case {
				target = StorageContext{
					target: SerializationContext{
						kind:          .workflow
						namespace:     config.namespace
						workflow_id:   variant.value.workflow_id
						workflow_type: variant.value.workflow_type
					}
					run_id: activation.run_id
				}
				storage_targets[activation.run_id] = target
			}
			if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_RemoveFromCache_Case {
				evicted = true
			}
		}
		if evicted { storage_targets.delete(activation.run_id) }
		preparations.add(1)
		spawn prepare_workflow_activation(bytes, config, target, tasks, mut preparations)
	}
	// Every outstanding retrieval publishes its activation before the final
	// shutdown marker; the coordinator keeps completing them while draining.
	preparations.wait()
	tasks <- PollResult{
		message: poll_error
	}
}

fn prepare_workflow_activation(bytes []u8, config WorkerOptions, target StorageContext, tasks chan PollResult, mut preparations sync.WaitGroup) {
	defer { preparations.done() }
	prepared := config.data_converter.storage_message(.workflow_activation, bytes, false, target, corebridge.PayloadMessageOptions{}) or {
		emit_log(config.logger, .error, 'EXTERNAL_STORAGE_RETRIEVE_FAILED message=${err.msg()}')
		tasks <- PollResult{
			bytes:         bytes
			storage_error: err.msg()
		}
		return
	}
	tasks <- PollResult{
		bytes: prepared
	}
}

fn complete_stored_activation(connection &corebridge.Connection, bytes []u8, activation temporal.CoresdkWorkflowActivation_WorkflowActivation, converter DataConverter, context WorkflowStorageTargets, logger Logger, interceptors []WorkerInterceptor, results chan string) {
	completion := temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion.decode(bytes) or {
		results <- err.msg()
		return
	}
	mut output := bytes
	if status := completion.status {
		if status is temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Successful_Case {
			commands := store_workflow_commands(status.value.commands, activation, converter,
				context) or {
				// Failed WFT completions make Core evict this run before any
				// subsequent activation. Never commit partially stored commands.
				emit_log(logger, .error,
					'EXTERNAL_STORAGE_STORE_FAILED run=${activation.run_id} message=${err.msg()}')
				complete_activation(connection, storage_activation_failure(activation.run_id, err),
					interceptors, logger, results)
				return
			}
			output = temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
				...completion
				status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Successful_Case{
					value: temporal.CoresdkWorkflowCompletion_Success{
						...status.value
						commands: commands
					}
				}
			}.encode()
		}
	}
	complete_activation(connection, output, interceptors, logger, results)
}

fn complete_activation(connection &corebridge.Connection, completion []u8, interceptors []WorkerInterceptor, logger Logger, results chan string) {
	notify_worker_completion(interceptors, completion, logger)
	connection.complete(completion, true) or {
		results <- err.msg()
		return
	}
	results <- ''
}

struct WorkflowActivationResult {
	run_id  string
	message string
	cached  bool
	failure ?IError
}

fn isolated_runtime(runtime &Runtime) &Runtime {
	return &Runtime{
		metrics:              runtime.metrics
		dynamic_factory:      runtime.dynamic_factory
		dynamic_registration: runtime.dynamic_registration
		factories:            runtime.factories.clone()
		registrations:        runtime.registrations.clone()
	}
}

// Only this task accesses the run's mutable state until it publishes its result.
fn execute_workflow_activation(connection &corebridge.Connection, config WorkerOptions, mut runtime Runtime, task PollResult, activation temporal.CoresdkWorkflowActivation_WorkflowActivation, draining bool, results chan WorkflowActivationResult) {
	completed := chan string{cap: 1}
	local_shutdown := interrupted_local_shutdown(config, activation)
	if draining || task.storage_error != '' || local_shutdown {
		runtime.forget(activation.run_id)
		completion := if local_shutdown {
			activation_observation_failure(activation.run_id,
				error('worker stopped while a local Activity awaited cancellation'))
		} else if task.storage_error != '' {
			storage_activation_failure(activation.run_id, error(task.storage_error))
		} else {
			temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
				run_id: activation.run_id
				status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Successful_Case{
					value: temporal.CoresdkWorkflowCompletion_Success{}
				}
			}.encode()
		}
		complete_activation(connection, completion, config.worker_interceptors, config.logger,
			completed)
	} else {
		observe_workflow_activation(runtime, activation, config) or {
			runtime.forget(activation.run_id)
			complete_activation(connection, activation_observation_failure(activation.run_id, err),
				config.worker_interceptors, config.logger, completed)
			message := <-completed
			mut fatal := ?IError(none)
			if err is WorkerInterceptorError { fatal = err }
			results <- WorkflowActivationResult{
				run_id:  activation.run_id
				message: if fatal != none {
					err.msg()
				} else {
					message
				}
				failure: fatal
			}
			return
		}
		storage_context := workflow_storage_context(runtime, activation, config)
		completion := activation_completion(mut runtime, activation, config)
		complete_stored_activation(connection, completion, activation, config.data_converter,
			storage_context, config.logger, config.worker_interceptors, completed)
	}
	message := <-completed
	results <- WorkflowActivationResult{
		run_id:  activation.run_id
		message: message
		cached:  runtime.cached_runs() > 0
	}
}

fn serve_workflows(connection &corebridge.Connection, config WorkerOptions, mut runtime Runtime) ! {
	tasks := chan PollResult{cap: 1}
	results := chan WorkflowActivationResult{cap: 1}
	poller := spawn poll_workflows(connection, config, tasks)
	mut actors := map[string]&Runtime{}
	mut busy := map[string]bool{}
	mut queued := []PollResult{}
	mut stopped := false
	mut pending := 0
	mut failure_message := ''
	mut fatal_failure := ?IError(none)
	defer {
		for _, mut actor in actors {
			for id in actor.runs.keys() {
				actor.forget(id)
			}
		}
		poller.wait()
	}
	for !stopped || pending > 0 || queued.len > 0 {
		// A coordinator owns registry and scheduling state. Each actor owns one
		// run, and is never touched concurrently, including during eviction.
		mut index := 0
		for index < queued.len && pending < config.workflow_concurrency {
			task := queued[index]
			activation := temporal.CoresdkWorkflowActivation_WorkflowActivation.decode(task.bytes) or {
				if failure_message == '' {
					failure_message = err.msg()
					connection.shutdown()
				}
				queued.delete(index)
				continue
			}
			if busy[activation.run_id] {
				index++
				continue
			}
			queued.delete(index)
			mut actor := actors[activation.run_id] or {
				created := isolated_runtime(runtime)
				actors[activation.run_id] = created
				created
			}
			busy[activation.run_id] = true
			pending++
			spawn execute_workflow_activation(connection, config, mut actor, task, activation,
				failure_message != '', results)
		}
		if stopped && pending == 0 && queued.len == 0 {
			break
		}
		select {
			task := <-tasks {
				if task.message != '' || task.bytes.len == 0 {
					stopped = true
					if task.message != '' && failure_message == '' {
						failure_message = task.message
						connection.shutdown()
					}
				} else {
					queued << task
				}
			}
			result := <-results {
				pending--
				busy.delete(result.run_id)
				if !result.cached { actors.delete(result.run_id) }
				if result.message != '' && failure_message == '' {
					failure_message = result.message
					fatal_failure = result.failure
					connection.shutdown()
				}
			}
		}
	}
	if failure := fatal_failure { return failure }
	if failure_message != '' { return error(failure_message) }
}

fn activation_completion(mut runtime Runtime, activation temporal.CoresdkWorkflowActivation_WorkflowActivation, config WorkerOptions) []u8 {
	emit_log(config.logger, .info,
		'ACTIVATION run=${activation.run_id} replay=${activation.is_replaying} history_length=${activation.history_length}')
	for job in activation.jobs {
		variant := job.variant or { continue }
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_SignalWorkflow_Case {
			emit_log(config.logger, .info,
				'SIGNAL run=${activation.run_id} replay=${activation.is_replaying} name=${variant.value.signal_name}')
		}
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_QueryWorkflow_Case {
			emit_log(config.logger, .info,
				'QUERY run=${activation.run_id} name=${variant.value.query_type}')
		}
	}
	behavior := runtime.activation_versioning(activation)
	commands := runtime.activate(activation, config) or {
		if err is NondeterminismError {
			target := workflow_storage_context(runtime, activation, config).current.target
			if config.nondeterminism_as_workflow_fail
				|| target.workflow_type in config.nondeterminism_as_workflow_fail_for_types {
				encoded := config.data_converter.with_context(target).encode_failure(ApplicationError{
					message:       err.msg()
					category:      'NondeterminismError'
					non_retryable: true
				}, 'NondeterminismError') or {
					runtime.forget(activation.run_id)
					return temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
						run_id: activation.run_id
						status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Failed_Case{
							value: temporal.CoresdkWorkflowCompletion_Failure{
								failure:     failure(err.msg(), 'VActivationError')
								force_cause: if err is NondeterminismError {
									.workflow_task_failed_cause_non_deterministic_error
								} else {
									.workflow_task_failed_cause_unspecified
								}
							}
						}
					}.encode()
				}
				runtime.forget(activation.run_id)
				command := temporal.CoresdkWorkflowCommands_WorkflowCommand{
					variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_FailWorkflowExecution_Case{
						value: temporal.CoresdkWorkflowCommands_FailWorkflowExecution{
							failure: encoded
						}
					}
				}
				return temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
					run_id: activation.run_id
					status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Successful_Case{
						value: temporal.CoresdkWorkflowCompletion_Success{
							commands:            [command]
							versioning_behavior: behavior
						}
					}
				}.encode()
			}
		}
		runtime.forget(activation.run_id)
		completion := temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
			run_id: activation.run_id
			status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Failed_Case{
				value: temporal.CoresdkWorkflowCompletion_Failure{
					failure:     activation_error_failure(err)
					force_cause: if err is NondeterminismError {
						.workflow_task_failed_cause_non_deterministic_error
					} else {
						.workflow_task_failed_cause_unspecified
					}
				}
			}
		}
		emit_log(config.logger, .error,
			'ACTIVATION_FAILED run=${activation.run_id} message=${err.msg()}')
		return completion.encode()
	}
	for command in commands {
		variant := command.variant or { continue }
		match variant {
			temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleActivity_Case {
				emit_log(config.logger, .info,
					'COMMAND run=${activation.run_id} replay=${activation.is_replaying} schedule_activity seq=${variant.value.seq}')
			}
			temporal.CoresdkWorkflowCommands_WorkflowCommand_StartTimer_Case {
				emit_log(config.logger, .info,
					'COMMAND run=${activation.run_id} replay=${activation.is_replaying} start_timer seq=${variant.value.seq}')
			}
			else {
				emit_log(config.logger, .info,
					'COMMAND run=${activation.run_id} replay=${activation.is_replaying} ${variant.type_name()}')
			}
		}
	}
	return temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
		run_id: activation.run_id
		status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Successful_Case{
			value: temporal.CoresdkWorkflowCompletion_Success{
				commands:            commands
				versioning_behavior: behavior
			}
		}
	}.encode()
}

fn run_activity_handlers(connection &corebridge.Connection, config WorkerOptions, activities Activities, local_activities Activities, combined bool, done chan string) {
	concurrency := config.local_activity_concurrency + if combined {
		config.activity_concurrency
	} else {
		0
	}
	serve_activities(connection, config, activities, local_activities, concurrency) or {
		connection.shutdown()
		done <- err.msg()
		return
	}
	done <- ''
}

fn run_nexus_handlers(connection &corebridge.Connection, config WorkerOptions, services NexusServices, done chan string) {
	serve_nexus(connection, config, services) or {
		connection.shutdown()
		done <- err.msg()
		return
	}
	done <- ''
}

fn interrupted_local_shutdown(config WorkerOptions, activation temporal.CoresdkWorkflowActivation_WorkflowActivation) bool {
	mut effective := false
	mut eviction := false
	for job in activation.jobs {
		variant := job.variant or { continue }
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_RemoveFromCache_Case {
			eviction = true
		} else if variant !is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_QueryWorkflow_Case {
			effective = true
		}
	}
	if !effective && !eviction { return false }
	interrupted := config.control.consume_local_shutdown(config.namespace, activation.run_id)
	return interrupted && effective
}
