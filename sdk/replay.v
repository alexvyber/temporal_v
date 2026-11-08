module sdk

import corebridge
import temporal
import time

pub struct ReplayOptions {
pub:
	worker_interceptors           []WorkerInterceptor      @[json: '-']
	patch_activation_callback     ?PatchActivationCallback @[json: '-']
	experimental_event_groups     bool
	workflow_panic_policy         WorkflowPanicPolicy
	tracing                       ?TraceOptions
	plugins                       []Plugin
	namespace                     string = 'default'
	task_queue                    string = 'replay'
	workflow_id                   string
	data_converter                DataConverter
	command_interceptors          []CommandInterceptor
	workflow_interceptors         []WorkflowInterceptor
	query_interceptors            []QueryInterceptor
	update_validator_interceptors []UpdateValidatorInterceptor
	timeout                       time.Duration = 30 * time.second
}

pub struct ReplayResult {
pub:
	run_id      string
	activations int
}

fn replay_deadline(connection &corebridge.Connection, timeout time.Duration, stop chan bool) {
	select {
		_ := <-stop {}
		timeout {
			connection.shutdown()
		}
	}
}

// Replays binary temporal.api.history.v1.History with no server connection and
// no Activity execution. Every invocation uses an isolated workflow cache.
pub fn (runtime &Runtime) replay(history []u8, original ReplayOptions) !ReplayResult {
	setup := prepare_replay_plugins(runtime, original)!
	return replay_worker_interceptors(setup, history)!
}

fn (runtime &Runtime) replay_configured(history []u8, options ReplayOptions) !ReplayResult {
	if options.workflow_id == '' || options.namespace == '' || options.task_queue == ''
		|| options.timeout <= 0 {
		return error('replay requires workflow ID, namespace, queue and positive timeout')
	}
	decoded := temporal.TemporalApiHistoryV1_History.decode(history)!
	if decoded.events.len == 0 { return error('cannot replay empty history') }
	connection := corebridge.replay_open(options.namespace, options.task_queue)!
	stop := chan bool{cap: 1}
	guard := spawn replay_deadline(connection, options.timeout, stop)
	defer {
		stop <- true
		guard.wait()
		connection.shutdown()
		// Acknowledge pending evictions before finalizing Core, including failures.
		for {
			bytes := connection.poll(true) or { break }
			if bytes.len == 0 {
				break
			}
			activation := temporal.CoresdkWorkflowActivation_WorkflowActivation.decode(bytes) or {
				break
			}
			complete_replay_activation(connection, temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
				run_id: activation.run_id
				status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Successful_Case{
					value: temporal.CoresdkWorkflowCompletion_Success{}
				}
			}.encode(), options) or { break }
		}
		notify_worker_shutdown(options.worker_interceptors, WorkerShutdownInfo{
			role:       .workflow
			replaying:  true
			namespace:  options.namespace
			task_queue: options.task_queue
		}, ConsoleLogger{})
		connection.close()
	}
	connection.replay_push(options.workflow_id, history)!
	mut isolated := Runtime{
		factories:            runtime.factories.clone()
		registrations:        runtime.registrations.clone()
		dynamic_factory:      runtime.dynamic_factory
		dynamic_registration: runtime.dynamic_registration
	}
	mut activations := 0
	mut run_id := ''
	for {
		bytes := connection.poll(true)!
		if bytes.len == 0 {
			return error('replay stopped before completion (deadline or shutdown)')
		}
		activation := temporal.CoresdkWorkflowActivation_WorkflowActivation.decode(bytes)!
		run_id = activation.run_id
		activations++
		mut eviction := ?temporal.CoresdkWorkflowActivation_RemoveFromCache(none)
		for job in activation.jobs {
			variant := job.variant or { continue }
			if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_RemoveFromCache_Case {
				eviction = variant.value
			}
		}
		observe_workflow_activation(&isolated, activation, WorkerOptions{
			worker_interceptors: options.worker_interceptors
			namespace:           options.namespace
			task_queue:          options.task_queue
			data_converter:      options.data_converter
		}) or {
			complete_replay_activation(connection, activation_observation_failure(run_id, err),
				options) or {}
			return err
		}
		behavior := isolated.activation_versioning(activation)
		commands := isolated.activate(activation, WorkerOptions{
			patch_activation_callback:     options.patch_activation_callback
			experimental_event_groups:     options.experimental_event_groups
			workflow_panic_policy:         options.workflow_panic_policy
			tracing:                       options.tracing
			namespace:                     options.namespace
			task_queue:                    options.task_queue
			data_converter:                options.data_converter
			command_interceptors:          options.command_interceptors
			workflow_interceptors:         options.workflow_interceptors
			query_interceptors:            options.query_interceptors
			update_validator_interceptors: options.update_validator_interceptors
		}) or {
			complete_replay_activation(connection, temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
				run_id: run_id
				status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Failed_Case{
					value: temporal.CoresdkWorkflowCompletion_Failure{
						failure: failure(err.msg(), 'VReplayError')
					}
				}
			}.encode(), options)!
			return error('replay activation failed: ${err.msg()}')
		}
		complete_replay_activation(connection, temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
			run_id: run_id
			status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Successful_Case{
				value: temporal.CoresdkWorkflowCompletion_Success{
					commands:            commands
					versioning_behavior: behavior
				}
			}
		}.encode(), options)!
		if removed := eviction {
			if removed.reason != .lang_requested && removed.reason != .cache_full {
				return error('replay failed (${removed.reason}): ${removed.message}')
			}
			return ReplayResult{
				run_id:      run_id
				activations: activations
			}
		}
	}
	return error('unreachable replay state')
}
