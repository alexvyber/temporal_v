module sdk

import temporal

// Observes codec/storage-decoded protobuf payloads before workflow factories or
// handlers run. Application payload conversion has not occurred. Each callback
// receives an owned snapshot. An error or panic stops and drains the worker.
pub interface WorkerActivationInterceptor {
	on_workflow_activation(activation temporal.CoresdkWorkflowActivation_WorkflowActivation) !
}

pub struct WorkerInterceptorError {
pub:
	phase string
	cause IError
}

pub fn (failure WorkerInterceptorError) msg() string {
	return 'worker ${failure.phase} interceptor failed: ${failure.cause.msg()}'
}

pub fn (failure WorkerInterceptorError) code() int {
	return failure.cause.code()
}

struct ObservedActivationOutcome {
mut:
	failure    ?IError
	activation temporal.CoresdkWorkflowActivation_WorkflowActivation
}

fn has_activation_interceptors(interceptors []WorkerInterceptor) bool {
	for interceptor in interceptors {
		if interceptor is WorkerActivationInterceptor { return true }
	}
	return false
}

fn observe_workflow_activation(runtime &Runtime, activation temporal.CoresdkWorkflowActivation_WorkflowActivation, options WorkerOptions) ! {
	if !has_activation_interceptors(options.worker_interceptors) { return }
	mut outcome := ObservedActivationOutcome{}
	recover_observed_activation(runtime, activation, options, mut outcome)
	if failure := outcome.failure { return failure }
	bytes := outcome.activation.encode()
	for interceptor in options.worker_interceptors {
		if interceptor is WorkerActivationInterceptor {
			mut callback := CallbackOutcome{}
			recover_activation_interceptor(interceptor, bytes, mut callback)
			if failure := callback.failure {
				return WorkerInterceptorError{
					phase: 'activation'
					cause: failure
				}
			}
		}
	}
}

fn recover_activation_interceptor(interceptor WorkerActivationInterceptor, bytes []u8, mut outcome CallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	owned := temporal.CoresdkWorkflowActivation_WorkflowActivation.decode(bytes.clone()) or {
		outcome.failure = err
		return
	}
	interceptor.on_workflow_activation(owned) or { outcome.failure = err }
}

fn recover_observed_activation(runtime &Runtime, activation temporal.CoresdkWorkflowActivation_WorkflowActivation, options WorkerOptions, mut outcome ObservedActivationOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	outcome.activation = decoded_observed_activation(runtime, activation, options) or {
		outcome.failure = err
		return
	}
}

fn decoded_observed_activation(runtime &Runtime, activation temporal.CoresdkWorkflowActivation_WorkflowActivation, options WorkerOptions) !temporal.CoresdkWorkflowActivation_WorkflowActivation {
	// Core may share buffers with the task coordinator. Observation must never
	// expose those buffers, even to a codec that modifies its input in place.
	owned := temporal.CoresdkWorkflowActivation_WorkflowActivation.decode(activation.encode())!
	target := workflow_storage_context(runtime, owned, options).current.target
	converter := if run := runtime.runs[owned.run_id] {
		run.context.converter
	} else {
		options.data_converter.with_context(workflow_serialization_context(options.namespace,
			target.workflow_id))
	}
	mut jobs := []temporal.CoresdkWorkflowActivation_WorkflowActivationJob{cap: owned.jobs.len}
	for job in owned.jobs {
		mut scoped := converter
		variant := job.variant or { return error('activation observation job has no variant') }
		if run := runtime.runs[owned.run_id] {
			match variant {
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveActivity_Case {
					sequence := run.context.local_activity_sequences[variant.value.seq] or {
						variant.value.seq
					}
					scope := run.context.activity_contexts[sequence] or {
						return error('activation observation has no Activity serialization scope')
					}
					scoped = converter.with_context(scope)
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveChildWorkflowExecution_Case {
					scope := run.context.child_contexts[variant.value.seq] or {
						return error('activation observation has no child serialization scope')
					}
					scoped = converter.with_context(scope)
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveNexusOperationStart_Case {
					scope := run.context.nexus_contexts[variant.value.seq] or {
						return error('activation observation has no Nexus serialization scope')
					}
					scoped = converter.with_context(scope)
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveNexusOperation_Case {
					scope := run.context.nexus_contexts[variant.value.seq] or {
						return error('activation observation has no Nexus serialization scope')
					}
					scoped = converter.with_context(scope)
				}
				else {}
			}
		}
		jobs << activation_decode_coresdkworkflowactivation_workflowactivationjob(job, scoped, 0)!
	}
	return temporal.CoresdkWorkflowActivation_WorkflowActivation{
		...owned
		jobs: jobs
	}
}

fn activation_observation_failure(run_id string, failure IError) []u8 {
	return temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion{
		run_id: run_id
		status: temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Failed_Case{
			value: temporal.CoresdkWorkflowCompletion_Failure{
				failure:     activation_error_failure(if failure is WorkerInterceptorError {
					failure.cause
				} else {
					failure
				})
				force_cause: if failure is ExternalStorageError {
					.workflow_task_failed_cause_external_storage_failure
				} else {
					.workflow_task_failed_cause_unspecified
				}
			}
		}
	}.encode()
}
