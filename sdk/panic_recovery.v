module sdk

import temporal

#flag linux -rdynamic

// PanicError represents a V panic recovered at an application callback boundary.
// Native faults and process termination are outside this boundary.
pub struct PanicError {
pub:
	message     string
	stack_trace string
}

pub fn (failure PanicError) msg() string {
	return failure.message
}

pub fn (failure PanicError) code() int {
	return 500
}

struct CallbackOutcome {
mut:
	failure ?IError
}

struct CallbackValueOutcome {
mut:
	failure ?IError
	value   PayloadValue
}

struct ActivationOutcome {
mut:
	failure  ?IError
	commands []temporal.CoresdkWorkflowCommands_WorkflowCommand
}

fn (mut runtime Runtime) activate(activation temporal.CoresdkWorkflowActivation_WorkflowActivation, options WorkerOptions) ![]temporal.CoresdkWorkflowCommands_WorkflowCommand {
	mut outcome := ActivationOutcome{}
	runtime.activate_recovered(activation, options, mut outcome)
	if failure := outcome.failure {
		if run := runtime.runs[activation.run_id] {
			if forced := run.context.forced_task_error { return forced }
		}
		if failure is PanicError && options.workflow_panic_policy == .fail_workflow {
			target := workflow_storage_context(runtime, activation, options).current.target
			encoded := options.data_converter.with_context(target).encode_failure(failure,
				'PanicError')!
			runtime.forget(activation.run_id)
			return [
				temporal.CoresdkWorkflowCommands_WorkflowCommand{
					variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_FailWorkflowExecution_Case{
						value: temporal.CoresdkWorkflowCommands_FailWorkflowExecution{
							failure: encoded
						}
					}
				},
			]
		}
		return failure
	}
	return outcome.commands
}

fn (mut runtime Runtime) activate_recovered(activation temporal.CoresdkWorkflowActivation_WorkflowActivation, options WorkerOptions, mut outcome ActivationOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	outcome.commands = runtime.activate_callback(activation, options) or {
		outcome.failure = err
		return
	}
}

struct NexusCallbackOutcome {
mut:
	failure ?IError
	result  NexusResult
}

fn invoke_nexus_start(handler NexusHandler, mut context NexusContext, input string) !NexusResult {
	mut outcome := NexusCallbackOutcome{}
	recover_nexus_start(handler, mut context, input, mut outcome)
	if failure := outcome.failure { return failure }
	return outcome.result
}

fn recover_nexus_start(handler NexusHandler, mut context NexusContext, input string, mut outcome NexusCallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	outcome.result = handler(mut context, input) or {
		outcome.failure = err
		return
	}
}

fn invoke_nexus_cancel(handler NexusCancelHandler, mut context NexusContext, token string) ! {
	mut outcome := CallbackOutcome{}
	recover_nexus_cancel(handler, mut context, token, mut outcome)
	if failure := outcome.failure { return failure }
}

fn recover_nexus_cancel(handler NexusCancelHandler, mut context NexusContext, token string, mut outcome CallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	handler(mut context, token) or {
		outcome.failure = err
		return
	}
}

fn activity_panic_failure(message string, stack_trace string) temporal.CoresdkActivityResult_ActivityExecutionResult {
	return temporal.CoresdkActivityResult_ActivityExecutionResult{
		status: temporal.CoresdkActivityResult_ActivityExecutionResult_Failed_Case{
			value: temporal.CoresdkActivityResult_Failure{
				failure: temporal.TemporalApiFailureV1_Failure{
					message:      protocol_text(message)
					source:       'V'
					stack_trace:  protocol_text(stack_trace)
					failure_info: temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case{
						value: temporal.TemporalApiFailureV1_ApplicationFailureInfo{
							type_: 'PanicError'
						}
					}
				}
			}
		}
	}
}

// Workflow-task failures bypass application converters so a failing converter
// cannot prevent a panic from being reported to Core.
fn activation_error_failure(err IError) temporal.TemporalApiFailureV1_Failure {
	mut result := failure(err.msg(), 'VActivationError')
	if err is PanicError { result.stack_trace = protocol_text(err.stack_trace) }
	return result
}
