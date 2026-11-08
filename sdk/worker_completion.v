module sdk

import temporal
import corebridge

// Called immediately before submitting a completion to Core, after outbound
// codecs and external storage. Each observer receives an independent snapshot.
// Calls for different runs may overlap. This hook cannot change the completion.
pub interface WorkerCompletionInterceptor {
	on_workflow_activation_completion(completion temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion)
}

fn notify_worker_completion(interceptors []WorkerInterceptor, bytes []u8, logger Logger) {
	for interceptor in interceptors {
		if interceptor is WorkerCompletionInterceptor {
			mut outcome := CallbackOutcome{}
			recover_worker_completion(interceptor, bytes, mut outcome)
			if failure := outcome.failure {
				log_worker_interceptor_failure(logger, 'completion', failure)
			}
		}
	}
}

fn recover_worker_completion(interceptor WorkerCompletionInterceptor, bytes []u8, mut outcome CallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	completion := temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion.decode(bytes.clone()) or {
		outcome.failure = err
		return
	}
	interceptor.on_workflow_activation_completion(completion)
}

fn complete_replay_activation(connection &corebridge.Connection, bytes []u8, options ReplayOptions) ! {
	notify_worker_completion(options.worker_interceptors, bytes, ConsoleLogger{})
	connection.complete(bytes, true)!
}
