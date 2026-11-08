module sdk

import sync

// Middleware runs outside deterministic workflow execution. Implementations may
// be shared by workers and must synchronize their own mutable state.
pub interface WorkerInterceptor {
	name() string
}

pub interface WorkerRunInterceptor {
	run_worker(input WorkerSetup, next WorkerRunNext) !
}

pub interface WorkerReplayInterceptor {
	replay_worker(input ReplaySetup, history []u8, next WorkerReplayNext) !ReplayResult
}

pub interface WorkerShutdownInterceptor {
	on_worker_shutdown(info WorkerShutdownInfo)
}

pub struct WorkerShutdownInfo {
pub:
	role       WorkerRole
	replaying  bool
	namespace  string
	task_queue string
}

struct WorkerContinuationState {
mut:
	mutex  &sync.Mutex = sync.new_mutex()
	used   bool
	active bool = true
}

fn (mut state WorkerContinuationState) consume() ! {
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.used { return error('worker interceptor continuation already used') }
	if !state.active { return error('worker interceptor continuation is no longer active') }
	state.used = true
}

fn (mut state WorkerContinuationState) deactivate() {
	state.mutex.lock()
	defer { state.mutex.unlock() }
	state.active = false
}

pub struct WorkerRunNext {
	interceptors []WorkerInterceptor
	index        int
	role         WorkerRole
	state        &WorkerContinuationState = &WorkerContinuationState{}
}

pub fn (next WorkerRunNext) execute(input WorkerSetup) ! {
	mut state := next.state
	state.consume()!
	if input.role != next.role { return error('worker interceptor cannot change worker role') }
	for index := next.index; index < next.interceptors.len; index++ {
		interceptor := next.interceptors[index]
		if interceptor is WorkerRunInterceptor {
			return call_worker_run(interceptor, input, WorkerRunNext{
				interceptors: next.interceptors
				index:        index + 1
				role:         next.role
			})
		}
	}
	config := WorkerOptions{
		...input.options
		worker_interceptors: next.interceptors
	}
	match input.role {
		.workflow, .combined {
			mut runtime := input.workflows
			run_workflow_worker(config, mut runtime, input.activities, input.role == .combined,
				input.nexus_services)!
		}
		.activity {
			run_activities_configured(config, input.activities)!
		}
		.nexus {
			run_nexus_worker_configured(config, input.nexus_services)!
		}
	}
}

pub struct WorkerReplayNext {
	interceptors []WorkerInterceptor
	index        int
	state        &WorkerContinuationState = &WorkerContinuationState{}
}

pub fn (next WorkerReplayNext) execute(input ReplaySetup, history []u8) !ReplayResult {
	mut state := next.state
	state.consume()!
	for index := next.index; index < next.interceptors.len; index++ {
		interceptor := next.interceptors[index]
		if interceptor is WorkerReplayInterceptor {
			return call_worker_replay(interceptor, input, history, WorkerReplayNext{
				interceptors: next.interceptors
				index:        index + 1
			})
		}
	}
	return input.workflows.replay_configured(history, ReplayOptions{
		...input.options
		worker_interceptors: next.interceptors
	})
}

fn call_worker_run(interceptor WorkerRunInterceptor, input WorkerSetup, next WorkerRunNext) ! {
	mut state := next.state
	defer { state.deactivate() }
	interceptor.run_worker(input, next)!
}

fn call_worker_replay(interceptor WorkerReplayInterceptor, input ReplaySetup, history []u8, next WorkerReplayNext) !ReplayResult {
	mut state := next.state
	defer { state.deactivate() }
	return interceptor.replay_worker(input, history, next)!
}

fn run_worker_interceptors(setup WorkerSetup) ! {
	mut outcome := CallbackOutcome{}
	recover_worker_run(setup, mut outcome)
	if failure := outcome.failure { return failure }
}

fn recover_worker_run(setup WorkerSetup, mut outcome CallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}

	WorkerRunNext{
		interceptors: setup.options.worker_interceptors.clone()
		role:         setup.role
	}.execute(setup) or { outcome.failure = err }
}

struct WorkerReplayOutcome {
mut:
	failure ?IError
	result  ReplayResult
}

fn replay_worker_interceptors(setup ReplaySetup, history []u8) !ReplayResult {
	mut outcome := WorkerReplayOutcome{}
	recover_worker_replay(setup, history, mut outcome)
	if failure := outcome.failure { return failure }
	return outcome.result
}

fn recover_worker_replay(setup ReplaySetup, history []u8, mut outcome WorkerReplayOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	outcome.result = WorkerReplayNext{
		interceptors: setup.options.worker_interceptors.clone()
	}.execute(setup, history) or {
		outcome.failure = err
		return
	}
}

fn notify_worker_shutdown(interceptors []WorkerInterceptor, info WorkerShutdownInfo, logger Logger) {
	for interceptor in interceptors {
		if interceptor is WorkerShutdownInterceptor {
			mut outcome := CallbackOutcome{}
			recover_worker_shutdown(interceptor, info, mut outcome)
			if failure := outcome.failure {
				log_worker_interceptor_failure(logger, 'shutdown', failure)
			}
		}
	}
}

fn recover_worker_shutdown(interceptor WorkerShutdownInterceptor, info WorkerShutdownInfo, mut outcome CallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	interceptor.on_worker_shutdown(info)
}

// Logging is another user callback; it cannot interrupt the remaining shutdown
// notifications or connection cleanup.
fn log_worker_interceptor_failure(logger Logger, phase string, failure IError) {
	defer {
		if message := recover() { _ = message }
	}
	emit_log(logger, .error,
		'WORKER_INTERCEPTOR_FAILED phase=${phase} message=${protocol_text(failure.msg())}')
}
