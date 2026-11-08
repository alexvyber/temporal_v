module main

import temporal_v.sdk
import os
import time

fn hook_record(message string) {
	path := os.getenv('V_WORKER_HOOK_LOG')
	if path == '' { return }
	mut file := os.open_append(path) or { panic(err) }
	defer { file.close() }
	file.writeln(message) or { panic(err) }
}

struct LifecycleTap {
	label string
}

fn (tap LifecycleTap) name() string {
	return tap.label
}

fn (tap LifecycleTap) run_worker(input sdk.WorkerSetup, next sdk.WorkerRunNext) ! {
	hook_record('run-before:${tap.label}:${input.role}:${input.options.namespace}:${input.options.task_queue}')
	defer { hook_record('run-after:${tap.label}') }
	next.execute(input)!
}

fn (tap LifecycleTap) replay_worker(input sdk.ReplaySetup, history []u8, next sdk.WorkerReplayNext) !sdk.ReplayResult {
	hook_record('replay-before:${tap.label}')
	defer { hook_record('replay-after:${tap.label}') }
	return next.execute(input, history)!
}

fn (tap LifecycleTap) on_worker_shutdown(info sdk.WorkerShutdownInfo) {
	if info.role == .activity && os.getenv('V_WORKER_HOOK_DRAIN') == 'true' {
		directory := os.getenv('V_WORKER_HOOK_DRAIN_DIR')
		assert os.exists(os.join_path(directory, 'end-' + os.getpid().str()))
	}
	hook_record('shutdown:${tap.label}:${info.role}:${info.replaying}:${info.namespace}:${info.task_queue}')
	if tap.label == 'inner' && os.getenv('V_WORKER_HOOK_SHUTDOWN_PANIC') == 'true' {
		panic('shutdown hook panic')
	}
}

struct ShutdownOnlyTap {}

fn (tap ShutdownOnlyTap) name() string {
	return 'observer'
}

fn (tap ShutdownOnlyTap) on_worker_shutdown(info sdk.WorkerShutdownInfo) {
	hook_record('shutdown:observer:${info.role}:${info.replaying}:${info.namespace}:${info.task_queue}')
}

fn configured_worker_hooks() []sdk.WorkerInterceptor {
	mut hooks := []sdk.WorkerInterceptor{}
	if os.getenv('V_WORKER_HOOK_LOG') != '' {
		hooks << sdk.WorkerInterceptor(LifecycleTap{
			label: 'outer'
		})
		hooks << LifecycleTap{
			label: 'inner'
		}
		hooks << ShutdownOnlyTap{}
	}
	hooks << configured_completion_hooks()
	hooks << configured_activation_hooks()
	return hooks
}

struct HeldWorkerNext {
mut:
	worker ?sdk.WorkerRunNext
	replay ?sdk.WorkerReplayNext
	calls  int
}

struct WorkerGate {
	mode string
	held &HeldWorkerNext
}

fn (gate WorkerGate) name() string {
	return 'gate'
}

fn (gate WorkerGate) run_worker(input sdk.WorkerSetup, next sdk.WorkerRunNext) ! {
	mut held := gate.held
	held.calls++
	if gate.mode == 'short' { return }
	if gate.mode == 'error' {
		return sdk.ApplicationError{
			message:  'worker middleware error'
			category: 'GateError'
		}
	}
	if gate.mode == 'panic' { panic('worker middleware panic') }
	if gate.mode == 'hold' {
		held.worker = next
		return
	}
	if gate.mode == 'race' {
		results := chan string{cap: 2}
		first := spawn run_hook_continuation(next, input, results)
		second := spawn run_hook_continuation(next, input, results)
		one := <-results
		two := <-results
		first.wait()
		second.wait()
		assert (one == '' && two.contains('already used'))
			|| (two == '' && one.contains('already used'))
		return
	}
	if gate.mode == 'role' {
		next.execute(sdk.WorkerSetup{ ...input, role: .activity })!
		return
	}
	next.execute(input)!
	next.execute(input) or {
		assert err.msg().contains('already used')
		return
	}
	return error('duplicate worker continuation accepted')
}

fn (gate WorkerGate) replay_worker(input sdk.ReplaySetup, history []u8, next sdk.WorkerReplayNext) !sdk.ReplayResult {
	mut held := gate.held
	held.calls++
	if gate.mode == 'short' {
		return sdk.ReplayResult{
			run_id:      'short'
			activations: 7
		}
	}
	if gate.mode == 'error' {
		return sdk.ApplicationError{
			message:  'replay middleware error'
			category: 'GateError'
		}
	}
	if gate.mode == 'panic' { panic('replay middleware panic') }
	if gate.mode == 'hold' {
		held.replay = next
		return sdk.ReplayResult{
			run_id: 'held'
		}
	}
	result := next.execute(input, history)!
	next.execute(input, history) or {
		assert err.msg().contains('already used')
		return result
	}
	return error('duplicate replay continuation accepted')
}

fn run_hook_continuation(next sdk.WorkerRunNext, input sdk.WorkerSetup, results chan string) {
	next.execute(input) or {
		results <- err.msg()
		return
	}
	results <- ''
}

fn hook_draining_activity(mut context sdk.ActivityContext) !sdk.PayloadValue {
	directory := os.getenv('V_WORKER_HOOK_DRAIN_DIR')
	os.write_file(os.join_path(directory, 'begin-' + os.getpid().str()), 'began')!
	defer {
		os.write_file(os.join_path(directory, 'end-' + os.getpid().str()), 'drained') or {
			panic(err)
		}
	}
	context.sleep(10 * time.second)!
	return sdk.PayloadValue('"drained"')
}

fn check_worker_hooks() ! {
	for mode in ['short', 'error', 'panic', 'double', 'hold', 'role', 'race'] {
		mut held := &HeldWorkerNext{}
		mut hooks := [sdk.WorkerInterceptor(WorkerGate{
			mode: mode
			held: held
		})]
		if mode in ['double', 'race'] {
			hooks << WorkerGate{
				mode: 'short'
				held: held
			}
		}
		options := sdk.WorkerOptions{
			worker_interceptors: hooks
			address:             'http://127.0.0.1:1'
		}
		mut failed := false
		sdk.run_workflows(options, &sdk.Runtime{}) or {
			failed = true
			if mode == 'error' {
				assert err is sdk.ApplicationError
			}
			if mode == 'panic' {
				assert err is sdk.PanicError
				if err is sdk.PanicError {
					assert err.stack_trace.contains('run_worker')
				}
			}
			if mode == 'role' {
				assert err.msg().contains('cannot change worker role')
			}
		}
		assert failed == (mode in ['error', 'panic', 'role'])
		assert held.calls == if mode in ['double', 'race'] {
			2
		} else {
			1
		}
		if next := held.worker {
			next.execute(sdk.WorkerSetup{ role: .workflow, options: options }) or {
				assert err.msg().contains('no longer active')
				continue
			}
			return error('escaped worker continuation accepted')
		}
	}
	for mode in ['short', 'error', 'panic', 'double', 'hold'] {
		mut held := &HeldWorkerNext{}
		mut hooks := [sdk.WorkerInterceptor(WorkerGate{
			mode: mode
			held: held
		})]
		if mode in ['double', 'race'] {
			hooks << WorkerGate{
				mode: 'short'
				held: held
			}
		}
		options := sdk.ReplayOptions{
			worker_interceptors: hooks
		}
		mut failed := false
		result := (&sdk.Runtime{}).replay([]u8{}, options) or {
			failed = true
			if mode == 'error' {
				assert err is sdk.ApplicationError
			}
			if mode == 'panic' {
				assert err is sdk.PanicError
			}
			sdk.ReplayResult{}
		}
		assert failed == (mode in ['error', 'panic'])
		assert held.calls == if mode in ['double', 'race'] {
			2
		} else {
			1
		}
		if !failed && mode != 'hold' {
			assert result.run_id == 'short' && result.activations == 7
		}
		if next := held.replay {
			next.execute(sdk.ReplaySetup{ options: options }, []u8{}) or {
				assert err.msg().contains('no longer active')
				continue
			}
			return error('escaped replay continuation accepted')
		}
	}
	println('WORKER_HOOK_UNIT_OK cases=12')
}

fn serve_hook_role(config sdk.WorkerOptions, converter sdk.DataConverter) ! {
	options := sdk.WorkerOptions{
		...config
		data_converter:      converter
		worker_interceptors: configured_worker_hooks()
	}
	match os.args[1] {
		'hook-workflows' {
			sdk.run_workflows(options, runtime()!)!
		}
		'hook-activities' {
			mut activities := sdk.Activities{}
			activities.register('v.panic.activity', panic_activity)!
			sdk.run_activities(options, activities)!
		}
		'hook-nexus' {
			mut services := sdk.NexusServices{}
			services.register('v.panic', 'sync', sdk.NexusOperation{ start: panic_nexus_start })!
			sdk.run_nexus_worker(options, services)!
		}
		else {
			return error('unknown hook role')
		}
	}
}
