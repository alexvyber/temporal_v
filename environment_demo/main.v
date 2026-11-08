module main

import flag
import x.json2
import os
import temporal_v.sdk
import time

struct LongTimer {}

fn new_long_timer(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return LongTimer{}
}

fn (mut workflow LongTimer) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		seconds := json2.decode[i64](event.data)!
		context.timer(seconds * time.second)!
	} else if event.kind == .timer_fired {
		context.complete('"timer complete"')!
	}
}

fn run_worker(options sdk.WorkerOptions, done chan string) {
	mut runtime := sdk.Runtime{}
	runtime.register('long.timer', new_long_timer) or {
		done <- err.msg()
		return
	}
	sdk.run_workflows(options, runtime) or {
		done <- err.msg()
		return
	}
	done <- ''
}

fn await_workflow(environment &sdk.TestEnvironment, handle sdk.WorkflowHandle, results chan string) {
	result := environment.get_result(handle, 30000) or {
		results <- err.msg()
		return
	}
	if result.status != .completed || result.data != '"timer complete"' {
		results <- 'unexpected timer result'
		return
	}
	results <- ''
}

fn run() ![]string {
	mut parser := flag.new_flag_parser(os.args)
	parser.skip_executable()
	executable := parser.string('executable', 0, '', 'Pinned Temporal server executable')
	skipping := parser.bool('time-skipping', 0, false, 'Use the Java time-skipping test server')
	parser.finalize()!
	environment := sdk.start_test_environment(sdk.TestEnvironmentOptions{
		executable:      executable
		mode:            if skipping { .time_skipping } else { .dev }
		startup_timeout: 30 * time.second
	})!
	defer {
		environment.close()
		environment.close()
	}
	control := &sdk.WorkerControl{}
	done := chan string{cap: 1}
	spawn run_worker(sdk.WorkerOptions{
		address:    environment.address
		task_queue: 'managed-environment'
		control:    control
	}, done)
	defer {
		control.shutdown()
		message := <-done
		if message != '' { eprintln(message) }
	}
	seconds := if skipping { i64(3 * 24 * 60 * 60) } else { i64(1) }
	before := environment.current_time()!
	first := environment.client.start(sdk.WorkflowStart{
		workflow_id:   'managed-first'
		workflow_type: 'long.timer'
		task_queue:    'managed-environment'
		arguments:     seconds.str()
	})!
	second := environment.client.start(sdk.WorkflowStart{
		workflow_id:   'managed-second'
		workflow_type: 'long.timer'
		task_queue:    'managed-environment'
		arguments:     (seconds / 2 + 1).str()
	})!
	results := chan string{cap: 2}
	watch := time.new_stopwatch()
	spawn await_workflow(environment, first, results)
	spawn await_workflow(environment, second, results)
	for _ in 0 .. 2 {
		message := <-results
		if message != '' { return error(message) }
	}
	if watch.elapsed() > 15 * time.second { return error('time skipping did not complete quickly') }
	if skipping {
		after := environment.current_time()!
		if after.unix() - before.unix() < seconds { return error('server clock did not advance') }
		environment.get_result(sdk.WorkflowHandle{ workflow_id: 'missing-workflow' }, 1000) or {
			if err.code() != 5 { return err }
		}
		environment.sleep(24 * time.hour)!
	}
	return [
		if skipping {
			'Two concurrent three-day/two-day waits completed through managed time skipping; failed wait preserved lock balance'
		} else {
			'Managed dev server served V workflows'
		},
		'Programmatic WorkerControl shutdown drained worker without OS signals; environment close was idempotent',
	]
}

fn main() {
	cases := run() or {
		eprintln(err.msg())
		exit(1)
	}
	println(json2.encode(cases))
}
