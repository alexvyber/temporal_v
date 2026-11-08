module main

import alexvyber.temporal_v.sdk
import os
import time
import x.json2
import alexvyber.temporal_v.example_converter

struct ConcurrentWorkflow {
mut:
	ready bool
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ConcurrentWorkflow{}
}

fn (mut workflow ConcurrentWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		mode := json2.decode[string](event.data)!
		// Deliberate blocking in this test fixture verifies worker isolation.
		// Replay emits exactly the same commands without touching host files.
		if (mode == 'hold' || mode == 'hold_complete') && !context.is_replaying() {
			root := os.getenv('V_SDK_CONCURRENCY_GATES')
			id := context.info().workflow_id
			os.write_file(os.join_path(root, id + '.entered'), 'entered')!
			watch := time.new_stopwatch()
			for !os.exists(os.join_path(root, id + '.release')) {
				if watch.elapsed() > 25 * time.second { return error('test callback gate expired') }
				time.sleep(5 * time.millisecond)
			}
		}
		workflow.ready = true
		if mode == 'fail' {
			return sdk.ApplicationError{
				message:       'isolated failure'
				category:      'ConcurrencyFixture'
				non_retryable: true
			}
		}
		if mode == 'fast' || mode == 'hold_complete' { context.complete('"done"')! }
	}
	if event.kind == .signal_received {
		if !workflow.ready { return error('same-run activations overlapped') }
		context.complete('"done"')!
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.concurrent', make_workflow)!
	return runtime
}

fn run() ! {
	role := os.args[1]
	conversion := example_converter.from_env()!
	if role == 'worker' {
		sdk.run_workflows(sdk.WorkerOptions{
			...sdk.options_from_env()!
			workflow_concurrency: 3
			max_cached_workflows: 100
			data_converter:       conversion
		}, definitions()!)!
		return
	}
	if role == 'replay' {
		definitions()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id:    os.args[3]
			task_queue:     os.args[4]
			data_converter: conversion
		})!
		println('OK')
		return
	}
	client := sdk.connect(sdk.ClientOptions{
		address:        os.getenv('TEMPORAL_ADDRESS')
		data_converter: conversion
	})!
	defer { client.close() }
	if role == 'start' {
		handle := client.start(sdk.WorkflowStart{
			workflow_id:          os.args[2]
			workflow_type:        'v.concurrent'
			task_queue:           os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:            json2.encode(os.args[3])
			task_timeout_seconds: 30
		})!
		println(json2.encode(handle))
		return
	}
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	if role == 'result' {
		println(json2.encode(client.get_result(handle, 30000)!))
		return
	}
	if role == 'signal' {
		client.signal(handle, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
		println('OK')
		return
	}
	if role == 'cancel' {
		client.cancel(handle, 'concurrency fixture')!
		println('OK')
		return
	}
	if role == 'history' {
		os.write_file_array(os.args[3], client.history(handle)!.encode())!
		println('OK')
		return
	}
	return error('unknown concurrency role')
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}
