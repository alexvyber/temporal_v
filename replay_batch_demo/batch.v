module main

import temporal_v.sdk
import os
import x.json2
import sync
import time

struct ReplayManifestEntry {
	path        string
	workflow_id string
	queue       string
}

struct ReplayProbeState {
	mutex &sync.Mutex = sync.new_mutex()
mut:
	active  int
	maximum int
}

struct ReplayCallbackProbe {
	state &ReplayProbeState
}

fn (probe ReplayCallbackProbe) intercept(event sdk.Event, mut context sdk.WorkflowContext, next sdk.WorkflowNext) ! {
	mut state := probe.state
	state.mutex.lock()
	state.active++
	if state.active > state.maximum { state.maximum = state.active }
	state.mutex.unlock()
	defer {
		state.mutex.lock()
		state.active--
		state.mutex.unlock()
	}
	if event.kind == .started { time.sleep(20 * time.millisecond) }
	next.execute(event, mut context)!
}

struct IncompatibleWorkflow {}

fn incompatible(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return IncompatibleWorkflow{}
}

fn (mut workflow IncompatibleWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started { context.timer(time.second)! }
}

struct BatchReport {
	outcomes []sdk.ReplayOutcome
	maximum  int
	active   int
}

fn run_batch() ! {
	cases := json2.decode[[]ReplayManifestEntry](os.read_file(os.args[2])!)!
	mode := os.args[3]
	conversion := converter()!
	state := &ReplayProbeState{}
	probe := sdk.WorkflowInterceptor(ReplayCallbackProbe{
		state: state
	})
	mut inputs := []sdk.ReplayInput{}
	for entry in cases {
		inputs << sdk.ReplayInput{
			history: os.read_bytes(entry.path)!
			options: sdk.ReplayOptions{
				workflow_id:           entry.workflow_id
				task_queue:            entry.queue
				data_converter:        conversion
				workflow_interceptors: [probe]
			}
		}
	}
	mut runtime := definitions()!
	if mode == 'mismatch' {
		runtime = &sdk.Runtime{}
		runtime.register('v.replay-batch', incompatible)!
	}
	if mode == 'validation' {
		for concurrency in [0, 33] {
			runtime.replay_many([]sdk.ReplayInput{}, concurrency: concurrency) or {
				if !err.msg().contains('between 1 and 32') { return err }
				continue
			}
			return error('invalid replay concurrency accepted')
		}
		if runtime.replay_many([]sdk.ReplayInput{})!.len != 0 {
			return error('empty replay batch produced results')
		}
		println('OK')
		return
	}
	fail_fast := mode.starts_with('fail-fast')
	if mode == 'mixed' || fail_fast {
		invalid := sdk.ReplayInput{
			history: [u8(255)]
			options: sdk.ReplayOptions{
				workflow_id: 'invalid-history'
			}
		}
		inputs.insert(if fail_fast { 0 } else { 1 }, invalid)
	}
	concurrency := if mode == 'sequential' || mode == 'fail-fast-serial' {
		1
	} else if mode == 'fail-fast-parallel' {
		2
	} else {
		3
	}
	outcomes := runtime.replay_many(inputs, concurrency: concurrency, fail_fast: fail_fast)!
	if state.active != 0 || state.maximum > concurrency {
		return error('replay callback concurrency or drain failed')
	}
	if mode == 'parallel' && state.maximum < 2 {
		return error('replay callbacks never ran concurrently')
	}
	println(json2.encode(BatchReport{
		outcomes: outcomes
		maximum:  state.maximum
		active:   state.active
	}))
}
