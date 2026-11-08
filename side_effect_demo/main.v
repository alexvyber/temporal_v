module main

import alexvyber.temporal_v.sdk
import os
import time
import x.json2
import crypto.rand as crypto_rand

struct Input {
	fail_once bool
	go_shape  bool
	mixed     bool
}

struct Configuration {
	stage   string
	padding string
}

struct EffectState {
	tokens        []string
	configuration string
	phase         int
}

struct EffectWorkflow {
mut:
	tokens        []string
	configuration string
	phase         int
}

fn factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return EffectWorkflow{}
}

fn journal(label string) ! {
	if os.getenv('V_EFFECT_FORBID') == '1' {
		return error('side effect callback ran during replay')
	}
	mut file := os.open_append(os.getenv('V_EFFECT_JOURNAL'))!
	file.writeln(label)!
	file.close()
}

fn random_value() !sdk.PayloadValue {
	journal('random')!
	return sdk.BinaryValue{
		data: crypto_rand.bytes(32)!
	}
}

fn failing_value() !sdk.PayloadValue {
	journal('failure-op')!
	path := os.getenv('V_EFFECT_CONFIG') + '.failed-once'
	if !os.exists(path) {
		os.write_file(path, 'failed')!
		return error('fixture refused side effect first attempt')
	}
	return random_value()!
}

fn configuration_value() !sdk.PayloadValue {
	journal('config')!
	stage := os.read_file(os.getenv('V_EFFECT_CONFIG'))!
	padding := if os.getenv('V_SDK_EXAMPLE_STORAGE_DIR') != '' {
		'x'.repeat(1024 * 1024)
	} else {
		'small'
	}
	return sdk.json_value(Configuration{ stage: stage, padding: padding })!
}

fn configuration_equal(previous sdk.PayloadValue, current sdk.PayloadValue) bool {
	if os.getenv('V_EFFECT_FORBID') == '1' {
		os.write_file(os.getenv('V_EFFECT_JOURNAL') + '.equal-replay', 'called') or {}
		return false
	}
	journal('equal') or { return false }

	before := json2.decode[Configuration](previous.logical_data()) or { return false }
	after := json2.decode[Configuration](current.logical_data()) or { return false }
	return before.stage == after.stage
}

fn token(value sdk.PayloadValue) !string {
	if value is sdk.BinaryValue {
		if value.data.len != 32 { return error('side effect binary value changed length') }
		return value.data.hex()
	}
	return error('side effect did not preserve its binary type')
}

fn (mut workflow EffectWorkflow) settings(mut context sdk.WorkflowContext) ! {
	value := context.mutable_side_effect(if os.getenv('V_EFFECT_MUTATION') == 'rename' {
		'renamed'
	} else {
		'configuration'
	}, configuration_value, configuration_equal)!
	workflow.configuration = json2.decode[Configuration](value.logical_data())!.stage
}

fn (mut workflow EffectWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		input := json2.decode[Input](event.data)!
		if input.fail_once {
			value := context.side_effect(failing_value) or {
				// Catching the error must not publish this poisoned completion.
				context.complete('"uncommitted"')!
				return
			}
			context.complete(sdk.json_value(token(value)!)!)!
			return
		}
		if os.getenv('V_EFFECT_MUTATION') == 'extra' { context.side_effect(random_value)! }
		if os.getenv('V_EFFECT_MUTATION') != 'remove' {
			workflow.tokens << token(context.side_effect(random_value)!)!
		}
		workflow.settings(mut context)!
		workflow.settings(mut context)!
		if input.go_shape {
			workflow.settings(mut context)!
			workflow.tokens << token(context.side_effect(random_value)!)!
			workflow.phase = 1
		} else if input.mixed {
			if !context.patched('side-effect-with-local-v1')! {
				return error('expected active patch')
			}
			context.local_activity('v.side-effect.local', 'null', sdk.LocalActivityOptions{})!
		} else {
			context.timer(10 * time.millisecond)!
		}
	}
	if event.kind == .activity_completed {
		if event.data != '42' { return error('local side effect fixture result changed') }
		workflow.tokens << token(context.side_effect(random_value)!)!
		context.timer(10 * time.millisecond)!
	}
	if event.kind == .timer_fired {
		workflow.settings(mut context)!
		workflow.tokens << token(context.side_effect(random_value)!)!
		workflow.phase = 1
	}
	if event.kind == .signal_received && event.name == 'change' {
		workflow.settings(mut context)!
		workflow.settings(mut context)!
		workflow.tokens << token(context.side_effect(random_value)!)!
		workflow.phase = 2
	}
	if event.kind == .signal_received && event.name == 'finish' {
		context.complete(sdk.json_value(EffectState{
			tokens:        workflow.tokens
			configuration: workflow.configuration
			phase:         workflow.phase
		})!)!
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (workflow &EffectWorkflow) query_names() []string {
	return ['state']
}

fn (workflow &EffectWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.json_value(EffectState{
		tokens:        workflow.tokens
		configuration: workflow.configuration
		phase:         workflow.phase
	})!
}

fn local_value(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	journal('local')!
	return sdk.PayloadValue('42')
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.side-effect', factory)!
	runtime.register_local_activity('v.side-effect.local', local_value)!
	return runtime
}

fn run() ! {
	role := os.args[1]
	conversion := converter()!
	if role == 'worker' {
		policy := os.getenv('V_EFFECT_POLICY')
		sdk.run_workflows(sdk.WorkerOptions{
			...sdk.options_from_env()!
			data_converter:                            conversion
			nondeterminism_as_workflow_fail:           policy == 'all'
			nondeterminism_as_workflow_fail_for_types: if policy == 'selected' {
				['v.side-effect']
			} else if policy == 'other' {
				['v.other']
			} else {
				[]string{}
			}
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
	if role == 'start-one' {
		handle := client.start(sdk.WorkflowStart{
			workflow_id:   os.getenv('TEMPORAL_TASK_QUEUE') + '-one'
			workflow_type: 'v.side-effect'
			task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:     json2.encode(Input{
				go_shape: true
			})
		})!
		println(json2.encode(handle))
		return
	}
	if role == 'start' || role == 'start-mixed' {
		mut handles := []sdk.WorkflowHandle{}
		for index in 0 .. 3 {
			handles << client.start(sdk.WorkflowStart{
				workflow_id:   os.getenv('TEMPORAL_TASK_QUEUE') + '-' + index.str()
				workflow_type: 'v.side-effect'
				task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
				arguments:     json2.encode(Input{
					fail_once: index == 2
					mixed:     role == 'start-mixed'
				})
			})!
		}
		println(json2.encode(handles))
		return
	}
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	if role == 'state' {
		println(client.query(handle, 'state', sdk.Arguments(sdk.TypedArguments{}))!)
		return
	}
	if role == 'change' || role == 'finish' || role == 'wake' {
		client.signal(handle, role, sdk.Arguments(sdk.TypedArguments{}))!
		println('OK')
		return
	}
	if role == 'cancel' {
		client.cancel(handle, 'side effect recovery')!
		println('OK')
		return
	}
	if role == 'result' {
		println(json2.encode(client.get_result(handle, 20000)!))
		return
	}
	if role == 'history' {
		os.write_file_array(os.args[3], client.history(handle)!.encode())!
		println('OK')
		return
	}
	return error('unknown side effect role')
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}
