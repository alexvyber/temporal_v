module main

import flag
import x.json2
import os
import temporal_v.sdk
import temporal_v.example_interceptors
import temporal_v.example_converter
import time
import temporal_v.workflows

struct WrongWorkflow {}

fn wrong_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return WrongWorkflow{}
}

fn (mut workflow WrongWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started { context.timer(time.second)! }
}

fn run() ! {
	mut parser := flag.new_flag_parser(os.args)
	parser.skip_executable()
	file := parser.string('history', 0, '', 'Binary History file')
	id := parser.string('workflow-id', 0, '', 'Original workflow ID')
	queue := parser.string('task-queue', 0, 'replay', 'Original task queue')
	namespace_name := parser.string('namespace', 0, 'default', 'Original namespace')
	wrong := parser.bool('wrong-code', 0, false,
		'Register intentionally incompatible v.restart code')
	parser.finalize()!
	mut runtime := sdk.Runtime{}
	if wrong {
		runtime.register('v.restart', wrong_workflow)!
	} else {
		workflows.register(mut runtime)!
	}
	interceptors := example_interceptors.worker(sdk.WorkerOptions{})
	result := runtime.replay(os.read_bytes(file)!, sdk.ReplayOptions{
		command_interceptors:          interceptors.command_interceptors
		workflow_interceptors:         interceptors.workflow_interceptors
		query_interceptors:            interceptors.query_interceptors
		update_validator_interceptors: interceptors.update_validator_interceptors
		workflow_id:                   id
		task_queue:                    queue
		namespace:                     namespace_name
		data_converter:                example_converter.from_env()!
	})!
	println(json2.encode(result))
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}
