module main

import os
import time
import alexvyber.temporal_v.sdk

struct Greeting {}

fn create(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return Greeting{}
}

fn (mut workflow Greeting) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started { context.timer(time.second)! }
	if event.kind == .timer_fired { context.complete('"Hello"')! }
	if event.kind == .cancel_requested { context.cancel()! }
}

fn main() {
	options := sdk.options_from_env() or { panic(err) }
	if os.args.len > 1 && os.args[1] == 'client' {
		client := sdk.connect(sdk.ClientOptions{
			address:   options.address
			namespace: options.namespace
		}) or { panic(err) }
		defer { client.close() }
		handle := client.start(sdk.WorkflowStart{
			workflow_id:   'normal-v-dependency'
			workflow_type: 'greeting'
			task_queue:    options.task_queue
		}) or { panic(err) }
		result := client.get_result(handle, 30000) or { panic(err) }
		assert result.status == .completed && result.data == '"Hello"'
		println('DEPENDENCY_OK')
		return
	}
	mut runtime := sdk.Runtime{}
	runtime.register('greeting', create) or { panic(err) }
	sdk.run_workflows(options, runtime) or { panic(err) }
}
