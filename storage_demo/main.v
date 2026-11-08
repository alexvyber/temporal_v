module main

import sdk
import os
import json2
import time
import example_converter

struct StorageWorkflow {
mut:
	data  string
	local bool
}

fn new_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return StorageWorkflow{}
}

fn new_local_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return StorageWorkflow{
		local: true
	}
}

fn (mut workflow StorageWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			if workflow.local {
				context.local_activity('echo', event.data, sdk.LocalActivityOptions{})!
			} else {
				context.activity('echo', event.data, sdk.ActivityOptions{})!
			}
		}
		.activity_completed {
			workflow.data = event.data
			context.timer(60 * time.second)!
		}
		.signal_received {
			if event.name == 'finish' {
				context.complete(workflow.data)!
			} else if event.name == 'fail' {
				context.fail_application(sdk.ApplicationError{
					message: 'storage failure'
					details: [
						workflow.data,
					]
				})!
			} else {
				workflow.data = event.data
			}
		}
		.update_received {
			if event.name == 'fail' {
				context.fail_update(event.id, sdk.ApplicationError{
					message: 'storage update failure'
					details: [workflow.data]
				})!
			} else {
				context.complete_update(event.id, workflow.data)!
			}
		}
		.cancel_requested {
			context.cancel()!
		}
		else {}
	}
}

fn (workflow &StorageWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name == 'fail' {
		return sdk.ApplicationError{
			message: 'storage query failure'
			details: [workflow.data]
		}
	}
	return workflow.data
}

fn (workflow &StorageWorkflow) update_names() []string {
	return ['read', 'fail']
}

fn (workflow &StorageWorkflow) validate_update(name string, arguments []string, view sdk.WorkflowContextView) ! {}

fn echo(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	return sdk.single_argument(arguments)!
}

fn run() ! {
	converter := example_converter.from_env()!
	role := os.args[1]
	address := os.getenv('TEMPORAL_ADDRESS')
	queue := os.getenv('TEMPORAL_TASK_QUEUE')
	id := os.getenv_opt('V_SDK_EXAMPLE_STORAGE_WORKFLOW_ID') or { queue }
	mut runtime := sdk.Runtime{}
	runtime.register('storage', new_workflow)!
	runtime.register('storage.parent', new_storage_parent)!
	runtime.register('storage.child', new_storage_child)!
	runtime.register('storage.continue', new_storage_continue)!
	runtime.register('storage.sender', new_storage_sender)!
	runtime.register('storage.receiver', new_storage_receiver)!
	runtime.register('storage.local', new_local_workflow)!
	runtime.register_local_activity('echo', echo)!
	if role == 'worker' {
		mut activities := sdk.Activities{}
		activities.register('echo', echo)!
		sdk.run_worker(sdk.WorkerOptions{
			address:        address
			task_queue:     queue
			data_converter: converter
		}, runtime, activities)!
		return
	}
	if role == 'replay' {
		result := runtime.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id:    id
			task_queue:     queue
			data_converter: converter
		})!
		println(json2.encode(result))
		return
	}
	client := sdk.connect(sdk.ClientOptions{ address: address, data_converter: converter })!
	defer { client.close() }
	if role == 'start' || role == 'targets' {
		value := json2.encode('x'.repeat(3 * 1024 * 1024))
		mut receiver := sdk.WorkflowHandle{}
		if os.getenv('V_SDK_EXAMPLE_STORAGE_TARGETS') == 'signal' {
			receiver = client.start(sdk.WorkflowStart{
				workflow_id:   id + '-receiver'
				workflow_type: 'storage.receiver'
				task_queue:    queue
				arguments:     'null'
			})!
		}
		handle := client.start(sdk.WorkflowStart{
			workflow_id:   id
			workflow_type: storage_workflow_type()
			task_queue:    queue
			arguments:     value
		})!
		if role == 'targets' {
			outcome := client.get_chain_result(handle, 15000)!
			assert outcome.status == .completed && outcome.data.len == 3 * 1024 * 1024 + 2
			if receiver.workflow_id != '' {
				received := client.get_result(receiver, 15000)!
				assert received.status == .completed && received.data.len == outcome.data.len
			}
		}
		println(json2.encode(handle))
		return
	}
	handle := sdk.WorkflowHandle{
		workflow_id: id
	}
	if role == 'history' {
		history := client.history(sdk.WorkflowHandle{
			workflow_id: id
			run_id:      if os.args.len > 3 { os.args[3] } else { '' }
		})!
		os.write_file(os.args[2], history.encode().bytestr())!
		return
	}
	if role == 'update' || role == 'update-failure' {
		update := client.start_update(handle, sdk.UpdateRequest{
			name:                if role == 'update' {
				'read'
			} else {
				'fail'
			}
			wait_for:            .completed
			wait_timeout_millis: 15000
		})!
		outcome := client.update_result(update, 15000)!
		if role == 'update' {
			assert !outcome.failed
			assert outcome.data.len == 3 * 1024 * 1024 + 2
		} else {
			assert outcome.failed
			failure := outcome.failure or { return error('missing Update failure') }
			assert failure.details[0].len == 3 * 1024 * 1024 + 2
		}
		println('update passed')
		return
	}
	if role == 'query-failure' {
		if _ := client.query(handle, 'fail', 'null') {
			return error('query failure accepted')
		} else {
			if err is sdk.QueryError {
				assert err.failure.details[0].len == 3 * 1024 * 1024 + 2
			} else {
				return err
			}
		}
		println('query failure passed')
		return
	}
	if role == 'concurrency' {
		results := chan string{cap: 12}
		for _ in 0 .. 12 {
			spawn concurrent_signal(client, handle, results)
		}
		for _ in 0 .. 12 {
			message := <-results
			if message != '' { return error(message) }
		}
		println('concurrent signals passed')
		return
	}
	if role == 'query' {
		value := client.query(handle, 'read', 'null')!
		assert value.len == 3 * 1024 * 1024 + 2
		println('query passed')
	} else if role == 'signal' {
		client.signal(handle, 'finish', 'null')!
		outcome := client.get_result(handle, 15000)!
		assert outcome.status == .completed
		assert outcome.data.len == 3 * 1024 * 1024 + 2
		println('result passed')
	} else {
		return error('unknown storage demo mode: ${role}')
	}
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}

fn concurrent_signal(client &sdk.Client, handle sdk.WorkflowHandle, results chan string) {
	client.signal(handle, 'replace', json2.encode('x'.repeat(3 * 1024 * 1024))) or {
		results <- err.msg()
		return
	}
	results <- ''
}
