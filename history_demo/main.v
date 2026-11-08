module main

import sdk
import temporal
import example_converter
import example_storage
import os
import json2
import time

struct RawWorkflow {
mut:
	data string
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RawWorkflow{}
}

fn (mut workflow RawWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			workflow.data = event.data
			context.timer(time.hour)!
		}
		.signal_received {
			if event.name == 'finish' { context.complete(workflow.data)! }
		}
		else {}
	}
}

fn run() ! {
	role := os.args[1]
	converter := example_storage.from_env(example_converter.from_env()!)!
	if role == 'replay' {
		mut runtime := &sdk.Runtime{}
		runtime.register('v.raw-history', make_workflow)!
		runtime.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id:    os.args[3]
			task_queue:     os.args[4]
			data_converter: converter
		})!
		println('OK')
		return
	}
	if role == 'worker' {
		mut runtime := &sdk.Runtime{}
		runtime.register('v.raw-history', make_workflow)!
		sdk.run_workflows(sdk.WorkerOptions{ ...sdk.options_from_env()!, data_converter: converter },
			runtime)!
		return
	}
	client := sdk.connect(sdk.ClientOptions{
		address:        os.getenv('TEMPORAL_ADDRESS')
		data_converter: converter
	})!
	defer { client.close() }
	if role == 'start' {
		value := if os.getenv('V_SDK_EXAMPLE_STORAGE_DIR') != '' {
			'z'.repeat(3 * 1024 * 1024)
		} else {
			'raw-history'
		}
		println(json2.encode(client.start(sdk.WorkflowStart{
			workflow_id:   os.args[2]
			workflow_type: 'v.raw-history'
			task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			arguments:     json2.encode(value)
		})!))
		return
	}
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	if role == 'finish' {
		client.signal(handle, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
		println('OK')
		return
	}
	if role == 'result' {
		result := client.get_result(handle, 20000)!
		expected := if os.getenv('V_SDK_EXAMPLE_STORAGE_DIR') != '' {
			3 * 1024 * 1024 + 2
		} else {
			13
		}
		if result.status != .completed || result.data.len != expected {
			return error('raw history result mismatch: ' + result.status.str() + ' length=' +
				result.data.len.str())
		}
		println('OK')
		return
	}
	if role == 'history' {
		request := temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryRequest{
			namespace:         'default'
			execution:         temporal.TemporalApiCommonV1_WorkflowExecution{
				workflow_id: handle.workflow_id
				run_id:      handle.run_id
			}
			maximum_page_size: 1
		}
		wire := temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse.decode(client.call_rpc(sdk.ClientRpcRequest{
			service:      .workflow
			method:       'GetWorkflowExecutionHistory'
			raw_payloads: true
			body:         request.encode()
		})!)!
		if wire.raw_history.len == 0 { return error('server did not return raw history') }
		mut events := 0
		mut token := []u8{}
		mut pages := 0
		for {
			page := client.history_page(handle, sdk.HistoryOptions{
				page_size:       1
				next_page_token: token
			})!
			if page.raw_history.len != 0 {
				return error('normal client history page was not normalized')
			}
			history := page.history or { return error('normalized history missing') }
			events += history.events.len
			pages++
			token = page.next_page_token
			if token.len == 0 {
				break
			}
		}
		history := client.history(handle)!
		if history.events.len != events || pages < 2 {
			return error('raw history pagination lost events')
		}
		os.write_file_array(os.args[3], history.encode())!
		println(json2.encode({
			'events':      events
			'pages':       pages
			'raw_batches': wire.raw_history.len
		}))
		return
	}
	return error('unknown history demo role')
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}
