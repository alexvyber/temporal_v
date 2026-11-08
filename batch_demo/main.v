module main

import sdk
import temporal
import os
import json2
import time
import example_converter

struct BatchInput {
	expect_headers bool
}

struct BatchWorkflow {
	converter sdk.DataConverter
mut:
	count          i64
	expect_headers bool
}

fn (mut workflow BatchWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			workflow.expect_headers = json2.decode[BatchInput](event.data)!.expect_headers
		}
		.signal_received {
			if event.name == 'add' {
				headers := workflow.converter.decode_map(event.headers)!
				if workflow.expect_headers && (headers['batch'] or {
					return error('missing batch header')
				}).logical_data() != '"header"' {
					return error('batch signal lost expected header')
				}
				if !workflow.expect_headers && headers.len != 0 {
					return error('legacy batch signal unexpectedly forwarded headers')
				}
				workflow.count += json2.decode[i64](event.data)!
			}
			if event.name == 'finish' { context.complete(json2.encode(workflow.count))! }
		}
		.cancel_requested {
			context.cancel()!
		}
		else {}
	}
}

fn (workflow &BatchWorkflow) query(name string, args []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return json2.encode(workflow.count)
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	converter := example_converter.from_env()!
	runtime.register('v.batch', fn [converter] (view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
		return BatchWorkflow{
			converter: converter
		}
	})!
	return runtime
}

fn run() ! {
	mode := os.args[1]
	if mode == 'validation' {
		validate_options()!
		return
	}
	if mode == 'worker' {
		sdk.run_workflows(sdk.WorkerOptions{
			...sdk.options_from_env()!
			data_converter: example_converter.from_env()!
		}, definitions()!)!
		return
	}
	if mode == 'replay' {
		definitions()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id:    os.args[3]
			task_queue:     os.args[4]
			data_converter: example_converter.from_env()!
		})!
		println('REPLAY_OK')
		return
	}
	client := sdk.connect(sdk.ClientOptions{
		address:        os.getenv('TEMPORAL_ADDRESS')
		data_converter: example_converter.from_env()!
	})!
	defer { client.close() }
	if mode in ['signal', 'cancel', 'terminate', 'slow'] {
		handles := json2.decode[[]sdk.WorkflowHandle](os.args[2])!
		mut targets := []temporal.TemporalApiCommonV1_Execution{}
		for handle in handles {
			targets << temporal.TemporalApiCommonV1_Execution{
				type_:       .execution_type_workflow
				business_id: handle.workflow_id
				run_id:      handle.run_id
			}
		}
		operation := match mode {
			'signal', 'slow' {
				client.batch_signal('add', '3', {
					'batch': '"header"'
				})!
			}
			'cancel' {
				client.batch_cancellation()
			}
			else {
				client.batch_termination(sdk.Arguments(['"batch-detail"', '7']))!
			}
		}

		println(json2.encode(client.start_batch(sdk.BatchStart{
			targets:                   targets
			reason:                    'disposable V SDK batch test'
			operation:                 operation
			max_operations_per_second: if mode == 'slow' {
				f32(0.1)
			} else {
				f32(100)
			}
			job_id:                    if os.args.len > 3 { os.args[3] } else { '' }
		})!))
		return
	}
	match mode {
		'signal-query' {
			println(json2.encode(client.start_batch(sdk.BatchStart{
				visibility_query: os.args[2]
				reason:           'disposable query batch'
				operation:        client.batch_signal('add', '3', {
					'batch': '"header"'
				})!
			})!))
		}
		'query-count' {
			println(client.rpc_list_workflow_executions(temporal.TemporalApiWorkflowserviceV1_ListWorkflowExecutionsRequest{
				namespace: 'default'
				query:     os.args[2]
			})!.executions.len)
		}
		'short-wait' {
			client.wait_batch(sdk.BatchHandle{ job_id: os.args[2] }, time.millisecond)!
			println('OK')
		}
		'result' {
			println(json2.encode(client.get_result(json2.decode[sdk.WorkflowHandle](os.args[2])!,
				10000)!))
		}
		'describe' {
			println(json2.encode(client.describe_batch(sdk.BatchHandle{ job_id: os.args[2] })!))
		}
		'wait' {
			println(json2.encode(client.wait_batch(sdk.BatchHandle{ job_id: os.args[2] },
				60 * time.second)!))
		}
		'stop' {
			client.stop_batch(sdk.BatchHandle{ job_id: os.args[2] }, 'test stop')!
			println('OK')
		}
		'list' {
			println(json2.encode(client.list_batches(sdk.BatchListOptions{
				page_size:  1
				page_token: if os.args.len > 2 {
					json2.decode[[]u8](os.args[2])!
				} else {
					[]u8{}
				}
			})!))
		}
		else {
			return error('unknown batch mode')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
