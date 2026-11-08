module main

import flag
import x.json2
import os
import temporal_v.sdk
import temporal_v.example_interceptors
import temporal_v.temporal
import time
import temporal_v.example_converter

struct HistoryLink {
	event_id           i64
	target_workflow_id string
	target_run_id      string
	namespace          string
}

struct Reply {
	status      string
	workflow_id string
	run_id      string
	data        string
	message     string
	next_run_id string
	update_id   string
}

fn run() ! {
	mut parser := flag.new_flag_parser(os.args)
	parser.skip_executable()
	parser.application('v-client')
	address := parser.string('address', 0, os.getenv_opt('TEMPORAL_ADDRESS') or {
		'http://127.0.0.1:7233'
	}, 'Temporal URL')
	namespace_name := parser.string('namespace', 0, 'default', 'Temporal namespace')
	id := parser.string('workflow-id', 0, '', 'Workflow ID')
	run_id := parser.string('run-id', 0, '', 'Run ID')
	workflow_type := parser.string('type', 0, 'v.counter', 'Workflow type')
	queue := parser.string('task-queue', 0, 'v-sdk-spike', 'Task queue')
	name := parser.string('name', 0, '', 'Signal/query name')
	update_id := parser.string('update-id', 0, '', 'Update ID')
	maximum_attempts := parser.int('maximum-attempts', 0, 1, 'Workflow retry maximum attempts')
	retry_ms := parser.int('retry-ms', 0, 100, 'Workflow retry initial interval')
	task_timeout := parser.int('task-timeout-seconds', 0, 10, 'Workflow task timeout')
	run_timeout := parser.int('run-timeout-seconds', 0, 0, 'Workflow run timeout')
	activity_id := parser.string('activity-id', 0, '', 'Activity ID for asynchronous completion')
	token_file := parser.string('token-file', 0, '', 'Private Activity task token JSON file')
	file := parser.string('file', 0, '', 'Binary history output path')
	inline_input := parser.string('input', 0, 'null', 'JSON argument')
	input_file := parser.string('input-file', 0, '', 'JSON argument file')
	memo_json := parser.string('memo', 0, '{}', 'Memo JSON object')
	wait_ms := parser.int('wait-ms', 0, 30000, 'Result deadline in milliseconds')
	rpc_ms := parser.int('rpc-ms', 0, 5000, 'RPC deadline in milliseconds')
	command := parser.finalize()!
	input := if input_file == '' { inline_input } else { os.read_file(input_file)! }
	if command.len != 1 { return error('expected start, signal, query, result or cancel') }
	if wait_ms < 1 || wait_ms > 300000 || rpc_ms < 1 || rpc_ms > 300000 {
		return error('invalid deadline')
	}
	mut client := sdk.connect(sdk.ClientOptions{
		interceptors:       example_interceptors.client()
		address:            address
		namespace:          namespace_name
		rpc_timeout_millis: u32(rpc_ms)
		transport:          sdk.transport_from_env()!
		data_converter:     example_converter.from_env()!
	})!
	defer { client.close() }
	handle := sdk.WorkflowHandle{
		workflow_id: id
		run_id:      run_id
	}
	if command[0].starts_with('activity-') {
		identifier := if token_file == '' {
			sdk.ActivityIdentifier(sdk.ActivityExecutionIdentifier{
				workflow:    handle
				activity_id: activity_id
			})
		} else {
			sdk.ActivityIdentifier(json2.decode[sdk.ActivityTaskToken](os.read_file(token_file)!)!)
		}
		match command[0] {
			'activity-heartbeat' {
				println(json2.encode(Reply{
					status: 'heartbeat'
					data:   json2.encode(client.heartbeat_activity(identifier, [
						input,
					])!)
				}))
			}
			'activity-complete' {
				client.complete_activity(identifier, input)!
				println(json2.encode(Reply{ status: 'completed' }))
			}
			'activity-fail' {
				client.fail_activity(identifier, sdk.ApplicationError{
					message:       'external Activity failure'
					non_retryable: true
					details:       [input]
				}, []string{})!
				println(json2.encode(Reply{ status: 'failed' }))
			}
			'activity-cancel' {
				client.report_activity_cancelled(identifier, [input])!
				println(json2.encode(Reply{ status: 'canceled' }))
			}
			else {
				return error('unknown Activity command')
			}
		}

		return
	}
	match command[0] {
		'describe' {
			response := client.rpc_describe_workflow_execution(temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionRequest{
				namespace: namespace_name
				execution: temporal.TemporalApiCommonV1_WorkflowExecution{
					workflow_id: id
					run_id:      run_id
				}
			})!
			info := response.workflow_execution_info or { return error('missing workflow info') }
			println(json2.encode(Reply{ status: info.status.str() }))
		}
		'metadata' {
			println(json2.encode(client.workflow_metadata(handle)!))
		}
		'history-links' {
			history := client.history(handle)!
			mut links := []HistoryLink{}
			for event in history.events {
				mut event_links := event.links.clone()
				if attributes := event.attributes {
					if attributes is temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionStartedEventAttributes_Case {
						for callback in attributes.value.completion_callbacks {
							event_links << callback.links
						}
					}
				}
				for link in event_links {
					variant := link.variant or { continue }
					if variant is temporal.TemporalApiCommonV1_Link_WorkflowEvent_Case {
						links << HistoryLink{
							event_id:           event.event_id
							target_workflow_id: variant.value.workflow_id
							target_run_id:      variant.value.run_id
							namespace:          variant.value.namespace
						}
					}
				}
			}
			println(json2.encode(links))
		}
		'history' {
			if file == '' { return error('history requires --file') }
			history := client.history(handle)!
			os.write_file_array(file, history.encode())!
			println(json2.encode(Reply{ status: 'exported', data: history.events.len.str() }))
		}
		'update' {
			accepted := client.start_update(handle, sdk.UpdateRequest{
				name:      name
				arguments: input
				id:        update_id
			})!
			if outcome := accepted.outcome {
				println(json2.encode(Reply{
					status:      if outcome.failed { 'failed' } else { 'completed' }
					update_id:   accepted.id
					workflow_id: accepted.workflow.workflow_id
					run_id:      accepted.workflow.run_id
					data:        outcome.data
					message:     outcome.message
				}))
				return
			}
			println(json2.encode(Reply{
				status:      'accepted'
				update_id:   accepted.id
				workflow_id: accepted.workflow.workflow_id
				run_id:      accepted.workflow.run_id
			}))
		}
		'update-result' {
			result := client.update_result(sdk.UpdateHandle{ workflow: handle, id: update_id },
				u32(wait_ms))!
			println(json2.encode(Reply{
				status:  if result.failed { 'failed' } else { 'completed' }
				data:    result.data
				message: result.message
			}))
		}
		'chain-result' {
			result := client.get_chain_result(handle, u32(wait_ms))!
			println(json2.encode(Reply{
				status:      result.status.str()
				data:        result.data
				message:     result.message
				next_run_id: result.next_run_id
			}))
		}
		'start' {
			mut memo := map[string]sdk.PayloadValue{}
			for key, value in json2.decode[map[string]json2.Any](memo_json)! {
				memo[key] = sdk.PayloadValue(json2.encode(value))
			}
			mut retry := ?sdk.RetryPolicy(none)
			if maximum_attempts != 1 {
				retry = sdk.RetryPolicy{
					maximum_attempts: maximum_attempts
					initial_interval: retry_ms * time.millisecond
				}
			}
			started := client.start(sdk.WorkflowStart{
				workflow_id:               id
				workflow_type:             workflow_type
				task_queue:                queue
				arguments:                 input
				memo:                      memo
				execution_timeout_seconds: 60
				task_timeout_seconds:      task_timeout
				run_timeout_seconds:       run_timeout
				retry:                     retry
			})!
			println(json2.encode(Reply{
				status:      'started'
				workflow_id: started.workflow_id
				run_id:      started.run_id
			}))
		}
		'signal' {
			client.signal(handle, name, input)!
			println(json2.encode(Reply{ status: 'signaled' }))
		}
		'query' {
			println(json2.encode(Reply{ status: 'queried', data: client.query(handle, name, input)! }))
		}
		'cancel' {
			client.cancel(handle, 'requested by V client')!
			println(json2.encode(Reply{ status: 'cancel_requested' }))
		}
		'result' {
			result := client.get_result(handle, u32(wait_ms))!
			println(json2.encode(Reply{
				status:      result.status.str()
				data:        result.data
				message:     result.message
				next_run_id: result.next_run_id
			}))
		}
		else {
			return error('unknown client operation')
		}
	}
}

fn main() {
	run() or {
		eprintln(json2.encode({
			'code':    err.code().str()
			'message': err.msg()
		}))
		exit(1)
	}
}
