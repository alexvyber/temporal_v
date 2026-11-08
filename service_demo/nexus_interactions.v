module main

import alexvyber.temporal_v.sdk
import x.json2
import time

struct NexusCounterState {
	total i64
}

fn nexus_interaction_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	for operation in ['signal-retry', 'signal-start-retry'] {
		id := options.task_queue + '-' + operation
		workflow := sdk.WorkflowHandle{
			workflow_id: id
		}
		if operation == 'signal-retry' {
			client.start(sdk.WorkflowStart{
				workflow_id:   id
				workflow_type: 'v.counter'
				task_queue:    options.task_queue
				arguments:     '{"initial":1}'
			})!
		}
		handle := client.start_nexus_operation(sdk.NexusExecutionStart{
			operation_id: id
			endpoint:     options.task_queue + '-endpoint'
			service:      'v-service'
			operation:    operation
			input:        json2.encode({
				'workflow_id': id
			})
		})!
		result := client.nexus_operation_result(handle, 20 * time.second)!
		if result.status != .nexus_operation_execution_status_completed
			|| json2.decode[NexusCounterState](result.data)!.total != 4 {
			return error('Nexus retry duplicated a signal or query lost its converted result')
		}
		description := client.describe_nexus_operation(handle)!.raw.info or {
			return error('missing Nexus execution info')
		}

		if description.attempt < 2 { return error('signal retry fixture did not retry') }
		query := client.start_nexus_operation(sdk.NexusExecutionStart{
			operation_id: id + '-query'
			endpoint:     options.task_queue + '-endpoint'
			service:      'v-service'
			operation:    'query'
			input:        json2.encode({
				'workflow_id': id
			})
		})!
		queried := client.nexus_operation_result(query, 10 * time.second)!
		if queried.data != result.data { return error('query-backed Nexus changed workflow state') }
		explode := client.start_nexus_operation(sdk.NexusExecutionStart{
			operation_id: id + '-explode'
			endpoint:     options.task_queue + '-endpoint'
			service:      'v-service'
			operation:    'query'
			input:        json2.encode({
				'workflow_id': id
				'query':       'explode'
			})
		})!
		failed := client.nexus_operation_result(explode, 10 * time.second)!
		failure := failed.failure or { return error('missing query-backed Nexus failure') }
		if failed.status != .nexus_operation_execution_status_failed
			|| failure.root_message() != 'requested query error' || failure.attributes_error != '' {
			return error('query failure did not cross workflow/Nexus converter scopes')
		}
		client.signal(workflow, 'finish', 'null')!
		if client.get_result(workflow, 10000)!.status != .completed {
			return error('Nexus interaction target did not close')
		}
		rejected := client.start_nexus_operation(sdk.NexusExecutionStart{
			operation_id: id + '-reject'
			endpoint:     options.task_queue + '-endpoint'
			service:      'v-service'
			operation:    'query-reject'
			input:        json2.encode({
				'workflow_id': id
			})
		})!
		if client.nexus_operation_result(rejected, 10 * time.second)!.status != .nexus_operation_execution_status_failed {
			return error('query rejection did not fail the Nexus operation')
		}
	}
	return [
		'Nexus retries deduplicated ordinary signals and signal-with-start; mixed second signals were rejected, queries preserved scoped results and protected failures, and closed-query rejection failed only its operation',
	]
}
