module main

import alexvyber.temporal_v.sdk
import x.json2
import time

fn nexus_update_begin(client &sdk.Client, options sdk.WorkerOptions, label string, operation string, id string) !sdk.NexusExecutionHandle {
	return client.start_nexus_operation(sdk.NexusExecutionStart{
		operation_id: options.task_queue + '-nexus-update-' + label
		endpoint:     options.task_queue + '-endpoint'
		service:      'v-service'
		operation:    operation
		input:        json2.encode({
			'workflow_id': options.task_queue + '-nexus-update-target'
			'update_id':   id
		})
	})!
}

fn nexus_update_start(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-nexus-update-target'
		workflow_type: 'v.update-context'
		task_queue:    options.task_queue
	})!
	held := nexus_update_begin(client, options, 'held', 'update-hold', 'nexus-held-update')!
	started := client.nexus_operation_started(held, 10 * time.second)!
	if started.operation_token == '' {
		return error('Update-backed operation did not expose its async identity')
	}
	return [
		'Update-backed Nexus accepted a pending Update with a durable completion callback before workflow/Nexus worker loss',
	]
}

fn nexus_update_finish(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	workflow := sdk.WorkflowHandle{
		workflow_id: options.task_queue + '-nexus-update-target'
	}
	client.signal(workflow, 'release', 'null')!
	held := sdk.NexusExecutionHandle{
		operation_id: options.task_queue + '-nexus-update-held'
	}
	result := client.nexus_operation_result(held, 10 * time.second)!
	if result.status != .nexus_operation_execution_status_completed {
		return error('Update callback did not survive worker loss')
	}
	update_scope_assert(sdk.UpdateResult{ data: result.data }, workflow.workflow_id, 'update-hold')!
	duplicate := nexus_update_begin(client, options, 'duplicate', 'update-hold',
		'nexus-held-update')!
	duplicate_result := client.nexus_operation_result(duplicate, 10 * time.second)!
	if duplicate_result.status != .nexus_operation_execution_status_completed
		|| duplicate_result.data != result.data {
		return error('completed Update retry did not return its original result')
	}
	for operation in ['update-immediate', 'update-fail', 'update-reject'] {
		handle := nexus_update_begin(client, options, operation, operation, operation)!
		outcome := client.nexus_operation_result(handle, 10 * time.second)!
		if operation == 'update-immediate' {
			if outcome.status != .nexus_operation_execution_status_completed {
				return error('immediate Update Nexus result failed')
			}
			update_scope_assert(sdk.UpdateResult{ data: outcome.data }, workflow.workflow_id,
				operation)!
		} else {
			failure := outcome.failure or {
				return error('missing Update-backed operation failure')
			}

			if outcome.status != .nexus_operation_execution_status_failed
				|| failure.attributes_error != '' || failure.details_error != '' {
				return error('Update-backed failure lost its scope')
			}
			if operation == 'update-fail' && failure.root_message() != 'Update root cause' {
				return error('Update-backed failure lost its root cause')
			}
			if operation == 'update-reject'
				&& !failure.root_message().contains('scoped validation rejected') {
				return error('Update validation rejection lost its message')
			}
		}
	}
	client.signal(workflow, 'finish', 'null')!
	if client.get_result(workflow, 10000)!.status != .completed {
		return error('backing workflow could not complete after its Updates')
	}
	return [
		'Update-backed callbacks survived both worker losses; completed Update deduplication, immediate results and scoped validation/execution failures reached Nexus callers',
	]
}
