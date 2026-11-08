module main

import sdk
import json2
import os
import crypto.sha256

struct InteractionInput {
	workflow_id string
	query       string = 'state'
}

fn interaction_start(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	request := json2.decode[InteractionInput](input)!
	config := sdk.options_from_env()!
	client := context.client()!
	defer { client.close() }
	workflow := sdk.WorkflowHandle{
		workflow_id: request.workflow_id
	}
	operation := context.info().operation
	if operation in ['signal-retry', 'signal-start-retry'] {
		if operation == 'signal-start-retry' {
			context.signal_with_start(client, sdk.WorkflowStart{
				workflow_id:   workflow.workflow_id
				workflow_type: 'v.counter'
				task_queue:    config.task_queue
				arguments:     '{"initial":1}'
			}, 'add', '{"amount":3}')!
		} else {
			context.signal_workflow(client, workflow, 'add', '{"amount":3}')!
		}
		if _ := context.signal_workflow(client, workflow, 'add', '{"amount":100}') {
			return error('second Nexus signal accepted')
		}
		if _ := context.signal_with_start(client, sdk.WorkflowStart{
			workflow_id:   workflow.workflow_id
			workflow_type: 'v.counter'
			task_queue:    config.task_queue
			arguments:     '{}'
		}, 'add', '{"amount":100}')
		{
			return error('mixed second Nexus signal accepted')
		}
		directory := os.getenv('V_SDK_EXAMPLE_NEXUS_RETRY_DIRECTORY')
		if directory == '' { return error('retry fixture directory is required') }
		marker := os.join_path(directory, sha256.hexhash(context.info().request_id))
		if !os.exists(marker) {
			os.write_file(marker, 'signaled')!
			return sdk.NexusHandlerError{
				message: 'retry after acknowledged signal'
				kind:    .unavailable
				retry:   .retryable
			}
		}
	}
	return context.query_workflow(client, workflow, request.query, 'null',
		reject_condition: if operation == 'query-reject' {
			.query_reject_condition_not_open
		} else {
			.query_reject_condition_none
		}
	)!
}
