module main

import temporal_v.sdk
import temporal_v.temporal
import x.json2

struct UpdateScopes {
	input  sdk.SerializationContext
	result sdk.SerializationContext
}

fn update_scope_request(id string, name string, operation string) sdk.UpdateRequest {
	return sdk.UpdateRequest{
		id:                                     id
		name:                                   name
		arguments:                              sdk.Arguments(['"one"', '"two"'])
		request_id:                             id
		propagated_nexus_serialization_context: temporal.TemporalApiNexusV1_PropagatedSerializationContext{
			endpoint:  'context-endpoint'
			service:   'context-service'
			operation: operation
		}
	}
}

fn update_scope_assert(result sdk.UpdateResult, workflow_id string, operation string) ! {
	if result.failed { return error('scoped Update failed unexpectedly: ${result.message}') }
	scopes := json2.decode[UpdateScopes](result.data)!
	if scopes.input.kind != .workflow || scopes.input.workflow_id != workflow_id
		|| scopes.result.kind != .nexus || scopes.result.operation != operation {
		return error('Update input/result contexts were mixed: ${result.data}')
	}
}

fn update_context_start(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-update-context'
		workflow_type: 'v.update-context'
		task_queue:    options.task_queue
	})!
	held := client.start_update(handle, update_scope_request('held', 'hold', 'held'))!
	if held.outcome != none || held.stage != .accepted {
		return error('held Update did not stay pending')
	}
	immediate := client.start_update(handle, sdk.UpdateRequest{
		...update_scope_request('immediate', 'immediate', 'immediate')
		wait_for: .completed
	})!
	update_scope_assert(immediate.outcome or { return error('missing immediate outcome') },
		handle.workflow_id, 'immediate')!
	for mode in ['fail', 'reject'] {
		result := client.start_update(handle, sdk.UpdateRequest{
			...update_scope_request(mode, mode, mode)
			wait_for: .completed
		})!.outcome or { return error('missing scoped failure') }

		failure := result.failure or { return error('missing decoded Update failure') }
		if !result.failed || failure.attributes_error != '' || failure.details_error != ''
			|| failure.details.len == 0 {
			return error('Update failure lost scoped attributes/details')
		}
		if mode == 'fail' && (failure.root_message() != 'Update root cause'
			|| failure.details != ['"Update detail"', 'binary\x00\xff']) {
			return error('Update execution failure lost its cause or binary details')
		}
	}
	return [
		'Concurrent Updates used independent Nexus output contexts while inputs retained workflow scope; execution and validation failures preserved protected details',
	]
}

fn update_context_finish(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	handle := sdk.WorkflowHandle{
		workflow_id: options.task_queue + '-update-context'
	}
	client.signal(handle, 'release', 'null')!
	// Reconstruct only the durable execution/update identity. The server must
	// return the original result scope even without the initial client handle.
	result := client.update_result(sdk.UpdateHandle{ workflow: handle, id: 'held' }, 10000)!
	update_scope_assert(result, handle.workflow_id, 'held')!
	duplicate := client.start_update(handle, sdk.UpdateRequest{
		...update_scope_request('held', 'hold', 'other-context')
		wait_for: .completed
	})!
	update_scope_assert(duplicate.outcome or { return error('missing deduplicated Update outcome') },
		handle.workflow_id, 'held')!
	if duplicate.link == none { return error('Update response link was dropped') }
	if _ := client.start_update(handle, sdk.UpdateRequest{
		...update_scope_request('guarded', 'immediate', 'guarded')
		first_execution_run_id: '00000000-0000-4000-8000-000000000000'
	})
	{
		return error('Update ignored execution-chain guard')
	}
	client.signal(handle, 'finish', 'null')!
	closed := client.get_result(handle, 10000)!
	if closed.status != .completed || closed.data != '"workflow scope"' {
		return error('Update result scopes altered workflow output scope')
	}
	return [
		'A pending Update replayed after worker loss; detached polling and duplicate requests retained the original result scope and links, while chain guards rejected stale targets',
	]
}
