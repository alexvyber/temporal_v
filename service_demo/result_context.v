module main

import sdk
import temporal
import json2

struct ResultContextState {
	input  sdk.SerializationContext
	result sdk.SerializationContext
}

fn result_context_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	scope := temporal.TemporalApiNexusV1_PropagatedSerializationContext{
		endpoint:  options.task_queue + '-endpoint'
		service:   'v-service'
		operation: 'async'
	}
	completed := client.start(sdk.WorkflowStart{
		workflow_id:                            options.task_queue + '-result-scope'
		workflow_type:                          'v.result-context'
		task_queue:                             options.task_queue
		propagated_nexus_serialization_context: scope
	})!
	result := client.get_result(completed, 10000)!
	if result.status != .completed { return error('propagated workflow did not complete') }
	state := json2.decode[ResultContextState](result.data)!
	if state.input.kind != .workflow || state.input.workflow_id != completed.workflow_id
		|| state.result.kind != .nexus || state.result.endpoint != scope.endpoint
		|| state.result.service != scope.service || state.result.operation != scope.operation {
		return error('workflow input/result contexts were not independently bound: ' +
			json2.encode(state))
	}
	first := client.history_page(completed, sdk.HistoryOptions{ page_size: 1 })!
	if first.propagated_nexus_serialization_context == none || first.next_page_token.len == 0 {
		return error('history did not return the propagated result context with its first page')
	}
	second := client.history_page(completed, sdk.HistoryOptions{
		page_size:       1
		next_page_token: first.next_page_token
	})!
	if repeated := second.propagated_nexus_serialization_context {
		if repeated != scope { return error('history page changed its result context') }
	}
	failed := client.start(sdk.WorkflowStart{
		workflow_id:                            options.task_queue + '-result-failure'
		workflow_type:                          'v.failure-chain.local'
		task_queue:                             options.task_queue
		propagated_nexus_serialization_context: scope
	})!
	failure_result := client.get_result(failed, 10000)!
	failure := failure_result.failure or { return error('missing Nexus-scoped workflow failure') }
	assert_failure_chain(failure)!
	if failure_result.status != .failed || failure.attributes_error != '' {
		return error('propagated failure did not decode')
	}
	canceled := client.start(sdk.WorkflowStart{
		workflow_id:                            options.task_queue + '-result-cancel'
		workflow_type:                          'v.cancel-details'
		task_queue:                             options.task_queue
		propagated_nexus_serialization_context: scope
	})!
	client.cancel(canceled, 'Nexus cancellation')!
	cancellation := client.get_result(canceled, 10000)!
	if cancellation.status != .canceled
		|| cancellation.details != ['"Nexus cancellation"', 'cancel 世界\x00\xff'] {
		return error('propagated cancellation did not decode')
	}
	continued := client.start(sdk.WorkflowStart{
		workflow_id:                            options.task_queue + '-result-continue'
		workflow_type:                          'v.arguments.continue'
		task_queue:                             options.task_queue
		arguments:                              sdk.Arguments(['continue',
			'Nexus continuation 世界', 'third'])
		propagated_nexus_serialization_context: scope
	})!
	if client.get_chain_result(continued, 10000)!.data != 'Nexus continuation 世界' {
		return error('continuation lost Nexus result context')
	}
	return [
		'Workflow inputs retained workflow scope while final results/failures/cancellation used propagated Nexus scope',
		'Client result decoding honored the server result context; history retained its original result context through pagination',
		'Continue-as-new inherited the result context across its run chain',
	]
}
