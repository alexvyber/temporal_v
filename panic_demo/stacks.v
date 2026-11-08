module main

import sdk
import os
import json2

fn check_panic_responses(options sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{ address: options.address, data_converter: converter })!
	defer { client.close() }
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	mut saw_query := false
	client.query(handle, 'panic', sdk.Arguments(sdk.TypedArguments{}), sdk.QueryOptions{}) or {
		if err !is sdk.QueryError { return err }
		if !err.failure.stack_trace.contains('PanicWorkflow__query') {
			return error('query panic lost its original stack')
		}
		saw_query = true
		''
	}
	if !saw_query { return error('query panic unexpectedly succeeded') }
	update := client.start_update(handle, sdk.UpdateRequest{
		name: 'validator-panic'
		id:   'stack-validator'
	})!
	result := client.update_result(update, 5000)!
	failure := result.failure or { return error('validator panic has no structured failure') }
	if !result.failed || !failure.stack_trace.contains('PanicWorkflow__validate_update') {
		return error('validator panic lost its original stack')
	}
	println('PANIC_RESPONSES_OK')
}
