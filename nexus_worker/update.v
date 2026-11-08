module main

import alexvyber.temporal_v.sdk
import x.json2

struct UpdateOperationInput {
	workflow_id string
	update_id   string
}

fn update_start(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	request := json2.decode[UpdateOperationInput](input)!
	client := context.client()!
	defer { client.close() }
	name := context.info().operation.trim_string_left('update-')
	return context.start_update(client, sdk.WorkflowHandle{ workflow_id: request.workflow_id }, sdk.UpdateRequest{
		name:      name
		id:        request.update_id
		arguments: sdk.Arguments([
			'"Nexus Update"',
			'"second"',
		])
	})!
}
