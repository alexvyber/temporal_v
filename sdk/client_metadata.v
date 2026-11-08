module sdk

import alexvyber.temporal_v.temporal
import x.json2

pub struct WorkflowMetadata {
pub:
	definition      WorkflowDefinition
	current_details string @[json: 'currentDetails']
}

pub fn (client &Client) workflow_metadata(handle WorkflowHandle) !WorkflowMetadata {
	response := client.rpc_query_workflow(temporal.TemporalApiWorkflowserviceV1_QueryWorkflowRequest{
		namespace: client.options.namespace
		execution: execution(handle)!
		query:     temporal.TemporalApiQueryV1_WorkflowQuery{
			query_type: '__temporal_workflow_metadata'
		}
	})!
	if rejected := response.query_rejected {
		return error_with_code('metadata query rejected: ${rejected.status}', 9)
	}
	result := response.query_result or { return error('metadata query has no result') }
	if result.payloads.len != 1 { return error('metadata query requires one payload') }
	data :=
		JsonPayloadConverter{}.from_payload(result.payloads[0], SerializationContext{})!.logical_data()
	return json2.decode[WorkflowMetadata](data)!
}
