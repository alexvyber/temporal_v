module sdk

import alexvyber.temporal_v.temporal
import alexvyber.temporal_v.corebridge

pub struct HistoryOptions {
pub:
	page_size       int = 1000
	next_page_token []u8
	wait_new_event  bool
	close_only      bool
	raw_payloads    bool
}

pub fn (client &Client) history_page(handle WorkflowHandle, options HistoryOptions) !temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse {
	if options.page_size < 1 || options.page_size > 10000 {
		return error('history page size must be 1–10000')
	}
	request := temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryRequest{
		namespace:                 client.options.namespace
		execution:                 execution(handle)!
		maximum_page_size:         options.page_size
		next_page_token:           options.next_page_token
		wait_new_event:            options.wait_new_event
		history_event_filter_type: if options.close_only {
			.history_event_filter_type_close_event
		} else {
			.history_event_filter_type_all_event
		}
	}
	return temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse.decode(client.call_rpc(ClientRpcRequest{
		service:        .workflow
		method:         'GetWorkflowExecutionHistory'
		body:           request.encode()
		timeout_millis: client.options.rpc_timeout_millis
		raw_payloads:   options.raw_payloads
		retry:          client.options.rpc_retry
		cancellation:   client.options.cancellation
	})!)!
}

// Returns the complete current wire history for binary replay. External-storage
// references remain references; payload converters and retrieval are bypassed.
pub fn (client &Client) history(handle WorkflowHandle) !temporal.TemporalApiHistoryV1_History {
	mut result := temporal.TemporalApiHistoryV1_History{}
	mut token := []u8{}
	for {
		page := client.history_page(handle, HistoryOptions{
			next_page_token: token
			raw_payloads:    true
		})!
		history := history_from_response(page)!
		result.events << history.events
		result.pb_unknown << history.pb_unknown
		if page.next_page_token.len == 0 { return result }
		if page.next_page_token == token {
			return error('server returned repeated history page token')
		}
		token = page.next_page_token.clone()
	}
	return result
}

// Decode raw History message batches in wire order. This helper does not run
// payload converters or retrieve external-storage references, so exports retain
// their durable wire payloads and binary unknown fields.
pub fn history_from_response(response temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse) !temporal.TemporalApiHistoryV1_History {
	if response.raw_history.len == 0 {
		return response.history or { temporal.TemporalApiHistoryV1_History{} }
	}
	if history := response.history {
		if history.events.len > 0 {
			return error('history response contains both structured and raw events')
		}
	}
	mut result := temporal.TemporalApiHistoryV1_History{}
	for blob in response.raw_history {
		if blob.data.len == 0 {
			continue
		}
		batch := match blob.encoding_type {
			.encoding_type_proto3 {
				temporal.TemporalApiHistoryV1_History.decode(blob.data)!
			}
			.encoding_type_json {
				temporal.TemporalApiHistoryV1_History.from_json(blob.data.bytestr())!
			}
			else {
				return error('unsupported raw history encoding: ' + int(blob.encoding_type).str())
			}
		}

		result.events << batch.events
		result.pb_unknown << batch.pb_unknown
	}
	return result
}

fn normalize_history_response(service corebridge.RpcService, method string, bytes []u8) ![]u8 {
	if service != .workflow || method != 'GetWorkflowExecutionHistory' { return bytes }
	mut response :=
		temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse.decode(bytes)!
	if response.raw_history.len == 0 { return bytes }
	response.history = history_from_response(response)!
	response.raw_history = []temporal.TemporalApiCommonV1_DataBlob{}
	return response.encode()
}
