module main

import temporal_v.sdk
import temporal_v.corebridge
import temporal_v.temporal
import sync
import time

@[heap]
struct RpcTraceAudit {
	mutex &sync.RwMutex = sync.new_rwmutex()
mut:
	requests []sdk.ClientRpcRequest
}

struct TraceRpcInterceptor {
	audit &RpcTraceAudit
}

fn (interceptor TraceRpcInterceptor) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	mut audit := interceptor.audit
	audit.mutex.lock()
	audit.requests << request
	audit.mutex.unlock()
	if request.method == 'GetSystemInfo' { return next.execute(request)! }
	if request.method == 'TraceFailure' {
		return sdk.RpcError{
			message: 'fixture rpc failure'
			status:  7
		}
	}
	return []u8{}
}

fn trace_lease_rpc(client &sdk.Client, results chan string) {
	defer {
		client.close()
		client.close()
	}
	client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
		results <- err.msg()
		return
	}
	results <- ''
}

fn client_validation() ! {
	exporter := RecordingTraceExporter{
		audit: &ExportAudit{}
	}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:       exporter
		flush_interval: time.millisecond
	})!
	defer { provider.shutdown(5 * time.second) or {} }
	parent := sdk.TraceContext{
		trace_id:    '1234567890abcdef1234567890abcdef'
		span_id:     'abcdef1234567890'
		trace_flags: 3
		trace_state: 'vendor=value'
	}
	audit := &RpcTraceAudit{}
	config := sdk.options_from_env()!
	client := sdk.connect(sdk.ClientOptions{
		address:      config.address
		tracing:      sdk.TraceOptions{
			provider: provider
			parent:   parent
		}
		interceptors: [sdk.ClientInterceptor(TraceRpcInterceptor{
			audit: audit
		})]
	})!
	defer { client.close() }
	header := temporal.TemporalApiCommonV1_Header{
		fields: {
			'application': sdk.DataConverter{}.encode_value(sdk.PayloadValue(sdk.JsonValue{
				data: '"value"'
			}))!
		}
	}
	start := temporal.TemporalApiWorkflowserviceV1_StartWorkflowExecutionRequest{
		namespace:   'default'
		workflow_id: 'trace-fixture'
		header:      header
		pb_unknown:  [
			u8(0xa0),
			0x06,
			0x01,
		]
	}
	update := temporal.TemporalApiWorkflowserviceV1_UpdateWorkflowExecutionRequest{
		request: temporal.TemporalApiUpdateV1_Request{
			input: temporal.TemporalApiUpdateV1_Input{
				name:   'update'
				header: header
			}
		}
	}
	multi := temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest{
		operations: [
			temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation{
				operation: temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_StartWorkflow_Case{
					value: start
				}
			},
			temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation{
				operation: temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_UpdateWorkflow_Case{
					value: update
				}
			},
		]
	}
	requests := [
		sdk.ClientRpcRequest{
			method: 'StartWorkflowExecution'
			body:   start.encode()
		},
		sdk.ClientRpcRequest{
			method: 'StartActivityExecution'
			body:   temporal.TemporalApiWorkflowserviceV1_StartActivityExecutionRequest{
				header: header
			}.encode()
		},
		sdk.ClientRpcRequest{
			method: 'SignalWithStartWorkflowExecution'
			body:   temporal.TemporalApiWorkflowserviceV1_SignalWithStartWorkflowExecutionRequest{
				header: header
			}.encode()
		},
		sdk.ClientRpcRequest{
			method: 'SignalWorkflowExecution'
			body:   temporal.TemporalApiWorkflowserviceV1_SignalWorkflowExecutionRequest{
				header: header
			}.encode()
		},
		sdk.ClientRpcRequest{
			method: 'QueryWorkflow'
			body:   temporal.TemporalApiWorkflowserviceV1_QueryWorkflowRequest{
				query: temporal.TemporalApiQueryV1_WorkflowQuery{
					header: header
				}
			}.encode()
		},
		sdk.ClientRpcRequest{
			method: 'UpdateWorkflowExecution'
			body:   update.encode()
		},
		sdk.ClientRpcRequest{
			method: 'ExecuteMultiOperation'
			body:   multi.encode()
		},
	]
	for request in requests {
		client.call_rpc(sdk.ClientRpcRequest{
			...request
			metadata: [
				corebridge.RpcMetadataEntry{
					name:  'application-bin'
					value: [u8(0), 255]
				},
				corebridge.RpcMetadataEntry{
					name:  'application-bin'
					value: [u8(1)]
				},
				corebridge.RpcMetadataEntry{
					name:  'traceparent'
					value: 'stale'.bytes()
				},
			]
		})!
	}
	for index, request in audit.requests {
		mut metadata := map[string]string{}
		mut application := []corebridge.RpcMetadataEntry{}
		for entry in request.metadata {
			if entry.name == 'application-bin' {
				application << entry
			} else {
				metadata[entry.name] = entry.value.bytestr()
			}
		}
		propagated := sdk.extract_trace_context(metadata) or {
			return error('RPC trace metadata missing')
		}
		if propagated.trace_id != parent.trace_id || propagated.span_id == parent.span_id
			|| propagated.trace_state != parent.trace_state || application.len != 2
			|| application[0].value != [u8(0), 255] || application[1].value != [u8(1)] {
			return error('RPC metadata changed')
		}
		mut headers := []temporal.TemporalApiCommonV1_Header{}
		match request.method {
			'StartWorkflowExecution' {
				value :=
					temporal.TemporalApiWorkflowserviceV1_StartWorkflowExecutionRequest.decode(request.body)!
				headers << value.header or { return error('missing start header') }
				if value.pb_unknown != start.pb_unknown {
					return error('unknown protobuf fields lost')
				}
			}
			'StartActivityExecution' {
				value :=
					temporal.TemporalApiWorkflowserviceV1_StartActivityExecutionRequest.decode(request.body)!
				headers << value.header or { return error('missing activity header') }
			}
			'SignalWithStartWorkflowExecution' {
				value :=
					temporal.TemporalApiWorkflowserviceV1_SignalWithStartWorkflowExecutionRequest.decode(request.body)!
				headers << value.header or { return error('missing start-signal header') }
			}
			'SignalWorkflowExecution' {
				value :=
					temporal.TemporalApiWorkflowserviceV1_SignalWorkflowExecutionRequest.decode(request.body)!
				headers << value.header or { return error('missing signal header') }
			}
			'QueryWorkflow' {
				value :=
					temporal.TemporalApiWorkflowserviceV1_QueryWorkflowRequest.decode(request.body)!
				query := value.query or { return error('missing query') }
				headers << query.header or { return error('missing query header') }
			}
			'UpdateWorkflowExecution' {
				value :=
					temporal.TemporalApiWorkflowserviceV1_UpdateWorkflowExecutionRequest.decode(request.body)!
				update_request := value.request or { return error('missing update') }
				input := update_request.input or { return error('missing input') }
				headers << input.header or { return error('missing update header') }
			}
			'ExecuteMultiOperation' {
				value :=
					temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest.decode(request.body)!
				for item in value.operations {
					operation := item.operation or { return error('missing multi operation') }
					match operation {
						temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_StartWorkflow_Case {
							headers << operation.value.header or {
								return error('missing multi start header')
							}
						}
						temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_UpdateWorkflow_Case {
							update_request := operation.value.request or {
								return error('missing multi update')
							}

							input := update_request.input or { return error('missing multi input') }
							headers << input.header or {
								return error('missing multi update header')
							}
						}
					}
				}
			}
			else {
				return error('unexpected request ${index}')
			}
		}

		for value in headers {
			context := sdk.extract_temporal_trace_context(value.fields) or {
				return error('Temporal carrier missing')
			}
			if context != propagated
				|| value.fields['application'].encode() != header.fields['application'].encode() {
				return error('Temporal carrier differs from RPC span')
			}
			application_headers := sdk.DataConverter{}.decode_headers(value.fields)!
			if application_headers.len != 1 { return error('system header leaked to application') }
		}
	}
	if _ := client.call_rpc(sdk.ClientRpcRequest{ method: 'TraceFailure' }) {
		return error('RPC failure was swallowed')
	}
	explicit := sdk.TraceContext{
		...parent
		span_id: '1234567890abcdef'
	}
	scoped := client.with_trace_parent(explicit)!
	mut leases := []&sdk.Client{}
	for _ in 0 .. 16 {
		leases << scoped.with_trace_parent(explicit)!
	}
	client.close()
	scoped.close()
	if _ := client.with_trace_parent(parent) { return error('closed client retained') }
	results := chan string{cap: 16}
	for lease in leases {
		spawn trace_lease_rpc(lease, results)
	}
	for _ in leases {
		if message := <-results {
			if message != '' { return error(message) }
		}
	}
	provider.force_flush(5 * time.second)!
	exporter.audit.mutex.rlock()
	defer { exporter.audit.mutex.runlock() }
	if exporter.audit.records.len != 24 {
		return error('client trace span count ${exporter.audit.records.len}')
	}
	mut failures := 0
	for span in exporter.audit.records {
		expected := if span.name == 'GetSystemInfo' { explicit.span_id } else { parent.span_id }
		if span.context.trace_id != parent.trace_id || span.parent_span_id != expected {
			return error('client parent context changed')
		}
		if span.name == 'TraceFailure' {
			if span.status != .error || span.events.len != 1 {
				return error('RPC failure not recorded')
			}
			failures++
		}
	}
	if failures != 1 { return error('missing failed RPC span') }
	println('CLIENT_OK')
}
