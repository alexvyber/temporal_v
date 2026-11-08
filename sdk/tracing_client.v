module sdk

import corebridge
import temporal

fn trace_rpc_service(service corebridge.RpcService) string {
	return match service {
		.workflow { 'temporal.api.workflowservice.v1.WorkflowService' }
		.operator { 'temporal.api.operatorservice.v1.OperatorService' }
		.cloud { 'temporal.api.cloud.cloudservice.v1.CloudService' }
		.test { 'temporal.api.testservice.v1.TestService' }
		.health { 'grpc.health.v1.Health' }
	}
}

fn traced_rpc_body(request ClientRpcRequest, context PropagationContext) ![]u8 {
	if request.service != .workflow { return request.body }
	payload := context.temporal_header()!
	match request.method {
		'StartWorkflowExecution' {
			mut value :=
				temporal.TemporalApiWorkflowserviceV1_StartWorkflowExecutionRequest.decode(request.body)!
			mut header := value.header or { temporal.TemporalApiCommonV1_Header{} }
			header.fields[trace_header_key] = payload
			value.header = header
			return value.encode()
		}
		'StartActivityExecution' {
			mut value :=
				temporal.TemporalApiWorkflowserviceV1_StartActivityExecutionRequest.decode(request.body)!
			mut header := value.header or { temporal.TemporalApiCommonV1_Header{} }
			header.fields[trace_header_key] = payload
			value.header = header
			return value.encode()
		}
		'ExecuteMultiOperation' {
			mut value :=
				temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest.decode(request.body)!
			for index, item in value.operations {
				operation := item.operation or { continue }
				match operation {
					temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_StartWorkflow_Case {
						encoded := traced_rpc_body(ClientRpcRequest{
							...request
							method: 'StartWorkflowExecution'
							body:   operation.value.encode()
						}, context)!
						value.operations[index] = temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation{
							...item
							operation: temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_StartWorkflow_Case{
								value: temporal.TemporalApiWorkflowserviceV1_StartWorkflowExecutionRequest.decode(encoded)!
							}
						}
					}
					temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_UpdateWorkflow_Case {
						encoded := traced_rpc_body(ClientRpcRequest{
							...request
							method: 'UpdateWorkflowExecution'
							body:   operation.value.encode()
						}, context)!
						value.operations[index] = temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation{
							...item
							operation: temporal.TemporalApiWorkflowserviceV1_ExecuteMultiOperationRequest_Operation_UpdateWorkflow_Case{
								value: temporal.TemporalApiWorkflowserviceV1_UpdateWorkflowExecutionRequest.decode(encoded)!
							}
						}
					}
				}
			}
			return value.encode()
		}
		'SignalWithStartWorkflowExecution' {
			mut value :=
				temporal.TemporalApiWorkflowserviceV1_SignalWithStartWorkflowExecutionRequest.decode(request.body)!
			mut header := value.header or { temporal.TemporalApiCommonV1_Header{} }
			header.fields[trace_header_key] = payload
			value.header = header
			return value.encode()
		}
		'SignalWorkflowExecution' {
			mut value :=
				temporal.TemporalApiWorkflowserviceV1_SignalWorkflowExecutionRequest.decode(request.body)!
			mut header := value.header or { temporal.TemporalApiCommonV1_Header{} }
			header.fields[trace_header_key] = payload
			value.header = header
			return value.encode()
		}
		'QueryWorkflow' {
			mut value :=
				temporal.TemporalApiWorkflowserviceV1_QueryWorkflowRequest.decode(request.body)!
			mut query := value.query or { return error('query is missing') }
			mut header := query.header or { temporal.TemporalApiCommonV1_Header{} }
			header.fields[trace_header_key] = payload
			query.header = header
			value.query = query
			return value.encode()
		}
		'UpdateWorkflowExecution' {
			mut value :=
				temporal.TemporalApiWorkflowserviceV1_UpdateWorkflowExecutionRequest.decode(request.body)!
			mut update := value.request or { return error('update request is missing') }
			mut input := update.input or { return error('update input is missing') }
			mut header := input.header or { temporal.TemporalApiCommonV1_Header{} }
			header.fields[trace_header_key] = payload
			input.header = header
			update.input = input
			value.request = update
			return value.encode()
		}
		else {
			return request.body
		}
	}
}

fn finish_rpc_error(span &TraceSpan, failure IError, options TraceOptions) {
	if options.record_errors {
		span.record_exception('RpcError', failure) or {}
		span.set_status(.error, failure.msg()) or {}
	}
	if failure is RpcError {
		span.set_attribute('rpc.grpc.status_code', TraceAttribute(i64(failure.status))) or {}
	}
}

fn traced_client_rpc(client &Client, request ClientRpcRequest, options TraceOptions) ![]u8 {
	mut parent := options.parent
	if explicit := request.trace_parent { parent = explicit }
	if value := parent { value.validate()! }
	baggage := request.baggage or { options.baggage }
	span := options.provider.start_span(request.method, TraceSpanOptions{
		parent:     parent
		baggage:    baggage
		kind:       .client
		attributes: {
			'rpc.system':         TraceAttribute('grpc')
			'rpc.service':        TraceAttribute(trace_rpc_service(request.service))
			'rpc.method':         TraceAttribute(request.method)
			'temporal.namespace': TraceAttribute(client.options.namespace)
		}
	})!
	defer { span.finish() }
	mut metadata := []corebridge.RpcMetadataEntry{}
	for entry in request.metadata {
		if entry.name.to_lower() !in ['traceparent', 'tracestate', 'baggage'] { metadata << entry }
	}
	propagation := PropagationContext{
		span:    span.context()
		baggage: baggage
	}
	for name, value in propagation.headers()! {
		metadata << corebridge.RpcMetadataEntry{
			name:  name
			value: value.bytes()
		}
	}
	// Invalid raw protobuf must still reach the service's normal validation path.
	body := traced_rpc_body(request, propagation) or { request.body }
	result := ClientNext{
		client: client
	}.execute(ClientRpcRequest{ ...request, body: body, metadata: metadata }) or {
		finish_rpc_error(span, err, options)
		return err
	}
	span.set_attribute('rpc.grpc.status_code', TraceAttribute(i64(0))) or {}
	return result
}
