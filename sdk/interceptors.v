module sdk

import alexvyber.temporal_v.corebridge
import alexvyber.temporal_v.temporal
import time

// Client interceptors run for every unary RPC, including long polls and Update waits.
// They may replace a request or return an error. They must be safe for concurrent calls.
pub struct ClientRpcRequest {
pub:
	service        corebridge.RpcService
	method         string
	body           []u8
	timeout_millis u32 = 5000
	metadata       []corebridge.RpcMetadataEntry
	retry          bool
	// Return wire payloads for history export/inspection, bypassing storage.
	raw_payloads bool
	cancellation ?&RpcCancellation
	trace_parent ?TraceContext
	baggage      ?Baggage
}

pub interface ClientInterceptor {
	intercept(request ClientRpcRequest, next ClientNext) ![]u8
}

pub fn (client &Client) call_rpc(request ClientRpcRequest) ![]u8 {
	if client.connection.is_closed() { return error('client is closed') }
	if tracing := client.options.tracing { return traced_client_rpc(client, request, tracing)! }
	return ClientNext{
		client: client
	}.execute(request)!
}

pub struct ClientNext {
	client &Client
	index  int
}

pub fn (next ClientNext) execute(request ClientRpcRequest) ![]u8 {
	if next.index < next.client.options.interceptors.len {
		interceptor := next.client.options.interceptors[next.index]
		return interceptor.intercept(request, ClientNext{ client: next.client, index: next.index + 1 })!
	}
	mut context := if !request.raw_payloads
		&& next.client.options.data_converter.external_storage != none {
		client_storage_context(request)!
	} else {
		StorageContext{}
	}
	if request.timeout_millis > 0 {
		context = StorageContext{
			...context
			deadline: time.now().add(time.Duration(request.timeout_millis) * time.millisecond)
		}
	}
	body := if request.raw_payloads {
		request.body
	} else {
		next.client.options.data_converter.storage_message(.rpc_request, request.body, true,
			context,
			service: request.service
			method:  request.method
		)!
	}
	mut timeout := request.timeout_millis
	if timeout > 0 {
		remaining := context.deadline - time.now()
		if remaining <= 0 {
			return RpcError{
				message: 'RPC deadline exceeded during external storage'
				status:  4
			}
		}
		timeout = u32(if remaining / time.millisecond > 0 {
			remaining / time.millisecond
		} else {
			1
		})
	}
	mut rpc_cancellation := ?&corebridge.RpcCancellation(none)
	if cancellation := request.cancellation { rpc_cancellation = cancellation.token }
	reply := next.client.connection.rpc(request.service, request.method, body, timeout,
		metadata:     request.metadata
		retry:        request.retry
		cancellation: rpc_cancellation
	) or {
		if err is corebridge.RpcError {
			return RpcError{
				message: err.message
				status:  err.status
				details: err.details
			}
		}
		return err
	}
	if request.raw_payloads { return reply }
	return next.client.options.data_converter.storage_message(.rpc_response, normalize_history_response(request.service,
		request.method, reply)!, false, context,
		service: request.service
		method:  request.method
	)!
}

// Activity interceptors apply to both local and remote Activities. They share the
// handler's context, including heartbeat, cancellation and deferred completion.
pub interface ActivityInterceptor {
	intercept(mut context ActivityContext, input []string, next ActivityNext) !PayloadValue
}

pub struct ActivityNext {
	handler      ActivityDispatch
	interceptors []ActivityInterceptor
	index        int
}

fn (next ActivityNext) execute_callback(mut context ActivityContext, input []string) !PayloadValue {
	if next.index < next.interceptors.len {
		interceptor := next.interceptors[next.index]
		return interceptor.intercept(mut context, input, ActivityNext{
			handler:      next.handler
			interceptors: next.interceptors
			index:        next.index + 1
		})!
	}
	if dynamic := next.handler.dynamic { return dynamic(mut context, context.input_values())! }
	handler := next.handler.named or { return error('Activity dispatch has no handler') }
	return handler(mut context, input)!
}

// Workflow interceptors execute during replay and must be deterministic. Logging
// through the workflow context suppresses replay duplicates.
pub interface WorkflowInterceptor {
	intercept(event Event, mut context WorkflowContext, next WorkflowNext) !
}

pub struct WorkflowNext {
	workflow     Workflow
	interceptors []WorkflowInterceptor
	index        int
}

fn (next WorkflowNext) execute_callback(event Event, mut context WorkflowContext) ! {
	if next.index < next.interceptors.len {
		interceptor := next.interceptors[next.index]
		interceptor.intercept(event, mut context, WorkflowNext{
			workflow:     next.workflow
			interceptors: next.interceptors
			index:        next.index + 1
		})!
		return
	}
	mut handler := next.workflow
	if event.kind == .signal_received && dynamic_signal(handler, event.name) {
		if mut handler is DynamicSignalsWorkflow {
			handler.signal_dynamic(event, mut context)!
			return
		}
	}
	handler.on_event(event, mut context)!
}

fn (mut run Run) invoke(event Event) ! {
	previous := run.context.event_group_state
	values := run.context.context_values
	defer { run.context.context_values = values }
	run.context.enter_handler_event_group(event)
	defer { run.context.event_group_state = previous }
	run.invoke_traced(event)!
}

// Query and Update validation chains deliberately expose read-only workflow info.
pub interface QueryInterceptor {
	intercept(name string, input []string, info WorkflowInfo, next QueryNext) !PayloadValue
}

pub struct QueryNext {
	context_values  WorkflowContextValues
	propagation     PropagationContext
	typed_arguments []PayloadValue
	argument_values []RawValue
	request_headers map[string]temporal.TemporalApiCommonV1_Payload
	run             &Run
	index           int
}

fn (next QueryNext) execute_callback(name string, input []string) !PayloadValue {
	if next.index < next.run.query_interceptors.len {
		interceptor := next.run.query_interceptors[next.index]
		return interceptor.intercept(name, input, WorkflowInfo{
			...next.run.context.info()
			context_values: next.context_values
		}, QueryNext{
			context_values:  next.context_values
			propagation:     next.propagation
			run:             next.run
			request_headers: next.request_headers
			index:           next.index + 1
			argument_values: next.argument_values
			typed_arguments: next.typed_arguments
		})!
	}
	handler := next.run.workflow
	if named_query(handler, name) {
		if handler is QueryableWorkflow {
			return handler.query(name, input, next.run.context.interaction_view(next.request_headers,
				next.argument_values, next.context_values)!)!
		}
	}
	if handler is DynamicQueriesWorkflow {
		return handler.query_dynamic(name, next.argument_values.clone(), next.run.context.interaction_view(next.request_headers,
			next.argument_values, next.context_values)!)!
	}
	if handler is QueryableWorkflow { return error('unknown query: ' + name) }
	return error('workflow does not implement queries')
}

pub interface UpdateValidatorInterceptor {
	intercept(name string, input []string, info WorkflowInfo, next UpdateValidatorNext) !
}

pub struct UpdateValidatorNext {
	context_values  WorkflowContextValues
	propagation     PropagationContext
	typed_arguments []PayloadValue
	argument_values []RawValue
	request_headers map[string]temporal.TemporalApiCommonV1_Payload
	run             &Run
	index           int
}

fn (next UpdateValidatorNext) execute_callback(name string, input []string) ! {
	if next.index < next.run.update_validator_interceptors.len {
		interceptor := next.run.update_validator_interceptors[next.index]
		interceptor.intercept(name, input, WorkflowInfo{
			...next.run.context.info()
			context_values: next.context_values
		}, UpdateValidatorNext{
			context_values:  next.context_values
			propagation:     next.propagation
			run:             next.run
			request_headers: next.request_headers
			index:           next.index + 1
			argument_values: next.argument_values
			typed_arguments: next.typed_arguments
		})!
		return
	}
	handler := next.run.workflow
	if handler is UpdatableWorkflow {
		if name in handler.update_names() {
			handler.validate_update(name, input, next.run.context.interaction_view(next.request_headers,
				next.argument_values, next.context_values)!)!
			return
		}
	}
	if handler is DynamicUpdatesWorkflow {
		handler.validate_dynamic_update(name, next.argument_values.clone(), next.run.context.interaction_view(next.request_headers,
			next.argument_values, next.context_values)!)!
		return
	}
	return error('unknown update: ' + name)
}

// Every workflow command passes through this chain after event handlers finish.
// It supports deterministic header propagation and instrumentation for all
// outbound operations, including commands created by the SDK itself.
pub interface CommandInterceptor {
	intercept(command temporal.CoresdkWorkflowCommands_WorkflowCommand, info WorkflowInfo, next CommandNext) !temporal.CoresdkWorkflowCommands_WorkflowCommand
}

pub struct CommandNext {
	interceptors []CommandInterceptor
	information  WorkflowInfo
	index        int
}

pub fn (next CommandNext) execute(command temporal.CoresdkWorkflowCommands_WorkflowCommand) !temporal.CoresdkWorkflowCommands_WorkflowCommand {
	if next.index < next.interceptors.len {
		interceptor := next.interceptors[next.index]
		return interceptor.intercept(command, next.information, CommandNext{
			interceptors: next.interceptors
			information:  next.information
			index:        next.index + 1
		})!
	}
	return command
}

pub fn (next QueryNext) headers() !map[string]PayloadValue {
	return next.run.context.converter.decode_headers(next.request_headers)!
}

pub fn (next UpdateValidatorNext) headers() !map[string]PayloadValue {
	return next.run.context.converter.decode_headers(next.request_headers)!
}

pub fn (next QueryNext) input_values() []RawValue {
	return next.argument_values.clone()
}

pub fn (next UpdateValidatorNext) input_values() []RawValue {
	return next.argument_values.clone()
}

pub fn (next QueryNext) typed_input_values() ![]PayloadValue {
	mut result := []PayloadValue{}
	for value in next.typed_arguments {
		result << clone_payload_value(value)
	}
	return result
}

pub fn (next UpdateValidatorNext) typed_input_values() ![]PayloadValue {
	mut result := []PayloadValue{}
	for value in next.typed_arguments {
		result << clone_payload_value(value)
	}
	return result
}

pub fn (next QueryNext) propagation_context() PropagationContext {
	return next.propagation
}

pub fn (next UpdateValidatorNext) propagation_context() PropagationContext {
	return next.propagation
}

pub fn (next ActivityNext) execute(mut context ActivityContext, input []string) !PayloadValue {
	mut outcome := CallbackValueOutcome{}
	next.execute_recovered(mut context, input, mut outcome)
	if failure := outcome.failure { return failure }
	return outcome.value
}

fn (next ActivityNext) execute_recovered(mut context ActivityContext, input []string, mut outcome CallbackValueOutcome) {
	$if v3_backend ? {
		defer {
			if message := recover() {
				outcome.failure = PanicError{
					message:     message
					stack_trace: recovery_stack_trace()
				}
			}
		}
	}
	outcome.value = next.execute_callback(mut context, input) or {
		outcome.failure = err
		return
	}
}

pub fn (next WorkflowNext) execute(event Event, mut context WorkflowContext) ! {
	mut outcome := CallbackOutcome{}
	next.execute_recovered(event, mut context, mut outcome)
	if failure := outcome.failure { return failure }
}

fn (next WorkflowNext) execute_recovered(event Event, mut context WorkflowContext, mut outcome CallbackOutcome) {
	$if v3_backend ? {
		defer {
			if message := recover() {
				outcome.failure = PanicError{
					message:     message
					stack_trace: recovery_stack_trace()
				}
			}
		}
	}
	next.execute_callback(event, mut context) or {
		outcome.failure = err
		return
	}
}

pub fn (next QueryNext) execute(name string, input []string) !PayloadValue {
	mut outcome := CallbackValueOutcome{}
	next.execute_recovered(name, input, mut outcome)
	if failure := outcome.failure { return failure }
	return outcome.value
}

fn (next QueryNext) execute_recovered(name string, input []string, mut outcome CallbackValueOutcome) {
	$if v3_backend ? {
		defer {
			if message := recover() {
				outcome.failure = PanicError{
					message:     message
					stack_trace: recovery_stack_trace()
				}
			}
		}
	}
	outcome.value = next.execute_callback(name, input) or {
		outcome.failure = err
		return
	}
}

pub fn (next UpdateValidatorNext) execute(name string, input []string) ! {
	mut outcome := CallbackOutcome{}
	next.execute_recovered(name, input, mut outcome)
	if failure := outcome.failure { return failure }
}

fn (next UpdateValidatorNext) execute_recovered(name string, input []string, mut outcome CallbackOutcome) {
	$if v3_backend ? {
		defer {
			if message := recover() {
				outcome.failure = PanicError{
					message:     message
					stack_trace: recovery_stack_trace()
				}
			}
		}
	}
	next.execute_callback(name, input) or {
		outcome.failure = err
		return
	}
}

// is_replaying exposes Core's activation flag to read-only query interceptors.
pub fn (next QueryNext) is_replaying() bool {
	return next.run.context.is_replaying()
}

// is_replaying_history_events is false for query-only activations, including
// cached queries whose activation is marked as replaying by Core.
pub fn (next QueryNext) is_replaying_history_events() bool {
	return next.run.context.is_replaying_history_events()
}
