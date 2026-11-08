module sdk

import temporal_v.corebridge
import encoding.base64
import sync

// Implementations execute on V threads and must return promptly when cancelled.
// Requests and replies contain protobuf message bytes, without HTTP/2 framing.
pub interface GrpcTransport {
	call(context &GrpcTransportContext, request corebridge.GrpcRequestSnapshot) !corebridge.GrpcReply
}

pub struct CustomGrpcTransport {
pub:
	transport           GrpcTransport
	control_concurrency int = 64
	poll_concurrency    int = 64
	wait_concurrency    int = 64
	max_pending         int = 1024
	max_pending_bytes   u64 = 64 * 1024 * 1024
}

@[heap]
pub struct GrpcTransportContext {
	event &corebridge.GrpcEvent
	mutex &sync.RwMutex = sync.new_rwmutex()
mut:
	closed         bool
	forwarders     map[u64]&corebridge.RpcCancellation
	next_forwarder u64
}

pub fn (context &GrpcTransportContext) cancelled() bool {
	context.mutex.rlock()
	defer { context.mutex.runlock() }
	return context.closed || context.event.cancelled()
}

fn (mut context GrpcTransportContext) cancel_forwarders() {
	context.mutex.rlock()
	defer { context.mutex.runlock() }
	for _, token in context.forwarders {
		token.cancel()
	}
}

fn (mut context GrpcTransportContext) register(token &corebridge.RpcCancellation) !u64 {
	context.mutex.lock()
	defer { context.mutex.unlock() }
	if context.closed || context.event.cancelled() { return error('custom gRPC call cancelled') }
	context.next_forwarder++
	identifier := context.next_forwarder
	context.forwarders[identifier] = token
	return identifier
}

fn (mut context GrpcTransportContext) unregister(identifier u64) {
	context.mutex.lock()
	defer { context.mutex.unlock() }
	context.forwarders.delete(identifier)
}

fn (mut context GrpcTransportContext) close() {
	context.mutex.lock()
	defer { context.mutex.unlock() }
	context.closed = true
	for _, token in context.forwarders {
		token.cancel()
	}
}

fn (options CustomGrpcTransport) validate() ! {
	for concurrency in [options.control_concurrency, options.poll_concurrency, options.wait_concurrency] {
		if concurrency < 1 || concurrency > 1024 {
			return error('custom gRPC concurrency must be between 1 and 1024')
		}
	}
	if options.max_pending < 1 || options.max_pending > 65536 || options.max_pending_bytes < 1 {
		return error('invalid custom gRPC pending limits')
	}
}

fn start_custom_grpc(options CustomGrpcTransport) !&corebridge.GrpcBridge {
	options.validate()!
	bridge := corebridge.new_grpc_bridge(options.max_pending, options.max_pending_bytes)!
	spawn dispatch_custom_grpc(bridge, options)
	return bridge
}

struct GrpcActiveCall {
	class   u32
	context &GrpcTransportContext
}

fn dispatch_custom_grpc(bridge &corebridge.GrpcBridge, options CustomGrpcTransport) {
	limits := [options.control_concurrency, options.poll_concurrency, options.wait_concurrency]
	mut running := [0, 0, 0]
	mut active := map[u64]GrpcActiveCall{}
	mut identifier := u64(0)
	completed := chan u64{cap: options.max_pending}
	for {
		for {
			select {
				finished := <-completed {
					if call := active[finished] {
						running[int(call.class)]--
						active.delete(finished)
					}
				}
				else {
					break
				}
			}
		}
		for _, call in active {
			if call.context.cancelled() {
				mut context := call.context
				context.cancel_forwarders()
			}
		}
		if bridge.done() && active.len == 0 {
			break
		}
		mut mask := u32(0)
		for class in 0 .. 3 {
			if running[class] < limits[class] { mask |= u32(1) << class }
		}
		if event := bridge.poll(mask, 10) {
			request := event.snapshot() or {
				event.respond(corebridge.GrpcReply{
					status:  13
					message: 'invalid custom gRPC request'
				}) or {}
				event.close()
				continue
			}
			identifier++
			context := &GrpcTransportContext{
				event: event
			}
			active[identifier] = GrpcActiveCall{
				class:   request.class
				context: context
			}
			running[int(request.class)]++
			spawn run_custom_grpc(options.transport, context, request, identifier, completed)
		}
	}
	bridge.finish() or { panic(err) }
}

fn run_custom_grpc(transport GrpcTransport, supplied &GrpcTransportContext, request corebridge.GrpcRequestSnapshot, identifier u64, completed chan u64) {
	mut context := supplied
	defer {
		context.close()
		context.event.close()
		completed <- identifier
	}
	if context.cancelled() {
		context.event.respond(corebridge.GrpcReply{ status: 1, message: 'custom gRPC call cancelled' }) or {}
		return
	}
	reply := invoke_grpc_transport(transport, context, request) or {
		context.event.respond(corebridge.GrpcReply{
			status:  13
			message: 'custom gRPC transport failed'
		}) or {}
		return
	}
	context.event.respond(reply) or {
		context.event.respond(corebridge.GrpcReply{
			status:  13
			message: 'invalid custom gRPC response'
		}) or {}
	}
}

// Forward a request over an independently connected Core client. This helper
// preserves status/details and propagates cancellation to the underlying RPC.
// The forwarding client must not use this same custom transport.
pub fn (mut context GrpcTransportContext) forward(client &Client, request corebridge.GrpcRequestSnapshot, timeout_millis u32) !corebridge.GrpcReply {
	service := match request.service {
		'temporal.api.workflowservice.v1.WorkflowService' { corebridge.RpcService.workflow }
		'temporal.api.operatorservice.v1.OperatorService' { corebridge.RpcService.operator }
		'temporal.api.cloud.cloudservice.v1.CloudService' { corebridge.RpcService.cloud }
		'temporal.api.testservice.v1.TestService' { corebridge.RpcService.test }
		'grpc.health.v1.Health' { corebridge.RpcService.health }
		else { return error('unsupported custom gRPC service') }
	}

	mut metadata := []corebridge.RpcMetadataEntry{}
	for header in request.headers {
		if header.name in ['content-type', 'te', 'grpc-timeout', 'grpc-encoding',
			'grpc-accept-encoding', 'user-agent'] {
			continue
		}
		if header.name.ends_with('-bin') {
			// HTTP/2 may join binary metadata with commas. Decode separately
			// so forwarding preserves multiplicity and byte boundaries.
			for value in header.value.bytestr().split(',') {
				metadata << corebridge.RpcMetadataEntry{
					name:  header.name
					value: decode_grpc_binary_header(value.trim(' \t').bytes())!
				}
			}
		} else {
			metadata << corebridge.RpcMetadataEntry{
				name:  header.name
				value: header.value.clone()
			}
		}
	}
	token := corebridge.new_rpc_cancellation()!
	defer { token.close() }
	identifier := context.register(token)!
	defer { context.unregister(identifier) }
	body := client.connection.rpc(service, request.method, request.body, timeout_millis,
		metadata:     metadata
		cancellation: token
	) or {
		if err is corebridge.RpcError {
			return corebridge.GrpcReply{
				status:  err.status
				message: err.message
				details: err.details
			}
		}
		return err
	}
	return corebridge.GrpcReply{
		body: body
	}
}

pub fn (client &Client) grpc_transport_stats() ?corebridge.GrpcStats {
	return client.connection.grpc_stats()
}

fn decode_grpc_binary_header(value []u8) ![]u8 {
	text := value.bytestr()
	if text.contains(',') { return error('invalid binary gRPC metadata') }
	mut padded := text
	for padded.len % 4 != 0 {
		padded += '='
	}
	for character in padded.bytes() {
		if !(character >= `A` && character <= `Z`) && !(character >= `a` && character <= `z`)
			&& !(character >= `0` && character <= `9`) && character !in [`+`, `/`, `=`] {
			return error('invalid binary gRPC metadata')
		}
	}
	decoded := base64.decode(padded)
	encoded := base64.encode(decoded)
	if (text.contains('=') && encoded != text)
		|| (!text.contains('=') && encoded.trim_right('=') != text) {
		return error('invalid binary gRPC metadata')
	}
	return decoded
}
