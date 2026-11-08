module corebridge

import encoding.utf8
import sync

#flag @DIR/../native/grpc_bridge.c
#include "@DIR/../native/grpc_bridge.h"

@[typedef]
struct C.SpikeGrpcBridge {}

@[typedef]
struct C.SpikeGrpcRequest {}

@[typedef]
struct C.SpikeGrpcStats {
	accepted       u64
	responded      u64
	rejected       u64
	cancelled      u64
	pending        u64
	pending_bytes  u64
	queued_control u64
	queued_polls   u64
	queued_waits   u64
	stopping       bool
	core_done      bool
}

@[typedef]
struct C.TemporalCoreClientGrpcOverrideResponse {
	status_code   i32
	headers       C.TemporalCoreByteArrayRefArray
	success_proto C.TemporalCoreByteArrayRef
	fail_message  C.TemporalCoreByteArrayRef
	fail_details  C.TemporalCoreByteArrayRef
}

fn C.spike_grpc_bridge_new(uint, u64) &C.SpikeGrpcBridge
fn C.spike_grpc_bridge_release_owner(&C.SpikeGrpcBridge)
fn C.spike_grpc_bridge_abort_unclaimed(&C.SpikeGrpcBridge)
fn C.spike_grpc_bridge_poll(&C.SpikeGrpcBridge, u32, u32) &C.SpikeGrpcRequest
fn C.spike_grpc_bridge_done(&C.SpikeGrpcBridge) bool
fn C.spike_grpc_bridge_stats(&C.SpikeGrpcBridge) C.SpikeGrpcStats
fn C.spike_grpc_request_class(&C.SpikeGrpcRequest) u32
fn C.spike_grpc_request_service(&C.SpikeGrpcRequest) C.TemporalCoreByteArrayRef
fn C.spike_grpc_request_rpc(&C.SpikeGrpcRequest) C.TemporalCoreByteArrayRef
fn C.spike_grpc_request_proto(&C.SpikeGrpcRequest) C.TemporalCoreByteArrayRef
fn C.spike_grpc_request_headers(&C.SpikeGrpcRequest) C.TemporalCoreByteArrayRefArray
fn C.spike_grpc_request_cancelled(&C.SpikeGrpcRequest) bool
fn C.spike_grpc_request_respond(&C.SpikeGrpcRequest, &C.TemporalCoreClientGrpcOverrideResponse) bool
fn C.spike_grpc_request_free(&C.SpikeGrpcRequest)

pub struct GrpcStats {
pub:
	accepted       u64
	responded      u64
	rejected       u64
	cancelled      u64
	pending        u64
	pending_bytes  u64
	queued_control u64
	queued_polls   u64
	queued_waits   u64
	stopping       bool
	core_done      bool
}

fn grpc_stats(value C.SpikeGrpcStats) GrpcStats {
	return GrpcStats{
		accepted:       value.accepted
		responded:      value.responded
		rejected:       value.rejected
		cancelled:      value.cancelled
		pending:        value.pending
		pending_bytes:  value.pending_bytes
		queued_control: value.queued_control
		queued_polls:   value.queued_polls
		queued_waits:   value.queued_waits
		stopping:       value.stopping
		core_done:      value.core_done
	}
}

@[heap]
struct GrpcBridgeState {
	mutex    &sync.RwMutex = sync.new_rwmutex()
	finished chan bool     = chan bool{}
mut:
	native      &C.SpikeGrpcBridge
	closed      bool
	final_stats GrpcStats
}

@[heap]
pub struct GrpcBridge {
	state &GrpcBridgeState
}

pub type GrpcStarter = fn () !&GrpcBridge

pub fn new_grpc_bridge(max_pending int, max_pending_bytes u64) !&GrpcBridge {
	if max_pending < 1 || max_pending_bytes < 1 {
		return error('custom gRPC pending limits must be positive')
	}
	native := C.spike_grpc_bridge_new(uint(max_pending), max_pending_bytes)
	if isnil(native) { return error('custom gRPC bridge allocation failed') }
	return &GrpcBridge{
		state: &GrpcBridgeState{
			native: native
		}
	}
}

fn (bridge &GrpcBridge) native_pointer() !&C.SpikeGrpcBridge {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('custom gRPC transport is closed') }
	return state.native
}

pub fn (bridge &GrpcBridge) stats() GrpcStats {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	return if state.closed {
		state.final_stats
	} else {
		grpc_stats(C.spike_grpc_bridge_stats(state.native))
	}
}

pub fn (bridge &GrpcBridge) abort_unclaimed() {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if !state.closed { C.spike_grpc_bridge_abort_unclaimed(state.native) }
}

pub fn (bridge &GrpcBridge) wait_if_core_done() {
	if bridge.stats().core_done { _ = <-bridge.state.finished }
}

pub fn (bridge &GrpcBridge) done() bool {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	return state.closed || C.spike_grpc_bridge_done(state.native)
}

pub fn (bridge &GrpcBridge) finish() ! {
	mut state := bridge.state
	state.mutex.lock()
	if state.closed {
		state.mutex.unlock()
		return
	}
	if !C.spike_grpc_bridge_done(state.native) {
		state.mutex.unlock()
		return error('custom gRPC requests have not drained')
	}
	state.final_stats = grpc_stats(C.spike_grpc_bridge_stats(state.native))
	C.spike_grpc_bridge_release_owner(state.native)
	state.closed = true
	state.mutex.unlock()
	state.finished.close()
}

pub struct GrpcHeader {
pub:
	name  string
	value []u8
}

pub struct GrpcRequestSnapshot {
pub:
	service string
	method  string
	body    []u8
	headers []GrpcHeader
	class   u32
}

pub struct GrpcReply {
pub:
	status  int
	body    []u8
	message string
	details []u8
	headers []GrpcHeader
}

@[heap]
struct GrpcEventState {
	mutex  &sync.RwMutex = sync.new_rwmutex()
	native &C.SpikeGrpcRequest
mut:
	responded bool
	closed    bool
}

@[heap]
pub struct GrpcEvent {
	state &GrpcEventState
}

pub fn (bridge &GrpcBridge) poll(allowed_classes u32, timeout_millis u32) ?&GrpcEvent {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return none }
	native := C.spike_grpc_bridge_poll(state.native, allowed_classes, timeout_millis)
	if isnil(native) { return none }
	return &GrpcEvent{
		state: &GrpcEventState{
			native: native
		}
	}
}

fn grpc_bytes(value C.TemporalCoreByteArrayRef) ![]u8 {
	if value.size > 2147483647 { return error('custom gRPC field is too large') }
	return unsafe { value.data.vbytes(int(value.size)).clone() }
}

pub fn (event &GrpcEvent) snapshot() !GrpcRequestSnapshot {
	mut state := event.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed || state.responded { return error('custom gRPC request is closed') }
	service := grpc_bytes(C.spike_grpc_request_service(state.native))!.bytestr()
	method := grpc_bytes(C.spike_grpc_request_rpc(state.native))!.bytestr()
	validate_text(service, 'custom gRPC service')!
	validate_text(method, 'custom gRPC method')!
	body := grpc_bytes(C.spike_grpc_request_proto(state.native))!
	native_headers := C.spike_grpc_request_headers(state.native)
	if native_headers.size > 2147483647 { return error('too many custom gRPC headers') }
	mut headers := []GrpcHeader{}
	for index in 0 .. int(native_headers.size) {
		entry := grpc_bytes(unsafe { native_headers.data[index] })!
		separator := entry.index(u8(`\n`))
		if separator < 1 { return error('malformed custom gRPC header') }
		name := entry[..separator].bytestr()
		validate_text(name, 'custom gRPC header name')!
		headers << GrpcHeader{
			name:  name
			value: entry[separator + 1..].clone()
		}
	}
	return GrpcRequestSnapshot{
		service: service
		method:  method
		body:    body
		headers: headers
		class:   C.spike_grpc_request_class(state.native)
	}
}

pub fn (event &GrpcEvent) cancelled() bool {
	mut state := event.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	return state.closed || state.responded || C.spike_grpc_request_cancelled(state.native)
}

fn grpc_response_headers(headers []GrpcHeader) ![][]u8 {
	mut packets := [][]u8{}
	for header in headers {
		if header.name == '' { return error('invalid custom gRPC response header name') }
		for character in header.name.bytes() {
			if !(character >= `a` && character <= `z`) && !(character >= `0` && character <= `9`)
				&& character !in [`-`, `_`, `.`] {
				return error('invalid custom gRPC response header name')
			}
		}
		if header.name in ['content-type', 'te', 'grpc-status', 'grpc-message',
			'grpc-status-details-bin', 'grpc-encoding', 'grpc-accept-encoding', 'grpc-timeout'] {
			return error('custom gRPC response headers must contain user metadata only')
		}
		for character in header.value {
			if character == 127 || (character < 32 && character != `\t`) {
				return error('invalid custom gRPC response header value')
			}
		}
		mut packet := (header.name + '\n').bytes()
		packet << header.value
		packets << packet
	}
	return packets
}

pub fn (event &GrpcEvent) respond(reply GrpcReply) ! {
	if reply.status < 0 || reply.status > 16 { return error('invalid custom gRPC response status') }
	if !utf8.validate_str(reply.message) {
		return error('custom gRPC response message must be UTF-8')
	}
	packets := grpc_response_headers(reply.headers)!
	values := refs(packets)
	response := C.TemporalCoreClientGrpcOverrideResponse{
		status_code:   i32(reply.status)
		headers:       C.TemporalCoreByteArrayRefArray{
			data: values.data
			size: uint(values.len)
		}
		success_proto: C.TemporalCoreByteArrayRef{
			data: reply.body.data
			size: uint(reply.body.len)
		}
		fail_message:  C.TemporalCoreByteArrayRef{
			data: reply.message.str
			size: uint(reply.message.len)
		}
		fail_details:  C.TemporalCoreByteArrayRef{
			data: reply.details.data
			size: uint(reply.details.len)
		}
	}
	mut state := event.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.closed || state.responded {
		return error('custom gRPC request already responded or closed')
	}
	if !C.spike_grpc_request_respond(state.native, &response) {
		return error('custom gRPC request already responded')
	}
	state.responded = true
}

pub fn (event &GrpcEvent) close() {
	mut state := event.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.closed { return }
	state.closed = true
	C.spike_grpc_request_free(state.native)
}

struct GrpcStartup {
	bridge ?&GrpcBridge
}

fn start_grpc(transport Transport) !GrpcStartup {
	if starter := transport.grpc_starter {
		return GrpcStartup{
			bridge: starter()!
		}
	}
	return GrpcStartup{}
}

fn abandon_grpc(bridge ?&GrpcBridge) {
	if value := bridge {
		value.abort_unclaimed()
		value.wait_if_core_done()
	}
}

pub fn (connection &Connection) grpc_stats() ?GrpcStats {
	if bridge := connection.grpc_bridge { return bridge.stats() }
	return none
}
