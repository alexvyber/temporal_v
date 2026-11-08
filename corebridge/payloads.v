module corebridge

import temporal

#include "@DIR/../native/payloads.h"

@[typedef]
struct C.TemporalVPayloadBytes {
	data &u8
	size uint
}

@[typedef]
struct C.TemporalVPayloadVisitResult {
	body    &C.TemporalVPayloadBytes
	failure &C.TemporalVPayloadBytes
}

fn C.temporal_v_payload_visit(u32, u32, &u8, uint, &u8, uint, bool) C.TemporalVPayloadVisitResult
fn C.temporal_v_payload_bytes_free(&C.TemporalVPayloadBytes)

pub enum PayloadMessageKind {
	workflow_command = 1
	workflow_activation
	activity_task
	activity_completion
	activity_heartbeat
	nexus_task
	nexus_completion
	payloads
	failure
	workflow_completion
	rpc_request      = 100
	rpc_response
}

@[params]
pub struct PayloadMessageOptions {
pub:
	service RpcService = .workflow
	method  string
}

pub struct PayloadMessage {
pub:
	template temporal.TemporalApiCommonV1_Payload
	values   []temporal.TemporalApiCommonV1_Payload
	paths    []string
}

fn visit_payload_message(kind PayloadMessageKind, input []u8, restore bool, options PayloadMessageOptions) ![]u8 {
	validate_text(options.method, 'payload RPC method')!
	result := C.temporal_v_payload_visit(u32(kind), u32(options.service), options.method.str,
		uint(options.method.len), input.data, uint(input.len), restore)
	defer {
		if !isnil(result.body) { C.temporal_v_payload_bytes_free(result.body) }
		if !isnil(result.failure) { C.temporal_v_payload_bytes_free(result.failure) }
	}
	if !isnil(result.failure) {
		return error(unsafe { result.failure.data.vbytes(int(result.failure.size)).bytestr() })
	}
	if isnil(result.body) { return error('native payload visitor returned no result') }
	if result.body.size == 0 { return []u8{} }
	return unsafe { result.body.data.vbytes(int(result.body.size)).clone() }
}

pub fn extract_payloads(kind PayloadMessageKind, input []u8, options PayloadMessageOptions) !PayloadMessage {
	bytes := visit_payload_message(kind, input, false, options)!
	envelope := temporal.TemporalApiCommonV1_Payloads.decode(bytes)!
	if envelope.payloads.len == 0 {
		return error('native payload visitor returned an empty envelope')
	}
	template := envelope.payloads[0]
	mut paths := []string{}
	for index in 0 .. envelope.payloads.len - 1 {
		paths << template.metadata['field/${index}'].bytestr()
	}
	return PayloadMessage{
		template: template
		values:   envelope.payloads[1..].clone()
		paths:    paths
	}
}

pub fn replace_payloads(kind PayloadMessageKind, message PayloadMessage, values []temporal.TemporalApiCommonV1_Payload, options PayloadMessageOptions) ![]u8 {
	if values.len != message.values.len {
		return error('payload visitor replacement count changed')
	}
	mut envelope := [message.template]
	envelope << values
	return visit_payload_message(kind, temporal.TemporalApiCommonV1_Payloads{
		payloads: envelope
	}.encode(), true, options)!
}
