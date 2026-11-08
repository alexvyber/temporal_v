module main

import sdk
import temporal
import json2
import crypto.sha256
import time

struct RawHeaderConverter {}

fn (converter RawHeaderConverter) to_payload(value sdk.PayloadValue, scope sdk.SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	return sdk.DefaultPayloadConverter{}.to_payload(value, scope)!
}

fn (converter RawHeaderConverter) from_payload(value temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) !sdk.PayloadValue {
	if value.data.bytestr() == '"HEADER_SENTINEL"' {
		if scope.kind != .activity || scope.workflow_id == '' || scope.activity_type == '' {
			return error('wrong explicit header conversion scope')
		}
		return error('explicit header sentinel conversion')
	}
	return sdk.DefaultPayloadConverter{}.from_payload(value, scope)!
}

fn header_payloads() !map[string]sdk.PayloadValue {
	return {
		'a-opaque':   sdk.PayloadValue(sdk.raw_value(temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding': 'application/x-opaque-header'.bytes()
				'private':  [u8(111), 112, 97, 113, 117, 101, 0, 255, 0]
			}
			data:     [u8(0), 255, 42].repeat(100001)
		})!)
		'm-sentinel': sdk.PayloadValue('"HEADER_SENTINEL"')
		'z-known':    sdk.PayloadValue('true')
	}
}

fn raw_header_handler(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	mut raw := context.raw_headers()
	if raw.len != 3 { return error('wrong raw application header count') }
	opaque := raw['a-opaque'] or { return error('missing opaque header') }
	if opaque.to_payload()!.metadata['private'] != [u8(111), 112, 97, 113, 117, 101, 0, 255, 0] {
		return error('binary header metadata changed')
	}
	if opaque.encoding() != 'application/x-opaque-header' || opaque.data().len != 300003
		|| sha256.sum(opaque.data()) != sha256.sum([u8(0), 255, 42].repeat(100001)) {
		return error('opaque header changed')
	}
	mut bytes := opaque.data()
	bytes[0] = 43
	mut wire := opaque.to_payload()!
	wire.data[0] = 44
	wire.metadata['encoding'][0] = 0
	raw.delete('z-known')
	mut info_headers := context.info().raw_headers
	info_headers.delete('a-opaque')
	if context.raw_headers().len != 3 || context.raw_headers()['a-opaque'].data()[0] != 0
		|| context.raw_headers()['a-opaque'].encoding() != 'application/x-opaque-header' {
		return error('header ownership violated')
	}
	mut explicit_error := false
	context.header_values() or { explicit_error = true }
	if !explicit_error { return error('opaque header was silently converted') }
	context.header_values() or { explicit_error = true }
	mut sentinel_error := false
	context.decode_raw_value(context.raw_headers()['m-sentinel']) or {
		if err.msg() != 'explicit header sentinel conversion' { return err }
		sentinel_error = true
	}
	if !sentinel_error { return error('header converter was not called explicitly') }
	if context.decode_raw_value(context.raw_headers()['z-known'])!.logical_data() != 'true' {
		return error('known explicit header conversion changed')
	}
	if json2.encode(context.info()).contains('raw_headers') {
		return error('raw headers serialized')
	}
	if context.raw_headers()['m-sentinel'].data().bytestr() != '"HEADER_SENTINEL"' {
		return error('conversion failure consumed raw header')
	}
	return sdk.PayloadValue('"raw-headers-ok"')
}

struct RawHeadersWorkflow {
mut:
	completed int
}

fn raw_headers_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RawHeadersWorkflow{}
}

fn (mut workflow RawHeadersWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		for name in ['v.raw-headers.named', 'v.raw-headers.dynamic'] {
			context.activity(name, 'null', sdk.ActivityOptions{
				headers:        header_payloads()!
				start_to_close: 30 * time.second
				retry:          sdk.RetryPolicy{
					maximum_attempts: 2
				}
			})!
			context.local_activity(name, 'null', sdk.LocalActivityOptions{
				headers:        header_payloads()!
				start_to_close: 30 * time.second
				retry:          sdk.RetryPolicy{
					maximum_attempts: 2
				}
			})!
		}
	}
	if event.kind == .activity_completed {
		if event.data != '"raw-headers-ok"' { return error('wrong raw header Activity result') }
		workflow.completed++
	}
	if event.kind in [.activity_failed, .activity_cancelled] {
		return error('raw header Activity failed: ' + event.message)
	}
	if event.kind == .signal_received {
		if workflow.completed != 4 { return error('raw header signal arrived early') }
		context.complete(json2.encode({
			'completed': workflow.completed
		}))!
	}
}

fn (workflow &RawHeadersWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.completed))
}
