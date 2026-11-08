module main

import sdk
import corebridge
import os
import json2
import temporal
import time

struct TraceCase {
	parent string
	state  string
}

struct TraceResult {
	context sdk.TraceContext
	headers map[string]string
}

fn validate_contexts() ! {
	parent := '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03'
	context := sdk.parse_trace_parent(parent, ' vendor=value,other= second ')!
	if !context.sampled() || !context.remote || context.trace_state != 'vendor=value,other= second' {
		return error('trace context normalization changed')
	}
	if context.trace_parent()! != parent { return error('traceparent round trip changed') }
	payload := context.temporal_trace_header()!
	if payload.metadata['encoding'].bytestr() != 'json/plain' {
		return error('Temporal trace carrier encoding changed')
	}
	restored := sdk.extract_temporal_trace_context({
		sdk.trace_header_key: payload
	}) or { return error('Temporal trace carrier was not decoded') }
	if restored != context { return error('Temporal trace context changed') }
	for value in [parent + '-', parent.to_upper(), parent[..parent.len - 2] + '04',
		'00-' + '0'.repeat(32) + '-00f067aa0ba902b7-01'] {
		sdk.parse_trace_parent(value, '') or { continue }
		return error('invalid traceparent accepted')
	}
	ignored := sdk.parse_trace_parent(parent, 'vendor=x,vendor=y')!
	if ignored.trace_state != '' || ignored.trace_id != context.trace_id {
		return error('invalid tracestate affected a valid traceparent')
	}
	if _ := sdk.extract_trace_context({
		'traceparent': parent
		'Traceparent': parent
	})
	{
		return error('ambiguous duplicate traceparent accepted')
	}
	invalid := temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'binary/plain'.bytes()
		}
		data:     payload.data
	}
	if _ := sdk.extract_temporal_trace_context({
		sdk.trace_header_key: invalid
	})
	{
		return error('wrong Temporal trace payload encoding accepted')
	}
	mut traces := map[string]bool{}
	for index in 0 .. 32 {
		root := sdk.new_trace_context(index % 2 == 0)!
		if root.remote || root.sampled() != (index % 2 == 0) || root.trace_id in traces {
			return error('trace root identity or flags changed')
		}
		traces[root.trace_id] = true
		child := root.child()!
		if child.remote || child.trace_id != root.trace_id || child.span_id == root.span_id
			|| child.trace_flags != root.trace_flags {
			return error('trace child identity or flags changed')
		}
	}
}

struct ExportOptions {
	root_ca_cert   string
	client_cert    string
	client_key     string
	endpoint       string
	protocol       string
	timeout_millis u64
	service_name   string
	headers        map[string]string
}

fn typed_export() ! {
	options := json2.decode[ExportOptions](os.read_file(os.args[2])!)!
	protocol := match options.protocol {
		'grpc' { sdk.TraceExportProtocol.grpc }
		'http/protobuf' { sdk.TraceExportProtocol.http_protobuf }
		'http/json' { sdk.TraceExportProtocol.http_json }
		else { return error('unknown fixture protocol') }
	}

	exporter := sdk.new_otlp_trace_exporter(sdk.OtlpTraceExporterOptions{
		tls:          sdk.TraceTlsOptions{
			root_ca_cert: options.root_ca_cert
			client_cert:  options.client_cert
			client_key:   options.client_key
		}
		endpoint:     options.endpoint
		protocol:     protocol
		timeout:      time.Duration(options.timeout_millis) * time.millisecond
		service_name: options.service_name
		headers:      options.headers
		resource:     {
			'environment': sdk.TraceAttribute('fixture')
		}
	})!
	defer { exporter.close() }
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:          exporter
		max_pending_spans: 8
		max_pending_bytes: 128 * 1024
		max_batch:         4
		flush_interval:    5 * time.millisecond
		limits:            sdk.TraceLimits{
			attributes: 9
			events:     1
			links:      1
		}
	})!
	defer { provider.shutdown(5 * time.second) or {} }
	parent := sdk.TraceContext{
		trace_id:    '1234567890abcdef1234567890abcdef'
		span_id:     'abcdef1234567890'
		trace_flags: 3
		trace_state: 'vendor=value'
		remote:      true
	}
	context := sdk.TraceContext{
		...parent
		span_id: '1234567890abcdef'
		remote:  false
	}
	linked := sdk.TraceLink{
		context:            sdk.TraceContext{
			trace_id:    'abcdef1234567890abcdef1234567890'
			span_id:     'abcdef1234567890'
			trace_flags: 1
			trace_state: 'other=value'
			remote:      true
		}
		attributes:         {
			'linked': sdk.TraceAttribute(true)
		}
		dropped_attributes: 1
	}
	span := provider.start_span('V span 世界', sdk.TraceSpanOptions{
		parent:           parent
		context:          context
		kind:             .client
		start_unix_nanos: u64(1794000000000000001)
		links:            [linked]
	})!
	minimum := i64(-9223372036854775807) - i64(1)
	maximum := i64(9223372036854775807)
	span.set_attribute('string', sdk.TraceAttribute('世界'))!
	span.set_attribute('boolean', sdk.TraceAttribute(true))!
	span.set_attribute('maximum', sdk.TraceAttribute(maximum))!
	span.set_attribute('minimum', sdk.TraceAttribute(minimum))!
	span.set_attribute('double', sdk.TraceAttribute(f64(1.25)))!
	span.set_attribute('strings', sdk.TraceAttribute(sdk.TraceStringArray{
		values: ['a', '世界']
	}))!
	span.set_attribute('booleans', sdk.TraceAttribute(sdk.TraceBooleanArray{
		values: [
			true,
			false,
		]
	}))!
	span.set_attribute('integers', sdk.TraceAttribute(sdk.TraceIntegerArray{
		values: [
			minimum,
			maximum,
		]
	}))!
	span.set_attribute('doubles', sdk.TraceAttribute(sdk.TraceDoubleArray{
		values: [
			f64(-1.5),
			2.25,
		]
	}))!
	for index in 0 .. 3 {
		span.set_attribute('drop-' + index.str(), sdk.TraceAttribute(i64(index)))!
	}
	span.add_event(sdk.TraceEvent{
		name:               'event'
		unix_nanos:         u64(1794000000000000002)
		attributes:         {
			'exception.type': sdk.TraceAttribute('FixtureError')
		}
		dropped_attributes: 2
	})!
	for _ in 0 .. 4 {
		span.add_event(sdk.TraceEvent{ name: 'dropped', unix_nanos: u64(1794000000000000002) })!
	}
	for _ in 0 .. 5 {
		span.add_link(linked)!
	}
	span.set_status(.error, 'fixture failure')!
	span.finish(sdk.TraceEndOptions{ unix_nanos: u64(1794000000001234567) })
	span.finish()
	unsampled := provider.start_span('unsampled', sdk.TraceSpanOptions{ sampled: false })!
	unsampled.set_attribute('', sdk.TraceAttribute('not-recorded'))!
	unsampled.finish()
	replay := provider.start_span('replay', sdk.TraceSpanOptions{
		context:         context
		suppress_export: true
	})!
	replay.finish()
	provider.force_flush(5 * time.second)!
	provider.shutdown(5 * time.second)!
	stats := provider.stats()
	if stats.accepted_spans != 1 || stats.sent_spans != 1 || stats.pending_spans != 0
		|| stats.pending_bytes != 0 || stats.failed_spans != 0 || stats.suppressed_spans != 2
		|| !stats.closed {
		return error('trace provider ownership or suppression statistics changed: ' +
			json2.encode(stats))
	}
	println('OK')
}

fn run() ! {
	mode := os.args[1]
	if mode == 'sampling-contexts' {
		sampling_contexts()!
		return
	}
	if mode == 'sampling-validation' {
		sampling_validation()!
		return
	}
	if mode == 'baggage-contexts' {
		baggage_contexts()!
		return
	}
	if mode == 'baggage-validation' {
		baggage_validation()!
		return
	}
	if mode.starts_with('query-observe-') {
		query_observation(mode)!
		return
	}
	if mode.starts_with('trace-') {
		workflow_tracing(mode)!
		return
	}
	if mode == 'client-validation' {
		client_validation()!
		return
	}
	if mode == 'exporter-panic-validation' {
		check_trace_panic_recovery()!
		return
	}
	if mode == 'provider-validation' {
		provider_validation()!
		return
	}
	if mode == 'typed-export' {
		typed_export()!
		return
	}
	if mode == 'validation' {
		validate_contexts()!
		println('OK')
		return
	}
	if mode == 'contexts' {
		cases := json2.decode[[]TraceCase](os.read_file(os.args[2])!)!
		for input in cases {
			context := sdk.parse_trace_parent(input.parent, input.state) or {
				println('0')
				continue
			}
			println('1|' + json2.encode(TraceResult{
				context: context
				headers: context.propagation_headers()!
			}))
		}
		return
	}
	if mode == 'native-export' {
		bridge := corebridge.new_trace_exporter_bridge(os.read_file(os.args[2])!)!
		defer { bridge.close() }
		bridge.export(os.read_bytes(os.args[3])!, os.args[4].u64())!
		println('OK')
		return
	}
	if mode == 'native-close' {
		bridge := corebridge.new_trace_exporter_bridge(os.read_file(os.args[2])!)!
		bridge.close()
		bridge.close()
		bridge.export('[]'.bytes(), 1000) or {
			println(err.msg())
			return
		}
		return error('closed trace exporter accepted a batch')
	}
	return error('unknown tracing fixture mode')
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}
