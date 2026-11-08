module sdk

import corebridge
import sync
import time
import json2

pub interface TraceExporter {
	export(context &TraceExportContext, spans []TraceSpanData) !
}

pub interface TraceExporterShutdown {
	shutdown(context &TraceExportContext) !
}

@[heap]
struct TraceExportCallState {
	mutex &sync.RwMutex = sync.new_rwmutex()
mut:
	closed bool
}

@[heap]
pub struct TraceExportContext {
	state         &TraceExportCallState = &TraceExportCallState{}
	deadline_mono i64
}

pub fn (context &TraceExportContext) cancelled() bool {
	context.state.mutex.rlock()
	defer { context.state.mutex.runlock() }
	return context.state.closed || time.sys_mono_now() >= context.deadline_mono
}

pub fn (context &TraceExportContext) check_cancelled() ! {
	if context.cancelled() { return error('trace export context closed or deadline exceeded') }
}

pub fn (context &TraceExportContext) remaining() time.Duration {
	remaining := context.deadline_mono - time.sys_mono_now()
	return if remaining > 0 { time.Duration(remaining) } else { time.Duration(0) }
}

fn (context &TraceExportContext) close() {
	mut state := context.state
	state.mutex.lock()
	state.closed = true
	state.mutex.unlock()
}

fn trace_export_context(timeout time.Duration) &TraceExportContext {
	return &TraceExportContext{
		deadline_mono: time.sys_mono_now() + i64(timeout)
	}
}

pub enum TraceExportProtocol {
	grpc
	http_protobuf
	http_json
}

pub struct TraceTlsOptions {
pub:
	root_ca_cert string
	client_cert  string
	client_key   string
}

pub struct OtlpTraceExporterOptions {
pub:
	tls          TraceTlsOptions
	endpoint     string
	protocol     TraceExportProtocol = .grpc
	timeout      time.Duration       = 5 * time.second
	service_name string
	headers      map[string]string
	resource     map[string]TraceAttribute
}

struct TraceWireExporterOptions {
	root_ca_cert   string
	client_cert    string
	client_key     string
	endpoint       string
	protocol       string
	timeout_millis u64
	service_name   string
	headers        map[string]string
	resource       map[string]json2.Any
}

@[heap]
pub struct OtlpTraceExporter {
	bridge &corebridge.TraceExporterBridge
}

pub fn new_otlp_trace_exporter(options OtlpTraceExporterOptions) !&OtlpTraceExporter {
	if options.timeout < time.millisecond || options.timeout > 300 * time.second
		|| options.service_name == '' {
		return error('invalid trace exporter timeout or service name')
	}
	if (options.tls.client_cert == '') != (options.tls.client_key == '') {
		return error('trace client certificate and key must be supplied together')
	}
	for value in [options.tls.root_ca_cert, options.tls.client_cert, options.tls.client_key] {
		if value.len > 512 * 1024 { return error('trace TLS material exceeds the byte limit') }
	}
	validate_trace_string(options.service_name, 'service name', 1024)!
	if trace_attributes_size(options.resource)! > 512 * 1024 {
		return error('trace resource exceeds the configuration limit')
	}
	protocol := match options.protocol {
		.grpc { 'grpc' }
		.http_protobuf { 'http/protobuf' }
		.http_json { 'http/json' }
	}

	configuration := json2.encode(TraceWireExporterOptions{
		root_ca_cert:   options.tls.root_ca_cert
		client_cert:    options.tls.client_cert
		client_key:     options.tls.client_key
		endpoint:       options.endpoint
		protocol:       protocol
		timeout_millis: u64(options.timeout / time.millisecond)
		service_name:   options.service_name
		headers:        options.headers.clone()
		resource:       trace_wire_attributes(options.resource)
	})
	if configuration.len > 1024 * 1024 {
		return error('trace exporter configuration exceeds the byte limit')
	}
	return &OtlpTraceExporter{
		bridge: corebridge.new_trace_exporter_bridge(configuration)!
	}
}

pub fn (exporter &OtlpTraceExporter) export(context &TraceExportContext, spans []TraceSpanData) ! {
	context.check_cancelled()!
	if spans.len < 1 || spans.len > 1024 { return error('invalid trace export batch size') }
	mut size := u64(2)
	for span in spans {
		size += trace_data_size(span)!
		if size > 8 * 1024 * 1024 { return error('trace batch exceeds the export byte limit') }
	}
	mut values := []TraceWireSpan{}
	for span in spans {
		values << trace_wire_span(span)!
	}
	encoded := json2.encode(values).bytes()
	context.check_cancelled()!
	remaining := context.remaining()
	budget := u64((remaining + time.millisecond - 1) / time.millisecond)
	if budget == 0 { return error('trace export deadline exceeded') }
	exporter.bridge.export(encoded, budget)!
}

pub fn (exporter &OtlpTraceExporter) close() {
	exporter.bridge.close()
}

pub fn (exporter &OtlpTraceExporter) shutdown(context &TraceExportContext) ! {
	exporter.close()
	context.check_cancelled()!
}
