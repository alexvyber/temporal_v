module corebridge

import sync

@[typedef]
struct C.TemporalCoreTraceExporter {}

@[typedef]
struct C.TemporalCoreTraceExporterOrFail {
	exporter &C.TemporalCoreTraceExporter
	failure  &char
}

fn C.temporal_core_trace_exporter_new(voidptr, C.TemporalCoreByteArrayRef) C.TemporalCoreTraceExporterOrFail
fn C.temporal_core_trace_exporter_export(&C.TemporalCoreTraceExporter, C.TemporalCoreByteArrayRef, u64) &char
fn C.temporal_core_trace_exporter_free(&C.TemporalCoreTraceExporter)
fn C.temporal_core_trace_string_free(&char)

@[heap]
pub struct TraceExporterBridge {
	state &TraceExporterBridgeState = &TraceExporterBridgeState{}
}

struct TraceExporterBridgeState {
	mutex &sync.RwMutex = sync.new_rwmutex()
mut:
	native &C.TemporalCoreTraceExporter = unsafe { nil }
	closed bool
}

pub fn new_trace_exporter_bridge(configuration string) !&TraceExporterBridge {
	if (C.spike_protocol_capabilities() & u64(2)) == 0 {
		return error('loaded Core library does not support tracing exports')
	}
	options := configuration.bytes()
	result := C.temporal_core_trace_exporter_new(unsafe { nil }, C.TemporalCoreByteArrayRef{
		data: options.data
		size: uint(options.len)
	})
	if !isnil(result.failure) {
		message := unsafe { result.failure.vstring().clone() }
		C.temporal_core_trace_string_free(result.failure)
		return error(message)
	}
	if isnil(result.exporter) { return error('trace exporter initialization returned no handle') }
	return &TraceExporterBridge{
		state: &TraceExporterBridgeState{
			native: result.exporter
		}
	}
}

pub fn (bridge &TraceExporterBridge) export(spans []u8, timeout_millis u64) ! {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('trace exporter is closed') }
	failure := C.temporal_core_trace_exporter_export(state.native, C.TemporalCoreByteArrayRef{
		data: spans.data
		size: uint(spans.len)
	}, timeout_millis)
	if !isnil(failure) {
		message := unsafe { failure.vstring().clone() }
		C.temporal_core_trace_string_free(failure)
		return error(message)
	}
}

pub fn (bridge &TraceExporterBridge) close() {
	mut state := bridge.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.closed { return }
	state.closed = true
	C.temporal_core_trace_exporter_free(state.native)
	state.native = unsafe { nil }
}
