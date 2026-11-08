module corebridge

import sync
import encoding.utf8

#flag @DIR/../native/metric_export.c
#include "@DIR/../native/metric_export.h"

@[typedef]
struct C.SpikeMetricExport {}

@[typedef]
struct C.SpikeMetricPoint {}

@[typedef]
struct C.SpikeMetricExportStats {
	accepted            u64
	dropped             u64
	allocation_failures u64
	pending             u64
	pending_bytes       u64
	live_definitions    u64
	live_labels         u64
	core_done           bool
}

fn C.spike_metric_export_new(uint, u64) &C.SpikeMetricExport
fn C.spike_metric_export_abort_unclaimed(&C.SpikeMetricExport)
fn C.spike_metric_export_mark_dispatcher(&C.SpikeMetricExport)
fn C.spike_metric_export_on_dispatcher(&C.SpikeMetricExport) bool
fn C.spike_metric_export_release_owner(&C.SpikeMetricExport)
fn C.spike_metric_export_stats(&C.SpikeMetricExport) C.SpikeMetricExportStats
fn C.spike_metric_export_done(&C.SpikeMetricExport) bool
fn C.spike_metric_export_poll(&C.SpikeMetricExport, u32) &C.SpikeMetricPoint
fn C.spike_metric_point_free(&C.SpikeMetricPoint)
fn C.spike_metric_point_kind(&C.SpikeMetricPoint) u32
fn C.spike_metric_point_name(&C.SpikeMetricPoint) C.TemporalCoreByteArrayRef
fn C.spike_metric_point_description(&C.SpikeMetricPoint) C.TemporalCoreByteArrayRef
fn C.spike_metric_point_unit(&C.SpikeMetricPoint) C.TemporalCoreByteArrayRef
fn C.spike_metric_point_integer(&C.SpikeMetricPoint) u64
fn C.spike_metric_point_signed(&C.SpikeMetricPoint) i64
fn C.spike_metric_point_float(&C.SpikeMetricPoint) f64
fn C.spike_metric_point_attribute_count(&C.SpikeMetricPoint) uint
fn C.spike_metric_point_attribute_key(&C.SpikeMetricPoint, uint) C.TemporalCoreByteArrayRef
fn C.spike_metric_point_attribute_type(&C.SpikeMetricPoint, uint) u32
fn C.spike_metric_point_attribute_string(&C.SpikeMetricPoint, uint) C.TemporalCoreByteArrayRef
fn C.spike_metric_point_attribute_integer(&C.SpikeMetricPoint, uint) i64
fn C.spike_metric_point_attribute_float(&C.SpikeMetricPoint, uint) f64
fn C.spike_metric_point_attribute_bool(&C.SpikeMetricPoint, uint) bool

pub struct MetricExportStats {
pub:
	finished            bool
	accepted            u64
	dropped             u64
	allocation_failures u64
	pending             u64
	pending_bytes       u64
	live_definitions    u64
	live_labels         u64
	core_done           bool
	exported            u64
	failed              u64
	export_failures     u64
}

fn export_stats(value C.SpikeMetricExportStats) MetricExportStats {
	return MetricExportStats{
		accepted:            value.accepted
		dropped:             value.dropped
		allocation_failures: value.allocation_failures
		pending:             value.pending
		pending_bytes:       value.pending_bytes
		live_definitions:    value.live_definitions
		live_labels:         value.live_labels
		core_done:           value.core_done
	}
}

@[heap]
struct MetricExportState {
	mutex    &sync.RwMutex = sync.new_rwmutex()
	finished chan bool     = chan bool{}
mut:
	native          &C.SpikeMetricExport
	closed          bool
	final_stats     MetricExportStats
	exported        u64
	failed          u64
	export_failures u64
}

@[heap]
pub struct MetricExportBridge {
	state &MetricExportState
}

pub type MetricExportStarter = fn () !&MetricExportBridge

pub fn new_metric_export_bridge(max_pending int, max_bytes u64) !&MetricExportBridge {
	if max_pending < 1 || max_bytes < 1 {
		return error('custom metric export limits must be positive')
	}
	native := C.spike_metric_export_new(uint(max_pending), max_bytes)
	if isnil(native) { return error('custom metric export allocation failed') }
	return &MetricExportBridge{
		state: &MetricExportState{
			native: native
		}
	}
}

fn (bridge &MetricExportBridge) native_pointer() !&C.SpikeMetricExport {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('custom metric exporter is closed') }
	return state.native
}

pub fn (bridge &MetricExportBridge) stats() MetricExportStats {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return state.final_stats }
	return MetricExportStats{
		...export_stats(C.spike_metric_export_stats(state.native))
		exported:        state.exported
		failed:          state.failed
		export_failures: state.export_failures
	}
}

pub fn (bridge &MetricExportBridge) record_export(count u64, failed bool) {
	mut state := bridge.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if failed {
		state.failed += count
		state.export_failures++
	} else {
		state.exported += count
	}
}

pub fn (bridge &MetricExportBridge) abort_unclaimed() {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if !state.closed { C.spike_metric_export_abort_unclaimed(state.native) }
}

pub fn (bridge &MetricExportBridge) mark_dispatcher() {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if !state.closed { C.spike_metric_export_mark_dispatcher(state.native) }
}

pub fn (bridge &MetricExportBridge) wait_if_core_done() {
	mut state := bridge.state
	state.mutex.rlock()
	done := state.closed || C.spike_metric_export_stats(state.native).core_done
	same_thread := !state.closed && C.spike_metric_export_on_dispatcher(state.native)
	state.mutex.runlock()
	// An exporter may close its own client. Its callback must return before
	// this dispatcher can finish, so joining itself would deadlock.
	if done && !same_thread { _ = <-state.finished }
}

pub fn (bridge &MetricExportBridge) finish() ! {
	mut state := bridge.state
	state.mutex.lock()
	if state.closed {
		state.mutex.unlock()
		return
	}
	if !C.spike_metric_export_done(state.native) {
		state.mutex.unlock()
		return error('custom metric points have not drained')
	}
	state.final_stats = MetricExportStats{
		...export_stats(C.spike_metric_export_stats(state.native))
		finished:        true
		exported:        state.exported
		failed:          state.failed
		export_failures: state.export_failures
	}
	C.spike_metric_export_release_owner(state.native)
	state.closed = true
	state.mutex.unlock()
	state.finished.close()
}

pub fn (bridge &MetricExportBridge) done() bool {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	return state.closed || C.spike_metric_export_done(state.native)
}

pub fn (bridge &MetricExportBridge) poll(timeout_millis u32) ?&MetricExportPoint {
	mut state := bridge.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return none }
	native := C.spike_metric_export_poll(state.native, timeout_millis)
	if isnil(native) { return none }
	return &MetricExportPoint{
		state: &MetricPointState{
			native: native
		}
	}
}

pub struct ExportAttribute {
pub:
	name     string
	kind     u32
	text     string
	integer  i64
	floating f64
	boolean  bool
}

pub struct ExportPointSnapshot {
pub:
	name        string
	description string
	unit        string
	kind        u32
	integer     u64
	signed      i64
	floating    f64
	attributes  []ExportAttribute
}

@[heap]
struct MetricPointState {
	mutex  &sync.RwMutex = sync.new_rwmutex()
	native &C.SpikeMetricPoint
mut:
	closed bool
}

@[heap]
pub struct MetricExportPoint {
	state &MetricPointState
}

fn export_text(value C.TemporalCoreByteArrayRef) !string {
	if value.size > 2147483647 { return error('custom metric text is too large') }
	result := unsafe { value.data.vbytes(int(value.size)).bytestr().clone() }
	if !utf8.validate_str(result) { return error('custom metric text must be UTF-8') }
	return result
}

pub fn (point &MetricExportPoint) snapshot() !ExportPointSnapshot {
	mut state := point.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('custom metric point is closed') }
	native := state.native
	count := C.spike_metric_point_attribute_count(native)
	if count > 2147483647 { return error('too many custom metric attributes') }
	mut attributes := []ExportAttribute{}
	for index in 0 .. int(count) {
		position := uint(index)
		name := export_text(C.spike_metric_point_attribute_key(native, position))!
		kind := C.spike_metric_point_attribute_type(native, position)
		attribute := match kind {
			1 {
				ExportAttribute{
					name: name
					kind: kind
					text: export_text(C.spike_metric_point_attribute_string(native, position))!
				}
			}
			2 {
				ExportAttribute{
					name:    name
					kind:    kind
					integer: C.spike_metric_point_attribute_integer(native, position)
				}
			}
			3 {
				ExportAttribute{
					name:     name
					kind:     kind
					floating: C.spike_metric_point_attribute_float(native, position)
				}
			}
			4 {
				ExportAttribute{
					name:    name
					kind:    kind
					boolean: C.spike_metric_point_attribute_bool(native, position)
				}
			}
			else {
				return error('invalid custom metric attribute kind')
			}
		}

		attributes << attribute
	}
	kind := C.spike_metric_point_kind(native)
	if kind < 1 || kind > 7 { return error('invalid custom metric kind') }
	return ExportPointSnapshot{
		name:        export_text(C.spike_metric_point_name(native))!
		description: export_text(C.spike_metric_point_description(native))!
		unit:        export_text(C.spike_metric_point_unit(native))!
		kind:        kind
		integer:     if kind in [
			u32(1),
			2,
			4,
			5,
		] {
			C.spike_metric_point_integer(native)
		} else {
			0
		}
		signed:      if kind == 7 { C.spike_metric_point_signed(native) } else { 0 }
		floating:    if kind in [
			u32(3),
			6,
		] {
			C.spike_metric_point_float(native)
		} else {
			0.0
		}
		attributes:  attributes
	}
}

pub fn (point &MetricExportPoint) close() {
	mut state := point.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.closed { return }
	state.closed = true
	C.spike_metric_point_free(state.native)
}

struct MetricExportStartup {
	bridge ?&MetricExportBridge
}

fn start_metric_export(telemetry Telemetry) !MetricExportStartup {
	if starter := telemetry.metric_starter {
		return MetricExportStartup{
			bridge: starter()!
		}
	}
	return MetricExportStartup{}
}

fn abandon_metric_export(bridge ?&MetricExportBridge) {
	if value := bridge {
		value.abort_unclaimed()
		value.wait_if_core_done()
	}
}

pub fn (connection &Connection) metric_export_stats() ?MetricExportStats {
	if bridge := connection.metric_export_bridge { return bridge.stats() }
	return none
}
