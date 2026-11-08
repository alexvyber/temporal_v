module corebridge

import math
import sync

#include "@DIR/../native/metrics.h"

@[typedef]
struct C.SpikeMetricMeter {}

@[typedef]
struct C.TemporalCoreMetric {}

@[typedef]
struct C.SpikeMetricAttribute {
	key           &char
	kind          i32
	string_value  &char
	integer_value i64
	float_value   f64
	boolean_value bool
}

fn C.spike_metric_meter_open(&C.SpikeCore) &C.SpikeMetricMeter
fn C.spike_metric_meter_close(&C.SpikeMetricMeter)
fn C.spike_metric_new(&C.SpikeMetricMeter, &char, &char, &char, i32) &C.TemporalCoreMetric
fn C.spike_metric_free(&C.TemporalCoreMetric)
fn C.spike_metric_record(&C.SpikeMetricMeter, &C.TemporalCoreMetric, i32, i32, u64, i64, f64, &C.SpikeMetricAttribute, u32) int

pub type MetricAttribute = bool | f64 | i64 | string

pub enum MetricKind {
	counter_integer = 1
	histogram_integer
	histogram_float
	histogram_duration
	gauge_integer
	gauge_float
	up_down_counter_integer
}

pub struct MetricOptions {
pub:
	name        string
	description string
	unit        string
	kind        MetricKind = .counter_integer
}

@[heap]
struct MetricOwner {
	metric_export_bridge ?&MetricExportBridge
	grpc_bridge          ?&GrpcBridge
	native               &C.SpikeMetricMeter
	mutex                &sync.RwMutex = sync.new_rwmutex()
mut:
	closed      bool
	instruments []&MetricState
}

@[heap]
struct MetricGroup {
	root bool
mut:
	closed bool
	cache  map[string]&MetricState
}

@[heap]
struct MetricState {
	group     &MetricGroup
	cache_key string
	native    &C.TemporalCoreMetric
	kind      MetricKind
mut:
	closed bool
}

@[heap]
pub struct MetricMeter {
	group &MetricGroup = &MetricGroup{
		root: true
	}
	owner &MetricOwner
}

@[heap]
pub struct Metric {
	owner &MetricOwner
	state &MetricState
}

pub fn noop_metric_meter() &MetricMeter {
	return &MetricMeter{
		owner: &MetricOwner{
			native: unsafe { nil }
		}
	}
}

pub fn (connection &Connection) metric_meter() !&MetricMeter {
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('client is closed') }
	native := C.spike_metric_meter_open(connection.core)
	if isnil(native) { return error('metric meter allocation failed') }
	return &MetricMeter{
		owner: &MetricOwner{
			native:               native
			metric_export_bridge: connection.metric_export_bridge
			grpc_bridge:          connection.grpc_bridge
		}
	}
}

pub fn (meter &MetricMeter) scope() !&MetricMeter {
	mut owner := meter.owner
	owner.mutex.lock()
	defer { owner.mutex.unlock() }
	if owner.closed || meter.group.closed { return error('metric meter is closed') }
	return &MetricMeter{
		owner: owner
		group: &MetricGroup{}
	}
}

pub fn (meter &MetricMeter) close() {
	mut owner := meter.owner
	owner.mutex.lock()

	if owner.closed || meter.group.closed {
		owner.mutex.unlock()
		return
	}
	mut group := meter.group
	group.closed = true
	if group.root { owner.closed = true }
	for index := owner.instruments.len - 1; index >= 0; index-- {
		mut state := owner.instruments[index]
		if !group.root && voidptr(state.group) != voidptr(group) {
			continue
		}
		if !state.closed {
			C.spike_metric_free(state.native)
			state.closed = true
		}
		mut instrument_group := state.group
		instrument_group.cache.clear()
		owner.instruments.delete(index)
	}
	group.cache.clear()
	if group.root && !isnil(owner.native) { C.spike_metric_meter_close(owner.native) }
	owner.mutex.unlock()
	if bridge := owner.grpc_bridge { bridge.wait_if_core_done() }
	if bridge := owner.metric_export_bridge { bridge.wait_if_core_done() }
}

pub fn (meter &MetricMeter) instrument(options MetricOptions) !&Metric {
	validate_text(options.name, 'metric name')!
	validate_text(options.description, 'metric description')!
	validate_text(options.unit, 'metric unit')!
	if options.name == '' || options.name.len > 255 {
		return error('metric name must contain 1 to 255 ASCII characters')
	}
	for index, character in options.name.bytes() {
		letter := (character >= `A` && character <= `Z`) || (character >= `a` && character <= `z`)
		if (!letter && index == 0)
			|| (!letter && !(character >= `0` && character <= `9`)
			&& character !in [`_`, `.`, `-`, `/`]) {
			return error('invalid metric name')
		}
	}
	if options.unit.len > 63 {
		return error('metric unit must contain at most 63 ASCII characters')
	}
	for character in options.unit.bytes() {
		if character > 127 { return error('metric unit must be ASCII') }
	}
	if int(options.kind) < 1 || int(options.kind) > 7 { return error('invalid metric kind') }
	mut owner := meter.owner
	owner.mutex.lock()
	defer { owner.mutex.unlock() }
	if owner.closed || meter.group.closed { return error('metric meter is closed') }
	key := options.name + '\x00' + options.description + '\x00' + options.unit + '\x00' +
		int(options.kind).str()
	mut group := meter.group
	if existing := group.cache[key] {
		return &Metric{
			owner: owner
			state: existing
		}
	}
	native := if isnil(owner.native) {
		unsafe { &C.TemporalCoreMetric(nil) }
	} else {
		C.spike_metric_new(owner.native, options.name.str, options.description.str,
			options.unit.str, i32(options.kind))
	}
	state := &MetricState{
		native:    native
		kind:      options.kind
		group:     group
		cache_key: key
	}
	group.cache[key] = state
	owner.instruments << state
	return &Metric{
		owner: owner
		state: state
	}
}

pub fn (metric &Metric) close() {
	mut owner := metric.owner
	owner.mutex.lock()
	defer { owner.mutex.unlock() }
	mut state := metric.state
	if owner.closed || state.closed { return }
	state.closed = true
	mut group := state.group
	group.cache.delete(state.cache_key)
	C.spike_metric_free(state.native)
	for index, registered in owner.instruments {
		if voidptr(registered) == voidptr(state) {
			owner.instruments.delete(index)
			break
		}
	}
}

fn metric_attributes(attributes map[string]MetricAttribute) ![]C.SpikeMetricAttribute {
	mut keys := attributes.keys()
	keys.sort()
	mut native := []C.SpikeMetricAttribute{}
	for key in keys {
		validate_text(key, 'metric attribute key')!
		if key == '' { return error('metric attribute key cannot be empty') }
		value := attributes[key] or { return error('metric attribute disappeared') }
		match value {
			string {
				validate_text(value, 'metric attribute value')!
				native << C.SpikeMetricAttribute{
					key:          key.str
					kind:         1
					string_value: value.str
				}
			}
			i64 {
				native << C.SpikeMetricAttribute{
					key:           key.str
					kind:          2
					integer_value: value
				}
			}
			f64 {
				if math.is_nan(value) || math.is_inf(value, 0) {
					return error('metric attribute value must be finite')
				}
				native << C.SpikeMetricAttribute{
					key:         key.str
					kind:        3
					float_value: value
				}
			}
			bool {
				native << C.SpikeMetricAttribute{
					key:           key.str
					kind:          4
					boolean_value: value
				}
			}
		}
	}
	return native
}

fn (metric &Metric) record(value_kind i32, integer_value u64, signed_value i64, float_value f64, attributes map[string]MetricAttribute, suppress bool) ! {
	native := metric_attributes(attributes)!
	mut owner := metric.owner
	owner.mutex.rlock()
	defer { owner.mutex.runlock() }
	if owner.closed || metric.state.closed { return error('metric is closed') }
	compatible := (value_kind == 1
		&& metric.state.kind in [.counter_integer, .histogram_integer, .gauge_integer])
		|| (value_kind == 2 && metric.state.kind == .up_down_counter_integer)
		|| (value_kind == 3 && metric.state.kind in [.histogram_float, .gauge_float])
		|| (value_kind == 4 && metric.state.kind == .histogram_duration)
	if !compatible { return error('metric value type does not match instrument kind') }
	if suppress || isnil(owner.native) { return }
	result := C.spike_metric_record(owner.native, metric.state.native, i32(metric.state.kind),
		value_kind, integer_value, signed_value, float_value, native.data, u32(native.len))
	if result == -2 { return error('metric value type does not match instrument kind') }
	if result != 0 { return error('metric attribute allocation failed') }
}

pub fn (metric &Metric) record_integer(value u64, attributes map[string]MetricAttribute, suppress bool) ! {
	metric.record(1, value, 0, 0, attributes, suppress)!
}

pub fn (metric &Metric) record_signed(value i64, attributes map[string]MetricAttribute, suppress bool) ! {
	metric.record(2, 0, value, 0, attributes, suppress)!
}

pub fn (metric &Metric) record_float(value f64, attributes map[string]MetricAttribute, suppress bool) ! {
	if math.is_nan(value) || math.is_inf(value, 0) { return error('metric value must be finite') }
	metric.record(3, 0, 0, value, attributes, suppress)!
}

pub fn (metric &Metric) record_duration_millis(value u64, attributes map[string]MetricAttribute, suppress bool) ! {
	metric.record(4, value, 0, 0, attributes, suppress)!
}
