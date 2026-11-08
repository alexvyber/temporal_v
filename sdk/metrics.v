module sdk

import alexvyber.temporal_v.corebridge
import sync
import time

pub type MetricAttribute = bool | f64 | i64 | string

pub enum MetricKind {
	counter_integer
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
	kind        MetricKind
}

@[heap]
struct MetricReplayState {
	mutex &sync.RwMutex = sync.new_rwmutex()
mut:
	replaying bool
}

fn (state &MetricReplayState) suppress() bool {
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	return state.replaying
}

fn (mut state MetricReplayState) set(replaying bool) {
	state.mutex.lock()
	defer { state.mutex.unlock() }
	state.replaying = replaying
}

@[heap]
pub struct MetricMeter {
	bridge     &corebridge.MetricMeter = corebridge.noop_metric_meter()
	attributes map[string]MetricAttribute
	replay     ?&MetricReplayState
}

@[heap]
pub struct Metric {
	bridge     &corebridge.Metric
	attributes map[string]MetricAttribute
	replay     ?&MetricReplayState
}

pub fn (client &Client) metric_meter() !&MetricMeter {
	return &MetricMeter{
		bridge: client.connection.metric_meter()!
	}
}

fn (meter &MetricMeter) scope() !&MetricMeter {
	return &MetricMeter{
		...meter
		bridge: meter.bridge.scope()!
	}
}

pub fn (meter &MetricMeter) close() {
	meter.bridge.close()
}

fn merged_metric_attributes(base map[string]MetricAttribute, extra map[string]MetricAttribute) map[string]MetricAttribute {
	mut merged := base.clone()
	for name, value in extra {
		merged[name] = value
	}
	return merged
}

pub fn (meter &MetricMeter) with_attributes(attributes map[string]MetricAttribute) &MetricMeter {
	return &MetricMeter{
		...meter
		attributes: merged_metric_attributes(meter.attributes, attributes)
	}
}

fn bridge_metric_kind(kind MetricKind) corebridge.MetricKind {
	return match kind {
		.counter_integer { .counter_integer }
		.histogram_integer { .histogram_integer }
		.histogram_float { .histogram_float }
		.histogram_duration { .histogram_duration }
		.gauge_integer { .gauge_integer }
		.gauge_float { .gauge_float }
		.up_down_counter_integer { .up_down_counter_integer }
	}
}

pub fn (meter &MetricMeter) instrument(options MetricOptions) !&Metric {
	native := meter.bridge.instrument(corebridge.MetricOptions{
		name:        options.name
		description: options.description
		unit:        options.unit
		kind:        bridge_metric_kind(options.kind)
	})!
	return &Metric{
		bridge:     native
		attributes: meter.attributes.clone()
		replay:     meter.replay
	}
}

fn (metric &Metric) record_attributes(attributes map[string]MetricAttribute) map[string]corebridge.MetricAttribute {
	merged := merged_metric_attributes(metric.attributes, attributes)
	mut result := map[string]corebridge.MetricAttribute{}
	for name, value in merged {
		match value {
			string { result[name] = value }
			bool { result[name] = value }
			i64 { result[name] = value }
			f64 { result[name] = value }
		}
	}
	return result
}

fn (metric &Metric) suppressed() bool {
	if state := metric.replay { return state.suppress() }
	return false
}

pub fn (metric &Metric) record_integer(value u64, attributes map[string]MetricAttribute) ! {
	metric.bridge.record_integer(value, metric.record_attributes(attributes), metric.suppressed())!
}

pub fn (metric &Metric) record_signed(value i64, attributes map[string]MetricAttribute) ! {
	metric.bridge.record_signed(value, metric.record_attributes(attributes), metric.suppressed())!
}

pub fn (metric &Metric) record_float(value f64, attributes map[string]MetricAttribute) ! {
	metric.bridge.record_float(value, metric.record_attributes(attributes), metric.suppressed())!
}

pub fn (metric &Metric) record_duration(value time.Duration, attributes map[string]MetricAttribute) ! {
	if value < 0 { return error('metric duration cannot be negative') }
	metric.bridge.record_duration_millis(u64(value / time.millisecond),
		metric.record_attributes(attributes), metric.suppressed())!
}

pub fn (context &WorkflowContext) metric_meter() &MetricMeter {
	return &MetricMeter{
		...context.metrics
		replay:     context.metric_replay
		attributes: merged_metric_attributes(context.metrics.attributes, {
			'namespace':     MetricAttribute(context.namespace)
			'task_queue':    MetricAttribute(context.queue)
			'workflow_type': MetricAttribute(context.workflow_type)
		})
	}
}

pub fn (context &ActivityContext) metric_meter() &MetricMeter {
	return context.metrics.with_attributes({
		'task_queue':    MetricAttribute(context.information.task_queue)
		'activity_type': MetricAttribute(context.information.activity_type)
		'workflow_type': MetricAttribute(context.information.workflow_type)
	})
}

pub fn (context &NexusContext) metric_meter() &MetricMeter {
	return context.metrics.with_attributes({
		'service':   MetricAttribute(context.information.service)
		'operation': MetricAttribute(context.information.operation)
	})
}
