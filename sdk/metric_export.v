module sdk

import alexvyber.temporal_v.corebridge
import sync
import time

pub struct MetricDurationValue {
pub:
	milliseconds u64
}

pub type MetricSampleValue = MetricDurationValue | f64 | i64 | u64

pub struct MetricSample {
pub:
	name        string
	description string
	unit        string
	kind        MetricKind
	value       MetricSampleValue
	attributes  map[string]MetricAttribute
}

pub interface MetricExporter {
	export(context &MetricExportContext, samples []MetricSample) !
}

pub interface MetricExporterShutdown {
	shutdown(context &MetricExportContext) !
}

pub struct CustomMetricExporter {
pub:
	exporter           MetricExporter
	max_pending_points int           = 65536
	max_pending_bytes  u64           = 64 * 1024 * 1024
	max_batch          int           = 256
	flush_interval     time.Duration = 100 * time.millisecond
	export_timeout     time.Duration = 5 * time.second
}

@[heap]
struct MetricExportCallState {
	mutex &sync.RwMutex = sync.new_rwmutex()
mut:
	closed bool
}

@[heap]
pub struct MetricExportContext {
	state         &MetricExportCallState = &MetricExportCallState{}
	deadline_mono i64
}

pub fn (context &MetricExportContext) cancelled() bool {
	context.state.mutex.rlock()
	defer { context.state.mutex.runlock() }
	return context.state.closed || time.sys_mono_now() >= context.deadline_mono
}

pub fn (context &MetricExportContext) check_cancelled() ! {
	if context.cancelled() {
		return error('custom metric export context closed or deadline exceeded')
	}
}

fn (context &MetricExportContext) close() {
	mut state := context.state
	state.mutex.lock()
	state.closed = true
	state.mutex.unlock()
}

fn (options CustomMetricExporter) validate() ! {
	if options.max_pending_points < 1 || options.max_pending_points > 1048576
		|| options.max_pending_bytes < 1 {
		return error('invalid custom metric export pending limits')
	}
	if options.max_batch < 1 || options.max_batch > options.max_pending_points {
		return error('custom metric export batch must fit within pending capacity')
	}
	if options.flush_interval < time.millisecond || options.flush_interval > time.hour
		|| options.export_timeout < time.millisecond || options.export_timeout > 24 * time.hour {
		return error('invalid custom metric export flush interval or cooperative timeout')
	}
}

fn start_metric_exporter(options CustomMetricExporter, tags map[string]string) !&corebridge.MetricExportBridge {
	options.validate()!
	bridge := corebridge.new_metric_export_bridge(options.max_pending_points,
		options.max_pending_bytes)!
	spawn drive_metric_export(bridge, options, tags)
	return bridge
}

fn sample_from_native(value corebridge.ExportPointSnapshot, tags map[string]string) !MetricSample {
	mut attributes := map[string]MetricAttribute{}
	for name, text in tags {
		attributes[name] = MetricAttribute(text)
	}
	for attribute in value.attributes {
		attributes[attribute.name] = match attribute.kind {
			1 { MetricAttribute(attribute.text) }
			2 { MetricAttribute(attribute.integer) }
			3 { MetricAttribute(attribute.floating) }
			4 { MetricAttribute(attribute.boolean) }
			else { return error('invalid custom metric attribute kind') }
		}
	}
	kind := match value.kind {
		1 { MetricKind.counter_integer }
		2 { MetricKind.histogram_integer }
		3 { MetricKind.histogram_float }
		4 { MetricKind.histogram_duration }
		5 { MetricKind.gauge_integer }
		6 { MetricKind.gauge_float }
		7 { MetricKind.up_down_counter_integer }
		else { return error('invalid custom metric kind') }
	}

	sample_value := match kind {
		.histogram_float, .gauge_float {
			MetricSampleValue(value.floating)
		}
		.histogram_duration {
			MetricSampleValue(MetricDurationValue{
				milliseconds: value.integer
			})
		}
		.up_down_counter_integer {
			MetricSampleValue(value.signed)
		}
		else {
			MetricSampleValue(value.integer)
		}
	}

	return MetricSample{
		name:        value.name
		description: value.description
		unit:        value.unit
		kind:        kind
		value:       sample_value
		attributes:  attributes
	}
}

fn flush_metric_batch(bridge &corebridge.MetricExportBridge, options CustomMetricExporter, samples []MetricSample, points []&corebridge.MetricExportPoint) {
	defer {
		for point in points {
			point.close()
		}
	}
	context := &MetricExportContext{
		deadline_mono: time.sys_mono_now() + options.export_timeout
	}
	defer { context.close() }
	invoke_metric_export(options.exporter, context, samples) or {
		bridge.record_export(u64(samples.len), true)
		return
	}
	bridge.record_export(u64(samples.len), false)
}

fn drive_metric_export(bridge &corebridge.MetricExportBridge, options CustomMetricExporter, tags map[string]string) {
	bridge.mark_dispatcher()
	mut samples := []MetricSample{}
	mut points := []&corebridge.MetricExportPoint{}
	mut age := time.new_stopwatch()
	for {
		if samples.len > 0
			&& (samples.len >= options.max_batch || age.elapsed() >= options.flush_interval
			|| bridge.stats().core_done) {
			flush_metric_batch(bridge, options, samples, points)
			samples = []MetricSample{}
			points = []&corebridge.MetricExportPoint{}
		}
		if bridge.done() {
			break
		}
		remaining := if samples.len > 0 {
			options.flush_interval - age.elapsed()
		} else {
			100 * time.millisecond
		}
		wait_millis := if remaining < time.millisecond {
			u32(1)
		} else if remaining > 100 * time.millisecond {
			u32(100)
		} else {
			u32(remaining / time.millisecond)
		}
		if point := bridge.poll(wait_millis) {
			raw := point.snapshot() or {
				bridge.record_export(1, true)
				point.close()
				continue
			}
			sample := sample_from_native(raw, tags) or {
				bridge.record_export(1, true)
				point.close()
				continue
			}
			if samples.len == 0 { age = time.new_stopwatch() }
			samples << sample
			points << point
		}
	}
	if options.exporter is MetricExporterShutdown {
		context := &MetricExportContext{
			deadline_mono: time.sys_mono_now() + options.export_timeout
		}
		invoke_metric_shutdown(options.exporter, context) or { bridge.record_export(0, true) }
		context.close()
	}
	bridge.finish() or { panic(err) }
}

pub fn (client &Client) metric_export_stats() ?corebridge.MetricExportStats {
	return client.connection.metric_export_stats()
}
