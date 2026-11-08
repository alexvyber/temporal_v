module main

import sdk
import corebridge
import json2
import os
import sync
import time
import temporal

type CloseAction = fn ()

@[heap]
struct Collected {
	mutex &sync.Mutex = sync.new_mutex()
mut:
	points          []sdk.MetricSample
	shutdowns       int
	context         ?&sdk.MetricExportContext
	self_action     ?CloseAction
	self_closed     bool
	exports         int
	export_defers   int
	shutdown_defers int
}

struct Collector {
	state &Collected
	mode  string
	path  string
	gate  string
}

fn (collector Collector) export(context &sdk.MetricExportContext, samples []sdk.MetricSample) ! {
	defer { collector.after_export() }
	collector.before_export(context)
	if collector.mode == 'throw' { return error('private-custom-export-secret') }
	if collector.mode == 'block' {
		for !os.exists(collector.gate) {
			context.check_cancelled()!
			time.sleep(time.millisecond)
		}
	}
	context.check_cancelled()!
	mut state := collector.state
	mut close_action := ?CloseAction(none)
	state.mutex.lock()
	state.context = context
	for sample in samples {
		state.points << sample
		if collector.path != '' && sample.name.ends_with('v_custom_workflow_events') {
			mut output := os.open_append(collector.path) or {
				state.mutex.unlock()
				return err
			}
			output.writeln(json2.encode(summary(sample))) or {
				output.close()
				state.mutex.unlock()
				return err
			}
			output.close()
		}
		if collector.mode == 'self-close' && sample.name.ends_with('v_custom_self')
			&& !state.self_closed {
			state.self_closed = true
			close_action = state.self_action
		}
	}
	state.mutex.unlock()
	if action := close_action { action() }
}

fn (collector Collector) shutdown(context &sdk.MetricExportContext) ! {
	defer {
		mut state := collector.state
		state.mutex.lock()
		state.shutdown_defers++
		state.mutex.unlock()
	}
	context.check_cancelled()!
	mut state := collector.state
	state.mutex.lock()
	state.shutdowns++
	state.mutex.unlock()
	if collector.mode == 'panic-shutdown' { panic('private-metric-shutdown-panic') }
}

struct SampleSummary {
pub:
	name       string
	kind       string
	value      string
	attributes map[string]string
}

fn summary(sample sdk.MetricSample) SampleSummary {
	value := match sample.value {
		u64 { sample.value.str() }
		i64 { sample.value.str() }
		f64 { sample.value.str() }
		sdk.MetricDurationValue { sample.value.milliseconds.str() }
	}

	mut attributes := map[string]string{}
	for name, attribute in sample.attributes {
		attributes[name] = match attribute {
			bool { attribute.str() }
			i64 { attribute.str() }
			f64 { attribute.str() }
			string { attribute }
		}
	}
	return SampleSummary{
		name:       sample.name
		kind:       sample.kind.str()
		value:      value
		attributes: attributes
	}
}

fn connect(state &Collected, mode string) !&sdk.Client {
	return sdk.connect(sdk.ClientOptions{
		address:   os.getenv('TEMPORAL_ADDRESS')
		telemetry: sdk.TelemetryOptions{
			metric_prefix:  'custom_'
			global_tags:    {
				'runtime': 'test'
			}
			custom_metrics: sdk.CustomMetricExporter{
				exporter:           Collector{
					state: state
					mode:  mode
					gate:  os.getenv('V_SDK_EXAMPLE_METRIC_GATE')
				}
				flush_interval:     time.millisecond
				max_pending_points: if mode == 'block' { 8 } else { 65536 }
				max_batch:          if mode == 'block' { 1 } else { 256 }
				export_timeout:     100 * time.millisecond
			}
		}
	})!
}

fn drained(client &sdk.Client) !corebridge.MetricExportStats {
	watch := time.new_stopwatch()
	for {
		stats := client.metric_export_stats() or { return error('missing export statistics') }
		if stats.finished {
			assert stats.core_done && stats.pending == 0 && stats.live_definitions == 0
				&& stats.live_labels == 0, stats.str()
			return stats
		}
		if watch.elapsed() > 5 * time.second { return error('exporter failed to finish') }
		time.sleep(time.millisecond)
	}
	return error('unreachable')
}

fn add_many(metric &sdk.Metric) ! {
	for _ in 0 .. 100 {
		metric.record_integer(1, {})!
	}
}

struct ValueReport {
pub:
	user_records []SampleSummary
	core_names   []string
	stats        corebridge.MetricExportStats
}

fn validate_startup() ! {
	state := &Collected{}
	base := sdk.CustomMetricExporter{
		exporter: Collector{
			state: state
		}
	}
	for options in [
		sdk.TelemetryOptions{
			custom_metrics: sdk.CustomMetricExporter{
				...base
				max_pending_points: 0
			}
		},
		sdk.TelemetryOptions{
			custom_metrics: sdk.CustomMetricExporter{
				...base
				max_pending_bytes: 0
			}
		},
		sdk.TelemetryOptions{
			custom_metrics: sdk.CustomMetricExporter{
				...base
				max_batch: 0
			}
		},
		sdk.TelemetryOptions{
			custom_metrics: sdk.CustomMetricExporter{
				...base
				flush_interval: 0
			}
		},
		sdk.TelemetryOptions{
			custom_metrics: sdk.CustomMetricExporter{
				...base
				export_timeout: 0
			}
		},
		sdk.TelemetryOptions{
			custom_metrics: base
			prometheus:     sdk.PrometheusMetrics{
				bind_address: '127.0.0.1:0'
			}
		},
	] {
		if _ := sdk.connect(sdk.ClientOptions{
			address:   os.getenv('TEMPORAL_ADDRESS')
			telemetry: options
		})
		{
			return error('invalid metric exporter settings accepted')
		}
	}
	state.mutex.lock()
	callbacks := state.shutdowns
	state.mutex.unlock()
	assert callbacks == 0, 'invalid settings transferred exporter ownership'
	for _ in 0 .. 10 {
		if _ := sdk.connect(sdk.ClientOptions{
			address:   'not a URL'
			telemetry: sdk.TelemetryOptions{
				custom_metrics: base
			}
		})
		{
			return error('invalid URL accepted')
		}
	}
	state.mutex.lock()
	shutdowns := state.shutdowns
	state.mutex.unlock()
	assert shutdowns == 10, 'failed client construction did not drain its exporter'
	println('STARTUP_OK')
}

fn independent_runtimes() ! {
	first_state := &Collected{}
	second_state := &Collected{}
	first := connect(first_state, 'values')!
	second := connect(second_state, 'values')!
	first_meter := first.metric_meter()!
	second_meter := second.metric_meter()!
	first_counter := first_meter.instrument(sdk.MetricOptions{ name: 'v_custom_isolated' })!
	second_counter := second_meter.instrument(sdk.MetricOptions{ name: 'v_custom_isolated' })!
	first_counter.record_integer(17, {})!
	second_counter.record_integer(23, {})!
	first_meter.close()
	first.close()
	drained(first)!
	second_counter.record_integer(2, {})!
	second.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
	second_meter.close()
	second.close()
	drained(second)!
	for index, state in [first_state, second_state] {
		state.mutex.lock()
		points := state.points.clone()
		state.mutex.unlock()
		mut total := u64(0)
		for point in points {
			if point.name.ends_with('v_custom_isolated') {
				if point.value is u64 {
					total += point.value
				} else {
					return error('incorrect counter value kind')
				}
			}
		}
		assert total == if index == 0 {
			u64(17)
		} else {
			u64(25)
		}, 'custom runtimes shared callback state'
	}
	println('ISOLATED_OK')
}

fn values() ! {
	state := &Collected{}
	client := connect(state, 'values')!
	meter := client.metric_meter()!
	scoped := meter.with_attributes({
		'scope':   sdk.MetricAttribute('bound')
		'boolean': sdk.MetricAttribute(true)
		'integer': sdk.MetricAttribute(i64(-2))
		'float':   sdk.MetricAttribute(f64(1.5))
	})
	counter := scoped.instrument(sdk.MetricOptions{ name: 'v_custom_counter' })!
	for options in [
		sdk.MetricOptions{
			name: 'v_custom_integer'
			kind: .histogram_integer
		},
		sdk.MetricOptions{
			name: 'v_custom_float'
			kind: .histogram_float
		},
		sdk.MetricOptions{
			name: 'v_custom_duration'
			kind: .histogram_duration
		},
		sdk.MetricOptions{
			name: 'v_custom_gauge'
			kind: .gauge_integer
		},
		sdk.MetricOptions{
			name: 'v_custom_gauge_float'
			kind: .gauge_float
		},
		sdk.MetricOptions{
			name: 'v_custom_signed'
			kind: .up_down_counter_integer
		},
	] {
		metric := scoped.instrument(options)!
		match options.kind {
			.histogram_integer { metric.record_integer(~u64(0), {})! }
			.histogram_float { metric.record_float(1.25, {})! }
			.histogram_duration { metric.record_duration(250 * time.millisecond, {})! }
			.gauge_integer { metric.record_integer(7, {})! }
			.gauge_float { metric.record_float(-1.5, {})! }
			.up_down_counter_integer { metric.record_signed(-7, {})! }
			else {}
		}
	}
	client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
	client.close()
	assert !(client.metric_export_stats() or { return error('missing stats') }).core_done, 'client close destroyed an active metric-meter lease'
	counter.record_integer(3, {})!
	mut writers := []thread !{}
	for _ in 0 .. 8 {
		writers << spawn add_many(counter)
	}
	for writer in writers {
		writer.wait()!
	}
	counter.record_integer(2, {
		'scope': sdk.MetricAttribute('override')
	})!
	meter.close()
	stats := drained(client)!
	assert stats.dropped == 0 && stats.failed == 0 && stats.exported == stats.accepted, stats.str()
	state.mutex.lock()
	shutdowns := state.shutdowns
	points := state.points.clone()
	context := state.context
	state.mutex.unlock()
	assert shutdowns == 1
	if retained := context {
		assert retained.cancelled(), 'retained export context remained active after return'
	}
	println(json2.encode(ValueReport{
		user_records: points.filter(it.name.contains('v_custom_')).map(summary(it))
		core_names:   points.filter(!it.name.contains('v_custom_')).map(it.name)
		stats:        stats
	}))
}

fn faults(mode string) ! {
	state := &Collected{}
	client := connect(state, mode)!
	meter := client.metric_meter()!
	counter := meter.instrument(sdk.MetricOptions{
		name: if mode == 'self-close' {
			'v_custom_self'
		} else {
			'v_custom_fault'
		}
	})!
	if mode == 'self-close' {
		mut saved := state
		saved.mutex.lock()
		// Both captured SDK handles are heap allocated and internally synchronized;
		// this late-bound action is published under the collector mutex.
		unsafe {
			saved.self_action = fn [client, meter] () {
				meter.close()
				client.close()
			}
		}
		saved.mutex.unlock()
	}
	for _ in 0 .. 1000 {
		counter.record_integer(1, {}) or {
			if mode != 'self-close' { return err }
			break
		}
	}
	client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
		if mode != 'self-close' {
			return err
		}
	}
	if mode != 'self-close' {
		client.close()
		meter.close()
	}
	stats := drained(client)!
	if mode in ['throw', 'panic-export'] {
		assert stats.failed > 0 && stats.export_failures > 0 && stats.exported == 0, stats.str()
	}
	if mode == 'panic-once' {
		assert stats.export_failures > 0 && stats.exported > 0, stats.str()
	}
	if mode == 'panic-shutdown' {
		assert stats.export_failures > 0 && stats.exported > 0, stats.str()
	}
	state.mutex.lock()
	exports := state.exports
	export_defers := state.export_defers
	shutdowns := state.shutdowns
	shutdown_defers := state.shutdown_defers
	retained := state.context
	state.mutex.unlock()
	assert exports > 0 && exports == export_defers
	assert shutdowns == 1 && shutdown_defers == 1
	if captured := retained {
		assert captured.cancelled()
	}
	if mode == 'block' {
		assert stats.dropped > 0 && stats.failed > 0, stats.str()
	}
	if mode == 'self-close' {
		state.mutex.lock()
		closed := state.self_closed
		state.mutex.unlock()
		assert closed
	}
	println(json2.encode(stats))
}

struct ObservedWorkflow {
mut:
	stage int
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ObservedWorkflow{}
}

fn (mut workflow ObservedWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	counter :=
		context.metric_meter().instrument(sdk.MetricOptions{ name: 'v_custom_workflow_events' })!
	match event.kind {
		.started {
			workflow.stage = 1
			counter.record_integer(1, {})!
			context.timer(time.minute)!
		}
		.signal_received {
			if event.name == 'finish' {
				counter.record_integer(1, {})!
				context.complete('"done"')!
			}
		}
		else {}
	}
}

fn (workflow &ObservedWorkflow) query(name string, args []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return workflow.stage.str()
}

fn worker() ! {
	state := &Collected{}
	mut runtime := sdk.Runtime{}
	runtime.register('v.custom-metrics', make_workflow)!
	sdk.run_workflows(sdk.WorkerOptions{
		address:    os.getenv('TEMPORAL_ADDRESS')
		task_queue: os.getenv('TEMPORAL_TASK_QUEUE')
		telemetry:  sdk.TelemetryOptions{
			custom_metrics: sdk.CustomMetricExporter{
				exporter:       Collector{
					state: state
					path:  os.getenv('V_SDK_EXAMPLE_METRIC_EXPORT_FILE')
				}
				flush_interval: time.millisecond
			}
		}
	}, runtime)!
	state.mutex.lock()
	shutdowns := state.shutdowns
	state.mutex.unlock()
	assert shutdowns == 1, 'worker returned before its exporter drained'
	println('WORKER_DRAINED')
}

fn run() ! {
	match os.args[1] {
		'values' {
			values()!
		}
		'validation' {
			validate_startup()!
		}
		'isolated' {
			independent_runtimes()!
		}
		'throw', 'block', 'self-close', 'panic-export', 'panic-once', 'panic-shutdown' {
			faults(os.args[1])!
		}
		'worker' {
			worker()!
		}
		'history' {
			client := sdk.connect(sdk.ClientOptions{ address: os.getenv('TEMPORAL_ADDRESS') })!
			defer { client.close() }
			os.write_file(os.args[2], client.history(sdk.WorkflowHandle{
				workflow_id: os.getenv('TEMPORAL_TASK_QUEUE')
			})!.encode().bytestr())!
		}
		'replay' {
			mut runtime := sdk.Runtime{}
			runtime.register('v.custom-metrics', make_workflow)!
			runtime.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
				workflow_id: os.getenv('TEMPORAL_TASK_QUEUE')
				task_queue:  os.getenv('TEMPORAL_TASK_QUEUE')
			})!
			println('REPLAY_OK')
		}
		else {
			return error('unknown role')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}

fn (collector Collector) before_export(context &sdk.MetricExportContext) {
	mut state := collector.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	state.exports++
	state.context = context
	if collector.mode == 'panic-export' || (collector.mode == 'panic-once' && state.exports == 1) {
		panic('private-metric-export-panic')
	}
}

fn (collector Collector) after_export() {
	mut state := collector.state
	state.mutex.lock()
	state.export_defers++
	state.mutex.unlock()
}
