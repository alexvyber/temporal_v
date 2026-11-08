module main

import temporal_v.corebridge
import x.json2
import math
import os
import temporal_v.sdk
import time

fn add_many(metric &sdk.Metric, count int) ! {
	for _ in 0 .. count {
		metric.record_integer(1, {})!
	}
}

fn record_until_closed(metric &sdk.Metric, ready chan bool) {
	metric.record_integer(1, {}) or { panic(err) }
	ready <- true
	for _ in 0 .. 1000000 {
		metric.record_integer(1, {}) or {
			assert err.msg().contains('closed')
			return
		}
	}
}

fn validate() ! {
	meter := &sdk.MetricMeter{}
	defer { meter.close() }
	counter := meter.instrument(sdk.MetricOptions{ name: 'v_noop', kind: .counter_integer })!
	counter.record_integer(1, {})!
	if _ := counter.record_float(1, {}) { return error('wrong metric type accepted') }
	if _ := counter.record_integer(1, {
		'invalid': sdk.MetricAttribute(math.nan())
	})
	{
		return error('nonfinite attribute accepted')
	}
	for name in ['', 'bad metric', '7metric', 'é', 'x'.repeat(256)] {
		if _ := meter.instrument(sdk.MetricOptions{ name: name }) {
			return error('invalid metric name accepted')
		}
	}
	duration :=
		meter.instrument(sdk.MetricOptions{ name: 'v_duration', kind: .histogram_duration })!
	if _ := duration.record_duration(-time.second, {}) {
		return error('negative duration accepted')
	}
	meter.close()
	meter.close()
	if _ := meter.instrument(sdk.MetricOptions{ name: 'v_closed' }) {
		return error('closed meter accepted')
	}
	println('VALIDATION_OK')
}

fn raw_scopes(address string) ! {
	connection := corebridge.connect(address, corebridge.Transport{}, corebridge.Telemetry{
		prometheus_address: '127.0.0.1:0'
	})!
	defer { connection.close() }
	root := connection.metric_meter()!
	defer { root.close() }
	first := root.scope()!
	second := root.scope()!
	first_metric := first.instrument(corebridge.MetricOptions{ name: 'v_scope_lifetime' })!
	sibling := second.instrument(corebridge.MetricOptions{ name: 'v_scope_lifetime' })!
	for _ in 0 .. 500 {
		repeated := first.instrument(corebridge.MetricOptions{ name: 'v_scope_lifetime' })!
		repeated.record_integer(1, {}, false)!
	}
	first.close()
	if _ := first_metric.record_integer(1, {}, false) {
		return error('closed metric scope remained usable')
	}
	sibling.record_integer(1, {}, false)!
	second.close()
	parent := root.instrument(corebridge.MetricOptions{ name: 'v_parent_lifetime' })!
	parent.record_integer(1, {}, false)!
	root.close()
	if _ := sibling.record_integer(1, {}, false) { return error('closed sibling remained usable') }
}

fn instruments(address string, endpoint string) ! {
	raw_scopes(address)!
	client := sdk.connect(sdk.ClientOptions{
		address:   address
		telemetry: sdk.TelemetryOptions{
			prometheus: sdk.PrometheusMetrics{
				bind_address: endpoint
			}
		}
	})!
	defer { client.close() }
	meter := client.metric_meter()!
	defer { meter.close() }
	scoped := meter.with_attributes({
		'scope':   sdk.MetricAttribute('bound')
		'flag':    sdk.MetricAttribute(true)
		'integer': sdk.MetricAttribute(i64(-2))
		'float':   sdk.MetricAttribute(f64(1.5))
	})
	counter := scoped.instrument(sdk.MetricOptions{ name: 'v_user_counter' })!
	integer_histogram := scoped.instrument(sdk.MetricOptions{
		name: 'v_user_integer'
		kind: .histogram_integer
	})!
	float_histogram := scoped.instrument(sdk.MetricOptions{
		name: 'v_user_float'
		kind: .histogram_float
	})!
	duration := scoped.instrument(sdk.MetricOptions{
		name: 'v_user_duration'
		kind: .histogram_duration
	})!
	integer_gauge :=
		scoped.instrument(sdk.MetricOptions{ name: 'v_user_gauge', kind: .gauge_integer })!
	float_gauge := scoped.instrument(sdk.MetricOptions{
		name: 'v_user_gauge_float'
		kind: .gauge_float
	})!
	signed := scoped.instrument(sdk.MetricOptions{
		name: 'v_user_signed'
		kind: .up_down_counter_integer
	})!
	client.close()
	if _ := client.metric_meter() { return error('meter created on closed client') }
	counter.record_integer(3, {})!
	mut writers := []thread !{}
	for _ in 0 .. 8 {
		writers << spawn add_many(counter, 100)
	}
	for writer in writers {
		writer.wait()!
	}
	integer_histogram.record_integer(4, {})!
	float_histogram.record_float(1.25, {})!
	duration.record_duration(250 * time.millisecond, {})!
	integer_gauge.record_integer(7, {})!
	float_gauge.record_float(-1.5, {})!
	signed.record_signed(2, {})!
	signed.record_signed(-3, {})!
	counter.record_integer(2, {
		'scope': sdk.MetricAttribute('override')
	})!
	println('READY')
	flush_stdout()
	os.input('')
	ready := chan bool{cap: 8}
	mut closers := []thread{}
	for _ in 0 .. 8 {
		closers << spawn record_until_closed(counter, ready)
	}
	for _ in 0 .. 8 {
		_ = <-ready
	}
	meter.close()
	for closer in closers {
		closer.wait()
	}

	if _ := counter.record_integer(1, {}) { return error('record after meter close accepted') }
	println('CLOSED')
}

struct MetricWorkflow {
mut:
	counter ?&sdk.Metric
	stage   int
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return MetricWorkflow{}
}

fn (mut workflow MetricWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		workflow.counter = context.metric_meter().instrument(sdk.MetricOptions{
			name: 'v_user_workflow_events'
		})!
	}
	counter := workflow.counter or { return error('missing workflow metric') }
	match event.kind {
		.started {
			counter.record_integer(1, {})!
			context.activity('v.metrics.remote', 'null', sdk.ActivityOptions{})!
		}
		.activity_completed {
			counter.record_integer(1, {})!
			if workflow.stage == 0 {
				workflow.stage = 1
				context.local_activity('v.metrics.local', 'null', sdk.LocalActivityOptions{})!
			} else {
				workflow.stage = 2
				context.nexus_operation('null', sdk.NexusOptions{
					service:   'metrics'
					operation: 'echo'
					endpoint:  context.info().task_queue + '-endpoint'
				})!
			}
		}
		.nexus_completed {
			counter.record_integer(1, {})!
			workflow.stage = 3
			context.timer(60 * time.second)!
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

fn (workflow &MetricWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name != 'stage' { return error('unknown query') }
	return json2.encode(workflow.stage)
}

fn metric_activity(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	counter := context.metric_meter().with_attributes({
		'local': sdk.MetricAttribute(context.info().is_local)
	}).instrument(sdk.MetricOptions{ name: 'v_user_activity_events' })!
	counter.record_integer(1, {})!
	return input[0]
}

fn metric_nexus(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	counter := context.metric_meter().instrument(sdk.MetricOptions{ name: 'v_user_nexus_events' })!
	counter.record_integer(1, {})!
	return sdk.NexusResult{
		value: input
	}
}

fn worker() ! {
	mut runtime := sdk.Runtime{}
	runtime.register('v.user-metrics', make_workflow)!
	runtime.register_local_activity('v.metrics.local', metric_activity)!
	mut activities := sdk.Activities{}
	activities.register('v.metrics.remote', metric_activity)!
	mut services := sdk.NexusServices{}
	services.register('metrics', 'echo', sdk.NexusOperation{ start: metric_nexus })!
	sdk.run_worker(sdk.WorkerOptions{
		address:    os.getenv('TEMPORAL_ADDRESS')
		task_queue: os.getenv('TEMPORAL_TASK_QUEUE')
		telemetry:  sdk.TelemetryOptions{
			prometheus: sdk.PrometheusMetrics{
				bind_address: os.getenv('V_SDK_EXAMPLE_USER_METRICS_ADDRESS')
			}
		}
	}, runtime, activities,
		nexus: services
	)!
}

fn run() ! {
	if os.args.len < 2 { return error('missing mode') }
	match os.args[1] {
		'validation' {
			validate()!
		}
		'instruments' {
			instruments(os.args[2], os.args[3])!
		}
		'worker' {
			worker()!
		}
		'replay' {
			mut runtime := sdk.Runtime{}
			runtime.register('v.user-metrics', make_workflow)!
			runtime.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
				workflow_id: os.getenv('TEMPORAL_TASK_QUEUE')
				task_queue:  os.getenv('TEMPORAL_TASK_QUEUE')
			})!
			println('REPLAY_OK')
		}
		'history' {
			client := sdk.connect(sdk.ClientOptions{ address: os.getenv('TEMPORAL_ADDRESS') })!
			defer { client.close() }
			history := client.history(sdk.WorkflowHandle{
				workflow_id: os.getenv('TEMPORAL_TASK_QUEUE')
			})!
			os.write_file(os.args[2], history.encode().bytestr())!
		}
		else {
			return error('unknown mode')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
