module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.example_converter
import alexvyber.temporal_v.temporal
import os
import x.json2
import time

struct QueryObservationWorkflow {
mut:
	counter ?&sdk.Metric
}

struct QueryObservationModes {
	activation bool
	history    bool
	data       string
}

struct QueryObservationInterceptor {}

fn query_observation_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return QueryObservationWorkflow{}
}

fn (mut workflow QueryObservationWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		workflow.counter = context.metric_meter().instrument(sdk.MetricOptions{
			name: 'v_query_observation'
		})!
		if metric := workflow.counter {
			metric.record_integer(1, {
				'phase': sdk.MetricAttribute('start')
			})!
		}
		context.log(.info, 'QUERY_OBSERVATION_START', {})
	}
	if event.kind == .signal_received { context.complete(sdk.PayloadValue('"done"'))! }
}

fn (workflow QueryObservationWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	metric := workflow.counter or { return error('query observation counter missing') }
	metric.record_integer(1, {
		'phase': sdk.MetricAttribute('query')
	})!
	return sdk.PayloadValue('"state"')
}

fn (interceptor QueryObservationInterceptor) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.QueryNext) !sdk.PayloadValue {
	value := next.execute(name, input)!
	return sdk.PayloadValue(json2.encode(QueryObservationModes{
		activation: next.is_replaying()
		history:    next.is_replaying_history_events()
		data:       value.logical_data()
	}))
}

fn query_observation(role string) ! {
	config := sdk.options_from_env()!
	converter := example_converter.from_env()!
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:       JournalTraceExporter{
			path: os.getenv('V_TRACE_JOURNAL')
		}
		flush_interval: time.millisecond
	})!
	defer { provider.shutdown(5 * time.second) or {} }
	tracing := sdk.TraceOptions{
		provider: provider
		parent:   sdk.TraceContext{
			trace_id:    os.getenv('V_TRACE_ID')
			span_id:     'abcdef1234567890'
			trace_flags: 3
		}
	}
	mut runtime := sdk.Runtime{}
	runtime.register('v.query-observation', query_observation_factory)!
	if role == 'query-observe-replay' {
		runtime.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id:        os.args[3]
			task_queue:         os.args[4]
			data_converter:     converter
			tracing:            tracing
			query_interceptors: [sdk.QueryInterceptor(QueryObservationInterceptor{})]
		})!
		provider.force_flush(5 * time.second)!
		if provider.stats().accepted_spans != 0 {
			return error('offline replay exported query-observation spans')
		}
		println('QUERY_REPLAY_OK')
		return
	}
	sdk.run_workflows(sdk.WorkerOptions{
		...config
		data_converter:     converter
		tracing:            tracing
		query_interceptors: [sdk.QueryInterceptor(QueryObservationInterceptor{})]
		telemetry:          sdk.TelemetryOptions{
			prometheus: sdk.PrometheusMetrics{
				bind_address: os.getenv('V_QUERY_METRICS_ADDRESS')
			}
		}
	}, runtime)!
}
