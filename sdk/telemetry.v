module sdk

import corebridge
import os
import time

pub enum OpenTelemetryProtocol {
	grpc
	http_protobuf
}

pub enum MetricTemporality {
	cumulative
	delta
}

pub enum HistogramAggregation {
	explicit_buckets
	base2_exponential
}

pub struct ExponentialHistogramOptions {
pub:
	max_size    u32    = 160
	max_scale   i8     = 20
	name_suffix string = '_native'
}

pub struct PrometheusMetrics {
pub:
	bind_address               string
	counters_total_suffix      bool = true
	unit_suffix                bool = true
	durations_as_seconds       bool = true
	histogram_bucket_overrides map[string][]f64
}

pub struct OpenTelemetryMetrics {
pub:
	temporality                MetricTemporality
	durations_as_seconds       bool = true
	histogram_bucket_overrides map[string][]f64
	histogram_aggregation      HistogramAggregation
	exponential_histogram      ExponentialHistogramOptions
	url                        string
	headers                    map[string]string
	protocol                   OpenTelemetryProtocol
	periodicity                time.Duration = time.second
}

pub enum RuntimeType {
	unspecified
	jvm
	cpython
	node
	bun
	cruby
	go
	dotnet_framework
	dotnet_core
	native
	roadrunner
}

pub struct RuntimeInfo {
pub:
	kind    RuntimeType = .native
	version string
}

pub struct TelemetryOptions {
pub:
	custom_metrics            ?CustomMetricExporter
	worker_heartbeat_interval time.Duration
	runtime_info              []RuntimeInfo = [RuntimeInfo{}]
	disable_environment_info  bool
	core_log_filter           string
	forward_core_logs         bool = true
	prometheus                ?PrometheusMetrics
	attach_service_name       bool = true
	opentelemetry             ?OpenTelemetryMetrics
	metric_prefix             string
	global_tags               map[string]string
}

fn delimited_map(values map[string]string) !string {
	mut keys := values.keys()
	keys.sort()
	mut entries := []string{}
	for key in keys {
		if key == '' || key.contains('\n') || values[key].contains('\n') {
			return error('invalid telemetry tag/header')
		}
		entries << key
		entries << values[key]
	}
	return entries.join('\n')
}

fn histogram_buckets(values map[string][]f64) !string {
	mut keys := values.keys()
	keys.sort()
	mut entries := []string{}
	for key in keys {
		if key == '' || key.contains('\n') || values[key].len == 0 {
			return error('invalid histogram bucket override')
		}
		entries << key
		entries << values[key].map(it.str()).join(',')
	}
	return entries.join('\n')
}

fn (options TelemetryOptions) core_telemetry() !corebridge.Telemetry {
	if options.worker_heartbeat_interval < 0
		|| (options.worker_heartbeat_interval > 0
		&& options.worker_heartbeat_interval < time.millisecond) {
		return error('worker heartbeat interval must be zero or at least one millisecond')
	}
	if options.prometheus != none && options.opentelemetry != none {
		return error('choose one metrics exporter per runtime')
	}
	mut result := corebridge.Telemetry{
		worker_heartbeat_millis:    u64(options.worker_heartbeat_interval / time.millisecond)
		runtime_info:               options.runtime_info.map(corebridge.RuntimeInfo{
			kind:    int(it.kind)
			version: it.version
		})
		disable_environment_info:   options.disable_environment_info
		log_filter:                 options.core_log_filter
		forward_logs:               options.forward_core_logs
		metric_attach_service_name: options.attach_service_name
		metric_prefix:              options.metric_prefix
		metric_tags:                delimited_map(options.global_tags)!
	}
	if exporter := options.prometheus {
		if exporter.bind_address == '' { return error('Prometheus bind address cannot be empty') }
		result = corebridge.Telemetry{
			...result
			prometheus_address:         exporter.bind_address
			prom_counters_total_suffix: exporter.counters_total_suffix
			prom_unit_suffix:           exporter.unit_suffix
			prom_durations_as_seconds:  exporter.durations_as_seconds
			prom_histogram_buckets:     histogram_buckets(exporter.histogram_bucket_overrides)!
		}
	}
	if exporter := options.opentelemetry {
		if exporter.url == '' || exporter.periodicity < time.millisecond
			|| exporter.periodicity / time.millisecond > 4294967295 {
			return error('invalid OpenTelemetry metrics exporter')
		}
		result = corebridge.Telemetry{
			...result
			otel_delta:                   exporter.temporality == .delta
			otel_durations_as_seconds:    exporter.durations_as_seconds
			otel_histogram_buckets:       histogram_buckets(exporter.histogram_bucket_overrides)!
			otel_exponential_histograms:  exporter.histogram_aggregation == .base2_exponential
			otel_exponential_max_size:    exporter.exponential_histogram.max_size
			otel_exponential_max_scale:   exporter.exponential_histogram.max_scale
			otel_exponential_name_suffix: exporter.exponential_histogram.name_suffix
			otel_url:                     exporter.url
			otel_headers:                 delimited_map(exporter.headers)!
			otel_http:                    exporter.protocol == .http_protobuf
			metric_period_millis:         u32(exporter.periodicity / time.millisecond)
		}
	}
	if custom := options.custom_metrics {
		if options.prometheus != none || options.opentelemetry != none {
			return error('custom, Prometheus and OpenTelemetry metric exporters are exclusive')
		}
		custom.validate()!
		tags := options.global_tags.clone()
		result = corebridge.Telemetry{
			...result
			metric_starter: fn [custom, tags] () !&corebridge.MetricExportBridge {
				return start_metric_exporter(custom, tags)!
			}
		}
	}
	return result
}

pub fn telemetry_from_env() !TelemetryOptions {
	mut exporter := ?OpenTelemetryMetrics(none)
	url := os.getenv('TEMPORAL_OTEL_METRICS_URL')
	if url != '' {
		protocol := match os.getenv_opt('TEMPORAL_OTEL_PROTOCOL') {
			'grpc' { OpenTelemetryProtocol.grpc }
			'http_protobuf' { OpenTelemetryProtocol.http_protobuf }
			else { return error('TEMPORAL_OTEL_PROTOCOL must be grpc or http_protobuf') }
		} or { 'grpc' }

		exporter = OpenTelemetryMetrics{
			url:      url
			protocol: protocol
		}
	}
	mut prometheus := ?PrometheusMetrics(none)
	if address := os.getenv_opt('TEMPORAL_METRICS_ADDRESS') {
		if address != '' {
			prometheus = PrometheusMetrics{
				bind_address: address
			}
		}
	}
	return TelemetryOptions{
		core_log_filter: os.getenv('TEMPORAL_CORE_LOG_FILTER')
		prometheus:      prometheus
		opentelemetry:   exporter
	}
}
