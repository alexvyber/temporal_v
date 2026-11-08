module corebridge

import math
import strconv

@[typedef]
struct C.SpikeTelemetryOptions {
	metric_exporter              &C.SpikeMetricExport = unsafe { nil }
	metric_attach_service_name   bool
	prom_counters_total_suffix   bool
	prom_unit_suffix             bool
	prom_durations_as_seconds    bool
	prom_histogram_buckets       &char
	otel_delta                   bool
	otel_durations_as_seconds    bool
	otel_histogram_buckets       &char
	otel_exponential_histograms  bool
	otel_exponential_max_size    u32
	otel_exponential_max_scale   i8
	otel_exponential_name_suffix &char
	worker_heartbeat_millis      u64
	runtime_info                 C.TemporalCoreRuntimeInfoArray
	disable_environment_info     bool
	log_filter                   &char
	forward_logs                 bool
	prometheus_address           &char
	otel_url                     &char
	otel_headers                 &char
	otel_http                    bool
	metric_period_millis         u32
	metric_tags                  &char
	metric_prefix                &char
}

pub struct RuntimeInfo {
pub:
	kind    int = 9
	version string
}

@[typedef]
struct C.TemporalCoreRuntimeInfo {
	runtime_type i32
	version      C.TemporalCoreByteArrayRef
}

@[typedef]
struct C.TemporalCoreRuntimeInfoArray {
	data &C.TemporalCoreRuntimeInfo
	size uint
}

pub struct Telemetry {
pub:
	metric_starter               ?MetricExportStarter
	metric_attach_service_name   bool = true
	prom_counters_total_suffix   bool = true
	prom_unit_suffix             bool = true
	prom_durations_as_seconds    bool = true
	prom_histogram_buckets       string
	otel_delta                   bool
	otel_durations_as_seconds    bool = true
	otel_histogram_buckets       string
	otel_exponential_histograms  bool
	otel_exponential_max_size    u32    = 160
	otel_exponential_max_scale   i8     = 20
	otel_exponential_name_suffix string = '_native'
	worker_heartbeat_millis      u64
	runtime_info                 []RuntimeInfo
	disable_environment_info     bool
	log_filter                   string
	forward_logs                 bool
	prometheus_address           string
	otel_url                     string
	otel_headers                 string
	otel_http                    bool
	metric_period_millis         u32 = 1000
	metric_tags                  string
	metric_prefix                string
}

fn validate_histogram_buckets(raw string) ! {
	if raw == '' { return }
	entries := raw.split('\n')
	if entries.len % 2 != 0 { return error('invalid histogram bucket override pairs') }
	mut names := map[string]bool{}
	for index := 0; index < entries.len; index += 2 {
		if entries[index] == '' || entries[index] in names {
			return error('histogram metric names must be nonempty and unique')
		}
		names[entries[index]] = true
		mut previous := math.inf(-1)
		for bucket in entries[index + 1].split(',') {
			value := strconv.atof64(bucket) or { return error('histogram buckets must be numbers') }
			if math.is_nan(value) || math.is_inf(value, 0) || value <= previous {
				return error('histogram buckets must be finite and strictly increasing')
			}
			previous = value
		}
	}
}

fn (options Telemetry) validate() ! {
	if options.metric_starter != none
		&& (options.prometheus_address != '' || options.otel_url != '') {
		return error('custom, Prometheus and OpenTelemetry metric exporters are exclusive')
	}

	if options.prometheus_address != '' && options.otel_url != '' {
		return error('choose one metrics exporter per runtime')
	}
	validate_histogram_buckets(options.prom_histogram_buckets)!
	validate_histogram_buckets(options.otel_histogram_buckets)!
	if options.otel_exponential_histograms {
		if options.otel_histogram_buckets != '' {
			return error('explicit bucket overrides cannot be combined with exponential histograms')
		}
		if options.otel_exponential_max_size == 0 || options.otel_exponential_max_size > 2147483647
			|| options.otel_exponential_max_scale < -10 || options.otel_exponential_max_scale > 20 {
			return error('invalid exponential histogram size or scale')
		}
		if options.otel_exponential_name_suffix == '' {
			return error('exponential histogram name suffix cannot be empty')
		}
	}

	for runtime in options.runtime_info {
		if runtime.kind < 0 || runtime.kind > 10 { return error('invalid runtime kind') }
		validate_text(runtime.version, 'runtime version')!
	}
	for name, value in {
		'prom_histogram_buckets':       options.prom_histogram_buckets
		'otel_histogram_buckets':       options.otel_histogram_buckets
		'otel_exponential_name_suffix': options.otel_exponential_name_suffix
		'log filter':                   options.log_filter
		'Prometheus address':           options.prometheus_address
		'OTLP URL':                     options.otel_url
		'OTLP headers':                 options.otel_headers
		'metric tags':                  options.metric_tags
		'metric prefix':                options.metric_prefix
	} {
		validate_text(value, name)!
	}
}

fn (options Telemetry) native_options(bridge ?&MetricExportBridge) !C.SpikeTelemetryOptions {
	mut runtimes := []C.TemporalCoreRuntimeInfo{}
	for runtime in options.runtime_info {
		runtimes << C.TemporalCoreRuntimeInfo{
			runtime_type: i32(runtime.kind)
			version:      C.TemporalCoreByteArrayRef{
				data: runtime.version.str
				size: uint(runtime.version.len)
			}
		}
	}

	mut pointer := unsafe { &C.SpikeMetricExport(nil) }
	if value := bridge { pointer = value.native_pointer()! }
	return C.SpikeTelemetryOptions{
		metric_exporter:              pointer
		metric_attach_service_name:   options.metric_attach_service_name
		prom_counters_total_suffix:   options.prom_counters_total_suffix
		prom_unit_suffix:             options.prom_unit_suffix
		prom_durations_as_seconds:    options.prom_durations_as_seconds
		prom_histogram_buckets:       options.prom_histogram_buckets.str
		otel_delta:                   options.otel_delta
		otel_durations_as_seconds:    options.otel_durations_as_seconds
		otel_histogram_buckets:       options.otel_histogram_buckets.str
		otel_exponential_histograms:  options.otel_exponential_histograms
		otel_exponential_max_size:    options.otel_exponential_max_size
		otel_exponential_max_scale:   options.otel_exponential_max_scale
		otel_exponential_name_suffix: options.otel_exponential_name_suffix.str
		worker_heartbeat_millis:      options.worker_heartbeat_millis
		runtime_info:                 C.TemporalCoreRuntimeInfoArray{
			data: runtimes.data
			size: uint(runtimes.len)
		}
		disable_environment_info:     options.disable_environment_info
		log_filter:                   options.log_filter.str
		forward_logs:                 options.forward_logs
		prometheus_address:           options.prometheus_address.str
		otel_url:                     options.otel_url.str
		otel_headers:                 options.otel_headers.str
		otel_http:                    options.otel_http
		metric_period_millis:         options.metric_period_millis
		metric_tags:                  options.metric_tags.str
		metric_prefix:                options.metric_prefix.str
	}
}
