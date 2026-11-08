module main

import math
import os
import sdk
import temporal
import time

fn expect_invalid(options sdk.TelemetryOptions, expected string) ! {
	client := sdk.connect(sdk.ClientOptions{ address: 'http://127.0.0.1:1', telemetry: options }) or {
		if !err.msg().contains(expected) { return error('expected ${expected}, received ${err}') }
		return
	}
	client.close()
	return error('invalid telemetry accepted')
}

fn validation() ! {
	for buckets in [[f64(1), 1], [f64(2), 1], [math.nan()], [math.inf(1)],
		[]f64{}] {
		expect_invalid(sdk.TelemetryOptions{
			prometheus: sdk.PrometheusMetrics{
				bind_address:               '127.0.0.1:0'
				histogram_bucket_overrides: {
					'request_latency': buckets
				}
			}
		}, 'histogram')!
	}
	for scale in [i8(-11), 21] {
		expect_invalid(sdk.TelemetryOptions{
			opentelemetry: sdk.OpenTelemetryMetrics{
				url:                   'http://127.0.0.1:1'
				histogram_aggregation: .base2_exponential
				exponential_histogram: sdk.ExponentialHistogramOptions{
					max_scale: scale
				}
			}
		}, 'exponential histogram')!
	}
	expect_invalid(sdk.TelemetryOptions{
		opentelemetry: sdk.OpenTelemetryMetrics{
			url:                        'http://127.0.0.1:1'
			histogram_aggregation:      .base2_exponential
			histogram_bucket_overrides: {
				'request_latency': [f64(1)]
			}
		}
	}, 'cannot be combined')!
	expect_invalid(sdk.TelemetryOptions{
		prometheus:    sdk.PrometheusMetrics{
			bind_address: '127.0.0.1:0'
		}
		opentelemetry: sdk.OpenTelemetryMetrics{
			url: 'http://127.0.0.1:1'
		}
	}, 'choose one')!
	for size in [u32(0), 2147483648] {
		expect_invalid(sdk.TelemetryOptions{
			opentelemetry: sdk.OpenTelemetryMetrics{
				url:                   'http://127.0.0.1:1'
				histogram_aggregation: .base2_exponential
				exponential_histogram: sdk.ExponentialHistogramOptions{
					max_size: size
				}
			}
		}, 'exponential histogram')!
	}
	expect_invalid(sdk.TelemetryOptions{
		opentelemetry: sdk.OpenTelemetryMetrics{
			url:                   'http://127.0.0.1:1'
			histogram_aggregation: .base2_exponential
			exponential_histogram: sdk.ExponentialHistogramOptions{
				name_suffix: ''
			}
		}
	}, 'name suffix')!
	println('VALIDATION_OK')
}

fn run() ! {
	if os.args.len == 2 && os.args[1] == 'validation' { return validation() }
	if os.args.len != 4 { return error('expected mode, Temporal address and metrics endpoint') }
	mode, address, endpoint := os.args[1], os.args[2], os.args[3]
	mut telemetry := sdk.TelemetryOptions{
		attach_service_name: false
		metric_prefix:       'vtest_'
		global_tags:         {
			'fixture': 'metrics'
		}
	}
	if mode == 'prometheus' {
		telemetry = sdk.TelemetryOptions{
			...telemetry
			prometheus: sdk.PrometheusMetrics{
				bind_address:               endpoint
				counters_total_suffix:      false
				unit_suffix:                false
				durations_as_seconds:       false
				histogram_bucket_overrides: {
					'request_latency': [f64(1), 5, 50]
				}
			}
		}
	} else {
		telemetry = sdk.TelemetryOptions{
			...telemetry
			opentelemetry: sdk.OpenTelemetryMetrics{
				url:                        endpoint
				protocol:                   .http_protobuf
				periodicity:                100 * time.millisecond
				temporality:                if mode == 'delta' { .delta } else { .cumulative }
				durations_as_seconds:       mode == 'exponential'
				histogram_aggregation:      if mode == 'exponential' {
					.base2_exponential
				} else {
					.explicit_buckets
				}
				histogram_bucket_overrides: if mode == 'exponential' {
					map[string][]f64{}
				} else {
					{
						'request_latency': [f64(1), 5, 50]
					}
				}
				exponential_histogram:      sdk.ExponentialHistogramOptions{
					max_size:    32
					max_scale:   4
					name_suffix: '_exp'
				}
			}
		}
	}
	client := sdk.connect(sdk.ClientOptions{ address: address, telemetry: telemetry })!
	defer { client.close() }
	for _ in 0 .. 10 {
		client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
	}
	println('READY')
	flush_stdout()
	os.input('')
	for _ in 0 .. 10 {
		client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
	}
	println('SECOND')
	flush_stdout()
	os.input('')
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
