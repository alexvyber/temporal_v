module main

import alexvyber.temporal_v.corebridge
import x.json2
import math
import os
import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import time

struct CallResult {
pub:
	ok           bool
	status       int
	milliseconds i64
	version      string
}

fn expect_invalid(options sdk.TransportOptions, expected string) ! {
	client := sdk.connect(sdk.ClientOptions{ address: 'http://127.0.0.1:1', transport: options }) or {
		if !err.msg().contains(expected) { return error('expected ${expected}, received ${err}') }
		return
	}
	client.close()
	return error('invalid transport was accepted')
}

fn validation() ! {
	expect_invalid(sdk.TransportOptions{
		retry: sdk.TransportRetryOptions{
			initial_interval: -time.second
		}
	}, 'retry initial interval')!
	expect_invalid(sdk.TransportOptions{
		retry: sdk.TransportRetryOptions{
			max_interval: time.microsecond
		}
	}, 'retry maximum interval')!
	expect_invalid(sdk.TransportOptions{
		retry: sdk.TransportRetryOptions{
			max_elapsed_time: -time.second
		}
	}, 'retry maximum elapsed time')!
	expect_invalid(sdk.TransportOptions{
		retry: sdk.TransportRetryOptions{
			max_interval: time.millisecond
		}
	}, 'maximum interval must be at least')!
	for value in [f64(-0.1), 1.1, math.nan(), math.inf(1)] {
		expect_invalid(sdk.TransportOptions{
			retry: sdk.TransportRetryOptions{
				randomization_factor: value
			}
		}, 'randomization factor')!
	}
	for value in [f64(0.9), math.nan(), math.inf(1)] {
		expect_invalid(sdk.TransportOptions{
			retry: sdk.TransportRetryOptions{
				multiplier: value
			}
		}, 'multiplier')!
	}
	expect_invalid(sdk.TransportOptions{
		dns_load_balancing: sdk.DnsLoadBalancingOptions{
			resolution_interval: 0
		}
	}, 'DNS resolution interval')!
	expect_invalid(sdk.TransportOptions{
		dns_load_balancing: sdk.DnsLoadBalancingOptions{
			resolution_interval: time.microsecond
		}
	}, 'DNS resolution interval')!
	expect_invalid(sdk.TransportOptions{
		dns_load_balancing: sdk.DnsLoadBalancingOptions{}
		proxy:              sdk.ProxyOptions{
			target: '127.0.0.1:1'
		}
	}, 'cannot be combined')!
	expect_invalid(sdk.TransportOptions{ client_name: '' }, 'client name and version')!
	expect_invalid(sdk.TransportOptions{ client_version: '' }, 'client name and version')!
	expect_invalid(sdk.TransportOptions{ client_name: 'bad\x00name' }, 'client name must be valid')!
	expect_invalid(sdk.TransportOptions{ client_version: 'bad\nversion' },
		'client version must use printable ASCII')!
	expect_invalid(sdk.TransportOptions{
		dns_load_balancing: sdk.DnsLoadBalancingOptions{
			resolution_interval: 500 * time.millisecond
		}
	}, 'DNS resolution interval')!
	// Raw users must receive the same finite-number validation before Rust creates a backoff.
	connection := corebridge.connect('http://127.0.0.1:1', corebridge.Transport{
		retry: corebridge.TransportRetryOptions{
			multiplier: math.inf(1)
		}
	}, corebridge.Telemetry{}) or {
		if !err.msg().contains('multiplier') { return err }
		println('VALIDATION_OK')
		return
	}
	connection.close()
	return error('raw invalid transport was accepted')
}

fn cancel_later(token &sdk.RpcCancellation) {
	time.sleep(75 * time.millisecond)
	token.cancel()
}

fn invoke(client &sdk.Client) {
	watch := time.new_stopwatch()
	result := client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
		println(json2.encode(CallResult{
			status:       err.code()
			milliseconds: watch.elapsed().milliseconds()
		}))
		return
	}
	println(json2.encode(CallResult{
		ok:           true
		milliseconds: watch.elapsed().milliseconds()
		version:      result.server_version
	}))
}

fn live(address string) ! {
	client := sdk.connect(sdk.ClientOptions{
		address:   address
		transport: sdk.TransportOptions{
			grpc_compression:   .gzip
			dns_load_balancing: sdk.DnsLoadBalancingOptions{
				resolution_interval: time.second
			}
			payloads_warn_size: 64
			memo_warn_size:     64
		}
		telemetry: sdk.TelemetryOptions{
			core_log_filter: 'warn'
		}
	})!
	defer { client.close() }
	value := json2.encode('x'.repeat(3072))
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   os.getenv('TEMPORAL_TASK_QUEUE') + '-transport'
		workflow_type: 'v.transport.no-worker'
		task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
		arguments:     value
		memo:          {
			'large': value
		}
	})!
	described := client.rpc_describe_workflow_execution(temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionRequest{
		namespace: 'default'
		execution: temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: handle.workflow_id
			run_id:      handle.run_id
		}
	})!
	info := described.workflow_execution_info or { return error('missing workflow information') }
	memo := info.memo or { return error('missing workflow memo') }
	if memo.fields['large'].data.bytestr() != value {
		return error('compressed memo did not round-trip')
	}
	client.terminate(handle, sdk.WorkflowTerminationOptions{ reason: 'transport probe complete' })!
	if client.get_result(handle, 5000)!.status != .terminated {
		return error('transport workflow did not terminate')
	}
	println(json2.encode(handle))
}

fn run() ! {
	if os.args.len == 2 && os.args[1] == 'validation' { return validation() }
	if os.args.len != 3 { return error('expected address and probe mode') }
	address, mode := os.args[1], os.args[2]
	if mode == 'live' { return live(address) }
	token := sdk.new_rpc_cancellation()!
	defer { token.close() }
	client := sdk.connect(sdk.ClientOptions{
		address:            address
		rpc_retry:          mode != 'no-retry'
		cancellation:       token
		rpc_timeout_millis: if mode == 'deadline' { u32(120) } else { u32(5000) }
		transport:          sdk.TransportOptions{
			client_name:        'temporal-v-transport-probe'
			client_version:     'test'
			dns_load_balancing: if mode == 'dns' {
				?sdk.DnsLoadBalancingOptions(sdk.DnsLoadBalancingOptions{
					resolution_interval: time.second
				})
			} else {
				none
			}
			grpc_compression:   if mode == 'plain' { .none } else { .gzip }
			retry:              sdk.TransportRetryOptions{
				initial_interval:     if mode == 'single' {
					time.Duration(0)
				} else {
					20 * time.millisecond
				}
				randomization_factor: 0
				multiplier:           2
				max_interval:         if mode == 'single' {
					time.Duration(0)
				} else {
					40 * time.millisecond
				}
				max_elapsed_time:     if mode == 'elapsed' {
					90 * time.millisecond
				} else {
					time.Duration(0)
				}
				max_retries:          if mode == 'single' {
					u32(1)
				} else if mode in ['deadline', 'elapsed', 'cancel'] {
					u32(0)
				} else {
					u32(3)
				}
			}
		}
	})!
	defer { client.close() }
	if mode == 'cancel' {
		canceller := spawn cancel_later(token)
		invoke(client)
		canceller.wait()
	} else {
		invoke(client)
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
