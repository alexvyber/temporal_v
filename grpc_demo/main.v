module main

import sdk
import corebridge
import temporal
import os
import time
import sync
import json2

@[heap]
struct Observations {
	mutex &sync.Mutex = sync.new_mutex()
mut:
	held          int
	cancelled     int
	metadata_seen bool
	calls         int
	polls         int
	panic_calls   int
	panic_defers  int
}

struct ForwardTransport {
	backend       &sdk.Client
	seen          &Observations
	fail_connect  bool
	panic_connect bool
}

fn (transport ForwardTransport) call(supplied &sdk.GrpcTransportContext, request corebridge.GrpcRequestSnapshot) !corebridge.GrpcReply {
	mut context := supplied
	mut seen := transport.seen
	seen.mutex.lock()
	seen.calls++
	if request.class == 1 { seen.polls++ }
	seen.mutex.unlock()
	if transport.fail_connect {
		return corebridge.GrpcReply{
			status:  7
			message: 'connect rejected'
		}
	}
	mut mode := ''
	mut authorization := ''
	mut binary := ''
	for header in request.headers {
		if header.name == 'x-grpc-test' { mode = header.value.bytestr() }
		if header.name == 'authorization' { authorization = header.value.bytestr() }
		if header.name == 'x-grpc-test-bin' { binary = header.value.bytestr() }
	}
	if transport.panic_connect || mode == 'panic' {
		transport_callback_panic(transport.seen)
	}
	if mode == 'metadata' {
		seen.mutex.lock()
		seen.metadata_seen = authorization == 'Bearer refreshed-disposable'
			&& binary.trim_right('=') == 'AAH/'
		seen.mutex.unlock()
	}
	if mode == 'hold' {
		seen.mutex.lock()
		seen.held++
		seen.mutex.unlock()
		for !context.cancelled() {
			time.sleep(time.millisecond)
		}
		seen.mutex.lock()
		seen.cancelled++
		seen.mutex.unlock()
		return corebridge.GrpcReply{
			status:  1
			message: 'cancelled by test'
		}
	}
	if mode == 'throw' { return error('private-transport-secret') }
	if mode == 'status' {
		return corebridge.GrpcReply{
			status:  7
			message: 'explicit denial'
			details: [
				u8(8),
				7,
			]
		}
	}
	if mode == 'invalid' {
		return corebridge.GrpcReply{
			status: 100
		}
	}
	return context.forward(transport.backend, request, 65000)!
}

fn custom(backend &sdk.Client, seen &Observations) sdk.TransportOptions {
	return sdk.TransportOptions{
		custom_grpc: sdk.CustomGrpcTransport{
			transport:           ForwardTransport{
				backend: backend
				seen:    seen
			}
			control_concurrency: 2
			poll_concurrency:    2
			wait_concurrency:    1
		}
	}
}

fn request(client &sdk.Client, mode string, timeout u32, cancellation ?&sdk.RpcCancellation) ![]u8 {
	return client.call_rpc(sdk.ClientRpcRequest{
		service:        .workflow
		method:         if mode == 'hold' {
			'GetWorkflowExecutionHistory'
		} else {
			'GetSystemInfo'
		}
		body:           []u8{}
		timeout_millis: timeout
		metadata:       [corebridge.RpcMetadataEntry{ name: 'x-grpc-test', value: mode.bytes() }]
		cancellation:   cancellation
	})!
}

fn expected(client &sdk.Client, mode string, code int) ! {
	if _ := request(client, mode, 2000, none) {
		return error('expected transport error')
	} else {
		assert err.code() == code, err.msg()
		assert !err.msg().contains('private-transport-secret'), err.msg()
		assert !err.msg().contains('private-grpc-transport-panic'), 'custom RPC panic was not redacted'
		if mode == 'status' {
			if err is sdk.RpcError {
				assert err.details == [u8(8), 7]
			} else {
				return error('missing typed RPC status')
			}
		}
	}
}

fn wait_held(seen &Observations, count int) ! {
	watch := time.new_stopwatch()
	for {
		seen.mutex.lock()
		held := seen.held
		seen.mutex.unlock()
		if held >= count { return }
		if watch.elapsed() > 4 * time.second { return error('handler did not start') }
		time.sleep(time.millisecond)
	}
}

fn burst(client &sdk.Client, result chan string) {
	for _ in 0 .. 10 {
		client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
			result <- err.msg()
			return
		}
	}
	result <- ''
}

fn validation(backend &sdk.Client, seen &Observations) ! {
	base := custom(backend, seen)
	configured := base.custom_grpc or { return error('missing custom transport') }
	for options in [
		sdk.TransportOptions{
			...base
			tls: sdk.TlsOptions{}
		},
		sdk.TransportOptions{
			...base
			grpc_compression: .gzip
		},
		sdk.TransportOptions{
			...base
			dns_load_balancing: sdk.DnsLoadBalancingOptions{}
		},
		sdk.TransportOptions{
			...base
			keep_alive_interval: time.second
			keep_alive_timeout:  time.second
		},
		sdk.TransportOptions{
			...base
			proxy: sdk.ProxyOptions{
				target: 'localhost:80'
			}
		},
		sdk.TransportOptions{
			custom_grpc: sdk.CustomGrpcTransport{
				...configured
				control_concurrency: 0
			}
		},
		sdk.TransportOptions{
			custom_grpc: sdk.CustomGrpcTransport{
				...configured
				max_pending: 0
			}
		},
		sdk.TransportOptions{
			...base
			metadata: {
				'invalid key': 'bad'
			}
		},
	] {
		if _ := sdk.connect(sdk.ClientOptions{
			address:   os.getenv('TEMPORAL_ADDRESS')
			transport: options
		})
		{
			return error('invalid custom transport settings accepted')
		}
	}
	seen.mutex.lock()
	calls := seen.calls
	seen.mutex.unlock()
	assert calls == 0, 'invalid configuration started transport callbacks'
	// Option conversion fails after Rust owns callback data; cleanup must drain.
	if _ := sdk.connect(sdk.ClientOptions{ address: 'not a URL', transport: base }) {
		return error('invalid URL accepted')
	}
}

fn bounded(backend &sdk.Client) ! {
	seen := &Observations{}
	base := (custom(backend, seen).custom_grpc or { return error('missing transport') })
	client := sdk.connect(sdk.ClientOptions{
		address:   os.getenv('TEMPORAL_ADDRESS')
		transport: sdk.TransportOptions{
			custom_grpc: sdk.CustomGrpcTransport{
				...base
				max_pending:       1
				max_pending_bytes: 1
			}
		}
	})!
	defer { client.close() }
	if _ := client.call_rpc(sdk.ClientRpcRequest{
		service: .workflow
		method:  'GetWorkflowExecutionHistory'
		body:    temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryRequest{
			namespace: 'default'
		}.encode()
	})
	{
		return error('protobuf byte limit ignored')
	} else {
		assert err.code() == 8, err.msg()
	}
	token := sdk.new_rpc_cancellation()!
	pending := spawn request(client, 'hold', 10000, token)
	wait_held(seen, 1)!
	if _ := request(client, 'metadata', 1000, none) {
		return error('pending request count limit ignored')
	} else {
		assert err.code() == 8, err.msg()
	}
	token.cancel()
	if _ := pending.wait() {
		return error('bounded call cancellation ignored')
	} else {
		assert err.code() == 1, err.msg()
	}
	token.close()
	client.close()
	stats := client.grpc_transport_stats() or { return error('missing statistics') }
	assert stats.rejected == 2 && stats.pending == 0 && stats.core_done, stats.str()
}

fn exercise(backend &sdk.Client, seen &Observations) ! {
	validation(backend, seen)!
	bounded(backend)!
	client := sdk.connect(sdk.ClientOptions{
		address:   os.getenv('TEMPORAL_ADDRESS')
		transport: custom(backend, seen)
	})!
	defer { client.close() }
	client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
	client.rpc_health_check(temporal.GrpcHealthV1_HealthCheckRequest{})!
	client.rpc_list_namespaces(temporal.TemporalApiWorkflowserviceV1_ListNamespacesRequest{})!
	completed := chan string{cap: 8}
	for _ in 0 .. 8 {
		spawn burst(client, completed)
	}
	for _ in 0 .. 8 {
		message := <-completed
		assert message == '', message
	}
	client.update_api_key('refreshed-disposable')!
	client.update_binary_metadata({
		'x-grpc-test-bin': [u8(0), 1, 255]
	})!
	request(client, 'metadata', 2000, none)!
	seen.mutex.lock()
	matched := seen.metadata_seen
	seen.mutex.unlock()
	assert matched, 'custom transport did not see refreshed ASCII/binary metadata'
	for mode, code in {
		'throw':   13
		'status':  7
		'invalid': 13
	} {
		expected(client, mode, code)!
	}
	watch := time.new_stopwatch()
	deadline := spawn request(client, 'hold', 120, none)
	wait_held(seen, 1)!
	// A wait-class call cannot consume the reserved control-class capacity.
	client.rpc_health_check(temporal.GrpcHealthV1_HealthCheckRequest{})!
	if _ := deadline.wait() {
		return error('deadline was ignored')
	} else {
		assert err.code() in [1, 4], err.msg()
	}
	assert watch.elapsed() < time.second, 'custom transport deadline exceeded bound'
	token := sdk.new_rpc_cancellation()!
	cancelled := spawn request(client, 'hold', 10000, token)
	wait_held(seen, 2)!
	token.cancel()
	if _ := cancelled.wait() {
		return error('cancellation ignored')
	} else {
		assert err.code() == 1, err.msg()
	}
	token.close()
	client.close()
	stats := client.grpc_transport_stats() or { return error('missing transport stats') }
	assert stats.core_done && stats.pending == 0 && stats.accepted == stats.responded, stats.str()
	println('EXERCISE_OK')
}

struct TransportWorkflow {
mut:
	total int
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return TransportWorkflow{}
}

fn (mut workflow TransportWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			workflow.total = 7
		}
		.signal_received {
			if event.name == 'add' { workflow.total += 9 }
			if event.name == 'finish' { context.complete(json2.encode(workflow.total))! }
		}
		else {}
	}
}

fn (workflow &TransportWorkflow) query(name string, args []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return json2.encode(workflow.total)
}

fn serve(options sdk.WorkerOptions, runtime &sdk.Runtime, result chan string) {
	sdk.run_workflows(options, runtime) or {
		result <- err.msg()
		return
	}
	result <- ''
}

fn run() ! {
	role := os.args[1]
	mut runtime := sdk.Runtime{}
	runtime.register('v.grpc', make_workflow)!
	if role == 'replay' {
		runtime.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id: os.getenv('TEMPORAL_TASK_QUEUE')
			task_queue:  os.getenv('TEMPORAL_TASK_QUEUE')
		})!
		println('REPLAY_OK')
		return
	}
	backend := sdk.connect(sdk.ClientOptions{ address: os.getenv('TEMPORAL_ADDRESS') })!
	defer { backend.close() }
	seen := &Observations{}
	if role == 'exercise' { return exercise(backend, seen) }
	if role == 'failed-connect' {
		for _ in 0 .. 10 {
			if _ := sdk.connect(sdk.ClientOptions{
				address:   os.getenv('TEMPORAL_ADDRESS')
				transport: sdk.TransportOptions{
					retry:       sdk.TransportRetryOptions{
						max_retries: 1
					}
					custom_grpc: sdk.CustomGrpcTransport{
						transport: ForwardTransport{
							backend:      backend
							seen:         seen
							fail_connect: true
						}
					}
				}
			})
			{
				return error('failed connect accepted')
			}
		}
		println('FAILED_CONNECT_OK')
		return
	}
	if role == 'panic-validation' {
		exercise_transport_panics(backend, seen)!
		println('GRPC_PANIC_OK')
		return
	}
	client := sdk.connect(sdk.ClientOptions{
		address:   os.getenv('TEMPORAL_ADDRESS')
		transport: custom(backend, seen)
	})!
	defer { client.close() }
	handle := sdk.WorkflowHandle{
		workflow_id: os.getenv('TEMPORAL_TASK_QUEUE')
	}
	match role {
		'worker' {
			control := &sdk.WorkerControl{}
			result := chan string{cap: 1}
			worker := spawn serve(sdk.WorkerOptions{
				client:     client
				task_queue: os.getenv('TEMPORAL_TASK_QUEUE')
				control:    control
			}, runtime, result)
			watch := time.new_stopwatch()
			for {
				seen.mutex.lock()
				polls := seen.polls
				seen.mutex.unlock()
				if polls > 0 {
					break
				}
				if watch.elapsed() > 5 * time.second { return error('worker did not poll') }
				time.sleep(time.millisecond)
			}
			client.close()
			println('CLIENT_CLOSED_WORKER_READY')
			message := <-result
			worker.wait()
			assert message == '', message
			stats := client.grpc_transport_stats() or { return error('missing transport stats') }
			assert stats.core_done && stats.pending == 0, stats.str()
		}
		'start' {
			started := client.start(sdk.WorkflowStart{
				workflow_id:   handle.workflow_id
				workflow_type: 'v.grpc'
				task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
			})!
			client.signal(started, 'add', 'null')!
			assert client.query(started, 'state', 'null')! == '16'
			println(json2.encode(started))
		}
		'finish' {
			assert client.query(handle, 'state', 'null')! == '16'
			client.signal(handle, 'finish', 'null')!
			result := client.get_result(handle, 5000)!
			assert result.status == .completed && result.data == '16', result.str()
			os.write_file(os.args[2], client.history(handle)!.encode().bytestr())!
			println('FINISHED')
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

@[noinline]
fn transport_callback_panic(observed &Observations) {
	mut seen := observed
	seen.mutex.lock()
	seen.panic_calls++
	seen.mutex.unlock()
	defer {
		seen.mutex.lock()
		seen.panic_defers++
		seen.mutex.unlock()
	}
	panic('private-grpc-transport-panic')
}

fn drive_transport_panics(client &sdk.Client) string {
	for _ in 0 .. 10 {
		expected(client, 'panic', 13) or { return err.msg() }
		request(client, 'normal', 2000, none) or { return err.msg() }
	}
	return ''
}

fn exercise_transport_panics(backend &sdk.Client, seen &Observations) ! {
	client := sdk.connect(sdk.ClientOptions{
		address:   os.getenv('TEMPORAL_ADDRESS')
		transport: custom(backend, seen)
	})!
	mut threads := []thread string{}
	for _ in 0 .. 8 {
		threads << spawn drive_transport_panics(client)
	}
	for task in threads {
		if task.wait() != '' { return error('concurrent panic/repair RPC failed') }
	}
	client.close()
	stats := client.grpc_transport_stats() or { return error('missing panic transport stats') }
	assert stats.core_done && stats.pending == 0 && stats.accepted == stats.responded
		&& stats.responded >= 160, stats.str()
	for _ in 0 .. 5 {
		rejected := sdk.connect(sdk.ClientOptions{
			address:   os.getenv('TEMPORAL_ADDRESS')
			transport: sdk.TransportOptions{
				retry:       sdk.TransportRetryOptions{
					max_retries: 1
				}
				custom_grpc: sdk.CustomGrpcTransport{
					transport: ForwardTransport{
						backend:       backend
						seen:          seen
						panic_connect: true
					}
				}
			}
		}) or {
			if err.msg().contains('private-grpc-transport-panic') {
				return error('transport panic leaked through connection error')
			}
			continue
		}
		rejected.close()
		return error('panicking connect transport unexpectedly succeeded')
	}
	seen.mutex.lock()
	calls := seen.panic_calls
	defers := seen.panic_defers
	seen.mutex.unlock()
	assert calls >= 85 && defers == calls
}
