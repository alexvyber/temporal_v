module main

import temporal_v.sdk
import temporal_v.temporal
import os
import sync
import time
import x.json2
import temporal_v.example_converter

@[heap]
struct Counters {
mut:
	mutex  &sync.Mutex = sync.new_mutex()
	phases []string
	calls  int
}

fn (state &Counters) record(phase string) {
	mut counters := unsafe { state }
	counters.mutex.lock()
	defer { counters.mutex.unlock() }
	counters.phases << phase
}

struct CountedPlugin {
	state &Counters
}

fn (plugin CountedPlugin) name() string {
	return 'counter'
}

fn (plugin CountedPlugin) version() string {
	return '1.2.3'
}

fn (plugin CountedPlugin) configure_connection(options sdk.ClientOptions) !sdk.ClientOptions {
	plugin.state.record('connection')
	return sdk.ClientOptions{
		...options
		namespace: 'default'
		telemetry: sdk.TelemetryOptions{
			...options.telemetry
			worker_heartbeat_interval: time.second
			runtime_info:              [
				sdk.RuntimeInfo{
					kind:    .native
					version: 'v-plugin-test'
				},
			]
			disable_environment_info:  os.getenv('V_SDK_EXAMPLE_PLUGIN_OMIT_ENV') == 'true'
		}
	}
}

fn (plugin CountedPlugin) configure_client(options sdk.ClientOptions) !sdk.ClientOptions {
	plugin.state.record('client')
	mut interceptors := options.interceptors.clone()
	interceptors << sdk.ClientInterceptor(CountedCalls{
		state: plugin.state
	})
	return sdk.ClientOptions{
		...options
		interceptors: interceptors
		plugins:      [sdk.Plugin(BrokenPlugin{})]
	}
}

fn (plugin CountedPlugin) configure_worker(mut setup sdk.WorkerSetup) ! {
	plugin.state.record('worker')
	// This registration must not be applied recursively.
	setup.options = sdk.WorkerOptions{
		...setup.options
		plugins: [sdk.Plugin(BrokenPlugin{})]
	}
}

fn (plugin CountedPlugin) configure_replay(mut setup sdk.ReplaySetup) ! {
	plugin.state.record('replay')
	setup.options = sdk.ReplayOptions{
		...setup.options
		plugins: [sdk.Plugin(BrokenPlugin{})]
	}
}

struct CountedCalls {
	state &Counters
}

fn (interceptor CountedCalls) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	interceptor.state.record('rpc:' + request.method)
	return next.execute(request)!
}

struct BrokenPlugin {}

fn (plugin BrokenPlugin) name() string {
	return 'broken'
}

fn (plugin BrokenPlugin) configure_worker(mut setup sdk.WorkerSetup) ! {
	return error('worker plugin failure')
}

fn (plugin BrokenPlugin) configure_client(options sdk.ClientOptions) !sdk.ClientOptions {
	return error('client plugin failure')
}

fn (plugin BrokenPlugin) configure_replay(mut setup sdk.ReplaySetup) ! {
	return error('replay plugin failure')
}

struct PluginWorkflow {
mut:
	stage int
	data  string
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return PluginWorkflow{}
}

fn (mut workflow PluginWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		workflow.data = event.data
		context.local_activity('plugin.echo', event.data, sdk.LocalActivityOptions{})!
	}
	if event.kind == .activity_completed {
		workflow.stage++
		if workflow.stage == 1 {
			context.activity('plugin.echo', event.data, sdk.ActivityOptions{})!
		} else {
			context.timer(60 * time.second)!
		}
	}
	if event.kind == .signal_received { context.complete(workflow.data)! }
}

fn (workflow &PluginWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return workflow.stage.str()
}

fn echo(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	mut file := os.open_append(os.getenv('V_SDK_EXAMPLE_PLUGIN_ACTIVITY_FILE'))!
	defer { file.close() }
	file.write_string(if context.info().is_local { 'local\n' } else { 'remote\n' })!
	return sdk.single_argument(arguments)!
}

fn nexus_echo(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	return sdk.NexusResult{
		value: input
	}
}

fn serve(options sdk.WorkerOptions, runtime &sdk.Runtime, result chan string) {
	sdk.run_worker(options, runtime, sdk.Activities{}) or {
		result <- err.msg()
		return
	}
	result <- ''
}

fn bundle() sdk.Plugin {
	return sdk.PluginBundle{
		information:      sdk.PluginInfo{
			name:    'bundle'
			version: '0.1.0'
		}
		data_converter:   example_converter.from_env() or { panic(err) }
		workflows:        [
			sdk.WorkflowRegistration{
				name:    'plugin.workflow'
				factory: make_workflow
			},
		]
		activities:       [sdk.ActivityRegistration{ name: 'plugin.echo', handler: echo }]
		local_activities: [sdk.ActivityRegistration{ name: 'plugin.echo', handler: echo }]
		nexus_services:   [
			sdk.NexusRegistration{
				service:   'plugin'
				operation: 'echo'
				handler:   sdk.NexusOperation{
					start: nexus_echo
				}
			},
		]
	}
}

fn run() ! {
	state := &Counters{}
	address := os.getenv('TEMPORAL_ADDRESS')
	queue := os.getenv('TEMPORAL_TASK_QUEUE')
	client := sdk.connect(sdk.ClientOptions{
		address:   address
		namespace: ''
		transport: sdk.transport_from_env()!
		plugins:   [
			sdk.Plugin(CountedPlugin{
				state: state
			}),
			bundle(),
		]
	})!
	defer { client.close() }
	if os.args.len > 1 {
		run_role(os.args[1], client, state, queue)!
		return
	}
	assert state.phases == ['connection', 'client']
	assert client.plugin_info() == [
		sdk.PluginInfo{
			name:    'counter'
			version: '1.2.3'
		},
		sdk.PluginInfo{
			name:    'bundle'
			version: '0.1.0'
		},
	]
	client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
	control := &sdk.WorkerControl{}
	mut runtime := sdk.Runtime{}
	result := chan string{cap: 1}
	worker := spawn serve(sdk.WorkerOptions{ client: client, task_queue: queue, control: control },
		&runtime, result)
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   queue
		workflow_type: 'plugin.workflow'
		task_queue:    queue
		arguments:     '"plugin data"'
	})!
	wait_stage(client, handle)!
	nexus := client.start_nexus_operation(sdk.NexusExecutionStart{
		operation_id: queue + '-nexus'
		endpoint:     queue + '-endpoint'
		service:      'plugin'
		operation:    'echo'
		input:        '"nexus plugin data"'
	})!
	nexus_result := client.nexus_operation_result(nexus, 15 * time.second)!
	assert nexus_result.status == .nexus_operation_execution_status_completed
		&& nexus_result.data == '"nexus plugin data"'
	check_worker_metadata(client, queue)!
	control.shutdown()
	message := <-result
	assert message == '', message
	worker.wait()
	replacement_control := &sdk.WorkerControl{}
	replacement := spawn serve(sdk.WorkerOptions{
		client:     client
		task_queue: queue
		control:    replacement_control
	}, &runtime, result)
	wait_stage(client, handle)!
	client.signal(handle, 'finish', 'null')!
	outcome := client.get_result(handle, 15000)!
	assert outcome.status == .completed && outcome.data == '"plugin data"'
	replacement_control.shutdown()
	replacement_message := <-result
	assert replacement_message == '', replacement_message
	replacement.wait()
	assert os.read_lines(os.getenv('V_SDK_EXAMPLE_PLUGIN_ACTIVITY_FILE'))! == [
		'local',
		'remote',
	]
	// The worker's plugin definitions did not leak into the caller's registry.
	runtime.register('plugin.workflow', make_workflow)!
	assert state.phases.filter(it == 'worker').len == 2, state.phases.str()
	page := client.history_page(handle, sdk.HistoryOptions{})!
	history := page.history or { return error('missing history') }
	mut replay_runtime := sdk.Runtime{}
	replay_runtime.replay(history.encode(), sdk.ReplayOptions{
		workflow_id: handle.workflow_id
		task_queue:  queue
		plugins:     [
			sdk.Plugin(CountedPlugin{
				state: state
			}),
			bundle(),
		]
	})!
	assert state.phases.filter(it == 'replay').len == 1
	if _ := sdk.connect(sdk.ClientOptions{
		address: 'http://127.0.0.1:1'
		plugins: [
			sdk.Plugin(BrokenPlugin{}),
		]
	})
	{
		return error('bad client plugin accepted')
	} else {
		assert err is sdk.PluginError
		if err is sdk.PluginError {
			assert err.target == .client && err.plugin_name == 'broken'
		}
	}
	sdk.run_worker(sdk.WorkerOptions{
		client:     client
		task_queue: queue
		plugins:    [
			sdk.Plugin(BrokenPlugin{}),
		]
	}, replay_runtime, sdk.Activities{}) or {
		assert err is sdk.PluginError
		if err is sdk.PluginError {
			assert err.target == .worker
		}
	}
	replay_runtime.register('plugin.workflow', make_workflow)!
	if _ := replay_runtime.replay(history.encode(), sdk.ReplayOptions{
		workflow_id: handle.workflow_id
		plugins:     [sdk.Plugin(BrokenPlugin{})]
	})
	{
		return error('bad replay plugin accepted')
	} else {
		assert err is sdk.PluginError
		if err is sdk.PluginError {
			assert err.target == .replay
		}
	}
	println('client/worker/replay plugins, propagation, isolated registration and failures passed')
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}

fn check_worker_metadata(client &sdk.Client, queue string) ! {
	watch := time.new_stopwatch()
	for {
		listed := client.rpc_list_workers(temporal.TemporalApiWorkflowserviceV1_ListWorkersRequest{
			namespace: 'default'
			page_size: 200
		})!
		for worker in listed.workers {
			if worker.task_queue != queue { continue
			 }
			described := client.rpc_describe_worker(temporal.TemporalApiWorkflowserviceV1_DescribeWorkerRequest{
				namespace:           'default'
				worker_instance_key: worker.worker_instance_key
			})!
			information := described.worker_info or { return error('missing worker metadata') }
			heartbeat := information.worker_heartbeat or {
				return error('missing worker heartbeat')
			}

			assert heartbeat.plugins.any(it.name == 'counter' && it.version == '1.2.3')
			assert heartbeat.plugins.any(it.name == 'bundle' && it.version == '0.1.0')
			assert heartbeat.drivers.any(it.type_ == 'fixture.filesystem')
			if os.getenv('V_SDK_EXAMPLE_PLUGIN_OMIT_ENV') == 'true' {
				assert heartbeat.environment == none
			} else {
				environment := heartbeat.environment or {
					return error('missing worker environment')
				}

				assert environment.runtimes.len == 1
				assert int(environment.runtimes[0].type_) == 9
				assert environment.runtimes[0].version == 'v-plugin-test'
				assert environment.platform != none
			}
			os.write_file(os.getenv('V_SDK_EXAMPLE_PLUGIN_METADATA_FILE'), json2.encode(heartbeat))!
			return
		}
		if watch.elapsed() > 20 * time.second {
			return error('worker heartbeat was not registered')
		}
		time.sleep(50 * time.millisecond)
	}
}

fn wait_stage(client &sdk.Client, handle sdk.WorkflowHandle) ! {
	watch := time.new_stopwatch()
	for {
		if client.query(handle, 'stage', 'null')! == '2' { return }
		if watch.elapsed() > 15 * time.second {
			return error('plugin workflow did not reach checkpoint')
		}
		time.sleep(20 * time.millisecond)
	}
}

fn run_role(role string, client &sdk.Client, state &Counters, queue string) ! {
	mut runtime := sdk.Runtime{}
	handle := sdk.WorkflowHandle{
		workflow_id: queue + '-crash'
	}
	match role {
		'construction-race' {
			construction_race(client, queue)!
		}
		'worker' {
			sdk.run_worker(sdk.WorkerOptions{ client: client, task_queue: queue }, runtime, sdk.Activities{})!
		}
		'start' {
			started := client.start(sdk.WorkflowStart{
				workflow_id:   handle.workflow_id
				workflow_type: 'plugin.workflow'
				task_queue:    queue
				arguments:     '"plugin crash data"'
			})!
			println(json2.encode(started))
		}
		'checkpoint' {
			wait_stage(client, handle)!
			println('checkpoint passed')
		}
		'finish' {
			client.signal(handle, 'finish', 'null')!
			outcome := client.get_result(handle, 15000)!
			assert outcome.status == .completed && outcome.data == '"plugin crash data"'
			println('crash recovery passed')
		}
		'history' {
			history := client.history(handle)!
			os.write_file(os.args[2], history.encode().bytestr())!
		}
		'replay' {
			runtime.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
				workflow_id: handle.workflow_id
				task_queue:  queue
				plugins:     [
					sdk.Plugin(CountedPlugin{
						state: state
					}),
					bundle(),
				]
			})!
			println('replay passed')
		}
		else {
			return error('unknown plugin demo mode: ${role}')
		}
	}
}
