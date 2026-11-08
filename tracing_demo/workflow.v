module main

import sdk
import example_storage
import example_converter
import os
import json2
import sync
import time

struct JournalTraceExporter {
	path  string
	mutex &sync.RwMutex = sync.new_rwmutex()
}

fn (exporter JournalTraceExporter) export(context &sdk.TraceExportContext, spans []sdk.TraceSpanData) ! {
	exporter.mutex.lock()
	defer { exporter.mutex.unlock() }
	mut file := os.open_append(exporter.path)!
	defer { file.close() }
	for span in spans {
		file.writeln(json2.encode(span))!
	}
}

struct TracedWorkflow {
mut:
	nexus_sequence u32
	completed      int
	mode           string
	trace          string
}

fn traced_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return TracedWorkflow{}
}

fn (mut workflow TracedWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	current := context.trace_context() or { return error('workflow trace context missing') }
	if current.trace_id != os.getenv('V_TRACE_ID') {
		return error('workflow trace identity changed')
	}
	expected_baggage := if event.kind == .signal_received && event.name == 'baggage' {
		os.getenv('V_TRACE_BAGGAGE_SIGNAL')
	} else if event.kind == .update_received && os.getenv('V_TRACE_BAGGAGE_UPDATE') != '' {
		os.getenv('V_TRACE_BAGGAGE_UPDATE')
	} else {
		os.getenv('V_TRACE_BAGGAGE')
	}
	if context.propagation_context().baggage.header() != expected_baggage {
		return error('workflow baggage scope changed: ' +
			context.propagation_context().baggage.header())
	}

	if event.kind == .started {
		workflow.mode = json2.decode[string](sdk.single_argument(event.arguments)!)!
		workflow.trace = current.trace_id
		headers := context.headers()!
		if headers.len != 1 || (headers['application'] or {
			return error('application workflow header missing')
		}).logical_data() != '"value"' {
			return error('application workflow header changed')
		}
		if workflow.mode.starts_with('nexus-') {
			workflow.nexus_sequence = context.nexus_operation(sdk.PayloadValue(json2.encode(workflow.mode)), sdk.NexusOptions{
				endpoint:     os.getenv('V_TRACE_NEXUS_ENDPOINT')
				service:      'v.trace'
				operation:    workflow.mode[6..]
				headers:      {
					'application': 'value'
				}
				cancellation: .wait_completed
			})!
			return
		}
		if workflow.mode == 'child' {
			context.complete(sdk.PayloadValue('"child"'))!
			return
		}
		if workflow.mode == 'next' {
			context.complete(sdk.PayloadValue('"continued"'))!
			return
		}
		context.activity('v.trace.activity', sdk.Arguments('"remote"'), sdk.ActivityOptions{
			retry:   sdk.RetryPolicy{
				maximum_attempts: 1
			}
			headers: {
				'application': sdk.PayloadValue('"value"')
			}
		})!
		context.local_activity('v.trace.activity', sdk.Arguments('"local"'), sdk.LocalActivityOptions{
			headers: {
				'application': sdk.PayloadValue('"value"')
			}
		})!
		context.child('v.trace.workflow', context.info().workflow_id + '-child',
			sdk.Arguments('"child"'), sdk.ChildOptions{
			headers: {
				'application': sdk.PayloadValue('"value"')
			}
		})!
	}
	if event.kind in [.activity_completed, .child_completed, .nexus_completed, .nexus_cancelled] {
		workflow.completed++
	}
	if event.kind in [.activity_failed, .child_failed, .nexus_failed] {
		return error('traced child execution failed: ' + event.message)
	}
	if event.kind == .signal_received && event.name == 'baggage' {
		context.local_activity('v.trace.activity', sdk.Arguments('"signal"'), sdk.LocalActivityOptions{
			headers: {
				'application': sdk.PayloadValue('"value"')
			}
		})!
	}
	if event.kind == .update_received {
		if os.getenv('V_TRACE_BAGGAGE_UPDATE') != '' {
			context.local_activity('v.trace.activity', sdk.Arguments('"update"'), sdk.LocalActivityOptions{
				headers: {
					'application': sdk.PayloadValue('"value"')
				}
			})!
		}
		context.complete_update(event.id, sdk.PayloadValue('"updated"'))!
	}
	if event.kind == .signal_received && event.name == 'finish' {
		headers := context.event_headers(event)!
		if headers.len != 1 { return error('signal application headers changed') }
		if workflow.mode == 'nexus-cancel' {
			context.cancel_nexus(workflow.nexus_sequence)!
			return
		}
		if workflow.mode == 'fail' { return error('fixture workflow failure') }
		if workflow.mode == 'continue' {
			context.continue_as_new(sdk.Arguments('"next"'), sdk.ContinueOptions{
				headers: {
					'application': sdk.PayloadValue('"value"')
				}
			})!
			return
		}
		context.complete(sdk.PayloadValue('"completed"'))!
	}
	if event.kind == .nexus_cancelled { context.complete(sdk.PayloadValue('"cancelled nexus"'))! }
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (workflow &TracedWorkflow) query_names() []string {
	return ['state']
}

fn (workflow &TracedWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.completed))
}

fn (workflow &TracedWorkflow) update_names() []string {
	return ['change']
}

fn (workflow &TracedWorkflow) validate_update(name string, arguments []string, view sdk.WorkflowContextView) ! {}

fn traced_activity(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	trace := context.trace_context() or { return error('Activity trace context missing') }
	if trace.trace_id != os.getenv('V_TRACE_ID') { return error('Activity trace identity changed') }
	mode := sdk.single_argument(arguments)!
	expected_baggage := if mode == '"signal"' {
		os.getenv('V_TRACE_BAGGAGE_SIGNAL')
	} else if mode == '"update"' {
		os.getenv('V_TRACE_BAGGAGE_UPDATE')
	} else {
		os.getenv('V_TRACE_BAGGAGE')
	}
	if context.propagation_context().baggage.header() != expected_baggage {
		return error('Activity baggage scope changed')
	}

	headers := context.header_values()!
	if headers.len != 1 || (headers['application'] or {
		return error('Activity application header missing')
	}).logical_data() != '"value"' {
		return error('Activity application header changed')
	}
	return sdk.PayloadValue('"activity"')
}

fn traced_runtime() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.trace.workflow', traced_factory)!
	runtime.register('v.trace.interop', trace_interop_factory)!
	runtime.register_local_activity('v.trace.activity', traced_activity)!
	return runtime
}

fn workflow_tracing(role string) ! {
	config := sdk.options_from_env()!
	converter := tracing_converter()!
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		sampler:        fixture_sampling_policy()!
		exporter:       JournalTraceExporter{
			path: os.getenv('V_TRACE_JOURNAL')
		}
		flush_interval: time.millisecond
	})!
	defer { provider.shutdown(5 * time.second) or {} }
	parent := sdk.TraceContext{
		trace_id:    os.getenv('V_TRACE_ID')
		span_id:     'abcdef1234567890'
		trace_flags: if os.getenv('V_TRACE_PARENT_SAMPLED') == '0' {
			u8(2)
		} else {
			u8(3)
		}
		trace_state: 'vendor=value'
	}
	tracing := sdk.TraceOptions{
		provider: provider
		parent:   parent
	}
	if role == 'trace-worker' {
		mut activities := sdk.Activities{}
		activities.register('v.trace.activity', traced_activity)!
		mut services := sdk.NexusServices{}
		if os.getenv('V_TRACE_NEXUS_ENDPOINT') != '' {
			for operation in ['sync', 'async', 'fail', 'cancel'] {
				services.register('v.trace', operation, sdk.NexusOperation{
					start:  traced_nexus_start
					cancel: traced_nexus_cancel
				})!
			}
		}
		sdk.run_worker(sdk.WorkerOptions{
			...config
			tracing:                       tracing
			data_converter:                converter
			query_interceptors:            [
				sdk.QueryInterceptor(BaggageQueryInterceptor{}),
			]
			update_validator_interceptors: [
				sdk.UpdateValidatorInterceptor(BaggageValidatorInterceptor{}),
			]
			activity_concurrency:          4
			workflow_concurrency:          8
		}, traced_runtime()!, activities, sdk.WorkerServices{
			nexus: services
		})!
		return
	}
	if role == 'trace-replay' {
		traced_runtime()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			tracing:        tracing
			workflow_id:    os.args[3]
			task_queue:     os.args[4]
			data_converter: converter
		})!
		provider.force_flush(5 * time.second)!
		if provider.stats().accepted_spans != 0 { return error('replay exported spans') }
		println('OK')
		return
	}
	baggage := sdk.parse_baggage(os.getenv_opt('V_TRACE_BAGGAGE_CLIENT') or {
		os.getenv('V_TRACE_BAGGAGE')
	})
	if baggage.invalid_members > 0 || baggage.truncated {
		return error('invalid baggage fixture configuration')
	}
	client := sdk.connect(sdk.ClientOptions{
		address:        config.address
		data_converter: converter
		tracing:        sdk.TraceOptions{
			...tracing
			baggage: baggage.baggage
		}
	})!
	defer { client.close() }
	if role == 'trace-interop-start' {
		handle := client.start(sdk.WorkflowStart{
			workflow_id:   config.task_queue + '-interop'
			workflow_type: 'v.trace.interop'
			task_queue:    config.task_queue
		})!
		println(json2.encode(handle))
		return
	}
	if role == 'trace-nexus-start' {
		mut handles := []sdk.WorkflowHandle{}
		for index, mode in ['nexus-sync', 'nexus-async', 'nexus-fail', 'nexus-cancel'] {
			handles << client.start(sdk.WorkflowStart{
				workflow_id:   config.task_queue + '-nexus-' + index.str()
				workflow_type: 'v.trace.workflow'
				task_queue:    config.task_queue
				arguments:     json2.encode(mode)
				headers:       {
					'application': sdk.PayloadValue('"value"')
				}
			})!
		}
		println(json2.encode(handles))
		return
	}
	if role == 'trace-start' {
		mut handles := []sdk.WorkflowHandle{}
		for index, mode in ['complete', 'fail', 'cancel', 'continue'] {
			handles << client.start(sdk.WorkflowStart{
				workflow_id:   config.task_queue + '-' + index.str()
				workflow_type: 'v.trace.workflow'
				task_queue:    config.task_queue
				arguments:     json2.encode(mode)
				headers:       {
					'application': sdk.PayloadValue('"value"')
				}
				memo:          {
					sdk.trace_header_key: sdk.PayloadValue('"application-owned-memo"')
				}
			})!
		}
		println(json2.encode(handles))
		return
	}
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	match role {
		'trace-ready' {
			client.signal(handle, 'ready', sdk.Arguments(sdk.TypedArguments{}))!
			println('OK')
		}
		'trace-state' {
			println(client.query(handle, 'state', sdk.Arguments(sdk.TypedArguments{}))!)
		}
		'trace-baggage-signal' {
			client.signal(handle, 'baggage', sdk.Arguments(sdk.TypedArguments{}), sdk.MessageOptions{
				headers: {
					'application': sdk.PayloadValue('"value"')
				}
			})!
			println('OK')
		}
		'trace-finish' {
			client.signal(handle, 'finish', sdk.Arguments(sdk.TypedArguments{}), sdk.MessageOptions{
				headers: {
					'application': sdk.PayloadValue('"value"')
				}
			})!
			println('OK')
		}
		'trace-update' {
			update := client.start_update(handle, sdk.UpdateRequest{
				name:     'change'
				wait_for: .completed
				headers:  {
					'application': sdk.PayloadValue('"value"')
				}
			})!
			result := client.update_result(update, 10000)!
			if result.failed { return error(result.message) }
			println('OK')
		}
		'trace-cancel' {
			client.cancel(handle, 'trace fixture')!
			println('OK')
		}
		'trace-result' {
			println(json2.encode(client.get_chain_result(handle, 20000)!))
		}
		'trace-history' {
			os.write_file_array(os.args[3], client.history(handle)!.encode())!
			println('OK')
		}
		else {
			return error('invalid tracing workflow fixture role')
		}
	}
}

fn traced_nexus_start(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	trace := context.trace_context() or { return error('Nexus trace context missing') }
	if context.propagation_context().baggage.header() != os.getenv('V_TRACE_BAGGAGE') {
		return error('Nexus baggage propagation changed')
	}
	if trace.trace_id != os.getenv('V_TRACE_ID') || context.info().headers['application'] != 'value' {
		return error('Nexus trace or application header changed')
	}
	mode := json2.decode[string](input)!
	if mode == 'nexus-sync' {
		return sdk.NexusResult{
			value: sdk.PayloadValue('"sync"')
		}
	}
	if mode == 'nexus-fail' {
		return sdk.ApplicationError{
			message:       'fixture Nexus failure'
			category:      'NexusFixture'
			non_retryable: true
		}
	}
	config := sdk.options_from_env()!
	client := sdk.connect(sdk.ClientOptions{
		address:        config.address
		data_converter: tracing_converter()!
	})!
	defer { client.close() }
	return context.start_workflow(client, sdk.WorkflowStart{
		workflow_type: 'v.trace.workflow'
		task_queue:    config.task_queue
		arguments:     if mode == 'nexus-cancel' {
			'"wait"'
		} else {
			'"child"'
		}
		headers:       {
			'application': sdk.PayloadValue('"value"')
		}
	})!
}

fn traced_nexus_cancel(mut context sdk.NexusContext, token string) ! {
	_ := context.trace_context() or { return error('Nexus cancellation trace context missing') }
	if context.propagation_context().baggage.header() != os.getenv('V_TRACE_BAGGAGE') {
		return error('Nexus cancel baggage changed')
	}
	config := sdk.options_from_env()!
	client := sdk.connect(sdk.ClientOptions{
		address:        config.address
		data_converter: tracing_converter()!
	})!
	defer { client.close() }
	context.cancel_workflow(client, token)!
}

fn tracing_converter() !sdk.DataConverter {
	converter := example_converter.from_env()!
	directory := os.getenv('V_SDK_EXAMPLE_STORAGE_DIR')
	if directory == '' { return converter }
	storage := sdk.new_external_storage(
		drivers:                   [
			sdk.StorageDriver(example_storage.FileStorageDriver{
				root: directory
			}),
		]
		threshold_bytes:           1
		max_concurrent_stores:     2
		max_concurrent_retrievals: 2
	)!
	return sdk.DataConverter{
		...converter
		external_storage: storage
	}
}
