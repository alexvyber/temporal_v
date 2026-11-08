module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import x.json2
import os
import sync
import time

@[heap]
struct ActivityClientAudit {
	mutex &sync.Mutex = sync.new_mutex()
mut:
	clients []&sdk.Client
	hooks   int
	calls   int
}

struct ActivityClientPlugin {
	audit &ActivityClientAudit
}

fn (plugin ActivityClientPlugin) name() string {
	return 'activity-client-fixture'
}

fn (plugin ActivityClientPlugin) configure_client(options sdk.ClientOptions) !sdk.ClientOptions {
	mut audit := plugin.audit
	audit.mutex.lock()
	audit.hooks++
	audit.mutex.unlock()
	return sdk.ClientOptions{
		...options
		interceptors: [sdk.ClientInterceptor(ActivityClientRpc{
			audit: audit
		})]
	}
}

struct ActivityClientRpc {
	audit &ActivityClientAudit
}

fn (interceptor ActivityClientRpc) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	mut audit := interceptor.audit
	audit.mutex.lock()
	audit.calls++
	audit.mutex.unlock()
	return next.execute(request)!
}

struct ActivityClientTap {
	audit    &ActivityClientAudit
	original ?&sdk.Client
}

fn (tap ActivityClientTap) intercept(mut context sdk.ActivityContext, arguments []string, next sdk.ActivityNext) !sdk.PayloadValue {
	// The worker owns its native client even after the caller closes its handle.
	if original := tap.original { original.close() }
	retained := context.client()!
	if retained.plugin_info().len != if tap.original != none {
		1
	} else {
		0
	} {
		return error('Activity client plugin snapshot changed')
	}
	mut audit := tap.audit
	audit.mutex.lock()
	audit.clients << retained
	audit.mutex.unlock()
	return next.execute(mut context, arguments)!
}

struct ActivityClientWorkflow {
mut:
	completed int
	called    int
	children  []sdk.WorkflowHandle
}

fn activity_client_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ActivityClientWorkflow{}
}

fn (mut workflow ActivityClientWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		context.activity('v.activity-client', '"remote"', sdk.ActivityOptions{
			start_to_close: 30 * time.second
			retry:          sdk.RetryPolicy{
				maximum_attempts: 1
			}
		})!
		context.local_activity('v.activity-client', '"local"', sdk.LocalActivityOptions{
			start_to_close: 30 * time.second
			retry:          sdk.RetryPolicy{
				maximum_attempts: 1
			}
		})!
	}
	if event.kind == .activity_completed {
		workflow.children << json2.decode[sdk.WorkflowHandle](event.data)!
		workflow.completed++
	}
	if event.kind == .activity_failed { return error('Activity client failed: ' + event.data) }
	if event.kind == .signal_received && event.name == 'client-called' { workflow.called++ }
	if event.kind == .signal_received && event.name == 'finish' {
		if workflow.completed != 2 || workflow.called != 2 {
			return error('Activity client effects incomplete')
		}
		context.complete('"client-workflow-ok"')!
	}
}

fn (workflow ActivityClientWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(ActivityClientState{
		completed: workflow.completed
		called:    workflow.called
		children:  workflow.children
	}))
}

struct ActivityClientChild {}

fn activity_client_child_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ActivityClientChild{}
}

fn (mut workflow ActivityClientChild) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started { context.complete(sdk.PayloadValue(event.data))! }
}

fn activity_client_handler(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	closed := context.client()!
	client := context.client()!
	defer { client.close() }
	closed.close()
	mut rejected := false
	closed.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
		if !err.msg().contains('closed') { return err }
		rejected = true
	}
	if !rejected { return error('closed Activity client remained usable') }
	client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
	info := context.info()
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   info.workflow_id + '-' + info.activity_id + '-client-child'
		workflow_type: 'v.activity-client.child'
		task_queue:    info.task_queue
		arguments:     sdk.Arguments(arguments)
	})!
	result := client.get_result(handle, 20000)!
	if result.status != .completed || result.data != arguments[0] {
		return error('Activity client child failed or lost conversion')
	}
	parent := context.workflow_handle() or {
		return error('workflow Activity lost its parent handle')
	}
	if parent.workflow_id != info.workflow_id || parent.run_id != info.run_id
		|| info.namespace != os.getenv('TEMPORAL_NAMESPACE') && !(info.namespace == 'default'
		&& os.getenv('TEMPORAL_NAMESPACE') == '') {
		return error('Activity parent handle or namespace changed')
	}
	client.signal(parent, 'client-called', 'null')!
	return sdk.PayloadValue(json2.encode(handle))
}

struct ActivityClientJournal {
	path  string
	mutex &sync.Mutex = sync.new_mutex()
}

fn (exporter ActivityClientJournal) export(context &sdk.TraceExportContext, spans []sdk.TraceSpanData) ! {
	exporter.mutex.lock()
	defer { exporter.mutex.unlock() }
	mut file := os.open_append(exporter.path)!
	defer { file.close() }
	for span in spans {
		file.writeln(json2.encode(span))!
	}
}

fn serve_activity_client(config sdk.WorkerOptions, converter sdk.DataConverter) ! {
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:       ActivityClientJournal{
			path: os.getenv('V_TRACE_JOURNAL')
		}
		flush_interval: time.millisecond
	})!
	defer { provider.shutdown(5 * time.second) or {} }
	tracing := sdk.TraceOptions{
		provider: provider
	}
	mut audit := &ActivityClientAudit{}
	use_shared_client := os.getenv('V_ACTIVITY_CLIENT_SHARED') == 'true'
	mut supplied := ?&sdk.Client(none)
	if use_shared_client {
		supplied = sdk.connect(sdk.ClientOptions{
			address:        config.address
			namespace:      config.namespace
			data_converter: converter
			tracing:        tracing
			plugins:        [sdk.Plugin(ActivityClientPlugin{
				audit: audit
			})]
		})!
	}
	defer {
		if parent := supplied { parent.close() }
	}
	mut activities := sdk.Activities{}
	activities.register('v.activity-client', activity_client_handler)!
	activities.register('v.parent-handle.none', standalone_parent_handle)!
	sdk.run_worker(sdk.WorkerOptions{
		...config
		client:                supplied
		data_converter:        converter
		tracing:               tracing
		activity_interceptors: [
			sdk.ActivityInterceptor(ActivityClientTap{
				audit:    audit
				original: supplied
			}),
		]
		activity_concurrency:  4
		workflow_concurrency:  8
	}, runtime()!, activities)!
	if audit.clients.len == 0 { return error('no retained Activity clients') }
	for client in audit.clients {
		client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
		client.close()
	}
	if use_shared_client && (audit.hooks != 1 || audit.calls < 8) {
		return error('Activity client reconfigured plugins or lost interceptors')
	}
	provider.force_flush(5 * time.second)!
	println('ACTIVITY_CLIENT_DRAIN_OK')
}

struct ActivityClientState {
	completed int
	called    int
	children  []sdk.WorkflowHandle
}

fn standalone_parent_handle(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	if context.workflow_handle() != none {
		return error('standalone Activity has a parent workflow handle')
	}
	info := context.info()
	namespace := json2.decode[string](sdk.single_argument(arguments)!)!
	if !info.is_standalone || info.workflow_id != '' || info.namespace != namespace {
		return error('standalone namespace or workflow identity changed')
	}
	client := context.client()!
	defer { client.close() }
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   info.activity_id + '-child'
		workflow_type: 'v.activity-client.child'
		task_queue:    info.task_queue
		arguments:     '"standalone-child"'
	})!
	if client.get_result(handle, 20000)!.data != '"standalone-child"' {
		return error('standalone context client used wrong namespace')
	}
	return sdk.PayloadValue(json2.encode(handle))
}

fn check_standalone_parent(config sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{
		address:        config.address
		namespace:      config.namespace
		data_converter: converter
	})!
	defer { client.close() }
	handle := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:   config.task_queue + '-standalone'
		activity_type: 'v.parent-handle.none'
		task_queue:    config.task_queue
		arguments:     json2.encode(config.namespace)
		retry:         sdk.RetryPolicy{
			maximum_attempts: 1
		}
	})!
	result := client.activity_result(handle, 30 * time.second)!
	if result.status != .completed || result.values.len != 1 {
		return error('standalone parent-handle check failed: ' + result.str())
	}
	println(result.values[0])
}
