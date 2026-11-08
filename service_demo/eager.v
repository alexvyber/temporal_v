module main

import sdk
import temporal
import workflows
import example_converter
import corebridge
import time

struct EagerCapture {
	responses chan bool
}

fn (capture EagerCapture) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	bytes := next.execute(request)!
	if request.method == 'StartWorkflowExecution' {
		response :=
			temporal.TemporalApiWorkflowserviceV1_StartWorkflowExecutionResponse.decode(bytes)!
		capture.responses <- (response.eager_workflow_task != none)
	}
	return bytes
}

struct EagerLogger {
	ready chan bool
}

fn (logger EagerLogger) log(entry sdk.LogEntry) {
	sdk.JsonLogger{}.log(entry)
	if entry.message.starts_with('WORKFLOW_READY') { logger.ready <- true }
}

fn eager_run(options sdk.WorkerOptions, completed chan string) {
	mut runtime := sdk.Runtime{}
	workflows.register(mut runtime) or {
		completed <- err.msg()
		return
	}
	sdk.run_workflows(options, runtime) or {
		completed <- err.msg()
		return
	}
	completed <- ''
}

fn eager_ready(ready chan bool, completed chan string) ! {
	select {
		_ := <-ready {}
		message := <-completed {
			return error('shared worker failed before ready: ' + message)
		}
		15 * time.second {
			return error('shared worker did not become ready')
		}
	}
}

fn eager_observed(responses chan bool, expected bool) ! {
	select {
		observed := <-responses {
			if observed != expected {
				return error('eager response presence ${observed}; expected ${expected}')
			}
		}
		5 * time.second {
			return error('missing captured eager response')
		}
	}
}

fn eager_rpc_loop(client &sdk.Client, ready chan bool, completed chan string) {
	mut started := false
	for {
		client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
			completed <- if err.msg().contains('closed') {
				''
			} else {
				err.msg()
			}
			return
		}
		if !started {
			ready <- true
			started = true
		}
		time.sleep(time.millisecond)
	}
}

fn shared_raw_rpc_loop(connection &corebridge.Connection, stop chan bool, ready chan bool, completed chan string) {
	mut started := false
	for {
		select {
			_ := <-stop {
				completed <- ''
				return
			}
			else {}
		}
		connection.rpc(.workflow, 'GetSystemInfo', []u8{}, 5000, corebridge.RpcOptions{}) or {
			completed <- err.msg()
			return
		}
		if !started {
			ready <- true
			started = true
		}
		time.sleep(time.millisecond)
	}
}

fn shared_raw_poll(connection &corebridge.Connection, completed chan string) {
	for {
		bytes := connection.poll(false) or {
			completed <- err.msg()
			return
		}
		if bytes.len == 0 {
			completed <- ''
			return
		}
	}
}

fn shared_raw_ownership(options sdk.WorkerOptions) ! {
	root := corebridge.connect(options.address, corebridge.Transport{}, corebridge.Telemetry{})!
	defer { root.close() }
	config := corebridge.Config{
		connection: root
		address:    options.address
		namespace:  options.namespace
		task_queue: options.task_queue + '-raw-shared'
	}
	worker := corebridge.open(config, .activity)!
	poll_completed := chan string{cap: 1}
	poller := spawn shared_raw_poll(worker, poll_completed)
	defer {
		worker.shutdown()
		poller.wait()
		worker.close()
	}
	stop := chan bool{cap: 1}
	ready := chan bool{cap: 1}
	completed := chan string{cap: 1}
	requests := spawn shared_raw_rpc_loop(worker, stop, ready, completed)
	defer {
		select {
			stop <- true {}
			else {}
		}
		requests.wait()
	}
	eager_ready(ready, completed)!
	for index in 0 .. 4 {
		sibling := corebridge.open(corebridge.Config{
			...config
			task_queue: config.task_queue + '-${index}'
		}, .activity)!
		sibling_completed := chan string{cap: 1}
		sibling_poller := spawn shared_raw_poll(sibling, sibling_completed)
		sibling.shutdown()
		sibling_poller.wait()
		sibling_message := <-sibling_completed
		sibling.close()
		if sibling_message != '' { return error(sibling_message) }
	}
	root.close()
	worker.rpc(.workflow, 'GetSystemInfo', []u8{}, 5000, corebridge.RpcOptions{})!
	worker.update_metadata({
		'x-shared-lease': 'alive'.bytes()
	}, false)!
	stop <- true
	message := <-completed
	if message != '' {
		return error('borrowed connection RPC raced with sibling construction: ' + message)
	}
	worker.shutdown()
	poll_message := <-poll_completed
	if poll_message != '' { return error(poll_message) }
}

fn eager_workflow_calls(observer &sdk.Client, options sdk.WorkerOptions) ![]string {
	shared_raw_ownership(options)!
	responses := chan bool{cap: 32}
	ready := chan bool{cap: 4}
	converter := example_converter.from_env()!
	shared_client := sdk.connect(sdk.ClientOptions{
		address:        options.address
		namespace:      options.namespace
		transport:      options.transport
		data_converter: converter
		interceptors:   [sdk.ClientInterceptor(EagerCapture{
			responses: responses
		})]
	})!
	defer { shared_client.close() }
	rpc_ready := chan bool{cap: 1}
	rpc_completed := chan string{cap: 1}
	rpc_worker := spawn eager_rpc_loop(shared_client, rpc_ready, rpc_completed)
	defer {
		shared_client.close()
		rpc_worker.wait()
	}
	eager_ready(rpc_ready, rpc_completed)!
	control := &sdk.WorkerControl{}
	completed := chan string{cap: 1}
	config := sdk.WorkerOptions{
		...options
		client:               shared_client
		data_converter:       converter
		control:              control
		logger:               EagerLogger{
			ready: ready
		}
		task_queue:           options.task_queue + '-eager'
		workflow_concurrency: 8
		max_cached_workflows: 16
		workflow_pollers:     2
	}
	worker := spawn eager_run(config, completed)
	defer {
		control.shutdown()
		worker.wait()
	}
	eager_ready(ready, completed)!
	mut invalid_runtime := sdk.Runtime{}
	if _ := sdk.run_workflows(sdk.WorkerOptions{ ...config, namespace: options.namespace + '-other' },
		invalid_runtime)
	{
		return error('worker accepted a namespace different from its shared client')
	}
	eager_start := sdk.WorkflowStart{
		workflow_id:             options.task_queue + '-invalid-eager'
		workflow_type:           'v.counter'
		task_queue:              config.task_queue
		request_eager_execution: true
	}
	if _ := shared_client.signal_with_start(eager_start, 'add', '{"amount":1}') {
		return error('signal-with-start accepted unsupported eager execution')
	}
	if _ := shared_client.update_with_start(eager_start, sdk.UpdateRequest{
		name:      'add'
		arguments: '{"amount":1}'
	})
	{
		return error('Update-with-start accepted unsupported eager execution')
	}
	first := shared_client.start(sdk.WorkflowStart{
		workflow_id:             options.task_queue + '-eager-main'
		workflow_type:           'v.counter'
		task_queue:              config.task_queue
		arguments:               '{"initial":11}'
		request_eager_execution: true
	})!
	eager_observed(responses, true)!
	if !observer.query(first, 'state', 'null')!.contains('11') {
		return error('eager workflow task was not dispatched')
	}
	// No matching worker: Core must clear the eager flag and leave a normal task.
	fallback := shared_client.start(sdk.WorkflowStart{
		workflow_id:             options.task_queue + '-eager-fallback'
		workflow_type:           'v.counter'
		task_queue:              options.task_queue + '-fallback'
		arguments:               '{"initial":22}'
		request_eager_execution: true
	})!
	eager_observed(responses, false)!
	other_control := &sdk.WorkerControl{}
	other_completed := chan string{cap: 1}
	other := spawn eager_run(sdk.WorkerOptions{
		...config
		task_queue: options.task_queue + '-fallback'
		control:    other_control
	}, other_completed)
	defer {
		other_control.shutdown()
		other.wait()
	}
	eager_ready(ready, other_completed)!
	if !observer.query(fallback, 'state', 'null')!.contains('22') {
		return error('normal fallback task did not run')
	}
	second := shared_client.start(sdk.WorkflowStart{
		workflow_id:             options.task_queue + '-eager-other'
		workflow_type:           'v.counter'
		task_queue:              options.task_queue + '-fallback'
		arguments:               '{"initial":33}'
		request_eager_execution: true
	})!
	eager_observed(responses, true)!
	if !observer.query(second, 'state', 'null')!.contains('33') {
		return error('eager dispatch selected the wrong shared worker')
	}
	// Each worker retains a native lease. Closing the public client stops its
	// API calls, while workers keep the shared transport/runtime alive.
	shared_client.close()
	shared_client.close()
	rpc_message := <-rpc_completed
	if rpc_message != '' {
		return error('RPC loop raced unsafely with worker construction/close: ' + rpc_message)
	}
	if _ := shared_client.query(first, 'state', 'null') {
		return error('closed client accepted an RPC')
	}
	if _ := sdk.run_workflows(config, invalid_runtime) {
		return error('closed shared client accepted another worker')
	}
	for handle in [first, fallback, second] {
		observer.signal(handle, 'finish', 'null')!
		if observer.get_result(handle, 10000)!.status != .completed {
			return error('closing client interrupted a shared worker')
		}
	}
	control.shutdown()
	message := <-completed
	if message != '' { return error(message) }
	// Releasing one worker lease must not interrupt the other worker.
	healthy := observer.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-eager-after-close'
		workflow_type: 'v.counter'
		task_queue:    options.task_queue + '-fallback'
		arguments:     '{"initial":44}'
	})!
	observer.signal(healthy, 'finish', 'null')!
	if observer.get_result(healthy, 10000)!.status != .completed {
		return error('first worker shutdown freed the shared transport')
	}
	other_control.shutdown()
	other_message := <-other_completed
	if other_message != '' { return error(other_message) }
	return [
		'Core returned actual eager workflow tasks to the matching shared workers; absent-worker starts fell back to normal polling',
		'Two workers retained the shared client transport after client close; RPC/new-worker use was rejected, and each worker drained independently without freeing the other lease',
		'Client RPCs ran concurrently with shared worker construction and stopped safely when the client closed',
		'Raw RPCs on a borrowed worker connection survived sibling construction and root-client close; transport metadata remained usable',
	]
}
