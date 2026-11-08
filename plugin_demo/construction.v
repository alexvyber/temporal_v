module main

import temporal_v.sdk
import os
import x.json2
import time

fn construction_query(client &sdk.Client, handle sdk.WorkflowHandle, marker string) !string {
	return client.query(handle, 'stage', json2.encode(marker))
}

fn construction_race(client &sdk.Client, queue string) ! {
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   queue + '-construction'
		workflow_type: 'plugin.workflow'
		task_queue:    queue
		arguments:     '"construction data"'
	})!
	query := spawn construction_query(client, handle,
		os.getenv('V_SDK_EXAMPLE_CONSTRUCTION_MARKER'))
	watch := time.new_stopwatch()
	for !os.exists(os.getenv('V_SDK_EXAMPLE_CONSTRUCTION_BARRIER')) {
		if watch.elapsed() > 4 * time.second {
			return error('proxy did not observe the query request')
		}
		time.sleep(time.millisecond)
	}
	control := &sdk.WorkerControl{}
	result := chan string{cap: 1}
	runtime := &sdk.Runtime{}
	worker := spawn serve(sdk.WorkerOptions{ client: client, task_queue: queue, control: control },
		runtime, result)
	defer {
		control.shutdown()
		message := <-result
		worker.wait()
		assert message == '', message
	}
	value := query.wait() or { return error('query blocked worker construction: ${err}') }
	assert value in ['0', '1', '2'], value
	wait_stage(client, handle)!
	client.signal(handle, 'finish', 'null')!
	assert client.get_result(handle, 10000)!.status == .completed
	println('CONSTRUCTION_OK')
}
