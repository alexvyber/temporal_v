module main

import temporal_v.sdk
import temporal_v.temporal
import x.json2
import os
import sync
import time

@[heap]
struct Proof {
	mutex &sync.Mutex = sync.new_mutex()
mut:
	inline map[string]bool
}

struct ObserveInline {
	proof &Proof
}

fn (interceptor ObserveInline) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	reply := next.execute(request)!
	if request.method == 'StartActivityExecution' {
		call :=
			temporal.TemporalApiWorkflowserviceV1_StartActivityExecutionRequest.decode(request.body)!
		response :=
			temporal.TemporalApiWorkflowserviceV1_StartActivityExecutionResponse.decode(reply)!
		mut proof := interceptor.proof
		proof.mutex.lock()
		proof.inline[call.activity_id] = response.eager_activity_task != none
		proof.mutex.unlock()
	}
	return reply
}

fn (proof &Proof) expect(identifier string, inline bool) ! {
	proof.mutex.lock()
	defer { proof.mutex.unlock() }
	observed := proof.inline[identifier] or { return error('start response was not observed') }
	if observed != inline { return error('unexpected inline task for ${identifier}: ${observed}') }
}

fn handler(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	value := arguments[0]
	if value == '"hold"' {
		for !os.exists(os.getenv('V_SDK_EXAMPLE_EAGER_GATE')) {
			context.check_cancelled()!
			time.sleep(time.millisecond)
		}
	}
	if value == '"restart"' {
		if context.info().attempt == 1 {
			context.heartbeat(['{"cursor":19}'])!
			for {
				context.check_cancelled()!
				time.sleep(10 * time.millisecond)
			}
		}
		return json2.encode(context.heartbeat_details()!)
	}
	return value
}

fn serve(options sdk.WorkerOptions, result chan string) {
	mut activities := sdk.Activities{}
	activities.register('v.eager-standalone', handler) or {
		result <- err.msg()
		return
	}
	sdk.run_activities(options, activities) or {
		result <- err.msg()
		return
	}
	result <- ''
}

fn start(client &sdk.Client, identifier string, queue string, value string, eager bool) !sdk.StandaloneActivityHandle {
	return client.start_activity(sdk.StandaloneActivityStart{
		activity_id:             identifier
		activity_type:           'v.eager-standalone'
		task_queue:              queue
		arguments:               json2.encode(value)
		request_eager_execution: eager
		start_to_close:          5 * time.second
		retry:                   sdk.RetryPolicy{
			maximum_attempts: 2
			initial_interval: 100 * time.millisecond
		}
	})!
}

fn result(client &sdk.Client, handle sdk.StandaloneActivityHandle, value string) ! {
	response := client.activity_result(handle, 10 * time.second)!
	assert response.status == .completed && response.values == [json2.encode(value)], response.str()
}

fn run() ! {
	role := os.args[1]
	queue := os.getenv('TEMPORAL_TASK_QUEUE')
	inline_expected := os.getenv('V_SDK_EXAMPLE_EAGER_EXPECT_INLINE') != 'false'
	proof := &Proof{}
	client := sdk.connect(sdk.ClientOptions{
		address:      os.getenv('TEMPORAL_ADDRESS')
		transport:    sdk.transport_from_env()!
		interceptors: [sdk.ClientInterceptor(ObserveInline{
			proof: proof
		})]
	})!
	defer { client.close() }
	if role == 'recover' {
		handle := sdk.StandaloneActivityHandle{
			activity_id: queue + '-restart'
		}
		control := &sdk.WorkerControl{}
		done := chan string{cap: 1}
		worker := spawn serve(sdk.WorkerOptions{ client: client, task_queue: queue, control: control }, done)
		defer {
			control.shutdown()
			message := <-done
			worker.wait()
			assert message == '', message
		}
		recovered := client.activity_result(handle, 15 * time.second)!
		assert recovered.status == .completed && recovered.values.len == 1, recovered.str()
		assert json2.decode[[]string](recovered.values[0])! == ['{"cursor":19}'], recovered.str()
		println('RECOVERED')
		return
	}
	pending := start(client, queue + '-no-worker', queue, 'before-worker', true)!
	proof.expect(pending.activity_id, false)!
	control := &sdk.WorkerControl{}
	done := chan string{cap: 1}
	worker := spawn serve(sdk.WorkerOptions{
		client:               client
		task_queue:           queue
		activity_concurrency: 8
		activity_pollers:     1
		control:              control
	}, done)
	mut joined := false
	defer {
		if !joined {
			control.shutdown()
			message := <-done
			worker.wait()
			assert message == '', message
		}
	}
	result(client, pending, 'before-worker')!
	// Core can prefetch poll permits. Reserve until the available capacity is
	// exhausted instead of assuming exactly one permit belongs to the poller.
	mut held := []sdk.StandaloneActivityHandle{}
	first := start(client, queue + '-inline', queue, 'hold', true)!
	proof.expect(first.activity_id, inline_expected)!
	held << first
	mut saturated := false
	for index in 0 .. 16 {
		candidate := start(client, queue + '-saturated-${index}', queue, 'hold', true)!
		held << candidate
		proof.mutex.lock()
		inline := proof.inline[candidate.activity_id]
		proof.mutex.unlock()
		if !inline {
			saturated = true
			break
		}
	}
	assert saturated, 'eager starts exceeded configured worker slot capacity'
	os.write_file(os.getenv('V_SDK_EXAMPLE_EAGER_GATE'), 'release')!
	for handle in held {
		result(client, handle, 'hold')!
	}
	for index in 0 .. 10 {
		if _ := client.rpc_start_activity_execution(temporal.TemporalApiWorkflowserviceV1_StartActivityExecutionRequest{
			namespace:               'default'
			activity_id:             queue + '-invalid-${index}'
			activity_type:           temporal.TemporalApiCommonV1_ActivityType{
				name: 'v.eager-standalone'
			}
			task_queue:              temporal.TemporalApiTaskqueueV1_TaskQueue{
				name: queue
			}
			request_id:              'invalid-${index}'
			request_eager_execution: true
		})
		{
			return error('invalid start accepted')
		} else {
			assert err.code() == 3, err.msg()
		}
	}
	post_error := start(client, queue + '-after-error', queue, 'after-error', true)!
	proof.expect(post_error.activity_id, inline_expected)!
	result(client, post_error, 'after-error')!
	control.shutdown()
	message := <-done
	worker.wait()
	joined = true
	assert message == '', message
	late := start(client, queue + '-after-shutdown', queue, 'late', true)!
	proof.expect(late.activity_id, false)!
	// A replacement worker with queue throttling must execute by normal polling.
	replacement_control := &sdk.WorkerControl{}
	replacement_done := chan string{cap: 1}
	replacement := spawn serve(sdk.WorkerOptions{
		client:                               client
		task_queue:                           queue
		activity_concurrency:                 4
		activity_pollers:                     1
		max_task_queue_activities_per_second: 100
		control:                              replacement_control
	}, replacement_done)
	result(client, late, 'late')!
	throttled := start(client, queue + '-throttled', queue, 'throttled', true)!
	proof.expect(throttled.activity_id, false)!
	result(client, throttled, 'throttled')!
	replacement_control.shutdown()
	replacement_message := <-replacement_done
	replacement.wait()
	assert replacement_message == '', replacement_message
	if role == 'exercise' {
		println('EXERCISE_OK')
		return
	}
	if role != 'restart' { return error('unknown role') }
	// Start and checkpoint an inline task, then the probe kills this process.
	restart_control := &sdk.WorkerControl{}
	restart_done := chan string{cap: 1}
	restart_worker := spawn serve(sdk.WorkerOptions{
		client:               client
		task_queue:           queue
		activity_concurrency: 4
		activity_pollers:     1
		control:              restart_control
	}, restart_done)
	warm := start(client, queue + '-warm', queue, 'warm', false)!
	result(client, warm, 'warm')!
	checkpoint := start(client, queue + '-restart', queue, 'restart', true)!
	proof.expect(checkpoint.activity_id, true)!
	watch := time.new_stopwatch()
	for {
		description := client.describe_activity(checkpoint)!
		if description.heartbeat_details == ['{"cursor":19}'] {
			break
		}
		if watch.elapsed() > 4 * time.second { return error('checkpoint did not persist') }
		time.sleep(10 * time.millisecond)
	}
	println('RESTART_READY')
	_ = <-restart_done
	restart_worker.wait()
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
