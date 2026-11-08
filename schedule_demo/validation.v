module main

import sdk
import sync
import os
import time
import temporal

@[heap]
struct RpcCount {
	mutex &sync.Mutex = sync.new_mutex()
mut:
	calls int
}

struct CountRpc {
	state &RpcCount
}

fn (interceptor CountRpc) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	mut state := interceptor.state
	state.mutex.lock()
	state.calls++
	state.mutex.unlock()
	return next.execute(request)!
}

fn validate_options() ! {
	state := &RpcCount{}
	client := sdk.connect(sdk.ClientOptions{
		address:      os.getenv('TEMPORAL_ADDRESS')
		interceptors: [
			sdk.ClientInterceptor(CountRpc{
				state: state
			}),
		]
	})!
	defer { client.close() }
	if _ := client.create_schedule('', sdk.ScheduleCreateOptions{}) {
		return error('empty schedule ID accepted')
	}
	if _ := client.describe_schedule('bad\x00id') { return error('NUL schedule ID accepted') }
	if _ := client.update_schedule('', sdk.ScheduleUpdate{}) {
		return error('empty update ID accepted')
	}
	if _ := client.pause_schedule('', '') { return error('empty pause ID accepted') }
	if _ := client.backfill_schedule('unused', []) { return error('empty backfill accepted') }
	if _ := client.backfill_schedule('unused', [
		sdk.ScheduleBackfill{ start: time.unix(10), end: time.unix(9) },
	])
	{
		return error('backwards backfill accepted')
	}
	if _ := client.schedule_matching_times('unused', time.unix(10), time.unix(10)) {
		return error('empty matching interval accepted')
	}
	if _ := client.list_schedules(sdk.ScheduleListOptions{ page_size: 0 }) {
		return error('zero page size accepted')
	}
	if _ := client.encode_schedule(sdk.ScheduleDefinition{
		state: temporal.TemporalApiScheduleV1_ScheduleState{
			remaining_actions: -1
		}
	})
	{
		return error('negative action limit accepted')
	}
	if _ := client.encode_schedule(sdk.ScheduleDefinition{}) {
		return error('missing workflow action accepted')
	}
	state.mutex.lock()
	calls := state.calls
	state.mutex.unlock()
	assert calls == 0, 'invalid schedule options reached RPC interception'
	println('VALIDATION_OK')
}
