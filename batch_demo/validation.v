module main

import sdk
import sync
import os
import time
import math
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
	operation := client.batch_cancellation()
	targets := [
		temporal.TemporalApiCommonV1_Execution{
			type_:       .execution_type_workflow
			business_id: 'unused'
		},
	]
	for options in [sdk.BatchStart{}, sdk.BatchStart{ reason: 'test', operation: operation },
		sdk.BatchStart{ reason: 'test', targets: targets }, sdk.BatchStart{
			reason:           'test'
			targets:          targets
			visibility_query: 'WorkflowType="unused"'
			operation:        operation
		}, sdk.BatchStart{
			reason:    'test'
			targets:   [
				temporal.TemporalApiCommonV1_Execution{
					business_id: 'unused'
				},
			]
			operation: operation
		}, sdk.BatchStart{
			reason:    'test'
			targets:   targets
			operation: operation
			job_id:    'bad\x00id'
		}] {
		if _ := client.start_batch(options) { return error('invalid batch start accepted') }
	}
	for rate in [f32(-1), f32(math.nan()), f32(math.inf(1))] {
		if _ := client.start_batch(sdk.BatchStart{
			reason:                    'test'
			targets:                   targets
			operation:                 operation
			max_operations_per_second: rate
		})
		{
			return error('invalid batch rate accepted')
		}
	}
	if _ := client.stop_batch(sdk.BatchHandle{ job_id: 'unused' }, '') {
		return error('empty stop reason accepted')
	}
	if _ := client.describe_batch(sdk.BatchHandle{}) { return error('empty batch handle accepted') }
	if _ := client.list_batches(sdk.BatchListOptions{ page_size: 0 }) {
		return error('invalid batch page accepted')
	}
	if _ := client.wait_batch(sdk.BatchHandle{ job_id: 'unused' }, time.Duration(0)) {
		return error('invalid batch wait accepted')
	}
	if _ := client.batch_signal('', 'not JSON', {}) { return error('empty signal name accepted') }
	state.mutex.lock()
	calls := state.calls
	state.mutex.unlock()
	assert calls == 0, 'invalid batch options reached RPC interception'
	println('VALIDATION_OK')
}
