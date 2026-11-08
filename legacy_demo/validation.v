module main

import alexvyber.temporal_v.sdk
import sync
import os

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
	for update in [sdk.BuildIdCompatibilityUpdate(sdk.NewBuildIdSet{}),
		sdk.BuildIdCompatibilityUpdate(sdk.CompatibleBuildId{
			build_id: 'new'
		}),
		sdk.BuildIdCompatibilityUpdate(sdk.PromoteBuildIdSet{
			build_id: 'bad\x00id'
		}),
		sdk.BuildIdCompatibilityUpdate(sdk.PromoteBuildId{}),
		sdk.BuildIdCompatibilityUpdate(sdk.MergeBuildIdSets{
			primary_build_id: 'a'
		})] {
		if _ := client.update_build_id_compatibility('queue', update) {
			return error('invalid compatibility update accepted')
		}
	}
	if _ := client.update_build_id_compatibility('', sdk.NewBuildIdSet{ build_id: 'a' }) {
		return error('empty queue accepted')
	}
	if _ := client.build_id_compatibility('queue', -1) {
		return error('negative set limit accepted')
	}
	if _ := client.build_id_reachability(sdk.BuildIdReachabilityOptions{}) {
		return error('empty reachability accepted')
	}
	if _ := client.build_id_reachability(sdk.BuildIdReachabilityOptions{
		build_ids:   [
			'a',
		]
		task_queues: ['']
	})
	{
		return error('empty reachability queue accepted')
	}
	state.mutex.lock()
	calls := state.calls
	state.mutex.unlock()
	assert calls == 0, 'invalid versioning options reached RPC interception'
	println('VALIDATION_OK')
}
