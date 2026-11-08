module main

import sdk
import sync
import math
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
	for value in [
		sdk.VersioningOverride(sdk.PinnedVersioningOverride{}),
		sdk.VersioningOverride(sdk.OneTimeVersioningOverride{
			version: sdk.WorkerDeploymentVersion{
				deployment_name: 'bad\x00name'
				build_id:        'build'
			}
		}),
	] {
		if _ := client.set_versioning_override(sdk.WorkflowHandle{ workflow_id: 'unused' }, value) {
			return error('invalid override accepted')
		}
	}
	for percentage in [f32(-1), f32(101), f32(math.nan()), f32(math.inf(1))] {
		if _ := client.set_ramping_deployment(sdk.DeploymentRoutingUpdate{
			deployment_name: 'unused'
			build_id:        'build'
		}, percentage)
		{
			return error('invalid ramp percentage accepted')
		}
	}
	if _ := client.set_current_deployment(sdk.DeploymentRoutingUpdate{}) {
		return error('empty deployment name accepted')
	}
	if _ := client.clear_versioning_override(sdk.WorkflowHandle{}) {
		return error('empty workflow ID accepted')
	}
	if _ := client.start(sdk.WorkflowStart{
		workflow_id:         'unused'
		workflow_type:       'unused'
		task_queue:          'unused'
		arguments:           'not JSON'
		versioning_override: sdk.PinnedVersioningOverride{
			version: sdk.WorkerDeploymentVersion{
				deployment_name: 'valid'
			}
		}
	})
	{
		return error('missing pinned build accepted')
	} else {
		assert err.msg().contains('deployment build ID'), err.msg()
	}
	state.mutex.lock()
	calls := state.calls
	state.mutex.unlock()
	assert calls == 0, 'invalid versioning options reached RPC interception'
	println('VALIDATION_OK')
}
