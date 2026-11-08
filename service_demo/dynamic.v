module main

import alexvyber.temporal_v.sdk

fn dynamic_validation_activity(mut context sdk.ActivityContext, arguments []sdk.RawValue) !sdk.PayloadValue {
	return 'null'
}

fn dynamic_validation() ![]string {
	mut runtime := sdk.Runtime{}
	runtime.register_dynamic(new_versioning_validation, versioning_behavior: .pinned)!
	mut rejected := false
	runtime.register_dynamic(new_versioning_validation) or { rejected = true }
	if !rejected { return error('duplicate dynamic workflow replaced its registration') }
	if _ := sdk.run_workflows(sdk.WorkerOptions{ address: 'http://127.0.0.1:1' }, runtime) {
		return error('versioned dynamic workflow opened an unversioned worker')
	} else {
		if !err.msg().contains('worker deployment versioning is disabled') {
			return error('dynamic versioning validation reached connection setup: ${err.msg()}')
		}
	}
	mut activities := sdk.Activities{}
	activities.register_dynamic(dynamic_validation_activity)!
	rejected = false
	activities.register_dynamic(dynamic_validation_activity) or { rejected = true }
	if !rejected { return error('duplicate dynamic Activity replaced its registration') }
	runtime.register_dynamic_local_activity(dynamic_validation_activity)!
	rejected = false
	runtime.register_dynamic_local_activity(dynamic_validation_activity) or { rejected = true }
	if !rejected { return error('duplicate dynamic local Activity replaced its registration') }
	return [
		'Duplicate dynamic registrations are rejected; dynamic workflow versioning is validated before native setup',
	]
}
