module main

import sdk
import os
import example_interceptors
import example_converter
import example_activities
import workflows

fn main() {
	original := sdk.options_from_env() or {
		eprintln(err)
		exit(1)
	}
	converter := example_converter.from_env() or {
		eprintln(err.msg())
		exit(1)
	}
	mut config := example_interceptors.worker(sdk.WorkerOptions{
		...original
		data_converter: converter
	})
	if os.getenv('V_SDK_EXAMPLE_AUTOSCALING') == 'true' {
		config = sdk.WorkerOptions{
			...config
			workflow_poller_autoscaling: sdk.PollerAutoscaling{}
			activity_poller_autoscaling: sdk.PollerAutoscaling{}
		}
	}

	match os.getenv('V_SDK_EXAMPLE_NONDETERMINISM_POLICY') {
		'global' {
			config = sdk.WorkerOptions{
				...config
				nondeterminism_as_workflow_fail: true
			}
		}
		'selected' {
			config = sdk.WorkerOptions{
				...config
				nondeterminism_as_workflow_fail_for_types: [
					'v.nondeterminism.selected',
				]
			}
		}
		else {}
	}

	mut runtime := sdk.Runtime{}
	workflows.register(mut runtime) or {
		eprintln(err)
		exit(1)
	}
	runtime.register_local_activity('v.nested-error', example_activities.nested_error) or {
		panic(err)
	}
	runtime.register_local_activity('v.arguments', example_activities.arguments) or { panic(err) }
	runtime.register_local_activity('v.echo', example_activities.echo) or { panic(err) }
	runtime.register_local_activity('v.delay', example_activities.delay) or { panic(err) }
	runtime.register_local_activity('v.attempt', example_activities.attempt) or { panic(err) }
	if os.getenv('V_SDK_EXAMPLE_COMBINED_WORKER') == 'true' {
		mut activities := sdk.Activities{}
		activities.register('v.heartbeat-state', example_activities.heartbeat_state) or {
			panic(err)
		}
		activities.register('v.nested-error', example_activities.nested_error) or { panic(err) }
		activities.register('v.arguments', example_activities.arguments) or { panic(err) }
		activities.register('v.echo', example_activities.echo) or { panic(err) }
		activities.register('v.delay', example_activities.delay) or { panic(err) }
		activities.register('v.attempt', example_activities.attempt) or { panic(err) }
		sdk.run_worker(config, runtime, activities) or {
			eprintln(err)
			exit(1)
		}
		return
	}
	sdk.run_workflows(config, runtime) or {
		eprintln(err)
		exit(1)
	}
}
