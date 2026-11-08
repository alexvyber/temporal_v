module main

import temporal_v.sdk
import os
import temporal_v.example_interceptors
import temporal_v.example_converter
import temporal_v.example_activities

fn main() {
	original := sdk.options_from_env() or {
		eprintln(err)
		exit(1)
	}
	converter := example_converter.from_env() or {
		eprintln(err.msg())
		exit(1)
	}
	config :=
		example_interceptors.worker(sdk.WorkerOptions{ ...original, data_converter: converter })

	mut activities := sdk.Activities{}
	activities.register('v.activity-result-context', example_activities.result_context) or {
		panic(err)
	}
	if os.getenv('V_SDK_EXAMPLE_DYNAMIC_HANDLERS') == 'true' {
		activities.register_dynamic(example_activities.dynamic) or { panic(err) }
	}

	activities.register('v.heartbeat-state', example_activities.heartbeat_state) or { panic(err) }
	activities.register('v.nested-error', example_activities.nested_error) or { panic(err) }
	activities.register('v.arguments', example_activities.arguments) or { panic(err) }
	activities.register('v.echo', example_activities.echo) or { panic(err) }
	activities.register('v.delay', example_activities.delay) or { panic(err) }
	activities.register('v.async', example_activities.asynchronous) or { panic(err) }
	activities.register('v.attempt', example_activities.attempt) or { panic(err) }
	sdk.run_activities(config, activities) or {
		eprintln(err)
		exit(1)
	}
}
