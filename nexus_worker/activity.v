module main

import sdk
import time

fn activity_start(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	config := sdk.options_from_env()!
	client := context.client()!
	defer { client.close() }
	operation := context.info().operation
	activity_type := match operation {
		'activity-heartbeat' { 'v.heartbeat-state' }
		'activity-deferred' { 'v.async' }
		'activity-fail' { 'v.nested-error' }
		else { 'v.delay' }
	}

	options := sdk.StandaloneActivityStart{
		activity_type:     activity_type
		task_queue:        config.task_queue
		arguments:         input
		heartbeat_timeout: if operation == 'activity-heartbeat' {
			time.second
		} else {
			time.Duration(0)
		}
		retry:             sdk.RetryPolicy{
			maximum_attempts: 3
			initial_interval: 100 * time.millisecond
		}
	}
	result := context.start_activity(client, options)!
	if operation == 'activity-twice' {
		if _ := context.start_activity(client, options) {
			return error('second async operation accepted')
		}
		if _ := context.start_workflow(client, sdk.WorkflowStart{
			workflow_type: 'v.nexus-target'
			task_queue:    config.task_queue
			arguments:     input
		})
		{
			return error('mixed second async operation accepted')
		}
	}
	return result
}

fn activity_cancel(mut context sdk.NexusContext, token string) ! {
	client := context.client()!
	defer { client.close() }
	context.cancel_activity(client, token)!
}
