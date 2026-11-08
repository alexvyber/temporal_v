module main

import temporal_v.sdk
import temporal_v.temporal
import time
import x.json2
import os

fn backing_activity(client &sdk.Client, operation sdk.NexusExecutionHandle) !sdk.StandaloneActivityHandle {
	info := client.describe_nexus_operation(operation)!.raw.info or {
		return error('missing Nexus info')
	}

	return sdk.StandaloneActivityHandle{
		activity_id: 'nexus-' + info.request_id
	}
}

fn nexus_activity_begin(client &sdk.Client, options sdk.WorkerOptions, label string, operation string, input string) !sdk.NexusExecutionHandle {
	handle := client.start_nexus_operation(sdk.NexusExecutionStart{
		operation_id: options.task_queue + '-activity-nexus-' + label
		endpoint:     options.task_queue + '-endpoint'
		service:      'v-service'
		operation:    operation
		input:        input
	})!
	started := client.nexus_operation_started(handle, 10 * time.second)!
	if started.operation_token == '' {
		return error('activity-backed operation did not return an async token')
	}
	return handle
}

fn nexus_activity_start(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	heartbeat := nexus_activity_begin(client, options, 'heartbeat', 'activity-heartbeat', '{}')!
	activity := backing_activity(client, heartbeat)!
	watch := time.new_stopwatch()
	for client.describe_activity(activity)!.heartbeat_details.len != 2 {
		if watch.elapsed() > 10 * time.second {
			return error('backing Activity checkpoint did not commit')
		}
		time.sleep(25 * time.millisecond)
	}
	deferred := nexus_activity_begin(client, options, 'deferred', 'activity-deferred', '{}')!
	deferred_activity := backing_activity(client, deferred)!
	description := client.describe_activity(deferred_activity)!
	info := description.raw.info or { return error('missing backing Activity info') }
	directory := os.getenv('V_SDK_EXAMPLE_ASYNC_TOKEN_DIR')
	for !os.exists(os.join_path(directory, info.run_id + '.json')) {
		if watch.elapsed() > 15 * time.second {
			return error('deferred backing Activity token not written')
		}
		time.sleep(25 * time.millisecond)
	}
	return [
		'Activity-backed Nexus operations attached durable callbacks and committed a checkpoint/deferred token before both workers were killed',
	]
}

fn nexus_activity_finish(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	heartbeat := sdk.NexusExecutionHandle{
		operation_id: options.task_queue + '-activity-nexus-heartbeat'
	}
	result := client.nexus_operation_result(heartbeat, 15 * time.second)!
	if result.status != .nexus_operation_execution_status_completed
		|| json2.decode[[]string](result.data)! != ['{"cursor":7}', '"opaque 世界"'] {
		return error('checkpoint retry did not complete its Nexus callback')
	}
	deferred := sdk.NexusExecutionHandle{
		operation_id: options.task_queue + '-activity-nexus-deferred'
	}
	info := client.describe_activity(backing_activity(client, deferred)!)!.raw.info or {
		return error('missing deferred backing Activity info')
	}

	path := os.join_path(os.getenv('V_SDK_EXAMPLE_ASYNC_TOKEN_DIR'), info.run_id + '.json')
	token := json2.decode[sdk.ActivityTaskToken](os.read_file(path)!)!
	client.complete_activity(token, '"deferred Nexus 世界"')!
	os.rm(path)!
	completed := client.nexus_operation_result(deferred, 10 * time.second)!
	if completed.status != .nexus_operation_execution_status_completed
		|| completed.data != '"deferred Nexus 世界"' {
		return error('deferred Nexus callback did not decode')
	}
	for operation in ['activity', 'activity-twice', 'activity-fail'] {
		handle := nexus_activity_begin(client, options, operation, operation,
			'{"name":"activity Nexus 世界","delay_ms":20}')!
		outcome := client.nexus_operation_result(handle, 10 * time.second)!
		if operation == 'activity-fail' {
			failure := outcome.failure or { return error('missing failed backing Activity') }
			if outcome.status != .nexus_operation_execution_status_failed
				|| failure.root_message() != 'root cause' || failure.attributes_error != '' {
				return error('backing Activity failure lost its protected cause')
			}
		} else if outcome.status != .nexus_operation_execution_status_completed
			|| outcome.data != '"activity Nexus 世界"' {
			return error('backing Activity success failed')
		}
	}
	cancel := nexus_activity_begin(client, options, 'cancel', 'activity',
		'{"name":"cancel","delay_ms":5000}')!
	client.cancel_nexus_operation(cancel, reason: 'cancel backing Activity')!
	if client.nexus_operation_result(cancel, 10 * time.second)!.status != .nexus_operation_execution_status_canceled {
		return error('Nexus cancellation did not close its operation')
	}
	backing := client.activity_result(backing_activity(client, cancel)!, 10 * time.second)!
	if backing.status != .canceled {
		return error('Nexus cancellation did not reach the backing Activity')
	}
	return [
		'Activity-backed callbacks survived both worker losses, decoded scoped checkpoint/deferred results and protected nested failures',
		'Nexus cancellation reached the backing Activity; a second Activity or workflow start in one invocation was rejected',
	]
}
