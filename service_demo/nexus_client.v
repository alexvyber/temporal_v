module main

import sdk
import temporal
import time
import json2

fn nexus_client_start(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	handle := client.start_nexus_operation(sdk.NexusExecutionStart{
		operation_id: options.task_queue + '-durable'
		endpoint:     options.task_queue + '-endpoint'
		service:      'v-service'
		operation:    'async'
		input:        '{"name":"standalone durable","delay_ms":4000}'
	})!
	started := client.nexus_operation_started(handle, 10 * time.second)!
	if !handle.started || started.run_id != handle.run_id || started.operation_token == '' {
		return error('standalone async Nexus start lost identity')
	}
	if _ := client.start_nexus_operation(sdk.NexusExecutionStart{
		operation_id: handle.operation_id
		endpoint:     options.task_queue + '-endpoint'
		service:      'v-service'
		operation:    'echo'
	})
	{
		return error('running Nexus conflict unexpectedly started')
	} else {
		if err.code() != 6 { return err }
	}
	reused := client.start_nexus_operation(sdk.NexusExecutionStart{
		operation_id:    handle.operation_id
		endpoint:        options.task_queue + '-endpoint'
		service:         'v-service'
		operation:       'echo'
		input:           '{"name":"unused"}'
		conflict_policy: .nexus_operation_id_conflict_policy_use_existing
	})!
	if reused.started || reused.run_id != handle.run_id {
		return error('Nexus use-existing changed execution identity')
	}
	description := client.describe_nexus_operation(handle)!
	info := description.raw.info or { return error('missing Nexus operation information') }
	if description.input != '{"name":"standalone durable","delay_ms":4000}' {
		return error('standalone Nexus description lost input')
	}
	// This fixture uses the SDK's default workflow operation ID.
	workflow := sdk.WorkflowHandle{
		workflow_id: 'nexus-' + info.request_id
	}
	watch := time.new_stopwatch()
	for {
		history := client.history(workflow)!
		mut durable := false
		for event in history.events {
			if event.event_type == .event_type_timer_started { durable = true }
		}
		if durable {
			break
		}
		if watch.elapsed() > 10 * time.second {
			return error('async Nexus workflow did not commit its timer')
		}
		time.sleep(25 * time.millisecond)
	}
	return [
		'Standalone Nexus start retained an asynchronous token and committed its backing workflow timer before worker loss',
	]
}

fn nexus_client_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	durable := client.nexus_operation_result(sdk.NexusExecutionHandle{
		operation_id: options.task_queue + '-durable'
	}, 15 * time.second)!
	if durable.status != .nexus_operation_execution_status_completed
		|| json2.decode[string](durable.data)! != 'Hello from async Nexus, standalone durable!' {
		return error('standalone Nexus callback did not survive worker loss')
	}
	endpoint := options.task_queue + '-endpoint'
	for operation in ['echo', 'fail'] {
		handle := client.start_nexus_operation(sdk.NexusExecutionStart{
			operation_id: options.task_queue + '-' + operation
			endpoint:     endpoint
			service:      'v-service'
			operation:    operation
			input:        '{"name":"standalone client"}'
			headers:      {
				'trace-id': 'standalone-test'
			}
		})!
		result := client.nexus_operation_result(handle, 10 * time.second)!
		if operation == 'echo' {
			if result.status != .nexus_operation_execution_status_completed
				|| json2.decode[string](result.data)! != 'Hello from Nexus, standalone client!' {
				return error('standalone sync Nexus result did not decode')
			}
		} else {
			failure := result.failure or { return error('missing standalone Nexus failure') }
			if result.status != .nexus_operation_execution_status_failed
				|| failure.root_message() != 'requested Nexus failure' {
				return error('standalone Nexus failure lost protected message')
			}
		}
		description := client.describe_nexus_operation(handle)!
		if description.input != '{"name":"standalone client"}' || description.outcome == none {
			return error('Nexus inspection lost outcome')
		}
	}
	for mode in ['cancel', 'terminate', 'timeout'] {
		handle := client.start_nexus_operation(sdk.NexusExecutionStart{
			operation_id:      options.task_queue + '-' + mode
			endpoint:          endpoint
			service:           'v-service'
			operation:         'async'
			input:             '{"name":"managed Nexus","delay_ms":3000}'
			schedule_to_close: if mode == 'timeout' {
				500 * time.millisecond
			} else {
				10 * time.second
			}
		})!
		if mode != 'timeout' {
			client.nexus_operation_started(handle, 10 * time.second)!
			if _ := client.nexus_operation_result(handle, 50 * time.millisecond) {
				return error('Nexus wait did not expire')
			} else {
				if err.code() != 4 { return err }
			}
		}
		if mode == 'cancel' {
			client.cancel_nexus_operation(handle, reason: 'standalone test cancellation')!
		} else if mode == 'terminate' {
			client.terminate_nexus_operation(handle, reason: 'standalone test termination')!
		}
		result := client.nexus_operation_result(handle, 10 * time.second)!
		expected := match mode {
			'cancel' {
				temporal.TemporalApiEnumsV1_NexusOperationExecutionStatus.nexus_operation_execution_status_canceled
			}
			'terminate' {
				.nexus_operation_execution_status_terminated
			}
			else {
				.nexus_operation_execution_status_timed_out
			}
		}

		if result.status != expected {
			return error('standalone Nexus ${mode} changed terminal status: ${result.status}')
		}
	}
	return [
		'Standalone sync and async Nexus results, stored-target input inspection and protected failures decoded correctly',
		'Async callback completed after both Nexus and workflow worker SIGKILL/restart',
		'Wait deadlines left operations running; explicit cancellation, termination and execution timeout retained their exact statuses',
	]
}
