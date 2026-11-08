module main

import sdk
import temporal
import time
import json2
import os

fn activity_result_scope(options sdk.WorkerOptions) temporal.TemporalApiNexusV1_PropagatedSerializationContext {
	return temporal.TemporalApiNexusV1_PropagatedSerializationContext{
		endpoint:  options.task_queue + '-endpoint'
		service:   'v-service'
		operation: 'activity'
	}
}

fn activity_result_context_start(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	handle := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:                            options.task_queue + '-propagated-retry'
		activity_type:                          'v.heartbeat-state'
		task_queue:                             options.task_queue
		heartbeat_timeout:                      time.second
		start_to_close:                         10 * time.second
		retry:                                  sdk.RetryPolicy{
			initial_interval: 100 * time.millisecond
			maximum_attempts: 3
		}
		propagated_nexus_serialization_context: activity_result_scope(options)
	})!
	watch := time.new_stopwatch()
	for {
		description := client.describe_activity(handle)!
		if description.heartbeat_details == ['{"cursor":7}', '"opaque 世界"'] {
			break
		}
		if watch.elapsed() > 10 * time.second {
			return error('propagated Activity did not record its checkpoint')
		}
		time.sleep(25 * time.millisecond)
	}
	return ['Propagated Activity checkpoint reached Temporal before worker loss']
}

fn propagated_activity_token(handle sdk.StandaloneActivityHandle) !sdk.ActivityTaskToken {
	file := os.join_path(os.getenv('V_SDK_EXAMPLE_ASYNC_TOKEN_DIR'), handle.run_id + '.json')
	watch := time.new_stopwatch()
	for {
		if os.exists(file) {
			if token := json2.decode[sdk.ActivityTaskToken](os.read_file(file)!) { return token }
		}
		if watch.elapsed() > 10 * time.second {
			return error('propagated Activity token was not persisted')
		}
		time.sleep(25 * time.millisecond)
	}
	return error('unreachable token wait')
}

fn activity_result_context_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	scope := activity_result_scope(options)
	recovered := client.activity_result(sdk.StandaloneActivityHandle{
		activity_id: options.task_queue + '-propagated-retry'
	}, 15 * time.second)!
	if recovered.status != .completed
		|| json2.decode[[]string](recovered.values[0])! != ['{"cursor":7}', '"opaque 世界"'] {
		return error('propagated Activity checkpoint did not survive retry')
	}
	handle := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:                            options.task_queue + '-propagated-success'
		activity_type:                          'v.activity-result-context'
		task_queue:                             options.task_queue
		arguments:                              '"Activity input"'
		propagated_nexus_serialization_context: scope
	})!
	result := client.activity_result(handle, 10000 * time.millisecond)!
	if result.status != .completed { return error('propagated Activity did not complete') }
	state := json2.decode[map[string]sdk.SerializationContext](result.values[0])!
	if state['input'].kind != .activity || state['input'].activity_id != handle.activity_id
		|| state['result'].kind != .nexus || state['result'].endpoint != scope.endpoint {
		return error('Activity input/output contexts were not independently bound')
	}
	description := client.describe_activity(handle)!
	if description.arguments != ['"Activity input"'] || description.outcome == none {
		return error('Activity inspection used the wrong input/result converter')
	}
	failed := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:                            options.task_queue + '-propagated-failure'
		activity_type:                          'v.nested-error'
		task_queue:                             options.task_queue
		propagated_nexus_serialization_context: scope
	})!
	failure_result := client.activity_result(failed, 10 * time.second)!
	assert_failure_chain(failure_result.failure or { return error('missing propagated failure') })!
	for mode in ['token', 'id', 'failure', 'cancellation', 'timeout'] {
		deferred := client.start_activity(sdk.StandaloneActivityStart{
			activity_id:                            options.task_queue + '-propagated-' + mode
			activity_type:                          'v.async'
			task_queue:                             options.task_queue
			heartbeat_timeout:                      time.second
			retry:                                  sdk.RetryPolicy{
				maximum_attempts: 1
			}
			propagated_nexus_serialization_context: scope
		})!
		token := propagated_activity_token(deferred)!
		input_scope := token.serialization_context or {
			return error('missing token input context')
		}

		result_scope := token.result_serialization_context or {
			return error('missing token output context')
		}

		if input_scope.kind != .activity || result_scope.kind != .nexus
			|| result_scope.operation != scope.operation {
			return error('deferred token lost its separate input/result scopes')
		}
		mut identifier := sdk.ActivityIdentifier(deferred)
		if mode == 'token' { identifier = token }
		checkpoint := ['{"cursor":9}', '"checkpoint 世界"']
		client.heartbeat_activity(identifier, checkpoint)!
		if client.describe_activity(deferred)!.heartbeat_details != checkpoint {
			return error('external heartbeat used the wrong scope')
		}
		if mode == 'failure' {
			client.fail_activity(identifier, sdk.ApplicationError{
				message:       'propagated deferred failure'
				non_retryable: true
				details:       ['"Nexus detail"']
			}, checkpoint)!
		} else if mode == 'cancellation' {
			client.cancel_activity(deferred, sdk.ActivityManagementOptions{})!
			client.report_activity_cancelled(identifier, ['"Nexus cancel"'])!
		} else if mode != 'timeout' {
			client.complete_activity(identifier, '"Nexus deferred result"')!
		}
		outcome := client.activity_result(deferred, 10 * time.second)!
		if mode in ['failure', 'cancellation', 'timeout'] {
			information := outcome.failure or { return error('missing propagated async failure') }
			if information.attributes_error != '' || information.details_error != '' {
				return error('propagated async failure decoder rejected scope')
			}
			if mode == 'failure'
				&& (outcome.status != .failed || information.details != ['"Nexus detail"']) {
				return error('deferred failure lost details')
			}
			if mode == 'cancellation'
				&& (outcome.status != .canceled || information.details != ['"Nexus cancel"']) {
				return error('deferred cancellation lost details')
			}
			if mode == 'timeout'
				&& (outcome.status != .timed_out || information.details != checkpoint) {
				return error('timeout failure lost checkpoint details')
			}
		} else if outcome.status != .completed || outcome.values != ['"Nexus deferred result"'] {
			return error('deferred result did not decode')
		}
	}
	return [
		'Activity inputs kept Activity scope; outputs, failures and checkpoints used propagated Nexus scope',
		'Checkpoint retry after SIGKILL, described inputs/results, and nested failures decoded correctly',
		'Deferred completion by token and execution ID retained output scope; failure/cancellation/timeout preserved details',
	]
}
