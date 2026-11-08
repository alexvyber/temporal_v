module main

import alexvyber.temporal_v.sdk
import x.json2
import os
import alexvyber.temporal_v.temporal
import time

fn wait_standalone(client &sdk.Client, handle sdk.StandaloneActivityHandle, paused bool) !sdk.StandaloneActivityDescription {
	watch := time.new_stopwatch()
	for {
		description := client.describe_activity(handle)!
		info := description.raw.info or { return error('standalone Activity has no information') }
		if (paused && info.run_state == .pending_activity_state_paused)
			|| (!paused && info.run_state == .pending_activity_state_started) {
			return description
		}
		if watch.elapsed() > 10 * time.second { return error('standalone Activity state deadline') }
		time.sleep(25 * time.millisecond)
	}
	return error('unreachable standalone state')
}

fn standalone_restart_start(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	handle := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:       options.task_queue + '-restart'
		activity_type:     'v.heartbeat-state'
		task_queue:        options.task_queue
		heartbeat_timeout: time.second
		start_to_close:    10 * time.second
		retry:             sdk.RetryPolicy{
			initial_interval: 100 * time.millisecond
			maximum_attempts: 3
		}
	})!
	watch := time.new_stopwatch()
	for {
		description := client.describe_activity(handle)!
		if description.heartbeat_details == ['{"cursor":7}', '"opaque 世界"'] {
			break
		}
		if watch.elapsed() > 10 * time.second {
			return error('standalone heartbeat was not recorded')
		}
		time.sleep(25 * time.millisecond)
	}
	return ['Standalone Activity checkpoint reached Temporal before worker loss']
}

fn standalone_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	recovered := client.activity_result(sdk.StandaloneActivityHandle{
		activity_id: options.task_queue + '-restart'
	}, 15 * time.second)!
	if recovered.status != .completed || recovered.values.len != 1
		|| json2.decode[[]string](recovered.values[0])! != ['{"cursor":7}', '"opaque 世界"'] {
		return error('standalone heartbeat retry failed: ${recovered.status}')
	}
	input := sdk.Arguments(['first', 'standalone 世界\x00payload', 'third'])
	handle := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:   options.task_queue + '-success'
		activity_type: 'v.arguments'
		task_queue:    options.task_queue
		arguments:     input
		summary:       if os.getenv('V_SDK_EXAMPLE_CODEC') == 'context' {
			''
		} else {
			'独立 summary'
		}
		details:       'standalone details'
		headers:       {
			'scope': 'standalone header'
		}
		priority:      temporal.TemporalApiCommonV1_Priority{
			priority_key: 2
		}
	})!
	result := client.activity_result(handle, 10 * time.second)!
	if !handle.started || handle.run_id == '' || result.status != .completed
		|| result.values != ['standalone 世界\x00payload'] {
		return error('standalone multi-value success lost arguments')
	}
	description := client.describe_activity(handle)!
	if description.arguments != ['first', 'standalone 世界\x00payload', 'third'] || description.outcome == none || (description.headers['scope'] or {
		return error('missing standalone header')
	}).logical_data() != 'standalone header' || description.details != 'standalone details' {
		return error('standalone description lost inputs or outcome')
	}
	if os.getenv('V_SDK_EXAMPLE_CODEC') != 'context' && description.summary != '独立 summary' {
		return error('standalone summary metadata did not decode')
	}
	information := description.raw.info or { return error('missing standalone information') }
	priority := information.priority or { return error('missing standalone priority') }
	if priority.priority_key != 2 { return error('standalone priority was not retained') }
	failed := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:   options.task_queue + '-failure'
		activity_type: 'v.nested-error'
		task_queue:    options.task_queue
	})!
	failure_result := client.activity_result(failed, 10 * time.second)!
	failure := failure_result.failure or { return error('missing standalone failure') }
	if failure_result.status != .failed { return error('standalone failure changed outcome') }
	assert_failure_chain(failure)!
	for mode in ['pause', 'reset', 'cancel', 'terminate'] {
		managed := client.start_activity(sdk.StandaloneActivityStart{
			activity_id:       options.task_queue + '-' + mode
			activity_type:     'v.delay'
			task_queue:        options.task_queue
			arguments:         '{"name":"managed standalone","delay_ms":1600}'
			heartbeat_timeout: time.second
			retry:             sdk.RetryPolicy{
				initial_interval: 100 * time.millisecond
			}
		})!
		wait_standalone(client, managed, false)!
		if mode == 'pause' {
			if _ := client.start_activity(sdk.StandaloneActivityStart{
				activity_id:   managed.activity_id
				activity_type: 'v.delay'
				task_queue:    options.task_queue
				arguments:     '{"name":"duplicate","delay_ms":1}'
			})
			{
				return error('running Activity conflict unexpectedly started')
			} else {
				if err.code() != 6 { return err }
			}
			existing := client.start_activity(sdk.StandaloneActivityStart{
				activity_id:     managed.activity_id
				activity_type:   'v.delay'
				task_queue:      options.task_queue
				arguments:       '{"name":"unused","delay_ms":1}'
				conflict_policy: .activity_id_conflict_policy_use_existing
			})!
			if existing.started || existing.run_id != managed.run_id {
				return error('use-existing changed standalone identity')
			}

			client.pause_activity(managed, sdk.ActivityManagementOptions{
				reason: 'standalone pause test'
			})!
			wait_standalone(client, managed, true)!
			// Expiring this wait must leave the paused execution available to resume.
			if _ := client.activity_result(managed, 100 * time.millisecond) {
				return error('paused standalone Activity completed unexpectedly')
			} else {
				if err.code() != 4 { return err }
			}
			client.unpause_activity(managed, sdk.ActivityManagementOptions{})!
		} else if mode == 'reset' {
			client.reset_activity(managed, sdk.ActivityManagementOptions{ reset_heartbeat: true })!
		} else if mode == 'cancel' {
			client.cancel_activity(managed, sdk.ActivityManagementOptions{
				reason: 'standalone cancellation'
			})!
		} else {
			client.terminate_activity(managed, sdk.ActivityManagementOptions{
				reason: 'standalone termination'
			})!
		}
		outcome := client.activity_result(managed, 15 * time.second)!
		expected := match mode {
			'cancel' { sdk.StandaloneActivityStatus.canceled }
			'terminate' { sdk.StandaloneActivityStatus.terminated }
			else { sdk.StandaloneActivityStatus.completed }
		}

		if outcome.status != expected {
			return error('unexpected standalone ${mode} outcome: ${outcome.status}')
		}
	}
	timed := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:   options.task_queue + '-timeout'
		activity_type: 'v.delay'
		task_queue:    options.task_queue
		// Leave room for server timer dispatch instead of racing a one-second
		// handler against a sub-second timeout under integration-test load.
		arguments:      '{"name":"timed","delay_ms":5000}'
		start_to_close: 150 * time.millisecond
		retry:          sdk.RetryPolicy{
			maximum_attempts: 1
		}
	})!
	timed_outcome := client.activity_result(timed, 10 * time.second)!
	if timed_outcome.status != .timed_out {
		return error('standalone timeout changed terminal status: ${timed_outcome.status}')
	}
	for mode in ['token', 'id', 'failure', 'cancellation'] {
		deferred := client.start_activity(sdk.StandaloneActivityStart{
			activity_id:   options.task_queue + '-deferred-' + mode
			activity_type: 'v.async'
			task_queue:    options.task_queue
		})!
		file := os.join_path(os.getenv('V_SDK_EXAMPLE_ASYNC_TOKEN_DIR'), deferred.run_id + '.json')
		watch := time.new_stopwatch()
		mut token := sdk.ActivityTaskToken{}
		for {
			if os.exists(file) {
				if value := json2.decode[sdk.ActivityTaskToken](os.read_file(file)!) {
					token = value
					break
				}
			}
			if watch.elapsed() > 10 * time.second {
				return error('standalone deferred token deadline')
			}
			time.sleep(25 * time.millisecond)
		}
		mut identifier := sdk.ActivityIdentifier(deferred)
		if mode == 'token' { identifier = token }
		client.heartbeat_activity(identifier, ['{"cursor":9}'])!
		if client.describe_activity(deferred)!.heartbeat_details != ['{"cursor":9}'] {
			return error('standalone external heartbeat lost scope')
		}
		if mode == 'failure' {
			client.fail_activity(identifier, sdk.ApplicationError{
				message:       'deferred standalone failure'
				non_retryable: true
				details:       ['"detail"']
			}, []string{})!
		} else if mode == 'cancellation' {
			client.cancel_activity(deferred, sdk.ActivityManagementOptions{})!
			client.report_activity_cancelled(identifier, ['"cancel detail"'])!
		} else {
			client.complete_activity(identifier, '"deferred standalone result"')!
		}
		deferred_result := client.activity_result(deferred, 10 * time.second)!
		if mode == 'failure' {
			deferred_failure := deferred_result.failure or {
				return error('missing deferred failure')
			}

			if deferred_result.status != .failed
				|| deferred_failure.message != 'deferred standalone failure'
				|| deferred_failure.details != ['"detail"'] {
				return error('deferred standalone failure lost encoded details')
			}
		} else if mode == 'cancellation' {
			deferred_failure := deferred_result.failure or {
				return error('missing deferred cancellation')
			}

			if deferred_result.status != .canceled
				|| deferred_failure.details != ['"cancel detail"'] {
				return error('deferred standalone cancellation lost details')
			}
		} else if deferred_result.status != .completed
			|| deferred_result.values != ['"deferred standalone result"'] {
			return error('deferred standalone completion lost result')
		}
	}
	return [
		'Standalone Activity retried after SIGKILL and decoded its recorded multi-value heartbeat',
		'Target-scoped standalone inputs, multi-argument boundaries, results, descriptions and nested protected failures round-tripped',
		'Pause/deadline/unpause, reset, cancellation and termination reached their expected terminal outcomes',
		'Deferred standalone completion by token and execution ID, external heartbeat, failure and cancellation preserved actor scope and details',
	]
}
