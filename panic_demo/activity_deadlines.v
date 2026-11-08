module main

import alexvyber.temporal_v.sdk
import x.json2
import time
import crypto.sha256

struct MetadataRequest {
	label string
	retry bool
}

struct MetadataInstant {
	seconds i64
	nanos   int
}

struct ActivityMetadataResult {
	label        string
	activity_id  string
	attempt      u32
	local        bool
	token_length int
	token_digest string
	scheduled    ?MetadataInstant
	started      ?MetadataInstant
	deadline     ?MetadataInstant
	information  string
}

fn metadata_instant(value ?time.Time) ?MetadataInstant {
	if instant := value {
		return MetadataInstant{
			seconds: instant.unix()
			nanos:   instant.nanosecond
		}
	}
	return none
}

struct ActivityMetadataWorkflow {
mut:
	results map[string]string
}

fn activity_metadata_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ActivityMetadataWorkflow{}
}

fn (mut workflow ActivityMetadataWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		for local in [false, true] {
			for label in ['start', 'schedule', 'retry', 'unlimited'] {
				name := label + if local { '-local' } else { '-remote' }
				input := sdk.Arguments(json2.encode(MetadataRequest{
					label: name
					retry: label == 'retry'
				}))
				retry := sdk.RetryPolicy{
					maximum_attempts: 2
					initial_interval: time.millisecond
				}
				start_timeout := (if label == 'schedule' { 90 } else { 30 }) * time.second +
					123456789 * time.nanosecond
				schedule_timeout := if label == 'unlimited' {
					time.Duration(0)
				} else {
					(if label == 'schedule' { 60 } else { 120 }) * time.second +
						987654321 * time.nanosecond
				}
				if local {
					context.local_activity('v.activity-deadline', input, sdk.LocalActivityOptions{
						activity_id:       name
						start_to_close:    start_timeout
						schedule_to_close: schedule_timeout
						retry:             retry
					})!
				} else {
					context.activity('v.activity-deadline', input, sdk.ActivityOptions{
						activity_id:       name
						start_to_close:    start_timeout
						schedule_to_close: schedule_timeout
						retry:             retry
					})!
				}
			}
		}
	}
	if event.kind == .activity_completed {
		value := json2.decode[ActivityMetadataResult](event.data)!
		workflow.results[value.label] = event.data
	}
	if event.kind == .activity_failed { return error('metadata Activity failed: ' + event.message) }
	if event.kind == .signal_received && event.name == 'finish' {
		context.complete(json2.encode(workflow.results))!
	}
}

fn (workflow ActivityMetadataWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.results))
}

fn activity_metadata_handler(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	request := json2.decode[MetadataRequest](arguments[0])!
	info := context.info()
	if info.task_token.len == 0 { return error('Activity task token missing') }
	mut token := context.info().task_token
	token[0] ^= u8(255)
	if context.info().task_token != info.task_token {
		return error('Activity task token was borrowed')
	}
	encoded := json2.encode(info)
	if encoded.contains('"task_token"') {
		return error('Activity task token was serialized into info')
	}
	if !info.is_local {
		client := context.client()!
		defer { client.close() }
		if request.retry && info.attempt > 1
			&& context.heartbeat_details()! != ['"metadata-progress"'] {
			return error('external token heartbeat checkpoint was lost')
		}
		status := client.heartbeat_activity(sdk.ActivityTaskToken{
			value:                        info.task_token
			result_serialization_context: context.result_serialization_context()
		}, sdk.Arguments('"metadata-progress"'))!
		if status.cancel_requested { return error('unexpected metadata Activity cancellation') }
	}
	if request.retry && info.attempt == 1 {
		return sdk.ApplicationError{
			message: 'metadata retry'
		}
	}
	return sdk.PayloadValue(json2.encode(ActivityMetadataResult{
		label:        request.label
		activity_id:  info.activity_id
		attempt:      info.attempt
		local:        info.is_local
		token_length: info.task_token.len
		token_digest: sha256.hexhash(info.task_token.bytestr())
		scheduled:    metadata_instant(info.scheduled_at)
		started:      metadata_instant(info.started_at)
		deadline:     metadata_instant(info.deadline)
		information:  encoded
	}))
}
