module main

import temporal_v.sdk
import x.json2
import os
import time
import crypto.sha256

struct RawHeartbeatWorkflow {
mut:
	results map[string]string
	labels  map[u32]string
}

fn raw_heartbeat_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RawHeartbeatWorkflow{}
}

fn (mut workflow RawHeartbeatWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		for name in ['v.heartbeat.named', 'v.heartbeat.dynamic'] {
			sequence := context.activity(name, sdk.Arguments(sdk.TypedArguments{}), sdk.ActivityOptions{
				heartbeat_timeout: 2 * time.second
				start_to_close:    60 * time.second
				retry:             sdk.RetryPolicy{
					initial_interval: time.millisecond
					maximum_attempts: 4
				}
			})!
			workflow.labels[sequence] = name
		}
	}
	if event.kind == .activity_completed {
		workflow.results[workflow.labels[event.sequence]] = event.data
	}
	if event.kind in [.activity_failed, .activity_cancelled] {
		return error('raw heartbeat Activity failed: ' + event.data)
	}
	if event.kind == .signal_received && event.name == 'finish' {
		context.complete(json2.encode(workflow.results))!
	}
}

fn (workflow RawHeartbeatWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.results))
}

fn raw_heartbeat_handler(mut context sdk.ActivityContext) !sdk.PayloadValue {
	info := context.info()
	if info.attempt == 1 {
		if context.raw_heartbeat_details().len != 0 || context.heartbeat_values()!.len != 0
			|| context.heartbeat_details()!.len != 0 {
			return error('new Activity had progress')
		}
		context.heartbeat(raw_activity_arguments()!)!
		raw_heartbeat_checkpoint(context)!
		for {
			context.sleep(10 * time.millisecond)!
		}
	}
	raw := context.raw_heartbeat_details()
	if raw.len != 5 || info.raw_heartbeat_details.len != 5 {
		return error('raw progress count changed')
	}
	mut data := raw[0].data()
	data[0] = 0
	if raw[0].data()[0] != 255 || context.raw_heartbeat_details()[0].data()[0] != 255 {
		return error('raw heartbeat data was borrowed')
	}
	mut rejected := false
	context.heartbeat_values() or {
		if !err.msg().contains('unsupported payload encoding') { return err }
		rejected = true
	}
	if !rejected { return error('opaque progress was unexpectedly converted') }
	mut rejected_strings := false
	context.heartbeat_details() or {
		if !err.msg().contains('unsupported payload encoding') { return err }
		rejected_strings = true
	}
	if !rejected_strings { return error('opaque progress was string-converted') }
	// A conversion failure cannot consume or poison the retained raw checkpoint.
	if context.raw_heartbeat_details()[0].data() != raw[0].data() {
		return error('conversion changed raw progress')
	}
	if info.attempt == 2 && os.getenv('V_HEARTBEAT_FINISH') != 'true' {
		context.heartbeat(sdk.Arguments(sdk.TypedArguments{ values: raw.map(sdk.PayloadValue(it)) }))!
		raw_heartbeat_checkpoint(context)!
		for {
			context.sleep(10 * time.millisecond)!
		}
	}
	return sdk.PayloadValue(json2.encode({
		'digest':   sha256.hexhash(raw[0].data().bytestr())
		'attempt':  info.attempt.str()
		'count':    raw.len.str()
		'encoding': raw[0].encoding()
		'protobuf': raw[4].message_type()
	}))
}

fn raw_named_heartbeat(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	return raw_heartbeat_handler(mut context)!
}

fn raw_heartbeat_checkpoint(context &sdk.ActivityContext) ! {
	directory := os.getenv('V_HEARTBEAT_CHECKPOINTS')
	if directory == '' { return error('checkpoint audit directory required') }
	info := context.info()
	os.write_file(os.join_path(directory, info.activity_type + '-' + info.attempt.str()), 'ready')!
}
