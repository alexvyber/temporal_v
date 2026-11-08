module main

import temporal_v.sdk
import temporal_v.temporal
import x.json2
import os
import time
import crypto.sha256
import sync

struct RawActivityDescriptor {
	message_type string
	encoding     string
	length       int
	digest       string
}

struct RawActivitySummary {
	kind          string
	activity_type string
	attempt       u32
	is_local      bool
	is_standalone bool
	values        []RawActivityDescriptor
}

struct RawActivitiesWorkflow {
mut:
	labels        map[u32]string
	records       map[string]string
	expected      int
	cancel_remote u32
	cancel_local  u32
}

fn raw_activities_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RawActivitiesWorkflow{}
}

fn raw_activity_arguments() !sdk.Arguments {
	mut opaque := [u8(255), 0, 128]
	opaque << 'x'.repeat(320000).bytes()
	payloads := [
		temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding': 'application/x-temporal-v-raw-activity'.bytes()
			}
			data:     opaque
		},
		temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding': 'json/plain'.bytes()
			}
			data:     '{"key":"json"}'.bytes()
		},
		temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding': 'binary/plain'.bytes()
			}
			data:     [u8(0), 255]
		},
		temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding': 'binary/null'.bytes()
			}
		},
		temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding':    'binary/protobuf'.bytes()
				'messageType': 'temporal.api.common.v1.WorkflowExecution'.bytes()
			}
			data:     temporal.TemporalApiCommonV1_WorkflowExecution{
				workflow_id: 'raw-message'
			}.encode()
		},
	]
	mut values := []sdk.PayloadValue{}
	for payload in payloads {
		values << sdk.PayloadValue(sdk.raw_value(payload)!)
	}
	return sdk.Arguments(sdk.TypedArguments{
		values: values
	})
}

fn (mut workflow RawActivitiesWorkflow) schedule(label string, name string, input sdk.Arguments, local bool, mut context sdk.WorkflowContext) !u32 {
	retry := sdk.RetryPolicy{
		initial_interval: time.millisecond
		maximum_attempts: 2
	}
	sequence := if local {
		context.local_activity(name, input, sdk.LocalActivityOptions{
			activity_id:                 label
			retry:                       retry
			include_arguments_in_marker: true
			start_to_close:              30 * time.second
		})!
	} else {
		context.activity(name, input, sdk.ActivityOptions{
			activity_id:       label
			retry:             retry
			cancellation:      .wait_completed
			heartbeat_timeout: time.second
			start_to_close:    30 * time.second
		})!
	}
	workflow.labels[sequence] = label
	return sequence
}

fn (mut workflow RawActivitiesWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		input := json2.decode[map[string]string](event.arguments[0])!
		values := raw_activity_arguments()!
		if input['mode'] == 'cancel' {
			workflow.expected = 2
			workflow.cancel_remote = workflow.schedule('cancel-r', 'v.raw.wait', values, false, mut
				context)!
			workflow.cancel_local = workflow.schedule('cancel-l', 'v.raw.wait', values, true, mut
				context)!
		} else {
			workflow.expected = 12
			for local in [false, true] {
				suffix := if local { '-l' } else { '-r' }
				workflow.schedule('named' + suffix, 'v.raw.named', sdk.Arguments('"named"'), local, mut
					context)!
				workflow.schedule('empty' + suffix, 'v.raw.empty',
					sdk.Arguments(sdk.TypedArguments{}), local, mut context)!
				workflow.schedule('mixed' + suffix, 'v.raw.mixed', values, local, mut context)!
				workflow.schedule('panic' + suffix, 'v.raw.panic', values, local, mut context)!
				workflow.schedule('failure' + suffix, 'v.raw.fail', values, local, mut context)!
				workflow.schedule('named-opaque' + suffix, 'v.raw.named', values, local, mut
					context)!
			}
		}
	}
	if event.kind in [.activity_completed, .activity_failed, .activity_cancelled] {
		label := workflow.labels[event.sequence] or { return error('unknown raw Activity result') }
		if label in workflow.records { return error('duplicate raw Activity completion') }
		if event.kind == .activity_completed {
			workflow.records[label] = event.data
		} else if event.kind == .activity_failed {
			failure := event.failure or { return error('raw Activity failed without details') }
			workflow.records[label] = 'failed:' + failure.root_message()
		} else {
			workflow.records[label] = 'cancelled'
		}
	}
	if event.kind == .signal_received {
		if event.name == 'cancel' {
			context.cancel_activity(workflow.cancel_remote)!
			context.cancel_activity(workflow.cancel_local)!
		}
		if event.name == 'finish' {
			if workflow.records.len != workflow.expected {
				return error('raw Activities unfinished')
			}
			context.complete(sdk.PayloadValue(json2.encode(workflow.records)))!
		}
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (workflow RawActivitiesWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.records))
}

fn raw_activity_audit(context &sdk.ActivityContext) ! {
	directory := os.getenv('V_RAW_ACTIVITY_LOG')
	if directory == '' { return }
	info := context.info()
	name := info.workflow_id + '-' + info.activity_id + '-' + info.attempt.str() + '-' +
		time.sys_mono_now().str() + '-' + sync.thread_id().str() + '.json'
	os.write_file(os.join_path(directory, name), json2.encode({
		'type':    info.activity_type
		'local':   info.is_local.str()
		'attempt': info.attempt.str()
	}))!
}

fn raw_dynamic_activity(mut context sdk.ActivityContext, arguments []sdk.RawValue) !sdk.PayloadValue {
	if context.info().activity_type == 'v.raw-headers.dynamic' {
		return raw_header_handler(mut context, []string{})!
	}
	raw_activity_audit(context)!
	if context.info().activity_type == 'v.heartbeat.dynamic' {
		return raw_heartbeat_handler(mut context)!
	}
	info := context.info()
	if info.activity_type == 'v.raw.named' { return error('named Activity lost precedence') }
	typed := context.typed_input_values()!
	if typed.len != arguments.len { return error('dynamic typed/raw counts differ') }
	for value in typed {
		if value !is sdk.RawValue { return error('dynamic Activity input was payload-converted') }
	}
	mut values := []RawActivityDescriptor{}
	for value in arguments {
		mut data := value.data()
		if data.len > 0 {
			first := data[0]
			data[0] = first ^ u8(255)
			if value.data()[0] != first { return error('dynamic Activity raw value is borrowed') }
		}
		values << RawActivityDescriptor{
			message_type: value.message_type()
			encoding:     value.encoding()
			length:       value.data().len
			digest:       sha256.hexhash(value.data().bytestr())
		}
	}
	if info.activity_type == 'v.raw.wait' {
		for {
			context.sleep(5 * time.millisecond)!
		}
	}
	if info.activity_type == 'v.raw.panic' && info.attempt == 1 {
		panic('raw dynamic Activity panic')
	}
	if info.activity_type == 'v.raw.fail' {
		return sdk.ApplicationError{
			message:       'raw dynamic failure'
			category:      'RawDynamicFailure'
			non_retryable: true
		}
	}
	return sdk.PayloadValue(json2.encode(RawActivitySummary{
		kind:          'dynamic'
		activity_type: info.activity_type
		attempt:       info.attempt
		is_local:      info.is_local
		is_standalone: info.is_standalone
		values:        values
	}))
}

fn raw_named_activity(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	raw_activity_audit(context)!
	typed := context.typed_input_values()!
	for value in typed {
		if value is sdk.RawValue { return error('named Activity bypassed its converter') }
	}
	info := context.info()
	return sdk.PayloadValue(json2.encode(RawActivitySummary{
		kind:          'named'
		activity_type: info.activity_type
		attempt:       info.attempt
		is_local:      info.is_local
	}))
}

fn raw_standalone_activity(options sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{ address: options.address, data_converter: converter })!
	defer { client.close() }
	handle := client.start_activity(sdk.StandaloneActivityStart{
		activity_id:   options.task_queue + '-raw-standalone'
		activity_type: 'v.raw.standalone'
		task_queue:    options.task_queue
		arguments:     raw_activity_arguments()!
		retry:         sdk.RetryPolicy{
			maximum_attempts: 1
		}
	})!
	result := client.activity_result(handle, 20 * time.second)!
	if result.status != .completed || result.values.len != 1 {
		return error('standalone raw Activity did not complete')
	}
	value := json2.decode[RawActivitySummary](result.values[0])!
	if !value.is_standalone || value.is_local || value.kind != 'dynamic' || value.values.len != 5 {
		return error('standalone raw Activity lost its type/arguments')
	}
	println(result.values[0])
}
