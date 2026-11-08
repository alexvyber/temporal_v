module main

import temporal_v.sdk
import x.json2
import crypto.sha256

struct RawStartWorkflow {
	named bool
mut:
	state string
}

fn raw_start_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	check_initializer_raw_inputs(view, inputs, false)!
	return RawStartWorkflow{}
}

fn named_start_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	check_initializer_raw_inputs(view, inputs, true)!
	return RawStartWorkflow{
		named: true
	}
}

fn (mut workflow RawStartWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		mut values := []RawActivityDescriptor{}
		for value in event.typed_arguments {
			if workflow.named && value is sdk.RawValue {
				return error('named start skipped conversion')
			}
			if !workflow.named && value !is sdk.RawValue {
				return error('dynamic start was converted')
			}
		}
		for raw in event.raw_arguments {
			mut data := raw.data()
			if data.len > 0 {
				first := data[0]
				data[0] ^= u8(255)
				if raw.data()[0] != first { return error('workflow start raw bytes borrowed') }
			}
			values << RawActivityDescriptor{
				encoding:     raw.encoding()
				message_type: raw.message_type()
				length:       raw.data().len
				digest:       sha256.hexhash(raw.data().bytestr())
			}
		}
		if workflow.named && event.arguments != ['"named"'] {
			return error('named workflow arguments changed')
		}
		workflow.state = json2.encode(RawActivitySummary{
			kind:          if workflow.named {
				'named'
			} else {
				'dynamic'
			}
			activity_type: context.info().workflow_type
			values:        values
		})
	}
	if event.kind == .signal_received && event.name == 'finish' {
		context.complete(workflow.state)!
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (workflow RawStartWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(workflow.state)
}

fn start_raw_workflows(options sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{ address: options.address, data_converter: converter })!
	defer { client.close() }
	mut handles := map[string]sdk.WorkflowHandle{}
	for label in ['empty', 'mixed', 'named', 'bad'] {
		kind := match label {
			'named' { 'v.raw-workflow.named' }
			'bad' { 'v.raw-workflow.bad' }
			else { 'v.raw-workflow.' + label }
		}

		arguments := if label == 'empty' {
			sdk.Arguments(sdk.TypedArguments{})
		} else if label == 'named' {
			sdk.Arguments('"named"')
		} else {
			raw_activity_arguments()!
		}
		handles[label] = client.start(sdk.WorkflowStart{
			workflow_id:   options.task_queue + '-' + label
			workflow_type: kind
			task_queue:    options.task_queue
			arguments:     arguments
		})!
	}
	println(json2.encode(handles))
}

fn check_initializer_raw_inputs(view sdk.WorkflowContextView, inputs []sdk.PayloadValue, named bool) ! {
	assert view.info().workflow_type.starts_with('v.raw-workflow.')
	assert view.info().run_id != '' && view.now().unix_milli() > 0
	raw := view.input_values()
	assert inputs.len == raw.len
	for index, value in inputs {
		if named {
			assert value !is sdk.RawValue && value.logical_data() == '"named"'
		} else {
			assert value is sdk.RawValue
			assert value.encoding() == raw[index].encoding() && value.data() == raw[index].data()
			mut bytes := value.data()
			if bytes.len > 0 {
				bytes[0] ^= 0xff
				assert value.data() == raw[index].data()
			}
		}
	}
}
