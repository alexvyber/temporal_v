module main

import sdk
import json2
import os

struct InitializerInput {
	mode  string
	value int
}

struct InitializedWorkflow {
	initial sdk.WorkflowContextView
	seeded  int
	mode    string
mut:
	count int
}

@[noinline]
fn initializer_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	assert inputs.len == 2 && view.input_values().len == 2
	input := json2.decode[InitializerInput](inputs[0].logical_data())!
	assert inputs[1] is sdk.BinaryValue
	mut binary := [inputs[1] as sdk.BinaryValue]
	assert binary[0].data == [u8(0), 255, 42].repeat(100001)
	// Initializer arguments must not alias the later started event.
	mut bytes := unsafe { binary[0].data }
	bytes[0] = 99
	assert binary[0].data[0] == 99
	assert view.input_values()[1].data()[0] == 0
	assert json2.encode(view) == '{}'
	information := view.info()
	assert information.workflow_type == 'v.initializer'
	assert information.workflow_id != '' && information.run_id != '' && information.namespace != ''
	assert information.original_execution_run_id != ''
	assert view.now().unix_milli() == information.time_millis
	assert view.now().unix_milli() > 0
	check_workflow_headers(view.raw_headers()!)!
	check_workflow_headers(view.raw_request_headers()!)!
	assert (view.memo()!['initializer-marker'] or { return error('initializer memo missing') }).logical_data() == '"seeded"'
	assert view.search_attributes()!.len == view.raw_search_attributes()!.len
	assert view.current_details() == ''
	if os.getenv('V_INITIALIZER_FIXED') != 'true' {
		if input.mode == 'error' {
			return sdk.ApplicationError{
				message:  'initializer typed error'
				category: 'InitializerError'
			}
		}
		if input.mode == 'panic' { panic('initializer panic') }
	}
	return InitializedWorkflow{
		initial: view
		seeded:  input.value
		count:   input.value
		mode:    input.mode
	}
}

fn (mut workflow InitializedWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		assert event.typed_arguments.len == 2 && event.typed_arguments[1] is sdk.BinaryValue
		value := event.typed_arguments[1] as sdk.BinaryValue
		assert value.data == [u8(0), 255, 42].repeat(100001)
		assert workflow.initial.info().run_id == context.info().run_id
		assert workflow.initial.now().unix_nano() == context.now().unix_nano()
		assert workflow.initial.is_replaying() == context.is_replaying()
		context.set_current_details('initialized')!
		if workflow.mode == 'immediate' { context.complete(workflow.count.str())! }
	}
	if event.kind == .signal_received {
		if event.name == 'finish' {
			context.complete(workflow.count.str())!
		} else {
			workflow.count++
		}
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (workflow InitializedWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	assert workflow.initial.current_details() == '' && view.current_details() == 'initialized'
	assert workflow.initial.now() <= view.now()
	return sdk.PayloadValue(workflow.count.str())
}

fn initializer_start(config sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{
		address:        config.address
		namespace:      config.namespace
		data_converter: converter
	})!
	defer { client.close() }
	input := InitializerInput{
		mode:  os.args[2]
		value: os.args[3].int()
	}
	handle := client.start(sdk.WorkflowStart{
		workflow_id:         config.task_queue + '-initializer-' + os.args[4]
		workflow_type:       'v.initializer'
		task_queue:          config.task_queue
		arguments:           sdk.TypedArguments{
			values: [
				sdk.PayloadValue(json2.encode(input)),
				sdk.BinaryValue{
					data: [u8(0), 255, 42].repeat(100001)
				},
			]
		}
		headers:             header_payloads()!
		memo:                {
			'initializer-marker': sdk.PayloadValue('"seeded"')
		}
		run_timeout_seconds: 180
	})!
	println(json2.encode(handle))
}
