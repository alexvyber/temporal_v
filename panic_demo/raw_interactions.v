module main

import sdk
import temporal
import os
import json2
import crypto.sha256

struct RawInteractionState {
mut:
	calls []string
}

struct RawInteractionsWorkflow {
mut:
	state RawInteractionState
}

struct NamedRawInteractionsWorkflow {
mut:
	state RawInteractionState
}

fn raw_interactions_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RawInteractionsWorkflow{}
}

fn named_raw_interactions_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return NamedRawInteractionsWorkflow{}
}

fn raw_interaction_query(state RawInteractionState, name string, arguments []sdk.RawValue) !sdk.PayloadValue {
	if name == 'state' { return sdk.PayloadValue(json2.encode(state.calls)) }
	if name == 'query-panic' { panic('dynamic query panic') }
	if arguments.len != 1 { return error('dynamic query requires one raw argument') }
	value := arguments[0]
	mut bytes := value.data()
	if bytes.len == 0 { return error('dynamic raw argument is empty') }
	original := bytes[0]
	bytes[0] = original ^ u8(255)
	if value.data()[0] != original { return error('raw data view retained mutable ownership') }
	mut payload := value.to_payload()!
	payload.metadata['encoding'] = 'mutated'.bytes()
	if value.encoding() == 'mutated' { return error('raw payload view retained mutable ownership') }
	return sdk.PayloadValue(json2.encode({
		'name':     name
		'encoding': value.encoding()
		'prefix':   value.data()[..3].hex()
		'length':   value.data().len.str()
		'sha256':   sha256.hexhash(value.data().bytestr())
	}))
}

fn raw_interaction_signal(mut state RawInteractionState, event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.name == 'finish' {
		context.complete(sdk.PayloadValue(json2.encode(state.calls)))!
		return
	}
	if event.name == 'signal-panic' && os.getenv('V_RAW_INTERACTIONS_FIXED') != 'true' {
		panic('dynamic signal panic')
	}
	if event.raw_arguments.len != 1 || event.typed_arguments.len != 1
		|| event.typed_arguments[0] !is sdk.RawValue {
		return error('dynamic signal did not receive raw arguments')
	}
	value := event.raw_arguments[0]
	state.calls << event.name + ':' + value.encoding() + ':' + value.data()[..3].hex()
}

fn (mut workflow RawInteractionsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .signal_received {
		return error('dynamic-only signal used named event callback')
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (mut workflow RawInteractionsWorkflow) signal_dynamic(event sdk.Event, mut context sdk.WorkflowContext) ! {
	raw_interaction_signal(mut workflow.state, event, mut context)!
}

fn (workflow RawInteractionsWorkflow) query_dynamic(name string, arguments []sdk.RawValue, view sdk.WorkflowContextView) !sdk.PayloadValue {
	check_dynamic_readonly_view(view, 'query', arguments.len)!
	return raw_interaction_query(workflow.state, name, arguments)!
}

fn (mut workflow NamedRawInteractionsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .signal_received {
		if event.name == 'finish' {
			context.complete(sdk.PayloadValue(json2.encode(workflow.state.calls)))!
			return
		}
		if event.name != 'fixed' || event.typed_arguments.len != 1
			|| event.typed_arguments[0] is sdk.RawValue {
			return error('named signal lost precedence or converter')
		}
		workflow.state.calls << 'named:' + event.arguments[0]
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (mut workflow NamedRawInteractionsWorkflow) signal_dynamic(event sdk.Event, mut context sdk.WorkflowContext) ! {
	raw_interaction_signal(mut workflow.state, event, mut context)!
}

fn (workflow NamedRawInteractionsWorkflow) query_dynamic(name string, arguments []sdk.RawValue, view sdk.WorkflowContextView) !sdk.PayloadValue {
	check_dynamic_readonly_view(view, 'query', arguments.len)!
	if name in ['fixed', 'state'] { return error('named query lost precedence') }
	return raw_interaction_query(workflow.state, name, arguments)!
}

fn (workflow NamedRawInteractionsWorkflow) query_names() []string {
	return ['fixed', 'state']
}

fn (workflow NamedRawInteractionsWorkflow) signal_names() []string {
	return ['finish', 'fixed']
}

fn (workflow NamedRawInteractionsWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	check_dynamic_readonly_view(view, 'query', input.len)!
	if name == 'state' { return sdk.PayloadValue(json2.encode(workflow.state.calls)) }
	return sdk.PayloadValue('"named-query"')
}

fn raw_interaction_client(options sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{ address: options.address, data_converter: converter })!
	defer { client.close() }
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	mut bytes := [u8(255), 0, 128]
	bytes << 'x'.repeat(320000).bytes()
	raw := sdk.raw_value(temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'application/x-temporal-v-dynamic'.bytes()
		}
		data:     bytes
	})!
	arguments := sdk.TypedArguments{
		values: [sdk.PayloadValue(raw)]
	}
	if os.args[3] == 'signal' {
		client.signal(handle, os.args[4], arguments)!
		println('SENT')
		return
	}
	result := client.query(handle, os.args[4], arguments) or {
		if os.args[3] == 'named-reject' && err.msg().contains('unsupported payload encoding') {
			println('NAMED_RAW_REJECTED')
			return
		}
		return err
	}
	if os.args[3] == 'named-reject' { return error('named query accepted an unknown encoding') }
	println(result)
}
