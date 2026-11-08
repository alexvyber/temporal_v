module main

import sdk
import temporal
import os
import json2

struct DynamicUpdateState {
mut:
	calls []string
}

struct NamedDynamicWorkflow {
mut:
	state DynamicUpdateState
}

struct DynamicOnlyWorkflow {
mut:
	state DynamicUpdateState
}

fn named_dynamic_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return NamedDynamicWorkflow{}
}

fn dynamic_only_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return DynamicOnlyWorkflow{}
}

fn dynamic_update_event(mut state DynamicUpdateState, named bool, event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .update_received {
		state.calls << event.name
		if event.name == 'deferred' { return }
		if event.name == 'handler-error' {
			return sdk.ApplicationError{
				message:  'dynamic handler error'
				category: 'DynamicHandler'
			}
		}
		if named && event.name == 'fixed' {
			if event.typed_arguments.len != 1 || event.typed_arguments[0] is sdk.RawValue {
				return error('named payload conversion was bypassed')
			}
			context.complete_update(event.id, sdk.PayloadValue('"named"'))!
			return
		}
		if event.raw_arguments.len != 1 || event.typed_arguments.len != 1
			|| event.typed_arguments[0] !is sdk.RawValue {
			return error('dynamic arguments were not raw')
		}
		value := event.raw_arguments[0]
		context.complete_update(event.id, sdk.PayloadValue(json2.encode({
			'handler':  'dynamic'
			'name':     event.name
			'encoding': value.encoding()
			'bytes':    value.data().hex()
		})))!
	}
	if event.kind == .signal_received {
		if event.name == 'release' {
			for pending in context.pending_updates() {
				context.complete_update(pending.protocol_instance_id,
					sdk.PayloadValue('"released"'))!
			}
		}
		if event.name == 'finish' { context.complete(sdk.PayloadValue(json2.encode(state.calls)))! }
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (mut workflow NamedDynamicWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	dynamic_update_event(mut workflow.state, true, event, mut context)!
}

fn (mut workflow DynamicOnlyWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	dynamic_update_event(mut workflow.state, false, event, mut context)!
}

fn (workflow NamedDynamicWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	check_dynamic_readonly_view(view, 'query', input.len)!
	return sdk.PayloadValue(json2.encode(workflow.state.calls))
}

fn (workflow DynamicOnlyWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	check_dynamic_readonly_view(view, 'query', input.len)!
	return sdk.PayloadValue(json2.encode(workflow.state.calls))
}

fn (workflow NamedDynamicWorkflow) update_names() []string {
	return ['fixed']
}

fn (workflow NamedDynamicWorkflow) validate_update(name string, input []string, view sdk.WorkflowContextView) ! {
	check_dynamic_readonly_view(view, 'validator', input.len)!
	if name != 'fixed' || input.len != 1 {
		return error('named validator received a dynamic request')
	}
}

fn dynamic_update_validation(name string, arguments []sdk.RawValue) ! {
	if arguments.len != 1 { return error('expected one dynamic argument') }
	if name == 'deferred' && os.getenv('V_DYNAMIC_VALIDATOR_REPLAY_POISON') == 'true' {
		panic('dynamic validator reran during replay')
	}
	if name == 'reject' {
		return sdk.ApplicationError{
			message:       'dynamic validation rejected'
			category:      'DynamicValidation'
			non_retryable: true
		}
	}
	if name == 'validator-panic' { panic('dynamic validator panic') }
	if name == 'opaque' && (arguments[0].encoding() != 'application/x-temporal-v-dynamic'
		|| arguments[0].data() != [u8(255), 0, 128]) {
		return error('opaque dynamic payload changed')
	}
}

fn (workflow NamedDynamicWorkflow) validate_dynamic_update(name string, arguments []sdk.RawValue, view sdk.WorkflowContextView) ! {
	check_dynamic_readonly_view(view, 'validator', arguments.len)!
	if name == 'fixed' { return error('named validator lost priority') }
	dynamic_update_validation(name, arguments)!
}

fn (workflow DynamicOnlyWorkflow) validate_dynamic_update(name string, arguments []sdk.RawValue, view sdk.WorkflowContextView) ! {
	check_dynamic_readonly_view(view, 'validator', arguments.len)!
	dynamic_update_validation(name, arguments)!
}

fn (workflow NamedDynamicWorkflow) interaction_definitions() sdk.WorkflowInteractions {
	return sdk.WorkflowInteractions{
		updates: [
			sdk.WorkflowInteractionDefinition{
				name:        'fixed'
				description: 'named Update'
			},
			sdk.WorkflowInteractionDefinition{
				description: 'dynamic Update'
			},
		]
	}
}

fn dynamic_opaque_client(options sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{ address: options.address, data_converter: converter })!
	defer { client.close() }
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	raw := sdk.raw_value(temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'application/x-temporal-v-dynamic'.bytes()
		}
		data:     [u8(255), 0, 128]
	})!
	reject := os.args[1] == 'dynamic-opaque-fixed'
	update := client.start_update(handle, sdk.UpdateRequest{
		name:      if reject {
			'fixed'
		} else {
			'opaque'
		}
		id:        if reject { 'opaque-fixed' } else { 'opaque' }
		arguments: sdk.TypedArguments{
			values: [sdk.PayloadValue(raw)]
		}
	})!
	result := client.update_result(update, 5000)!
	if reject {
		if !result.failed || !result.message.contains('unsupported payload encoding') {
			return error('named raw input did not use its configured converter: ' + result.message)
		}
		println('NAMED_RAW_REJECTED')
		return
	}
	if result.failed { return error(result.message) }
	println(result.data)
}
