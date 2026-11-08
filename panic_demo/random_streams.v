module main

import temporal_v.sdk
import os
import x.json2

struct RandomSnapshot {
	alpha         []string
	beta          string
	default_value string
	uuid          string
	boolean       bool
	fraction      string
}

struct RandomActivityInput {
	snapshot RandomSnapshot
	padding  string
}

struct RandomStreamsWorkflow {
mut:
	alpha   ?sdk.WorkflowRandomStream
	beta    ?sdk.WorkflowRandomStream
	pending ?RandomSnapshot
	samples []RandomSnapshot
}

fn random_stream_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RandomStreamsWorkflow{}
}

fn (mut workflow RandomStreamsWorkflow) sample(mut context sdk.WorkflowContext) ! {
	alpha := workflow.alpha or { return error('alpha stream missing') }
	beta := workflow.beta or { return error('beta stream missing') }
	if os.getenv('V_RANDOM_NOISE') == 'true' {
		noise := context.random_stream('plugin/noise 🌍')!
		for _ in 0 .. 11 {
			noise.random_u64()
		}
	}
	another_alpha := context.random_stream('orders/alpha')!
	snapshot := RandomSnapshot{
		alpha:         [alpha.random_u64().str(), another_alpha.random_u64().str()]
		beta:          beta.random_u64().str()
		default_value: context.random_u64()!.str()
		uuid:          alpha.random_uuid()
		boolean:       beta.random_bool()
		fraction:      beta.random_f64().str()
	}
	workflow.pending = snapshot
	context.activity('v.version.echo', json2.encode(RandomActivityInput{
		snapshot: snapshot
		padding:  'x'.repeat(320000)
	}), sdk.ActivityOptions{})!
}

fn (mut workflow RandomStreamsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		workflow.alpha = context.random_stream('orders/alpha')!
		workflow.beta = context.random_stream('orders/beta')!
		workflow.sample(mut context)!
	}
	if event.kind == .activity_completed {
		value := json2.decode[RandomActivityInput](event.data)!
		expected := workflow.pending or { return error('unexpected random Activity result') }
		if json2.encode(value.snapshot) != json2.encode(expected) {
			return error('random Activity snapshot changed')
		}
		workflow.samples << value.snapshot
		workflow.pending = none
	}
	if event.kind == .signal_received {
		if event.name == 'sample' { workflow.sample(mut context)! }
		if event.name == 'finish' {
			context.complete(sdk.PayloadValue(json2.encode(workflow.samples)))!
		}
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (workflow RandomStreamsWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode(workflow.samples))
}

fn reset_random_streams(options sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{ address: options.address, data_converter: converter })!
	defer { client.close() }
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	result := client.reset_workflow(handle,
		workflow_task_finish_event_id: os.args[3].i64()
		reason:                        'named random stream reseed fixture'
		exclude_types:                 [.reset_reapply_exclude_type_signal]
	)!
	println(json2.encode(result))
}
