module main

import temporal_v.sdk
import temporal_v.temporal
import os
import x.json2
import time

struct ActivationBatchCodec {}

fn (codec ActivationBatchCodec) encode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	if values.len == 0 { return []temporal.TemporalApiCommonV1_Payload{} }
	return [
		temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding': 'binary/activation-batch'.bytes()
			}
			data:     temporal.TemporalApiCommonV1_Payloads{
				payloads: values
			}.encode()
		},
	]
}

fn (codec ActivationBatchCodec) decode(values []temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for value in values {
		if value.metadata['encoding'].bytestr() != 'binary/activation-batch' {
			return error('expected batched activation payload')
		}
		result << temporal.TemporalApiCommonV1_Payloads.decode(value.data)!.payloads
	}
	return result
}

fn activation_fixture_converter(converter sdk.DataConverter) sdk.DataConverter {
	if os.getenv('V_ACTIVATION_PACKED') != 'true' { return converter }
	mut codecs := converter.codecs.clone()
	codecs << ActivationBatchCodec{}
	return sdk.DataConverter{
		...converter
		codecs: codecs
	}
}

struct PackingWorkflow {
mut:
	ready int
	count int
}

fn packing_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return PackingWorkflow{}
}

fn (mut workflow PackingWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		assert event.arguments == ['1', '2']
		headers := context.headers()!
		header := headers['packed-header'] or { return error('missing packed header') }
		assert header.logical_data() == '3'
		context.activity('v.panic.activity', '"activity-retry"', sdk.ActivityOptions{
			retry: sdk.RetryPolicy{
				maximum_attempts: 2
				initial_interval: 10 * time.millisecond
			}
		})!
		context.child('v.activation-packed.child', context.info().workflow_id + '-child', sdk.Arguments([
			'"child"',
			'"input"',
		]), sdk.ChildOptions{})!
	}
	if event.kind == .activity_completed {
		assert event.data == '"activity-recovered"'
		workflow.ready++
	}
	if event.kind == .child_completed {
		assert event.data == '9'
		workflow.ready++
	}
	if event.kind == .signal_received {
		assert event.arguments == ['4', '5']
		workflow.count++
		if event.name == 'finish' {
			assert workflow.ready == 2
			context.complete(workflow.count.str())!
		}
	}
	if event.kind == .update_received {
		assert event.arguments == ['8', '9']
		workflow.count++
		context.complete_update(event.id, workflow.count.str())!
	}
}

fn (workflow PackingWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	assert arguments == ['6', '7']
	return sdk.PayloadValue(json2.encode({
		'ready': workflow.ready
		'count': workflow.count
	}))
}

fn (workflow PackingWorkflow) update_names() []string {
	return ['packed']
}

fn (workflow PackingWorkflow) validate_update(name string, arguments []string, view sdk.WorkflowContextView) ! {
	assert arguments == ['8', '9']
}

struct PackingChild {}

fn packing_child_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return PackingChild{}
}

fn (mut workflow PackingChild) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		assert event.arguments == ['"child"', '"input"']
		context.complete('9')!
	}
}

fn activation_packed_client(config sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{
		address:            config.address
		namespace:          config.namespace
		data_converter:     converter
		rpc_timeout_millis: 20000
	})!
	defer { client.close() }
	handle := sdk.WorkflowHandle{
		workflow_id: os.args[3]
	}
	match os.args[2] {
		'start' {
			started := client.start(sdk.WorkflowStart{
				workflow_id:   handle.workflow_id
				workflow_type: 'v.activation-packed'
				task_queue:    config.task_queue
				arguments:     sdk.Arguments([
					'1',
					'2',
				])
				headers:       {
					'packed-header': sdk.PayloadValue('3')
				}
				memo:          {
					'memo':         sdk.PayloadValue('4')
					'_tracer-data': sdk.PayloadValue('5')
				}
			})!
			println(json2.encode(started))
		}
		'query' {
			println(client.query(handle, 'state', sdk.Arguments(['6', '7']), sdk.QueryOptions{})!)
		}
		'signal', 'finish' {
			client.signal(handle, os.args[2], sdk.Arguments(['4', '5']), sdk.MessageOptions{})!
			println('OK')
		}
		'update' {
			update := client.start_update(handle, sdk.UpdateRequest{
				name:                'packed'
				arguments:           sdk.Arguments([
					'8',
					'9',
				])
				id:                  'activation-packed-update'
				wait_for:            .completed
				wait_timeout_millis: 20000
			})!
			println(client.update_result(update, 20000)!.data)
		}
		'result' {
			println(client.get_result(handle, 20000)!.data)
		}
		else {
			return error('unknown packed activation client action')
		}
	}
}
