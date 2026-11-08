module main

import temporal_v.sdk
import temporal_v.temporal
import temporal_v.example_converter
import temporal_v.example_storage
import os
import x.json2
import time

struct VisibilityWorkflow {
mut:
	checks int
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return VisibilityWorkflow{}
}

fn (mut workflow VisibilityWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started { validate_memo(context.memo()!)! }
	if event.kind == .signal_received && event.name == 'check' {
		expected := if os.getenv('V_SDK_EXAMPLE_STORAGE_DIR') != '' { 1024 * 1024 + 2 } else { 7 }
		if event.data.len != expected { return error('stored visibility signal changed') }
		workflow.checks++
	}
	if event.kind == .signal_received && event.name == 'finish' {
		if workflow.checks != 1 { return error('visibility signal was not delivered exactly once') }
		context.complete('true')!
	}
}

struct ScopedVisibilityStorage {
	root string
}

fn (driver ScopedVisibilityStorage) name() string {
	return 'visibility.files'
}

fn (driver ScopedVisibilityStorage) driver_type() string {
	return 'visibility.fixture'
}

fn (driver ScopedVisibilityStorage) store(context sdk.StorageContext, values []temporal.TemporalApiCommonV1_Payload) ![]sdk.StorageClaim {
	if context.target.workflow_id == '' {
		return error('visibility storage write has no workflow scope')
	}
	delegate := sdk.StorageDriver(example_storage.FileStorageDriver{
		root: driver.root
	})
	claims := delegate.store(context, values)!
	mut result := []sdk.StorageClaim{}
	for claim in claims {
		mut data := claim.data.clone()
		data['workflow'] = context.target.workflow_id
		data['namespace'] = context.target.namespace
		data['_type'] = 'VisibilityClaim'
		result << sdk.StorageClaim{
			data: data
		}
	}
	return result
}

fn (driver ScopedVisibilityStorage) retrieve(context sdk.StorageContext, claims []sdk.StorageClaim) ![]temporal.TemporalApiCommonV1_Payload {
	for claim in claims {
		if context.target.kind != .workflow || context.target.workflow_id == ''
			|| context.target.workflow_id != claim.data['workflow']
			|| context.target.namespace != claim.data['namespace'] {
			return error('visibility storage read used a different workflow scope')
		}
	}
	mut file := os.open_append(os.join_path(driver.root, 'retrievals.log'))!
	file.writeln(context.target.workflow_id)!
	file.close()
	delegate := sdk.StorageDriver(example_storage.FileStorageDriver{
		root: driver.root
	})
	return delegate.retrieve(context, claims)!
}

fn (workflow &VisibilityWorkflow) query_names() []string {
	return ['state']
}

fn (workflow &VisibilityWorkflow) query(name string, values []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.json_value(workflow.checks)!
}

fn converter() !sdk.DataConverter {
	base := example_converter.from_env()!
	root := os.getenv('V_SDK_EXAMPLE_STORAGE_DIR')
	if root == '' { return base }
	storage := sdk.new_external_storage(
		drivers: [sdk.StorageDriver(ScopedVisibilityStorage{
			root: root
		})]
	)!
	return sdk.DataConverter{
		...base
		external_storage: storage
	}
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.visibility', make_workflow)!
	return runtime
}

fn memo() !map[string]sdk.PayloadValue {
	mut values := {
		'json':   sdk.PayloadValue(sdk.JsonValue{
			data: '{"label":"visibility"}'
		})
		'binary': sdk.PayloadValue(sdk.BinaryValue{
			data: [u8(0), 255, 128]
		})
		'proto':  sdk.PayloadValue(sdk.protobuf_value(temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: 'memo'
		}, 'temporal.api.common.v1.WorkflowExecution')!)
		'null':   sdk.PayloadValue(sdk.NullValue{})
	}
	if os.getenv('V_SDK_EXAMPLE_STORAGE_DIR') != '' {
		values['large'] = sdk.JsonValue{
			data: json2.encode('z'.repeat(1024 * 1024))
		}
	}
	return values
}

fn validate_memo(values map[string]sdk.PayloadValue) ! {
	json_memo := values['json'] or { return error('visibility JSON memo missing') }
	if json_memo.logical_data() != '{"label":"visibility"}' {
		return error('visibility JSON memo changed')
	}
	binary := values['binary'] or { return error('visibility binary memo missing') }
	if binary is sdk.BinaryValue {
		if binary.data != [u8(0), 255, 128] { return error('visibility binary memo changed') }
	} else {
		return error('visibility binary memo type changed')
	}
	proto := values['proto'] or { return error('visibility protobuf memo missing') }
	if proto is sdk.ProtobufValue {
		if temporal.TemporalApiCommonV1_WorkflowExecution.decode(proto.data)!.workflow_id != 'memo' {
			return error('visibility protobuf memo changed')
		}
	} else {
		return error('visibility protobuf memo type changed')
	}
	if os.getenv('V_SDK_EXAMPLE_CODEC') == '' {
		if 'null' in values { return error('server did not filter null memo') }
	} else {
		value := values['null'] or { return error('encoded null memo missing') }
		if value !is sdk.NullValue { return error('encoded null memo changed') }
	}
	if os.getenv('V_SDK_EXAMPLE_STORAGE_DIR') != '' {
		large_memo := values['large'] or { return error('visibility stored memo missing') }
		if large_memo.logical_data().len != 1024 * 1024 + 2 {
			return error('visibility stored memo changed')
		}
	}
}

fn run() ! {
	role := os.args[1]
	if role == 'validation' {
		validate_pagination()!
		return
	}
	conversion := converter()!
	if role == 'worker' {
		sdk.run_workflows(sdk.WorkerOptions{ ...sdk.options_from_env()!, data_converter: conversion },
			definitions()!)!
		return
	}
	if role == 'replay' {
		definitions()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id:    os.args[3]
			task_queue:     os.args[4]
			data_converter: conversion
		})!
		println('OK')
		return
	}
	client := sdk.connect(sdk.ClientOptions{
		address:        os.getenv('TEMPORAL_ADDRESS')
		data_converter: conversion
	})!
	defer { client.close() }
	queue := os.getenv('TEMPORAL_TASK_QUEUE')
	query := "TaskQueue = '${queue}'"
	if role == 'start' {
		mut handles := []sdk.WorkflowHandle{}
		for index in 0 .. 6 {
			handles << client.start(sdk.WorkflowStart{
				workflow_id:   queue + '-' + index.str()
				workflow_type: 'v.visibility'
				task_queue:    queue
				arguments:     'null'
				memo:          memo()!
			})!
		}
		println(json2.encode(handles))
		return
	}
	if role == 'count' {
		println(client.count_workflows(query + if os.args.len > 2 {
			' AND ExecutionStatus = ' + os.args[2]
		} else {
			''
		})!.count)
		return
	}
	if role == 'list' || role == 'raw-list' {
		mut pages := client.workflow_pages(sdk.WorkflowListOptions{
			query:       query
			page_size:   2
			decode_memo: role != 'raw-list'
		})!
		mut identities := []string{}
		mut page_count := 0
		for !pages.exhausted() {
			page := pages.next_page()!
			page_count++
			for entry in page.executions {
				if role == 'list' { validate_memo(entry.memo)! }
				identities << entry.handle.workflow_id
			}
		}
		if pages.next_page()!.executions.len != 0 || page_count < 3 {
			return error('visibility pagination/EOF changed')
		}
		println(json2.encode(identities))
		return
	}
	handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
	if role == 'check' {
		if client.query(handle, 'state', sdk.Arguments(sdk.TypedArguments{}))! != '0' {
			return error('visibility workflow was not initialized')
		}
		input := if os.getenv('V_SDK_EXAMPLE_STORAGE_DIR') != '' {
			'c'.repeat(1024 * 1024)
		} else {
			'check'
		}
		client.signal(handle, 'check', json2.encode(input))!
		watch := time.new_stopwatch()
		query_arguments := sdk.Arguments(sdk.TypedArguments{})
		for client.query(handle, 'state', query_arguments)! != '1' {
			if watch.elapsed() > 10 * time.second {
				return error('stored signal was not delivered')
			}
			time.sleep(10 * time.millisecond)
		}
		println('OK')
		return
	}
	if role == 'finish' {
		client.signal(handle, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
		if client.get_result(handle, 15000)!.status != .completed {
			return error('visibility workflow failed')
		}
		println('OK')
		return
	}
	if role == 'history' {
		os.write_file_array(os.args[3], client.history(handle)!.encode())!
		println('OK')
		return
	}
	return error('unknown visibility role')
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}
