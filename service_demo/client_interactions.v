module main

import temporal_v.sdk
import temporal_v.temporal
import x.json2

struct InteractionState {
	total i64
}

fn interaction_total(client &sdk.Client, handle sdk.WorkflowHandle) !i64 {
	state := client.query(handle, 'state', 'null',
		reject_condition: .query_reject_condition_not_open
	)!
	return json2.decode[InteractionState](state)!.total
}

fn concurrent_signal_start(client &sdk.Client, options sdk.WorkerOptions) !sdk.SignalWithStartResult {
	return client.signal_with_start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-interaction-race'
		workflow_type: 'v.counter'
		task_queue:    options.task_queue
		arguments:     '{}'
	}, 'add', '{"amount":1}')!
}

fn client_interactions_start(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	start := sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-interaction-main'
		workflow_type: 'v.counter'
		task_queue:    options.task_queue
		arguments:     '{"initial":1}'
		request_id:    options.task_queue + '-signal-request'
	}
	first := client.signal_with_start(start, 'add', '{"amount":2}')!
	if !first.started || first.workflow.run_id == '' {
		return error('signal-with-start did not start an execution')
	}
	duplicate := client.signal_with_start(start, 'add', '{"amount":2}')!
	if duplicate.workflow != first.workflow || interaction_total(client, first.workflow)! != 3 {
		return error('signal-with-start request deduplication changed state')
	}
	second := client.signal_with_start(sdk.WorkflowStart{
		...start
		request_id: ''
		arguments:  '{"initial":1000}'
	}, 'add', '{"amount":4}')!
	if second.started || second.workflow != first.workflow
		|| interaction_total(client, first.workflow)! != 7 {
		return error('signal-with-start did not signal the existing execution')
	}
	mut tasks := []thread !sdk.SignalWithStartResult{}
	for _ in 0 .. 20 {
		tasks << spawn concurrent_signal_start(client, options)
	}
	mut started := 0
	mut run_id := ''
	for task in tasks {
		result := task.wait()!
		if result.started { started++ }
		if run_id != '' && result.workflow.run_id != run_id {
			return error('concurrent signal-with-start created another run')
		}
		run_id = result.workflow.run_id
	}
	if started != 1 || interaction_total(client, sdk.WorkflowHandle{
		workflow_id: options.task_queue + '-interaction-race'
		run_id:      run_id
	})! != 20 {
		return error('concurrent signal-with-start lost a signal or created multiple executions')
	}
	if _ := client.signal_with_start(sdk.WorkflowStart{
		...start
		conflict_policy: .workflow_id_conflict_policy_fail
	}, 'add', '{}')
	{
		return error('invalid signal-with-start fail policy accepted')
	}
	return [
		'Signal-with-start deduplicated a request, signaled an existing execution and atomically started one execution under twenty concurrent callers',
	]
}

fn expect_query_rejected(client &sdk.Client, handle sdk.WorkflowHandle, condition temporal.TemporalApiEnumsV1_QueryRejectCondition, status temporal.TemporalApiEnumsV1_WorkflowExecutionStatus) ! {
	if _ := client.query(handle, 'state', 'null', reject_condition: condition) {
		return error('closed workflow query unexpectedly accepted')
	} else {
		if err is sdk.QueryRejectedError {
			if err.workflow == handle && err.status == status && err.code() == 9 { return }
		}
		return error('query rejection lost its typed workflow/status: ${err}')
	}
}

fn client_interactions_finish(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	for label, expected in {
		'main': i64(7)
		'race': i64(20)
	} {
		handle := sdk.WorkflowHandle{
			workflow_id: options.task_queue + '-interaction-' + label
		}
		if interaction_total(client, handle)! != expected {
			return error('restart lost signal-with-start state')
		}
		client.signal(handle, 'finish', 'null')!
		if client.get_result(handle, 10000)!.status != .completed {
			return error('interaction workflow did not close')
		}
		expect_query_rejected(client, handle, .query_reject_condition_not_open,
			.workflow_execution_status_completed)!
		state := client.query(handle, 'state', 'null',
			reject_condition: .query_reject_condition_not_completed_cleanly
		)!
		if json2.decode[InteractionState](state)!.total != expected {
			return error('completed query lost reconstructed state')
		}
	}
	failed := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-interaction-failed'
		workflow_type: 'v.counter'
		task_queue:    options.task_queue
		arguments:     '{}'
	})!
	client.signal(failed, 'unknown', 'null')!
	if client.get_result(failed, 10000)!.status != .failed {
		return error('failure fixture did not fail')
	}
	expect_query_rejected(client, failed, .query_reject_condition_not_completed_cleanly,
		.workflow_execution_status_failed)!
	if json2.decode[InteractionState](client.query(failed, 'state', 'null')!)!.total != 0 {
		return error('default closed query failed')
	}
	return [
		'Signal-with-start histories survived worker loss; typed query rejection distinguished completed/failed executions while allowed closed queries rebuilt their state',
	]
}
