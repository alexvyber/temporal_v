module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import time

fn lifecycle_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	canceled := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-cancel-details'
		workflow_type: 'v.cancel-details'
		task_queue:    options.task_queue
	})!
	client.cancel(canceled, 'cancel reason 世界')!
	result := client.get_result(canceled, 10000)!
	if result.status != .canceled
		|| result.details != ['"cancel reason 世界"', 'cancel 世界\x00\xff'] {
		return error('workflow cancellation reason/details lost their payload boundaries')
	}
	terminated := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-terminate-details'
		workflow_type: 'v.counter'
		arguments:     '{}'
		task_queue:    options.task_queue
	})!
	if _ := client.terminate(terminated,
		first_execution_run_id: '00000000-0000-4000-8000-000000000000'
	)
	{
		return error('termination ignored the execution-chain identity guard')
	}
	description := client.rpc_describe_workflow_execution(temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionRequest{
		namespace: options.namespace
		execution: temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: terminated.workflow_id
			run_id:      terminated.run_id
		}
	})!
	information := description.workflow_execution_info or {
		return error('missing termination target')
	}

	if information.status != .workflow_execution_status_running {
		return error('rejected termination closed its target')
	}
	// Terminating an in-flight initial workflow task intentionally records a
	// server FORCE_CLOSE_COMMAND task failure. Commit initialization first so
	// this fixture can keep asserting zero SDK workflow-task failures.
	watch := time.new_stopwatch()
	for {
		history := client.history(terminated)!
		if history.events.any(it.event_type == .event_type_workflow_task_completed) {
			break
		}
		if watch.elapsed() > 10 * time.second {
			return error('termination target initialization did not commit')
		}
		time.sleep(10 * time.millisecond)
	}
	details := sdk.Arguments(['{"termination":true}', 'terminated 世界\x00\xff'])
	client.terminate(terminated,
		reason:                 'forced stop'
		details:                details
		first_execution_run_id: terminated.run_id
	)!
	termination := client.get_result(terminated, 10000)!
	if termination.status != .terminated || termination.message != 'forced stop'
		|| termination.details != ['{"termination":true}', 'terminated 世界\x00\xff'] {
		return error('workflow termination lost reason/details')
	}
	timed := client.start(sdk.WorkflowStart{
		workflow_id:         options.task_queue + '-timeout-result'
		workflow_type:       'v.counter'
		arguments:           '{}'
		task_queue:          options.task_queue
		run_timeout_seconds: 1
	})!
	timeout_result := client.get_result(timed, 10000)!
	if timeout_result.status != .timed_out { return error('workflow timeout status changed') }
	history := client.history(timed)!
	last := history.events[history.events.len - 1].attributes or {
		return error('timeout event has no attributes')
	}

	if last is temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionTimedOutEventAttributes_Case {
		if timeout_result.retry_state != last.value.retry_state {
			return error('workflow timeout lost retry state')
		}
	} else {
		return error('workflow timeout returned the wrong close event')
	}
	failed := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-failed-result'
		workflow_type: 'v.failure-chain.local'
		task_queue:    options.task_queue
	})!
	failure_result := client.get_result(failed, 10000)!
	if failure_result.status != .failed || failure_result.failure == none {
		return error('workflow failure outcome was lost')
	}
	failure_history := client.history(failed)!
	last_failure := failure_history.events[failure_history.events.len - 1].attributes or {
		return error('failure event has no attributes')
	}

	if last_failure is temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionFailedEventAttributes_Case {
		if failure_result.retry_state != last_failure.value.retry_state
			|| failure_result.retry_state == .retry_state_unspecified {
			return error('workflow failure lost retry state')
		}
	} else {
		return error('workflow failure returned the wrong close event')
	}
	return [
		'Workflow cancellation exposed its request reason and returned multiple scoped binary detail values',
		'Termination preserved reason/details and rejected a mismatched execution-chain guard without closing its target',
		'Failure and timeout results retained the server retry state',
	]
}
