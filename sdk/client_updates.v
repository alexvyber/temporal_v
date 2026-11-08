module sdk

import temporal_v.temporal
import time

pub struct UpdateRequest {
pub:
	first_execution_run_id                 string
	request_id                             string
	completion_callbacks                   []temporal.TemporalApiCommonV1_Callback
	links                                  []temporal.TemporalApiCommonV1_Link
	propagated_nexus_serialization_context ?temporal.TemporalApiNexusV1_PropagatedSerializationContext
	headers                                map[string]PayloadValue
	name                                   string
	arguments                              Arguments = Arguments([]string{})
	id                                     string
	wait_for                               UpdateStage = .accepted
	wait_timeout_millis                    u32
}

pub struct UpdateHandle {
pub:
	link                         ?temporal.TemporalApiCommonV1_Link
	result_serialization_context ?SerializationContext
	workflow                     WorkflowHandle
	id                           string
	outcome                      ?UpdateResult
	stage                        UpdateStage
}

pub struct UpdateResult {
pub:
	value     ?PayloadValue @[json: '-']
	raw_value ?RawValue     @[json: '-']
	failed    bool
	data      string
	message   string
	failure   ?Failure
}

pub fn (client &Client) update_result(handle UpdateHandle, wait_timeout_millis u32) !UpdateResult {
	if client.connection.is_closed() { return error('client is closed') }
	if result := handle.outcome { return result }
	if handle.id == '' || wait_timeout_millis == 0 {
		return error('update ID and positive deadline are required')
	}
	mut converter := client.workflow_converter(handle.workflow.workflow_id)
	if scope := handle.result_serialization_context { converter = converter.with_context(scope) }
	run := execution(handle.workflow)!
	watch := time.new_stopwatch()
	for {
		remaining := i64(wait_timeout_millis) - watch.elapsed().milliseconds()
		if remaining <= 0 { return error_with_code('update result deadline exceeded', 4) }
		request := temporal.TemporalApiWorkflowserviceV1_PollWorkflowExecutionUpdateRequest{
			namespace:   client.options.namespace
			update_ref:  temporal.TemporalApiUpdateV1_UpdateRef{
				workflow_execution: run
				update_id:          handle.id
			}
			identity:    'v-sdk-client'
			wait_policy: temporal.TemporalApiUpdateV1_WaitPolicy{
				lifecycle_stage: .update_workflow_execution_lifecycle_stage_completed
			}
		}
		timeout := u32(if remaining < i64(client.options.rpc_timeout_millis) {
			remaining
		} else {
			i64(client.options.rpc_timeout_millis)
		})
		bytes := client.call_with_timeout(.workflow, 'PollWorkflowExecutionUpdate',
			request.encode(), timeout) or {
			if watch.elapsed() >= time.Duration(wait_timeout_millis) * time.millisecond {
				return error_with_code('update result deadline exceeded', 4)
			}
			if err.code() == 4 {
				continue
			}
			return err
		}
		response :=
			temporal.TemporalApiWorkflowserviceV1_PollWorkflowExecutionUpdateResponse.decode(bytes)!
		converter = converter.with_result_context(response.propagated_nexus_serialization_context)
		if outcome := response.outcome {
			return update_outcome(outcome, converter)!
		}
	}
	return error('update result polling stopped unexpectedly')
}

fn update_outcome(outcome temporal.TemporalApiUpdateV1_Outcome, converter DataConverter) !UpdateResult {
	value := outcome.value or { return error('update outcome has no value') }
	if value is temporal.TemporalApiUpdateV1_Outcome_Failure_Case {
		decoded := failure_from_proto(value.value, converter)
		return UpdateResult{
			failed:  true
			message: decoded.message
			failure: decoded
		}
	}
	if value is temporal.TemporalApiUpdateV1_Outcome_Success_Case {
		if value.value.payloads.len == 0 {
			return UpdateResult{
				data:  'null'
				value: PayloadValue(NullValue{})
			}
		}
		decoded := converter.decode_single_argument(value.value.payloads)!
		return UpdateResult{
			data:      decoded.values[0]
			value:     decoded.typed_values[0]
			raw_value: decoded.raw_values[0]
		}
	}
	return error('unsupported update outcome')
}

pub fn (client &Client) get_chain_result(handle WorkflowHandle, wait_timeout_millis u32) !WorkflowResult {
	mut current := handle
	watch := time.new_stopwatch()
	for _ in 0 .. 1000 {
		remaining := i64(wait_timeout_millis) - watch.elapsed().milliseconds()
		if remaining <= 0 { return error_with_code('workflow chain deadline exceeded', 4) }
		result := client.get_result(current, u32(remaining))!
		if result.next_run_id == '' || (result.status != .continued_as_new
			&& result.status != .failed && result.status != .timed_out) {
			return result
		}
		current = WorkflowHandle{
			workflow_id: handle.workflow_id
			run_id:      result.next_run_id
		}
	}
	return error('workflow chain exceeded 1000 runs')
}
