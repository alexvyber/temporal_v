module sdk

import alexvyber.temporal_v.temporal
import math
import time

pub struct BatchHandle {
pub:
	job_id string
}

pub struct BatchStart {
pub:
	job_id                    string
	reason                    string
	visibility_query          string
	targets                   []temporal.TemporalApiCommonV1_Execution
	max_operations_per_second f32
	operation                 ?temporal.TemporalApiWorkflowserviceV1_StartBatchOperationRequest_Operation
}

pub fn (client &Client) start_batch(options BatchStart) !BatchHandle {
	validate_version_name(options.reason, 'batch reason', true)!
	if (options.visibility_query == '') == (options.targets.len == 0) {
		return error('exactly one batch query or explicit target list is required')
	}
	if math.is_nan(f64(options.max_operations_per_second))
		|| math.is_inf(f64(options.max_operations_per_second), 0)
		|| options.max_operations_per_second < 0 {
		return error('batch rate must be finite and nonnegative')
	}
	for target in options.targets {
		validate_version_name(target.business_id, 'batch target ID', true)!
		if target.type_ == .execution_type_unspecified {
			return error('batch target execution type is required')
		}
	}
	operation := options.operation or { return error('batch operation is required') }
	job := if options.job_id != '' { options.job_id } else { random_request_id()! }
	validate_version_name(job, 'batch job ID', true)!
	client.rpc_start_batch_operation(temporal.TemporalApiWorkflowserviceV1_StartBatchOperationRequest{
		namespace:                 client.options.namespace
		job_id:                    job
		reason:                    options.reason
		visibility_query:          options.visibility_query
		target_executions:         options.targets
		max_operations_per_second: options.max_operations_per_second
		operation:                 operation
	})!
	return BatchHandle{
		job_id: job
	}
}

pub fn (client &Client) describe_batch(handle BatchHandle) !temporal.TemporalApiWorkflowserviceV1_DescribeBatchOperationResponse {
	validate_version_name(handle.job_id, 'batch job ID', true)!
	return client.rpc_describe_batch_operation(temporal.TemporalApiWorkflowserviceV1_DescribeBatchOperationRequest{
		namespace: client.options.namespace
		job_id:    handle.job_id
	})!
}

pub fn (client &Client) stop_batch(handle BatchHandle, reason string) ! {
	validate_version_name(handle.job_id, 'batch job ID', true)!
	validate_version_name(reason, 'batch stop reason', true)!
	client.rpc_stop_batch_operation(temporal.TemporalApiWorkflowserviceV1_StopBatchOperationRequest{
		namespace: client.options.namespace
		job_id:    handle.job_id
		reason:    reason
		identity:  client.options.transport.identity
	})!
}

pub struct BatchListOptions {
pub:
	page_size  int = 100
	page_token []u8
}

pub fn (client &Client) list_batches(options BatchListOptions) !temporal.TemporalApiWorkflowserviceV1_ListBatchOperationsResponse {
	if options.page_size < 1 { return error('batch page size must be positive') }
	return client.rpc_list_batch_operations(temporal.TemporalApiWorkflowserviceV1_ListBatchOperationsRequest{
		namespace:       client.options.namespace
		page_size:       options.page_size
		next_page_token: options.page_token
	})!
}

pub fn (client &Client) wait_batch(handle BatchHandle, timeout time.Duration) !temporal.TemporalApiWorkflowserviceV1_DescribeBatchOperationResponse {
	validate_version_name(handle.job_id, 'batch job ID', true)!
	if timeout <= 0 { return error('batch wait timeout must be positive') }
	watch := time.new_stopwatch()
	for {
		remaining := (timeout - watch.elapsed()).milliseconds()
		if remaining <= 0 { return error_with_code('batch wait deadline exceeded', 4) }
		request := temporal.TemporalApiWorkflowserviceV1_DescribeBatchOperationRequest{
			namespace: client.options.namespace
			job_id:    handle.job_id
		}
		limit := u32(if remaining < i64(client.options.rpc_timeout_millis) {
			remaining
		} else {
			i64(client.options.rpc_timeout_millis)
		})
		bytes := client.call_with_timeout(.workflow, 'DescribeBatchOperation', request.encode(),
			limit) or {
			// Preserve early transport failures; report the overall deadline once spent.
			if watch.elapsed() >= timeout {
				return error_with_code('batch wait deadline exceeded', 4)
			}
			return err
		}
		result :=
			temporal.TemporalApiWorkflowserviceV1_DescribeBatchOperationResponse.decode(bytes)!
		if result.state == .batch_operation_state_completed
			|| result.state == .batch_operation_state_failed {
			return result
		}
		remaining_duration := timeout - watch.elapsed()
		if remaining_duration > 0 {
			time.sleep(if remaining_duration < 100 * time.millisecond {
				remaining_duration
			} else {
				100 * time.millisecond
			})
		}
	}
	return error('unreachable batch state')
}

// Batch payloads address multiple executions, so they use the client's unbound
// converter rather than claiming the identity of one particular workflow.
pub fn (client &Client) batch_signal(name string, arguments Arguments, headers map[string]PayloadValue) !temporal.TemporalApiWorkflowserviceV1_StartBatchOperationRequest_Operation {
	validate_version_name(name, 'batch signal name', true)!
	converter := client.options.data_converter
	return temporal.TemporalApiWorkflowserviceV1_StartBatchOperationRequest_SignalOperation_Case{
		value: temporal.TemporalApiBatchV1_BatchOperationSignal{
			signal:   name
			input:    temporal.TemporalApiCommonV1_Payloads{
				payloads: converter.encode_arguments(arguments)!
			}
			header:   temporal.TemporalApiCommonV1_Header{
				fields: converter.encode_headers(headers)!
			}
			identity: client.options.transport.identity
		}
	}
}

pub fn (client &Client) batch_termination(details Arguments) !temporal.TemporalApiWorkflowserviceV1_StartBatchOperationRequest_Operation {
	return temporal.TemporalApiWorkflowserviceV1_StartBatchOperationRequest_TerminationOperation_Case{
		value: temporal.TemporalApiBatchV1_BatchOperationTermination{
			details:  temporal.TemporalApiCommonV1_Payloads{
				payloads: client.options.data_converter.encode_arguments(details)!
			}
			identity: client.options.transport.identity
		}
	}
}

pub fn (client &Client) batch_cancellation() temporal.TemporalApiWorkflowserviceV1_StartBatchOperationRequest_Operation {
	return temporal.TemporalApiWorkflowserviceV1_StartBatchOperationRequest_CancellationOperation_Case{
		value: temporal.TemporalApiBatchV1_BatchOperationCancellation{
			identity: client.options.transport.identity
		}
	}
}
