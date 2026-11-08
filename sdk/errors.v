module sdk

import alexvyber.temporal_v.temporal
import time

pub enum FailureKind {
	unspecified
	application
	timeout
	canceled
	terminated
	server
	reset
	activity
	child_workflow
	nexus_operation
	nexus_handler
}

pub struct Failure {
	source_context SerializationContext
pub:
	message        string
	source         string
	stack_trace    string
	category       string
	error_category ErrorCategory
	non_retryable  bool
	details        []string
	detail_values  []PayloadValue @[json: '-']
	raw_details    []RawValue     @[json: '-']
	cause          ?&Failure
	// Keep all protocol-specific information (timeouts, retry state, IDs, codecs).
	protobuf         []u8
	kind             FailureKind
	details_error    string
	attributes_error string
}

pub fn (failure &Failure) root_message() string {
	if cause := failure.cause { return cause.root_message() }
	return failure.message
}

pub fn (failure &Failure) root_kind() FailureKind {
	if cause := failure.cause { return cause.root_kind() }
	return failure.kind
}

fn decode_standard_failure(value temporal.TemporalApiFailureV1_Failure, converter DataConverter) Failure {
	mut message := value.message
	mut stack_trace := value.stack_trace
	mut attributes_error := ''
	if payload := value.encoded_attributes {
		attributes := decode_common_failure_attributes(payload, converter) or {
			attributes_error = protocol_text(err.msg())
			CommonFailureAttributes{
				message:     value.message
				stack_trace: value.stack_trace
			}
		}
		message = attributes.message
		stack_trace = attributes.stack_trace
	}
	mut result := Failure{
		source_context:   converter.serialization_context
		message:          message
		source:           value.source
		stack_trace:      stack_trace
		protobuf:         value.encode()
		attributes_error: attributes_error
	}
	if info := value.failure_info {
		kind := match info {
			temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case {
				FailureKind.application
			}
			temporal.TemporalApiFailureV1_Failure_TimeoutFailureInfo_Case {
				FailureKind.timeout
			}
			temporal.TemporalApiFailureV1_Failure_CanceledFailureInfo_Case {
				FailureKind.canceled
			}
			temporal.TemporalApiFailureV1_Failure_TerminatedFailureInfo_Case {
				FailureKind.terminated
			}
			temporal.TemporalApiFailureV1_Failure_ServerFailureInfo_Case {
				FailureKind.server
			}
			temporal.TemporalApiFailureV1_Failure_ResetWorkflowFailureInfo_Case {
				FailureKind.reset
			}
			temporal.TemporalApiFailureV1_Failure_ActivityFailureInfo_Case {
				FailureKind.activity
			}
			temporal.TemporalApiFailureV1_Failure_ChildWorkflowExecutionFailureInfo_Case {
				FailureKind.child_workflow
			}
			temporal.TemporalApiFailureV1_Failure_NexusOperationExecutionFailureInfo_Case {
				FailureKind.nexus_operation
			}
			temporal.TemporalApiFailureV1_Failure_NexusHandlerFailureInfo_Case {
				FailureKind.nexus_handler
			}
		}

		result = Failure{
			...result
			kind: kind
		}
		mut payloads := ?temporal.TemporalApiCommonV1_Payloads(none)
		match info {
			temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case {
				result = Failure{
					...result
					category:       info.value.type_
					non_retryable:  info.value.non_retryable
					error_category: if info.value.category == .application_error_category_benign {
						.benign
					} else {
						.unspecified
					}
				}
				payloads = info.value.details
			}
			temporal.TemporalApiFailureV1_Failure_CanceledFailureInfo_Case {
				payloads = info.value.details
			}
			temporal.TemporalApiFailureV1_Failure_TimeoutFailureInfo_Case {
				payloads = info.value.last_heartbeat_details
			}
			temporal.TemporalApiFailureV1_Failure_ResetWorkflowFailureInfo_Case {
				payloads = info.value.last_heartbeat_details
			}
			temporal.TemporalApiFailureV1_Failure_ServerFailureInfo_Case {
				result = Failure{
					...result
					non_retryable: info.value.non_retryable
				}
			}
			else {}
		}

		if details := payloads {
			if decoded := converter.decode_arguments(details.payloads) {
				result = Failure{
					...result
					details:       decoded.values
					detail_values: decoded.typed_values
					raw_details:   decoded.raw_values
				}
			} else {
				result = Failure{
					...result
					details_error: protocol_text(err.msg())
				}
			}
		}
	}
	if cause := value.cause {
		nested := decode_standard_failure(*cause, converter)
		result = Failure{
			...result
			cause: &nested
		}
	}
	return result
}

pub enum ErrorCategory {
	unspecified
	benign
}

// Captured failures are throwable without losing their original protocol metadata.
pub fn (failure Failure) msg() string {
	return failure.message
}

pub fn (failure Failure) code() int {
	return 500
}

fn (failure Failure) as_proto() !temporal.TemporalApiFailureV1_Failure {
	if failure.protobuf.len == 0 {
		return error('captured Failure has no protocol bytes; construct ApplicationError for new errors')
	}
	return temporal.TemporalApiFailureV1_Failure.decode(failure.protobuf)!
}

pub struct ApplicationError {
pub:
	message          string
	category         string = 'VApplicationError'
	error_category   ErrorCategory
	non_retryable    bool
	details          Arguments = Arguments([]string{})
	stack_trace      string
	cause            ?IError
	next_retry_delay time.Duration
}

pub fn (failure ApplicationError) msg() string {
	return failure.message
}

pub fn (failure ApplicationError) code() int {
	return 0
}

fn encode_error(err IError, converter DataConverter, generic_type string, depth int) !temporal.TemporalApiFailureV1_Failure {
	if depth >= 64 { return error('error cause chain exceeds the supported depth') }
	if err is Failure { return transcode_captured_failure(err, converter)! }
	if err is CancelledError { return cancellation_failure(err.msg(), err.details, converter)! }
	if err is ActivityCancelledError {
		if err.information.reason !in [.paused, .reset] {
			return cancellation_failure(err.msg(), err.details, converter)!
		}
	}
	if err is ApplicationError {
		if err.next_retry_delay < 0 { return error('negative application retry delay') }
		encoded_details := converter.encode_arguments(err.details)!
		mut information := temporal.TemporalApiFailureV1_ApplicationFailureInfo{
			type_:         protocol_text(err.category)
			non_retryable: err.non_retryable
			category:      if err.error_category == .benign {
				.application_error_category_benign
			} else {
				.application_error_category_unspecified
			}
			details:       temporal.TemporalApiCommonV1_Payloads{
				payloads: encoded_details
			}
		}
		if err.next_retry_delay > 0 {
			information.next_retry_delay = duration_proto(err.next_retry_delay)!
		}
		mut result := temporal.TemporalApiFailureV1_Failure{
			message:      protocol_text(err.message)
			source:       'V'
			stack_trace:  protocol_text(err.stack_trace)
			failure_info: temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case{
				value: information
			}
		}
		if cause := err.cause {
			nested := encode_error(cause, converter, 'VError', depth + 1)!
			result.cause = &nested
		}
		return result
	}
	mut information := temporal.TemporalApiFailureV1_ApplicationFailureInfo{
		type_: if err is PanicError {
			'PanicError'
		} else {
			generic_type
		}
	}
	if err is ActivityCancelledError {
		if err.information.reason in [.paused, .reset] {
			information.category = .application_error_category_benign
		}
	}
	return temporal.TemporalApiFailureV1_Failure{
		message:      protocol_text(err.msg())
		source:       'V'
		stack_trace:  if err is PanicError {
			protocol_text(err.stack_trace)
		} else {
			''
		}
		failure_info: temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case{
			value: information
		}
	}
}

fn cancellation_failure(message string, details Arguments, converter DataConverter) !temporal.TemporalApiFailureV1_Failure {
	return temporal.TemporalApiFailureV1_Failure{
		message:      protocol_text(message)
		source:       'V'
		failure_info: temporal.TemporalApiFailureV1_Failure_CanceledFailureInfo_Case{
			value: temporal.TemporalApiFailureV1_CanceledFailureInfo{
				details: temporal.TemporalApiCommonV1_Payloads{
					payloads: converter.encode_arguments(details)!
				}
			}
		}
	}
}

fn (err ApplicationError) as_proto(converter DataConverter) !temporal.TemporalApiFailureV1_Failure {
	return converter.encode_failure(err, 'VApplicationError')!
}

fn (mut context WorkflowContext) fail_captured(failure Failure) ! {
	context.ensure_open()!
	context.finish_with_failure(context.result_converter().encode_failure(failure, 'VWorkflowError')!)
}

pub fn (mut context WorkflowContext) fail_application(err ApplicationError) ! {
	context.ensure_open()!
	context.finish_with_failure(err.as_proto(context.result_converter())!)
}

fn (mut context WorkflowContext) finish_with_failure(encoded temporal.TemporalApiFailureV1_Failure) {
	context.stop_pending(false, false)
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_FailWorkflowExecution_Case{
			value: temporal.CoresdkWorkflowCommands_FailWorkflowExecution{
				failure: encoded
			}
		}
	})
	context.closed = true
}

fn activity_error(err IError, converter DataConverter) temporal.CoresdkActivityResult_ActivityExecutionResult {
	if err is ExternalStorageError { return storage_activity_failure(err) }
	encoded := converter.encode_failure(err, 'VActivityError') or {
		return activity_conversion_failure('failure encoding failed: ${err.msg()}')
	}
	return temporal.CoresdkActivityResult_ActivityExecutionResult{
		status: temporal.CoresdkActivityResult_ActivityExecutionResult_Failed_Case{
			value: temporal.CoresdkActivityResult_Failure{
				failure: encoded
			}
		}
	}
}

// RPC failures retain the server's google.rpc.Status bytes and typed Any details.
pub struct RpcError {
pub:
	message string
	status  int
	details []u8
}

pub fn (err RpcError) msg() string {
	return err.message
}

pub fn (err RpcError) code() int {
	return err.status
}

pub fn (err RpcError) status_details() !temporal.GoogleRpc_Status {
	if err.details.len == 0 { return error('RPC failure has no status details') }
	return temporal.GoogleRpc_Status.decode(err.details)!
}

// Query failures retain both the decoded application failure and transport
// status so callers can inspect encoded details without losing RPC diagnostics.
pub struct QueryError {
pub:
	failure     Failure
	rpc_failure RpcError
}

pub fn (err QueryError) msg() string {
	return err.failure.message
}

pub fn (err QueryError) code() int {
	return err.rpc_failure.status
}

fn query_failure_error(rpc_failure RpcError, converter DataConverter) IError {
	status := rpc_failure.status_details() or { return rpc_failure }
	for detail in status.details {
		if !detail.type_url.ends_with('/temporal.api.errordetails.v1.QueryFailedFailure') {
			continue
		}
		query := temporal.TemporalApiErrordetailsV1_QueryFailedFailure.decode(detail.value) or {
			return rpc_failure
		}
		if failure := query.failure {
			return QueryError{
				failure:     converter.decode_failure(failure)
				rpc_failure: rpc_failure
			}
		}
	}
	return rpc_failure
}

// Use for retryable workflow-task failures (for example, a temporarily unavailable
// decoder dependency). No workflow commands from that activation are committed.
pub struct WorkflowTaskError {
pub:
	message string
}

pub fn (err WorkflowTaskError) msg() string {
	return err.message
}

pub fn (err WorkflowTaskError) code() int {
	return 500
}

// A converter outage must not turn an otherwise retryable Activity into a
// permanent failure. This diagnostic deliberately bypasses the broken converter.
fn activity_conversion_failure(message string) temporal.CoresdkActivityResult_ActivityExecutionResult {
	return temporal.CoresdkActivityResult_ActivityExecutionResult{
		status: temporal.CoresdkActivityResult_ActivityExecutionResult_Failed_Case{
			value: temporal.CoresdkActivityResult_Failure{
				failure: temporal.TemporalApiFailureV1_Failure{
					message:      protocol_text(message)
					source:       'V'
					failure_info: temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case{
						value: temporal.TemporalApiFailureV1_ApplicationFailureInfo{
							type_: 'VFailureConversionError'
						}
					}
				}
			}
		}
	}
}
