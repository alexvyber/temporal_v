module sdk

import alexvyber.temporal_v.temporal

pub enum NexusHandlerErrorKind {
	bad_request
	unauthenticated
	unauthorized
	not_found
	resource_exhausted
	internal
	not_implemented
	unavailable
	upstream_timeout
}

pub enum NexusHandlerRetry {
	server_default
	retryable
	non_retryable
}

// Handler errors describe request/transport failures. ApplicationError instead
// fails the operation, without treating its application failure as a handler retry.
pub struct NexusHandlerError {
pub:
	message     string
	kind        NexusHandlerErrorKind = .internal
	retry       NexusHandlerRetry
	stack_trace string
	metadata    map[string]string
	details     []u8
	cause       ?&temporal.TemporalApiNexusV1_Failure
}

pub fn (err NexusHandlerError) msg() string {
	return err.message
}

pub fn (err NexusHandlerError) code() int {
	return 500
}

fn nexus_error_status(err IError) temporal.CoresdkNexus_NexusTaskCompletion_Status {
	failure := if err is NexusHandlerError {
		NexusHandlerError{
			...err
		}
	} else if err is PanicError {
		NexusHandlerError{
			message:     err.message
			stack_trace: err.stack_trace
		}
	} else if err is ExternalStorageError {
		NexusHandlerError{
			message: err.msg()
			retry:   .retryable
		}
	} else {
		NexusHandlerError{
			message: err.msg()
		}
	}
	return temporal.CoresdkNexus_NexusTaskCompletion_Error_Case{
		value: temporal.TemporalApiNexusV1_HandlerError{
			error_type:     failure.kind.str()
			retry_behavior: match failure.retry {
				.server_default { .nexus_handler_error_retry_behavior_unspecified }
				.retryable { .nexus_handler_error_retry_behavior_retryable }
				.non_retryable { .nexus_handler_error_retry_behavior_non_retryable }
			}
			failure:        temporal.TemporalApiNexusV1_Failure{
				message:     protocol_text(failure.message)
				stack_trace: protocol_text(failure.stack_trace)
				metadata:    protocol_fields(failure.metadata)
				details:     failure.details
				cause:       failure.cause
			}
		}
	}
}

// Core accepts application/cancellation roots for operation failures. Preserve
// other captured protocol kinds as a cause under an application root.
pub fn (converter DataConverter) encode_nexus_failure(err IError) !temporal.TemporalApiFailureV1_Failure {
	encoded := converter.encode_failure(err, 'VNexusOperationError')!
	if nexus_operation_failure_kind(encoded) { return encoded }
	wrapped := converter.encode_failure(ApplicationError{
		message:       err.msg()
		category:      'VNexusOperationError'
		non_retryable: true
		cause:         err
	}, 'VNexusOperationError')!
	if !nexus_operation_failure_kind(wrapped) {
		return error('failure converter must produce an application or cancellation root for Nexus operations')
	}
	return wrapped
}

fn nexus_operation_failure_kind(failure temporal.TemporalApiFailureV1_Failure) bool {
	if info := failure.failure_info {
		return info is temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case
			|| info is temporal.TemporalApiFailureV1_Failure_CanceledFailureInfo_Case
	}
	return false
}
