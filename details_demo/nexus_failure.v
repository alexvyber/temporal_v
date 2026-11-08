module main

import sdk
import temporal

fn validate_nexus_failures() ! {
	for protected in [false, true] {
		converter := sdk.DataConverter{
			failure_converter: sdk.DefaultFailureConverter{
				encode_common_attributes: protected
			}
		}
		application := converter.encode_failure(sdk.ApplicationError{
			message: 'nested'
			details: detail_arguments()!
		}, 'test')!
		for info in [
			temporal.TemporalApiFailureV1_Failure_FailureInfo(temporal.TemporalApiFailureV1_Failure_ActivityFailureInfo_Case{
				value: temporal.TemporalApiFailureV1_ActivityFailureInfo{
					activity_id: 'original'
				}
			}),
			temporal.TemporalApiFailureV1_Failure_TimeoutFailureInfo_Case{
				value: temporal.TemporalApiFailureV1_TimeoutFailureInfo{}
			},
			temporal.TemporalApiFailureV1_Failure_ChildWorkflowExecutionFailureInfo_Case{
				value: temporal.TemporalApiFailureV1_ChildWorkflowExecutionFailureInfo{}
			},
		] {
			original := temporal.TemporalApiFailureV1_Failure{
				message:      'captured'
				failure_info: info
				cause:        &application
			}
			failure := converter.decode_failure(original)
			encoded := converter.encode_nexus_failure(failure)!
			decoded := converter.decode_failure(encoded)
			if decoded.kind != .application {
				return error('unsupported Nexus failure root was not normalized')
			}
			cause := decoded.cause or { return error('captured Nexus cause missing') }
			if cause.kind != failure.kind || cause.message != failure.message
				|| (!protected && cause.protobuf != failure.protobuf)
				|| validate_failure(*cause)! != 1 {
				return error('captured Nexus cause changed')
			}
		}
		for err in [
			IError(sdk.ApplicationError{
				message: 'application'
				details: detail_arguments()!
			}),
			IError(sdk.CancelledError{
				details: detail_arguments()!
			}),
		] {
			encoded := converter.encode_nexus_failure(err)!
			decoded := converter.decode_failure(encoded)
			if decoded.kind !in [.application, .canceled] || validate_failure(decoded)! != 1 {
				return error('supported Nexus failure changed')
			}
		}
	}
	invalid := sdk.DataConverter{
		failure_converter: InvalidNexusFailureConverter{}
	}
	if _ := invalid.encode_nexus_failure(sdk.ApplicationError{ message: 'invalid converter' }) {
		return error('unsupported custom Nexus failure was accepted')
	}
	println('NEXUS_FAILURE_VALIDATION_OK')
}

fn captured_nexus_failure(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	index := context.typed_input()!.logical_data().int()
	// Simulate a captured Activity-scoped failure being forwarded by a Nexus handler.
	scoped := converter()!.with_context(sdk.SerializationContext{
		kind:        .activity
		namespace:   'default'
		workflow_id: 'captured'
		activity_id: 'one'
	})
	application := scoped.encode_failure(sdk.ApplicationError{
		message: 'nested'
		details: detail_arguments()!
	}, 'test')!
	info := match index {
		0 {
			temporal.TemporalApiFailureV1_Failure_FailureInfo(temporal.TemporalApiFailureV1_Failure_ActivityFailureInfo_Case{
				value: temporal.TemporalApiFailureV1_ActivityFailureInfo{
					activity_id: 'original'
				}
			})
		}
		1 {
			temporal.TemporalApiFailureV1_Failure_FailureInfo(temporal.TemporalApiFailureV1_Failure_TimeoutFailureInfo_Case{
				value: temporal.TemporalApiFailureV1_TimeoutFailureInfo{}
			})
		}
		else {
			temporal.TemporalApiFailureV1_Failure_FailureInfo(temporal.TemporalApiFailureV1_Failure_ChildWorkflowExecutionFailureInfo_Case{
				value: temporal.TemporalApiFailureV1_ChildWorkflowExecutionFailureInfo{}
			})
		}
	}

	return scoped.decode_failure(temporal.TemporalApiFailureV1_Failure{
		message:      'captured'
		failure_info: info
		cause:        &application
	})
}

fn check_nexus_failure(failure sdk.Failure, index int) ! {
	if failure.category == 'VNexusOperationError' {
		if failure.kind != .application { return error('invalid Nexus operation failure kind') }
		cause := failure.cause or { return error('captured failure missing') }
		if cause.kind != [sdk.FailureKind.activity, .timeout, .child_workflow][index]
			|| cause.message != 'captured' || validate_failure(*cause)! != 1 {
			return error('captured Nexus details changed')
		}
		return
	}
	if cause := failure.cause {
		check_nexus_failure(*cause, index)!
		return
	}
	return error('normalized Nexus failure missing')
}

struct InvalidNexusFailureConverter {}

fn (encoder InvalidNexusFailureConverter) to_failure(err IError, converter sdk.DataConverter, generic_type string) !temporal.TemporalApiFailureV1_Failure {
	return temporal.TemporalApiFailureV1_Failure{
		message:      err.msg()
		failure_info: temporal.TemporalApiFailureV1_Failure_ServerFailureInfo_Case{
			value: temporal.TemporalApiFailureV1_ServerFailureInfo{}
		}
	}
}

fn (decoder InvalidNexusFailureConverter) from_failure(value temporal.TemporalApiFailureV1_Failure, converter sdk.DataConverter) sdk.Failure {
	return sdk.DefaultFailureConverter{}.from_failure(value, converter)
}
