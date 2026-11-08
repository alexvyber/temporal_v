module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import os
import time

fn validate_cancellation_errors() ! {
	policies := [sdk.PayloadConverter(sdk.DefaultPayloadConverter{}),
		sdk.PayloadConverter(sdk.BinaryPayloadConverter{}), sdk.PayloadConverter(OpaqueConverter{}),
		sdk.PayloadConverter(sdk.ProtobufPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{}),
		sdk.PayloadConverter(sdk.ProtobufJsonPayloadConverter[temporal.TemporalApiCommonV1_WorkflowExecution]{})]
	for policy in policies {
		before := sdk.DataConverter{
			payload_converter:     policy
			serialization_context: sdk.SerializationContext{
				kind:        .activity
				namespace:   'default'
				workflow_id: 'parent'
				activity_id: 'one'
			}
			codecs:                [
				sdk.PayloadCodec(BoundCodec{
					tag: 65
				}),
				sdk.PayloadCodec(BoundCodec{
					tag: 66
				}),
				sdk.PayloadCodec(PackCodec{}),
				sdk.PayloadCodec(SplitCodec{}),
				sdk.PayloadCodec(PackCodec{}),
			]
			failure_converter:     sdk.DefaultFailureConverter{
				encode_common_attributes: true
			}
		}
		after := before.with_context(sdk.SerializationContext{
			kind:        .workflow
			namespace:   'default'
			workflow_id: 'parent'
		})
		for err in [
			IError(sdk.CancelledError{
				scope:   3
				details: detail_arguments()!
			}),
			IError(sdk.ActivityCancelledError{
				information: sdk.ActivityCancelInfo{
					reason: .cancelled
				}
				details:     detail_arguments()!
			}),
		] {
			encoded := before.encode_failure(err, 'cancel')!
			captured := before.decode_failure(encoded)
			if captured.kind != .canceled || validate_failure(captured)! != 1 {
				return error('cancellation details were not encoded')
			}
			rebound := after.decode_failure(after.encode_failure(captured, 'cancel')!)
			if rebound.kind != .canceled || validate_failure(rebound)! != 1 {
				return error('cancellation details did not rebind')
			}
		}
		for reason in [temporal.CoresdkActivityTask_ActivityCancelReason.paused, .reset] {
			failure := before.decode_failure(before.encode_failure(sdk.ActivityCancelledError{
				information: sdk.ActivityCancelInfo{
					reason: reason
				}
			}, 'management')!)
			if failure.kind != .application || failure.non_retryable {
				return error('pause/reset was converted into terminal cancellation')
			}
		}
	}
	println('CANCELLATION_DETAILS_VALIDATION_OK')
}

fn cancel_details_activity(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	validate_detail_values(context.typed_input_values()!)!
	info := context.info()
	path := os.join_path(os.getenv('V_SDK_DETAILS_TOKEN_DIR'), '${info.run_id}-${info.is_local}')
	mut file := os.open_file(path, 'w', 0o600)!
	file.write_string('started')!
	file.close()
	// Local Activities delay workflow Signals while their task is executing.
	// Return a short local cancellation; request the remote cancellation by Signal.
	if info.is_local {
		context.sleep(200 * time.millisecond)!
		return sdk.ActivityCancelledError{
			information: sdk.ActivityCancelInfo{
				reason: .cancelled
			}
			details:     detail_arguments()!
		}
	}
	context.sleep(time.minute) or {
		if err is sdk.ActivityCancelledError {
			return sdk.ActivityCancelledError{
				information: err.information
				details:     detail_arguments()!
			}
		}
		return err
	}
	return error('cancellation fixture was not canceled')
}

fn cancel_details_nexus(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	validate_one_value(context.typed_input()!, 1)!
	return sdk.CancelledError{
		details: detail_arguments()!
	}
}
