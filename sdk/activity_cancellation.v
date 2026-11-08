module sdk

import temporal

pub struct ActivityCancelInfo {
pub:
	reason  temporal.CoresdkActivityTask_ActivityCancelReason
	details ?temporal.CoresdkActivityTask_ActivityCancellationDetails
}

pub struct ActivityCancelledError {
pub:
	information ActivityCancelInfo
	details     Arguments = Arguments([]string{})
}

pub fn (err ActivityCancelledError) msg() string {
	return 'Activity cancelled (${err.information.reason})'
}

pub fn (err ActivityCancelledError) code() int {
	return 499
}

pub fn (context &ActivityContext) cancellation_info() ?ActivityCancelInfo {
	if !context.cancelled { return none }
	return context.cancellation_snapshot()
}

fn (context &ActivityContext) cancellation_snapshot() ActivityCancelInfo {
	return ActivityCancelInfo{
		reason:  context.cancel_information.reason
		details: owned_optional_proto(context.cancel_information.details)
	}
}
