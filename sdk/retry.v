module sdk

import math
import temporal
import time

pub struct RetryPolicy {
pub:
	initial_interval          time.Duration = time.second
	backoff_coefficient       f64           = 2
	maximum_interval          time.Duration
	maximum_attempts          int
	non_retryable_error_types []string
}

fn (policy RetryPolicy) encode() !temporal.TemporalApiCommonV1_RetryPolicy {
	if policy.initial_interval <= 0 || policy.maximum_interval < 0 || policy.maximum_attempts < 0
		|| policy.maximum_attempts > 2147483647 || math.is_nan(policy.backoff_coefficient)
		|| math.is_inf(policy.backoff_coefficient, 0) || policy.backoff_coefficient < 1 {
		return error('invalid retry policy')
	}
	if policy.maximum_interval > 0 && policy.maximum_interval < policy.initial_interval {
		return error('maximum retry interval must be at least the initial interval')
	}
	mut encoded := temporal.TemporalApiCommonV1_RetryPolicy{
		initial_interval:          duration_proto(policy.initial_interval)!
		backoff_coefficient:       policy.backoff_coefficient
		maximum_attempts:          policy.maximum_attempts
		non_retryable_error_types: policy.non_retryable_error_types
	}
	if policy.maximum_interval > 0 {
		encoded.maximum_interval = duration_proto(policy.maximum_interval)!
	}
	return encoded
}

pub enum ActivityCancellation {
	try_cancel
	wait_completed
	abandon
}

fn (cancellation ActivityCancellation) encode() temporal.CoresdkWorkflowCommands_ActivityCancellationType {
	return match cancellation {
		.try_cancel { .try_cancel }
		.wait_completed { .wait_cancellation_completed }
		.abandon { .abandon }
	}
}
