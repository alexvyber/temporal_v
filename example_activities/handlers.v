module example_activities

import x.json2
import alexvyber.temporal_v.sdk
import time
import os

struct DelayInput {
	name     string
	delay_ms int
	fail     bool
}

pub fn echo(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	input := sdk.single_argument(arguments)!
	context.check_cancelled()!
	name := json2.decode[string](input)!
	return json2.encode('Hello from V, ${name}!')
}

pub fn delay(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	input := sdk.single_argument(arguments)!
	request := json2.decode[DelayInput](input)!
	if request.delay_ms < 0 || request.delay_ms > 30000 {
		return error('delay_ms must be between 0 and 30000')
	}
	context.sleep(request.delay_ms * time.millisecond) or {
		if err is sdk.ActivityCancelledError {
			mut attributes := {
				'reason': err.information.reason.str()
			}
			snapshot := context.cancellation_info() or {
				return error('cancellation information was not retained')
			}
			if snapshot.reason != err.information.reason {
				return error('cancellation snapshot disagreed with error')
			}
			if details := err.information.details {
				attributes['paused'] = details.is_paused.str()
				attributes['reset'] = details.is_reset.str()
			}
			context.log(.info, 'Activity cancellation observed', attributes)
		}
		return err
	}
	if request.fail { return error('requested Activity failure') }
	return json2.encode(request.name)
}

struct AttemptInput {
	fail_until    u32
	non_retryable bool
	delay_ms      int
}

pub fn attempt(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	input := sdk.single_argument(arguments)!
	request := json2.decode[AttemptInput](input)!
	if request.delay_ms < 0 || request.delay_ms > 30000 { return error('invalid attempt delay') }
	context.sleep(request.delay_ms * time.millisecond) or {
		if err is sdk.ActivityCancelledError {
			mut attributes := {
				'reason': err.information.reason.str()
			}
			snapshot := context.cancellation_info() or {
				return error('cancellation information was not retained')
			}
			if snapshot.reason != err.information.reason {
				return error('cancellation snapshot disagreed with error')
			}
			if details := err.information.details {
				attributes['paused'] = details.is_paused.str()
				attributes['reset'] = details.is_reset.str()
			}
			context.log(.info, 'Activity cancellation observed', attributes)
		}
		return err
	}
	info := context.info()
	if info.attempt <= request.fail_until {
		return sdk.ApplicationError{
			message:       'requested attempt failure ${info.attempt}'
			category:      'AttemptFailure'
			non_retryable: request.non_retryable
			details:       [json2.encode({
				'attempt': info.attempt
			})]
		}
	}
	return json2.encode(info)
}

pub fn asynchronous(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	// This example consumes typed progress before deferring a retried attempt.
	context.heartbeat_values() or {
		return sdk.ApplicationError{
			message:       'heartbeat details decode failed: ' + err.msg()
			category:      'HeartbeatDecodeError'
			non_retryable: true
		}
	}
	directory := os.getenv('V_SDK_EXAMPLE_ASYNC_TOKEN_DIR')
	if directory == '' { return error('async example requires a token directory') }
	token := context.defer_completion()!
	info := context.info()
	mut file := os.open_file(os.join_path(directory, info.run_id + '.json'), 'wx', 0o600)!
	defer { file.close() }
	file.write_string(json2.encode(token))!
	return 'null'
}

pub fn arguments(mut context sdk.ActivityContext, values []string) !sdk.PayloadValue {
	context.check_cancelled()!
	if values.len != 3 { return error('Activity lost multi-argument payload boundary') }
	return values[1]
}

pub fn nested_application_error() sdk.ApplicationError {
	return sdk.ApplicationError{
		message:        'outer application failure'
		category:       'OuterFailure'
		error_category: .benign
		non_retryable:  true
		details:        ['{"step":1}', '"世界"']
		stack_trace:    'outer_stack'
		cause:          sdk.ApplicationError{
			message:     'inner application failure'
			category:    'InnerFailure'
			details:     ['3']
			stack_trace: 'inner_stack'
			cause:       error('root cause')
		}
	}
}

pub fn nested_error(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	return nested_application_error()
}

pub fn heartbeat_state(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	expected := ['{"cursor":7}', '"opaque 世界"']
	if context.info().attempt == 1 {
		context.heartbeat(expected)!
		context.sleep(5 * time.second)!
		return error('heartbeat worker should be killed before completing this attempt')
	}
	if context.heartbeat_details()! != expected {
		return error('heartbeat progress was lost across worker restart')
	}
	context.sleep(100 * time.millisecond)!
	return json2.encode(context.heartbeat_details()!)
}
