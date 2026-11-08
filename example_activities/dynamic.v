module example_activities

import sdk
import json2

pub fn dynamic(mut context sdk.ActivityContext, raw_arguments []sdk.RawValue) !sdk.PayloadValue {
	context.check_cancelled()!
	mut arguments := []string{}
	for value in raw_arguments {
		arguments << context.decode_raw_value(value)!.logical_data()
	}
	if arguments.len != 3 { return error('dynamic Activity lost argument boundaries') }
	if arguments[0] == 'fail' {
		return sdk.ApplicationError{
			message:       'dynamic Activity failure'
			category:      'DynamicFailure'
			non_retryable: true
			details:       [json2.encode(context.info().activity_type)]
		}
	}
	if arguments[0] == 'retry' && context.info().attempt == 1 {
		return error('transient dynamic failure')
	}
	return json2.encode({
		'activity_type': context.info().activity_type
		'workflow_id':   context.info().workflow_id
		'value':         arguments[1]
		'is_local':      context.info().is_local.str()
		'attempt':       context.info().attempt.str()
	})
}
