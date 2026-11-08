module example_activities

import sdk
import json2

pub fn result_context(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	context.check_cancelled()!
	return json2.encode({
		'input':  context.serialization_context()
		'result': context.result_serialization_context()
	})
}
