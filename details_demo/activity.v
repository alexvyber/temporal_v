module main

import temporal_v.sdk
import os
import x.json2
import time

fn fail_details(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	validate_detail_values(context.typed_input_values()!)!
	nested := sdk.ApplicationError{
		message:       'nested typed failure'
		non_retryable: true
		details:       detail_arguments()!
	}
	return sdk.ApplicationError{
		message:       'typed Activity failure'
		non_retryable: true
		details:       detail_arguments()!
		cause:         nested
	}
}

fn heartbeat_details(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	validate_detail_values(context.typed_input_values()!)!
	info := context.info()
	if info.attempt == 1 {
		context.heartbeat(detail_arguments()!)!
		context.sleep(500 * time.millisecond)!
		return sdk.ApplicationError{
			message: 'typed heartbeat retry'
		}
	}
	validate_detail_values(context.heartbeat_values()!)!
	if info.raw_heartbeat_details.len != 7 { return error('raw checkpoint details missing') }
	return sdk.json_value('heartbeat-typed-ok')!
}

fn defer_details(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	validate_detail_values(context.typed_input_values()!)!
	token := context.defer_completion()!
	path := os.join_path(os.getenv('V_SDK_DETAILS_TOKEN_DIR'), context.info().run_id + '.json')
	temporary := path + '.tmp'
	mut file := os.open_file(temporary, 'w', 0o600)!
	file.write_string(json2.encode(token))!
	file.close()
	os.mv(temporary, path)!
	return sdk.NullValue{}
}
