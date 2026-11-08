module main

import temporal_v.sdk
import temporal_v.temporal

fn test_json_payload_preserves_application_type_fields_and_numbers() ! {
	converter := sdk.DataConverter{}
	for data in [
		'{"_type":"Customer","nested":{"_type":"JsonValue","data":"hello"}}',
		'[{"_type":"unknown"},{"_type":7},{"_type":null}]',
		'{"large":18446744073709551615,"fraction":1.0000000000000000001,"exponent":1e309}',
		'{"headers":{"x-trace":{"data":"trace","_type":"JsonValue"}}}',
	] {
		encoded := converter.encode_value(data)!
		assert encoded.data.bytestr() == data
		assert converter.decode_value(encoded)!.logical_data() == data
	}
}

fn test_json_payload_rejects_malformed_documents_on_both_boundaries() ! {
	converter := sdk.DataConverter{}
	for data in ['{"a":[1,2', '{"a":1} trailing', '[1,]', '{"a":1.}', '{"a":NaN}', '{"a":01}',
		'"unterminated'] {
		if _ := converter.encode_value(data) {
			assert false, 'accepted malformed input: ' + data
		}
		payload := temporal.TemporalApiCommonV1_Payload{
			metadata: {
				'encoding': 'json/plain'.bytes()
			}
			data:     data.bytes()
		}
		if _ := converter.decode_value(payload) {
			assert false, 'accepted malformed output: ' + data
		}
	}
	invalid := temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'json/plain'.bytes()
		}
		data:     [u8(34), 255, 34]
	}
	if _ := converter.decode_value(invalid) {
		assert false, 'accepted invalid UTF-8'
	}
}
