module main

import temporal
import os
import encoding.hex

fn roundtrip[T](text string) string {
	value := if text.starts_with('wire:') {
		data := hex.decode(text[5..]) or { return '0' }
		T.decode(data) or { return '0' }
	} else {
		T.from_json(text) or { return '0' }
	}
	encoded := value.json() or { return '0' }
	return '1|' + encoded
}

fn json_wire_oracle(path string) ! {
	for line in os.read_lines(path)! {
		kind := line.all_before('|')
		text := line.all_after('|')
		println(match kind {
			'double' { roundtrip[temporal.GoogleProtobuf_DoubleValue](text) }
			'float' { roundtrip[temporal.GoogleProtobuf_FloatValue](text) }
			'unsigned' { roundtrip[temporal.GoogleProtobuf_UInt64Value](text) }
			'boolean' { roundtrip[temporal.GoogleProtobuf_BoolValue](text) }
			'string' { roundtrip[temporal.GoogleProtobuf_StringValue](text) }
			'int32' { roundtrip[temporal.GoogleProtobuf_Int32Value](text) }
			'uint32' { roundtrip[temporal.GoogleProtobuf_UInt32Value](text) }
			'workflow' { roundtrip[temporal.TemporalApiCommonV1_WorkflowExecution](text) }
			'payload' { roundtrip[temporal.TemporalApiCommonV1_Payload](text) }
			'reach' { roundtrip[temporal.TemporalApiTaskqueueV1_TaskQueueReachability](text) }
			'link' { roundtrip[temporal.TemporalApiCommonV1_Link](text) }
			'failure' { roundtrip[temporal.TemporalApiFailureV1_Failure](text) }
			'timestamp' { roundtrip[temporal.GoogleProtobuf_Timestamp](text) }
			'duration' { roundtrip[temporal.GoogleProtobuf_Duration](text) }
			'any' { roundtrip[temporal.GoogleProtobuf_Any](text) }
			'mask' { roundtrip[temporal.GoogleProtobuf_FieldMask](text) }
			'value' { roundtrip[temporal.GoogleProtobuf_Value](text) }
			'struct' { roundtrip[temporal.GoogleProtobuf_Struct](text) }
			'list' { roundtrip[temporal.GoogleProtobuf_ListValue](text) }
			'integer' { roundtrip[temporal.GoogleProtobuf_Int64Value](text) }
			'bytes' { roundtrip[temporal.GoogleProtobuf_BytesValue](text) }
			'retry' { roundtrip[temporal.TemporalApiCommonV1_RetryPolicy](text) }
			'priority' { roundtrip[temporal.TemporalApiCommonV1_Priority](text) }
			else { panic('unknown JSON type') }
		})
	}
}
