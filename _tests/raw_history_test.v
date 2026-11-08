module main

import sdk
import temporal

fn test_raw_history_preserves_order_payloads_and_unknown_wire_fields() ! {
	first := temporal.TemporalApiHistoryV1_History{
		events:     [
			temporal.TemporalApiHistoryV1_HistoryEvent{
				event_id:   1
				pb_unknown: [u8(160), 6, 7]
			},
		]
		pb_unknown: [u8(160), 6, 9]
	}
	second := temporal.TemporalApiHistoryV1_History{
		events: [temporal.TemporalApiHistoryV1_HistoryEvent{ event_id: 2 }]
	}
	response := temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse{
		raw_history: [
			temporal.TemporalApiCommonV1_DataBlob{
				encoding_type: .encoding_type_proto3
				data:          first.encode()
			},
			temporal.TemporalApiCommonV1_DataBlob{
				encoding_type: .encoding_type_json
				data:          second.json()!.bytes()
			},
		]
	}
	decoded := sdk.history_from_response(response)!
	assert decoded.events.map(it.event_id) == [i64(1), 2]
	assert decoded.events[0].pb_unknown == first.events[0].pb_unknown
	assert decoded.pb_unknown == first.pb_unknown
	assert sdk.history_from_response(temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse{
		history: first
	})!.encode() == first.encode()
}

fn test_raw_history_rejects_unknown_encodings_corruption_and_ambiguous_events() ! {
	for blob in [
		temporal.TemporalApiCommonV1_DataBlob{
			data: [u8(1)]
		},
		temporal.TemporalApiCommonV1_DataBlob{
			encoding_type: .encoding_type_proto3
			data:          [u8(10), 127]
		},
		temporal.TemporalApiCommonV1_DataBlob{
			encoding_type: .encoding_type_json
			data:          '{"events":['.bytes()
		},
	] {
		if _ := sdk.history_from_response(temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse{
			raw_history: [
				blob,
			]
		})
		{
			assert false
		}
	}
	if _ := sdk.history_from_response(temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryResponse{
		history:     temporal.TemporalApiHistoryV1_History{
			events: [temporal.TemporalApiHistoryV1_HistoryEvent{ event_id: 1 }]
		}
		raw_history: [
			temporal.TemporalApiCommonV1_DataBlob{
				encoding_type: .encoding_type_proto3
			},
		]
	})
	{
		assert false
	}
}
