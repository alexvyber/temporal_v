module main

import sdk
import time
import temporal

fn detail_map() !map[string]sdk.PayloadValue {
	mut result := map[string]sdk.PayloadValue{}
	for index, value in detail_arguments()!.values {
		result['item-${index}'] = value
	}
	return result
}

fn validate_detail_map(values map[string]sdk.PayloadValue) ! {
	for index in 0 .. 7 {
		value := values['item-${index}'] or { return error('typed metadata item missing') }
		validate_one_value(value, index)!
	}
}

fn headers_activity(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	validate_detail_map(context.header_values()!)!
	validate_detail_values(context.typed_input_values()!)!
	// An owned information snapshot must not expose mutable retained bytes.
	info := context.info()
	raw := info.raw_headers['item-1'] or { return error('missing raw binary header') }
	mut bytes_snapshot := raw.data()
	bytes_snapshot[0] = 42
	if context.raw_headers()['item-1'].data()[0] == 42 {
		return error('raw header mutation changed retained data')
	}
	value := context.header_values()!['item-1'] or { return error('missing binary header') }
	if value is sdk.BinaryValue {
		mut bytes := unsafe { value.data }
		bytes[0] = 42
	} else {
		return error('binary header lost its type')
	}
	validate_detail_map(context.header_values()!)!
	return sdk.json_value(true)!
}

fn (mut workflow DetailsWorkflow) start_metadata(mut context sdk.WorkflowContext) ! {
	validate_detail_map(context.headers()!)!
	workflow.memo_preserves_null = 'item-4' in context.memo()!
	validate_memo_map(context.memo()!, workflow.memo_preserves_null)!
	context.delete_memo(['removed'])!
	context.upsert_memo({
		'null-kept': sdk.PayloadValue(sdk.NullValue{})
	})!
	memo := context.memo()!
	if 'removed' in memo { return error('memo deletion was not visible') }
	if workflow.memo_preserves_null {
		value := memo['null-kept'] or { return error('protected null memo was deleted') }
		if value !is sdk.NullValue { return error('typed null memo changed') }
	} else if 'null-kept' in memo {
		return error('unprotected null memo deletion was not reflected')
	}
	context.activity('v.details.headers', detail_arguments()!, sdk.ActivityOptions{
		headers:        detail_map()!
		start_to_close: time.minute
	})!
	context.local_activity('v.details.headers', detail_arguments()!, sdk.LocalActivityOptions{
		headers:        detail_map()!
		start_to_close: time.minute
	})!
}

fn (mut workflow DetailsWorkflow) metadata_signal(event sdk.Event, mut context sdk.WorkflowContext) ! {
	validate_detail_values(event.typed_arguments)!
	validate_detail_map(context.event_headers(event)!)!
	context.upsert_memo({
		'signal': detail_arguments()!.values[2]
	})!
	workflow.received_metadata_signal = true
}

fn (mut workflow DetailsWorkflow) metadata_update(event sdk.Event, mut context sdk.WorkflowContext) ! {
	validate_detail_values(event.typed_arguments)!
	validate_detail_map(context.event_headers(event)!)!
	context.upsert_memo({
		'update': detail_arguments()!.values[1]
	})!
	workflow.received_metadata_update = true
	context.complete_update(event.id, sdk.BinaryValue{ data: [u8(0), 255, 128] })!
}

fn (workflow &DetailsWorkflow) finish_metadata(context &sdk.WorkflowContext) ! {
	if workflow.metadata_completions != 2 || !workflow.received_metadata_signal
		|| !workflow.received_metadata_update {
		return error('typed metadata workflow did not observe every boundary')
	}
	memo := context.memo()!
	validate_memo_map(memo, workflow.memo_preserves_null)!
	validate_one_value(memo['signal'] or { return error('signal memo missing') }, 2)!
	validate_one_value(memo['update'] or { return error('update memo missing') }, 1)!
}

fn validate_memo_map(values map[string]sdk.PayloadValue, preserve_null bool) ! {
	for index in 0 .. 7 {
		key := 'item-${index}'
		if !preserve_null && index in [4, 5] {
			if key in values { return error('server-null memo unexpectedly retained') }
			continue
		}
		value := values[key] or { return error('typed memo item missing') }
		validate_one_value(value, index)!
	}
}

struct RawMetadataConverter {}

fn (converter RawMetadataConverter) to_payload(value sdk.PayloadValue, context sdk.SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	return sdk.DefaultPayloadConverter{}.to_payload(value, context)!
}

fn (converter RawMetadataConverter) from_payload(payload temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) !sdk.PayloadValue {
	return sdk.raw_value(payload)!
}

fn validate_metadata() ! {
	mut payload := temporal.TemporalApiCommonV1_Payload{
		metadata:   {
			'encoding': 'test/raw'.bytes()
			'opaque':   [u8(0), 255]
		}
		data:       [u8(0), 128, 255]
		pb_unknown: [u8(160), 6, 7]
	}
	raw := sdk.raw_value(payload)!
	payload.data[0] = 42
	payload.metadata['opaque'] = [u8(42)]
	if raw.data() != [u8(0), 128, 255] || raw.to_payload()!.metadata['opaque'] != [u8(0), 255] {
		return error('raw constructor exposed caller-owned bytes')
	}
	converter := sdk.DataConverter{
		payload_converter: RawMetadataConverter{}
		codecs:            [
			sdk.PayloadCodec(PackCodec{}),
			sdk.PayloadCodec(SplitCodec{}),
			sdk.PayloadCodec(PackCodec{}),
		]
	}
	encoded := converter.encode_map({
		'opaque': sdk.PayloadValue(raw)
		'null':   sdk.PayloadValue(sdk.NullValue{})
		'binary': sdk.PayloadValue(sdk.BinaryValue{ data: 'null'.bytes() })
	})!
	decoded := converter.decode_map(encoded)!
	forwarded := decoded['opaque'] or { return error('opaque map value missing') }
	if forwarded is sdk.RawValue {
		if forwarded.to_payload()!.encode() != raw.to_payload()!.encode() {
			return error('raw map forwarding changed unknown fields or metadata')
		}
	} else {
		return error('opaque converter did not retain a raw value')
	}
	if _ := sdk.raw_value(temporal.TemporalApiCommonV1_Payload{ pb_unknown: [u8(255)] }) {
		return error('malformed unknown wire data accepted')
	}
	println('TYPED_METADATA_VALIDATION_OK')
}
