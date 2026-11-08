module sdk

import json2
import math
import strconv
import temporal
import time

pub enum SearchAttributeType {
	text
	keyword
	integer
	double
	boolean
	date_time
	keyword_list
}

pub struct SearchAttribute {
pub:
	name       string
	type_      SearchAttributeType
	value_json string = 'null'
	unset      bool
}

fn (attribute SearchAttribute) encode() !temporal.TemporalApiCommonV1_Payload {
	if attribute.name == '' { return error('search attribute name is required') }
	type_name := match attribute.type_ {
		.text { 'Text' }
		.keyword { 'Keyword' }
		.integer { 'Int' }
		.double { 'Double' }
		.boolean { 'Bool' }
		.date_time { 'Datetime' }
		.keyword_list { 'KeywordList' }
	}

	data := if attribute.unset { 'null' } else { attribute.value_json }
	if !attribute.unset {
		value := json2.decode[json2.Any](data)!
		match attribute.type_ {
			.text, .keyword {
				if value !is string {
					return error('text/keyword search attribute requires a string')
				}
			}
			.date_time {
				if value !is string {
					return error('datetime search attribute requires an RFC3339 string')
				}
				time.parse_rfc3339(value)!
			}
			.integer {
				strconv.atoi64(data)!
			}
			.double {
				number := strconv.atof64(data)!
				if math.is_nan(number) || math.is_inf(number, 0) {
					return error('search attribute number must be finite')
				}
			}
			.boolean {
				if value !is bool { return error('boolean search attribute requires a bool') }
			}
			.keyword_list {
				if value !is []json2.Any {
					return error('keyword-list search attribute requires an array')
				}
				for item in value {
					if item !is string { return error('keyword-list entries must be strings') }
				}
			}
		}
	}
	// Search attributes are indexed JSON, deliberately independent of payload codecs.
	return temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'json/plain'.bytes()
			'type':     type_name.bytes()
		}
		data:     data.bytes()
	}
}

fn search_attributes_proto(attributes []SearchAttribute) !temporal.TemporalApiCommonV1_SearchAttributes {
	mut fields := map[string]temporal.TemporalApiCommonV1_Payload{}
	for attribute in attributes {
		if attribute.name in fields { return error('duplicate search attribute update') }
		fields[attribute.name] = attribute.encode()!
	}
	return temporal.TemporalApiCommonV1_SearchAttributes{
		indexed_fields: fields
	}
}

pub fn (mut context WorkflowContext) upsert_search_attributes(attributes []SearchAttribute, options EventGroupOptions) ! {
	context.ensure_open()!
	if attributes.len == 0 { return }
	encoded := search_attributes_proto(attributes)!
	groups := context.command_event_groups(options.event_groups)!
	for attribute in attributes {
		if attribute.unset {
			context.search_attribute_values.delete(attribute.name)
		} else {
			context.search_attribute_values[attribute.name] = encoded.indexed_fields[attribute.name]
		}
	}
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_UpsertWorkflowSearchAttributes_Case{
			value: temporal.CoresdkWorkflowCommands_UpsertWorkflowSearchAttributes{
				search_attributes: encoded
			}
		}
	})
}

pub fn (mut context WorkflowContext) upsert_memo(values map[string]PayloadValue, options EventGroupOptions) ! {
	context.ensure_open()!
	if values.len == 0 { return }
	for key, _ in values {
		if key == '' { return error('memo key cannot be empty') }
	}
	groups := context.command_event_groups(options.event_groups)!
	context.modify_memo(context.converter.encode_map(values)!, groups.markers)
}

pub fn (mut context WorkflowContext) delete_memo(keys []string, options EventGroupOptions) ! {
	context.ensure_open()!
	if keys.len == 0 { return }
	mut fields := map[string]temporal.TemporalApiCommonV1_Payload{}
	for key in keys {
		if key == '' { return error('memo key cannot be empty') }
		fields[key] = temporal.TemporalApiCommonV1_Payload{}
	}
	groups := context.command_event_groups(options.event_groups)!
	context.modify_memo(fields, groups.markers)
}

fn (mut context WorkflowContext) modify_memo(fields map[string]temporal.TemporalApiCommonV1_Payload, groups []temporal.TemporalApiSdkV1_EventGroupMarker) {
	for name, payload in fields {
		if payload.data.len == 0 || payload.data == 'null'.bytes() || payload.data == '[]'.bytes() {
			context.memo_values.delete(name)
		} else {
			context.memo_values[name] = payload
		}
	}
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_ModifyWorkflowProperties_Case{
			value: temporal.CoresdkWorkflowCommands_ModifyWorkflowProperties{
				upserted_memo: temporal.TemporalApiCommonV1_Memo{
					fields: fields
				}
			}
		}
	})
}

// Values reflect initialization and decisions issued by this workflow, including
// updates made earlier in the current activation. Return fresh decoded values.
pub fn (context &WorkflowContext) memo() !map[string]PayloadValue {
	return context.converter.decode_map(context.memo_values)!
}

pub fn (context &WorkflowContext) search_attributes() ![]SearchAttribute {
	return decode_search_attributes(context.search_attribute_values)!
}

fn decode_search_attributes(values map[string]temporal.TemporalApiCommonV1_Payload) ![]SearchAttribute {
	mut names := values.keys()
	names.sort()
	mut result := []SearchAttribute{}
	for name in names {
		value := values[name]
		if value.data.bytestr() == 'null' {
			continue
		}
		kind := match value.metadata['type'].bytestr() {
			'Text' { SearchAttributeType.text }
			'Keyword' { SearchAttributeType.keyword }
			'Int' { SearchAttributeType.integer }
			'Double' { SearchAttributeType.double }
			'Bool' { SearchAttributeType.boolean }
			'Datetime' { SearchAttributeType.date_time }
			'KeywordList' { SearchAttributeType.keyword_list }
			else { return error('unknown search attribute type for ${name}') }
		}

		attribute := SearchAttribute{
			name:       name
			type_:      kind
			value_json: value.data.bytestr()
		}
		attribute.encode()!
		result << attribute
	}
	return result
}

pub fn (context &WorkflowContext) last_completion_result() !DecodedArguments {
	if result := context.initialization.last_completion_result {
		return context.result_converter().decode_arguments(result.payloads)!
	}
	return DecodedArguments{}
}

pub fn (context &WorkflowContext) continued_failure() ?Failure {
	if failure := context.initialization.continued_failure {
		return context.result_converter().decode_failure(failure)
	}
	return none
}
