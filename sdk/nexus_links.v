module sdk

import net.urllib
import strconv
import temporal
import encoding.utf8

const nexus_link_prefix = 'temporal.api.common.v1.Link.'

fn nexus_event_type(name string) !temporal.TemporalApiEnumsV1_EventType {
	normalized := name.replace('_', '').to_lower().trim_string_left('eventtype')
	$for value in temporal.TemporalApiEnumsV1_EventType.values {
		if value.name.replace('_', '').to_lower().trim_string_left('eventtype') == normalized {
			return temporal.TemporalApiEnumsV1_EventType.from_string(value.name) or {
				return error('invalid event type')
			}
		}
	}
	return error('unknown workflow event type in Nexus link')
}

fn nexus_link_text(value string) ! {
	if value.contains('\x00') || !utf8.validate_str(value) {
		return error('invalid Nexus link text')
	}
}

fn nexus_url(link temporal.TemporalApiNexusV1_Link) !urllib.URL {
	if !link.url.starts_with('temporal:///') { return error('invalid Nexus link scheme/authority') }
	url := urllib.parse(link.url.trim_string_left('temporal://'))!
	if url.scheme != '' || url.host != '' || url.fragment != '' {
		return error('invalid Nexus link URL')
	}
	return url
}

fn link_segments(url urllib.URL, collection string, tail string) ![]string {
	parts := url.escaped_path().split('/')
	if parts.len != (if tail == '' {
		6
	} else {
		7
	}) || parts[0] != '' || parts[1] != 'namespaces' || parts[3] != collection {
		return error('invalid Nexus link path')
	}
	if tail != '' && parts[6] != tail { return error('invalid Nexus link path tail') }
	mut result := []string{}
	for index in [2, 4, 5] {
		value := urllib.path_unescape(parts[index])!
		if value == '' { return error('Nexus link requires namespace, ID and run ID') }
		nexus_link_text(value)!
		result << value
	}
	return result
}

fn link_url(namespace string, collection string, id string, run string, tail string) !string {
	for value in [namespace, id, run] {
		if value == '' { return error('Nexus link requires namespace, ID and run ID') }
		nexus_link_text(value)!
	}
	return 'temporal:///namespaces/' + urllib.path_escape(namespace) + '/' + collection + '/' + urllib.path_escape(id) + '/' + urllib.path_escape(run) + if tail != '' {
		'/' + tail
	} else {
		''
	}
}

fn execution_collection(kind temporal.TemporalApiEnumsV1_ExecutionType) !string {
	return match kind {
		.execution_type_workflow { 'workflows' }
		.execution_type_activity { 'activities' }
		.execution_type_nexus_operation { 'nexus-operations' }
		else { return error('unsupported Nexus callback execution type') }
	}
}

pub fn common_link_from_nexus(link temporal.TemporalApiNexusV1_Link) !temporal.TemporalApiCommonV1_Link {
	url := nexus_url(link)!
	query := urllib.parse_query(url.raw_query)!
	match link.type_ {
		nexus_link_prefix + 'WorkflowEvent' {
			parts := link_segments(url, 'workflows', 'history')!
			mut event := temporal.TemporalApiCommonV1_Link_WorkflowEvent{
				namespace:   parts[0]
				workflow_id: parts[1]
				run_id:      parts[2]
			}
			if url.raw_query != '' {
				event_type := nexus_event_type(query.get('eventType') or {
					return error('missing event type')
				})!
				match query.get('referenceType') {
					'EventReference' {
						id := strconv.atoi64(query.get('eventID') or { '0' })!
						if id < 0 { return error('negative event ID in Nexus link') }
						event.reference = temporal.TemporalApiCommonV1_Link_WorkflowEvent_EventRef_Case{
							value: temporal.TemporalApiCommonV1_Link_WorkflowEvent_EventReference{
								event_id:   id
								event_type: event_type
							}
						}
					}
					'RequestIdReference' {
						request := query.get('requestID') or { return error('missing request ID') }
						if request == '' { return error('empty request ID') }
						event.reference = temporal.TemporalApiCommonV1_Link_WorkflowEvent_RequestIdRef_Case{
							value: temporal.TemporalApiCommonV1_Link_WorkflowEvent_RequestIdReference{
								request_id: request
								event_type: event_type
							}
						}
					}
					else {
						return error('unknown workflow event reference type')
					}
				} or { return error('missing event reference type') }
			}
			return temporal.TemporalApiCommonV1_Link{
				variant: temporal.TemporalApiCommonV1_Link_WorkflowEvent_Case{
					value: event
				}
			}
		}
		nexus_link_prefix + 'Activity' {
			parts := link_segments(url, 'activities', 'details')!
			return temporal.TemporalApiCommonV1_Link{
				variant: temporal.TemporalApiCommonV1_Link_Activity_Case{
					value: temporal.TemporalApiCommonV1_Link_Activity{
						namespace:   parts[0]
						activity_id: parts[1]
						run_id:      parts[2]
					}
				}
			}
		}
		nexus_link_prefix + 'NexusOperation' {
			parts := link_segments(url, 'nexus-operations', 'details')!
			return temporal.TemporalApiCommonV1_Link{
				variant: temporal.TemporalApiCommonV1_Link_NexusOperation_Case{
					value: temporal.TemporalApiCommonV1_Link_NexusOperation{
						namespace:    parts[0]
						operation_id: parts[1]
						run_id:       parts[2]
					}
				}
			}
		}
		nexus_link_prefix + 'Workflow' {
			parts := link_segments(url, 'workflows', '')!
			return temporal.TemporalApiCommonV1_Link{
				variant: temporal.TemporalApiCommonV1_Link_Workflow_Case{
					value: temporal.TemporalApiCommonV1_Link_Workflow{
						namespace:   parts[0]
						workflow_id: parts[1]
						run_id:      parts[2]
						reason:      query.get('reason') or { '' }
					}
				}
			}
		}
		nexus_link_prefix + 'Callback' {
			parts := url.escaped_path().split('/')
			if parts.len != 8 || parts[0] != '' || parts[1] != 'namespaces'
				|| parts[6] != 'callbacks' {
				return error('invalid Nexus callback path')
			}
			kind := match parts[3] {
				'workflows' {
					temporal.TemporalApiEnumsV1_ExecutionType.execution_type_workflow
				}
				'activities' {
					temporal.TemporalApiEnumsV1_ExecutionType.execution_type_activity
				}
				'nexus-operations' {
					temporal.TemporalApiEnumsV1_ExecutionType.execution_type_nexus_operation
				}
				else {
					return error('unsupported Nexus callback execution type')
				}
			}

			mut decoded := []string{}
			for index in [2, 4, 5, 7] {
				value := urllib.path_unescape(parts[index])!
				if value == '' { return error('empty Nexus callback identifier') }
				nexus_link_text(value)!
				decoded << value
			}
			return temporal.TemporalApiCommonV1_Link{
				variant: temporal.TemporalApiCommonV1_Link_Callback_Case{
					value: temporal.TemporalApiCommonV1_Link_Callback{
						namespace:      decoded[0]
						execution:      temporal.TemporalApiCommonV1_Execution{
							type_:       kind
							business_id: decoded[1]
							run_id:      decoded[2]
						}
						request_id:     decoded[3]
						component_path: query.get_all('componentPath')
					}
				}
			}
		}
		else {
			return error('unsupported Nexus link type')
		}
	}
}

pub fn nexus_link_from_common(link temporal.TemporalApiCommonV1_Link) !temporal.TemporalApiNexusV1_Link {
	variant := link.variant or { return error('Nexus link has no variant') }
	mut kind := ''
	mut url := ''
	mut query := urllib.Values{}
	match variant {
		temporal.TemporalApiCommonV1_Link_WorkflowEvent_Case {
			event := variant.value
			kind = 'WorkflowEvent'
			url =
				link_url(event.namespace, 'workflows', event.workflow_id, event.run_id, 'history')!
			if reference := event.reference {
				match reference {
					temporal.TemporalApiCommonV1_Link_WorkflowEvent_EventRef_Case {
						if reference.value.event_id < 0 {
							return error('negative event ID in Nexus link')
						}
						query.set('referenceType', 'EventReference')
						if reference.value.event_id > 0 {
							query.set('eventID', reference.value.event_id.str())
						}
						query.set('eventType', reference.value.event_type.str().to_upper())
					}
					temporal.TemporalApiCommonV1_Link_WorkflowEvent_RequestIdRef_Case {
						if reference.value.request_id == '' { return error('empty request ID') }
						query.set('referenceType', 'RequestIdReference')
						query.set('requestID', reference.value.request_id)
						query.set('eventType', reference.value.event_type.str().to_upper())
					}
				}
			}
		}
		temporal.TemporalApiCommonV1_Link_Activity_Case {
			kind = 'Activity'
			url = link_url(variant.value.namespace, 'activities', variant.value.activity_id,
				variant.value.run_id, 'details')!
		}
		temporal.TemporalApiCommonV1_Link_NexusOperation_Case {
			kind = 'NexusOperation'
			url = link_url(variant.value.namespace, 'nexus-operations', variant.value.operation_id,
				variant.value.run_id, 'details')!
		}
		temporal.TemporalApiCommonV1_Link_Workflow_Case {
			kind = 'Workflow'
			url = link_url(variant.value.namespace, 'workflows', variant.value.workflow_id,
				variant.value.run_id, '')!
			if variant.value.reason != '' { query.set('reason', variant.value.reason) }
		}
		temporal.TemporalApiCommonV1_Link_Callback_Case {
			kind = 'Callback'
			execution := variant.value.execution or {
				return error('callback link has no execution')
			}

			collection := execution_collection(execution.type_)!
			if variant.value.request_id == '' {
				return error('callback link requires a request ID')
			}
			url =
				link_url(variant.value.namespace, collection, execution.business_id, execution.run_id, 'callbacks')! +
				'/' + urllib.path_escape(variant.value.request_id)
			for component in variant.value.component_path {
				query.add('componentPath', component)
			}
		}
		else {
			return error('unsupported common link variant for Nexus')
		}
	}

	encoded := query.encode()
	return temporal.TemporalApiNexusV1_Link{
		type_: nexus_link_prefix + kind
		url:   url + if encoded != '' {
			'?' + encoded
		} else {
			''
		}
	}
}
