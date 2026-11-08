module sdk

import temporal
import encoding.base64
import json2

struct ActivityOperationToken {
	version     int    = 1
	kind        string = 'activity'
	namespace   string
	activity_id string
	run_id      string
}

pub fn (mut context NexusContext) start_activity(original &Client, options StandaloneActivityStart) !NexusResult {
	client := context.trace_client(original)!
	defer { client.close() }
	context.check_cancelled()!
	if context.async_started {
		return error('only one async operation can be started per Nexus invocation')
	}
	info := context.info()
	if info.request_id == '' || info.callback_url == '' {
		return error('activity-backed Nexus operation requires request ID and completion callback')
	}
	id := if options.activity_id == '' { 'nexus-' + info.request_id } else { options.activity_id }
	mut links := options.links.clone()
	for link in info.links {
		if link.type_.starts_with(nexus_link_prefix) { links << common_link_from_nexus(link)! }
	}
	mut headers := info.callback_headers.clone()
	headers['Nexus-Operation-Token'] = base64.url_encode_str(json2.encode(ActivityOperationToken{
		namespace:   client.options.namespace
		activity_id: id
	}))
	mut callbacks := options.completion_callbacks.clone()
	callbacks << temporal.TemporalApiCommonV1_Callback{
		links:   links
		variant: temporal.TemporalApiCommonV1_Callback_Nexus_Case{
			value: temporal.TemporalApiCommonV1_Callback_Nexus{
				url:    info.callback_url
				header: headers
			}
		}
	}
	started := client.start_activity(StandaloneActivityStart{
		...options
		activity_id:                            id
		request_id:                             info.request_id
		conflict_policy:                        .activity_id_conflict_policy_use_existing
		on_conflict_options:                    temporal.TemporalApiCommonV1_OnConflictOptions{
			attach_request_id:           true
			attach_completion_callbacks: true
			attach_links:                true
		}
		completion_callbacks:                   callbacks
		links:                                  links
		propagated_nexus_serialization_context: temporal.TemporalApiNexusV1_PropagatedSerializationContext{
			endpoint:  info.endpoint
			service:   info.service
			operation: info.operation
		}
	})!
	context.async_started = true
	link := nexus_link_from_common(temporal.TemporalApiCommonV1_Link{
		variant: temporal.TemporalApiCommonV1_Link_Activity_Case{
			value: temporal.TemporalApiCommonV1_Link_Activity{
				namespace:   client.options.namespace
				activity_id: started.activity_id
				run_id:      started.run_id
			}
		}
	})!
	return NexusResult{
		links:           [link]
		operation_token: base64.url_encode_str(json2.encode(ActivityOperationToken{
			namespace:   client.options.namespace
			activity_id: started.activity_id
			run_id:      started.run_id
		}))
	}
}

pub fn (mut context NexusContext) cancel_activity(original &Client, operation_token string) ! {
	client := context.trace_client(original)!
	defer { client.close() }
	context.check_cancelled()!
	if operation_token.len > 8192 { return error('invalid Activity operation token') }
	token := json2.decode[ActivityOperationToken](base64.url_decode_str(operation_token))!
	if token.version != 1 || token.kind != 'activity' || token.namespace != client.options.namespace
		|| token.activity_id == '' || token.run_id == '' {
		return error('invalid Activity operation token')
	}
	client.cancel_activity(StandaloneActivityHandle{
		activity_id: token.activity_id
		run_id:      token.run_id
	},
		reason: 'Nexus operation cancellation'
	)!
}
