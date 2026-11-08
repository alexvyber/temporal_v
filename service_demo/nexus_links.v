module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal

fn nexus_resource_links() !string {
	namespace := 'namespace +/世界'
	id := 'resource +/世界'
	run := 'run +/世界'
	mut links := [
		temporal.TemporalApiCommonV1_Link{
			variant: temporal.TemporalApiCommonV1_Link_Activity_Case{
				value: temporal.TemporalApiCommonV1_Link_Activity{
					namespace:   namespace
					activity_id: id
					run_id:      run
				}
			}
		},
		temporal.TemporalApiCommonV1_Link{
			variant: temporal.TemporalApiCommonV1_Link_NexusOperation_Case{
				value: temporal.TemporalApiCommonV1_Link_NexusOperation{
					namespace:    namespace
					operation_id: id
					run_id:       run
				}
			}
		},
		temporal.TemporalApiCommonV1_Link{
			variant: temporal.TemporalApiCommonV1_Link_Workflow_Case{
				value: temporal.TemporalApiCommonV1_Link_Workflow{
					namespace:   namespace
					workflow_id: id
					run_id:      run
					reason:      'query /?& +世界'
				}
			}
		},
		temporal.TemporalApiCommonV1_Link{
			variant: temporal.TemporalApiCommonV1_Link_WorkflowEvent_Case{
				value: temporal.TemporalApiCommonV1_Link_WorkflowEvent{
					namespace:   namespace
					workflow_id: id
					run_id:      run
					reference:   temporal.TemporalApiCommonV1_Link_WorkflowEvent_RequestIdRef_Case{
						value: temporal.TemporalApiCommonV1_Link_WorkflowEvent_RequestIdReference{
							request_id: 'request /?& +世界'
							event_type: .event_type_workflow_execution_signaled
						}
					}
				}
			}
		},
	]
	for kind in [temporal.TemporalApiEnumsV1_ExecutionType.execution_type_workflow,
		.execution_type_activity, .execution_type_nexus_operation] {
		links << temporal.TemporalApiCommonV1_Link{
			variant: temporal.TemporalApiCommonV1_Link_Callback_Case{
				value: temporal.TemporalApiCommonV1_Link_Callback{
					namespace:      namespace
					execution:      temporal.TemporalApiCommonV1_Execution{
						type_:       kind
						business_id: id
						run_id:      run
					}
					request_id:     'callback /?& +世界'
					component_path: ['Update', 'id /?& +世界', 'same', 'same']
				}
			}
		}
	}
	for original in links {
		encoded := sdk.nexus_link_from_common(original)!
		decoded := sdk.common_link_from_nexus(encoded)!
		if decoded.encode() != original.encode() {
			return error('resource Nexus link roundtrip lost IDs or metadata: ${encoded}')
		}
	}
	if _ := sdk.nexus_link_from_common(temporal.TemporalApiCommonV1_Link{
		variant: temporal.TemporalApiCommonV1_Link_BatchJob_Case{
			value: temporal.TemporalApiCommonV1_Link_BatchJob{
				job_id: 'unsupported'
			}
		}
	})
	{
		return error('unsupported batch link accepted')
	}
	for invalid in [
		temporal.TemporalApiNexusV1_Link{
			type_: 'temporal.api.common.v1.Link.Activity'
			url:   'temporal:///namespaces/ns/activities/id/run/details/extra'
		},
		temporal.TemporalApiNexusV1_Link{
			type_: 'temporal.api.common.v1.Link.Callback'
			url:   'temporal:///namespaces/ns/unknown/id/run/callbacks/request'
		},
		temporal.TemporalApiNexusV1_Link{
			type_: 'temporal.api.common.v1.Link.WorkflowEvent'
			url:   'temporal:///namespaces/ns/workflows/id/run/history?referenceType=Wrong&eventType=WorkflowExecutionStarted'
		},
		temporal.TemporalApiNexusV1_Link{
			type_: 'temporal.api.common.v1.Link.WorkflowEvent'
			url:   'temporal:///namespaces/ns/workflows/id/run/history?referenceType=RequestIdReference&eventType=WorkflowExecutionStarted'
		},
		temporal.TemporalApiNexusV1_Link{
			type_: 'temporal.api.common.v1.Link.Activity'
			url:   'temporal:///namespaces/ns/activities/id/%FF/details'
		},
	] {
		if _ := sdk.common_link_from_nexus(invalid) {
			return error('malformed resource Nexus link accepted')
		}
	}
	return 'All Nexus resource link variants and request-ID references preserved escaped IDs, workflow reasons and ordered repeated callback components; malformed links were rejected'
}
