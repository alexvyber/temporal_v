module sdk

import alexvyber.temporal_v.temporal

// Snapshots can be retained after a read-only callback returns.
fn owned_workflow_info(value WorkflowInfo) WorkflowInfo {
	return WorkflowInfo{
		...value
		context_values:                  WorkflowContextValues{
			entries: value.context_values.entries.clone()
		}
		suggest_continue_as_new_reasons: value.suggest_continue_as_new_reasons.clone()
		current_task_deployment_version: owned_proto_coresdkcommon_workerdeploymentversion(value.current_task_deployment_version)
		execution_timeout:               owned_proto_googleprotobuf_duration(value.execution_timeout)
		run_timeout:                     owned_proto_googleprotobuf_duration(value.run_timeout)
		task_timeout:                    owned_proto_googleprotobuf_duration(value.task_timeout)
		start_time:                      owned_proto_googleprotobuf_timestamp(value.start_time)
		execution_expiration_time:       owned_proto_googleprotobuf_timestamp(value.execution_expiration_time)
		parent_workflow:                 owned_proto_coresdkcommon_namespacedworkflowexecution(value.parent_workflow)
		root_workflow:                   owned_proto_temporalapicommonv1_workflowexecution(value.root_workflow)
		retry_policy:                    owned_proto_temporalapicommonv1_retrypolicy(value.retry_policy)
		priority:                        owned_proto_temporalapicommonv1_priority(value.priority)
	}
}
