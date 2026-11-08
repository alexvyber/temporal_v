module sdk

import temporal

// Snapshots can be retained after a read-only callback returns.
fn owned_workflow_info(value WorkflowInfo) WorkflowInfo {
	return WorkflowInfo{
		...value
		context_values:                  WorkflowContextValues{
			entries: value.context_values.entries.clone()
		}
		suggest_continue_as_new_reasons: value.suggest_continue_as_new_reasons.clone()
		current_task_deployment_version: owned_optional_proto[temporal.CoresdkCommon_WorkerDeploymentVersion](value.current_task_deployment_version)
		execution_timeout:               owned_optional_proto[temporal.GoogleProtobuf_Duration](value.execution_timeout)
		run_timeout:                     owned_optional_proto[temporal.GoogleProtobuf_Duration](value.run_timeout)
		task_timeout:                    owned_optional_proto[temporal.GoogleProtobuf_Duration](value.task_timeout)
		start_time:                      owned_optional_proto[temporal.GoogleProtobuf_Timestamp](value.start_time)
		execution_expiration_time:       owned_optional_proto[temporal.GoogleProtobuf_Timestamp](value.execution_expiration_time)
		parent_workflow:                 owned_optional_proto[temporal.CoresdkCommon_NamespacedWorkflowExecution](value.parent_workflow)
		root_workflow:                   owned_optional_proto[temporal.TemporalApiCommonV1_WorkflowExecution](value.root_workflow)
		retry_policy:                    owned_optional_proto[temporal.TemporalApiCommonV1_RetryPolicy](value.retry_policy)
		priority:                        owned_optional_proto[temporal.TemporalApiCommonV1_Priority](value.priority)
	}
}
