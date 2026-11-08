module sdk

import temporal_v.temporal
import math
import encoding.utf8

pub struct WorkerDeploymentVersion {
pub:
	deployment_name string
	build_id        string
}

pub struct PinnedVersioningOverride {
pub:
	version WorkerDeploymentVersion
}

pub struct AutoUpgradeVersioningOverride {}

pub struct OneTimeVersioningOverride {
pub:
	version WorkerDeploymentVersion
}

pub type VersioningOverride = AutoUpgradeVersioningOverride
	| OneTimeVersioningOverride
	| PinnedVersioningOverride

fn validate_version_name(value string, name string, required bool) ! {
	if (required && value == '') || value.contains('\x00') || !utf8.validate_str(value) {
		return error('${name} must be valid UTF-8 without NUL' + if required {
			' and nonempty'
		} else {
			''
		})
	}
}

fn (version WorkerDeploymentVersion) proto() !temporal.TemporalApiDeploymentV1_WorkerDeploymentVersion {
	validate_version_name(version.deployment_name, 'deployment name', true)!
	validate_version_name(version.build_id, 'deployment build ID', true)!
	return temporal.TemporalApiDeploymentV1_WorkerDeploymentVersion{
		deployment_name: version.deployment_name
		build_id:        version.build_id
	}
}

fn versioning_override_proto(value VersioningOverride) !temporal.TemporalApiWorkflowV1_VersioningOverride {
	return match value {
		PinnedVersioningOverride {
			temporal.TemporalApiWorkflowV1_VersioningOverride{
				override: temporal.TemporalApiWorkflowV1_VersioningOverride_Pinned_Case{
					value: temporal.TemporalApiWorkflowV1_VersioningOverride_PinnedOverride{
						behavior: .pinned_override_behavior_pinned
						version:  value.version.proto()!
					}
				}
			}
		}
		AutoUpgradeVersioningOverride {
			temporal.TemporalApiWorkflowV1_VersioningOverride{
				override: temporal.TemporalApiWorkflowV1_VersioningOverride_AutoUpgrade_Case{
					value: true
				}
			}
		}
		OneTimeVersioningOverride {
			temporal.TemporalApiWorkflowV1_VersioningOverride{
				override: temporal.TemporalApiWorkflowV1_VersioningOverride_OneTime_Case{
					value: temporal.TemporalApiWorkflowV1_VersioningOverride_OneTimeOverride{
						target_deployment_version: value.version.proto()!
					}
				}
			}
		}
	}
}

// The mask makes a versioning change independent of priority/time-skipping fields.
pub fn (client &Client) set_versioning_override(handle WorkflowHandle, value VersioningOverride) !temporal.TemporalApiWorkflowV1_WorkflowExecutionOptions {
	return client.apply_versioning_override(handle, versioning_override_proto(value)!)!
}

pub fn (client &Client) clear_versioning_override(handle WorkflowHandle) !temporal.TemporalApiWorkflowV1_WorkflowExecutionOptions {
	return client.apply_versioning_override(handle, none)!
}

fn (client &Client) apply_versioning_override(handle WorkflowHandle, value ?temporal.TemporalApiWorkflowV1_VersioningOverride) !temporal.TemporalApiWorkflowV1_WorkflowExecutionOptions {
	response := client.rpc_update_workflow_execution_options(temporal.TemporalApiWorkflowserviceV1_UpdateWorkflowExecutionOptionsRequest{
		namespace:                  client.options.namespace
		workflow_execution:         execution(handle)!
		identity:                   client.options.transport.identity
		workflow_execution_options: temporal.TemporalApiWorkflowV1_WorkflowExecutionOptions{
			versioning_override: value
		}
		update_mask:                temporal.GoogleProtobuf_FieldMask{
			paths: ['versioning_override']
		}
	})!
	return response.workflow_execution_options or {
		return error('server omitted updated workflow execution options')
	}
}

pub struct DeploymentRoutingUpdate {
pub:
	deployment_name string
	// Empty selects unversioned workers, as defined by the service API.
	build_id                   string
	conflict_token             []u8
	ignore_missing_task_queues bool
	allow_no_pollers           bool
}

fn (options DeploymentRoutingUpdate) validate() ! {
	validate_version_name(options.deployment_name, 'deployment name', true)!
	validate_version_name(options.build_id, 'deployment build ID', false)!
}

pub fn (client &Client) describe_worker_deployment(name string) !temporal.TemporalApiWorkflowserviceV1_DescribeWorkerDeploymentResponse {
	validate_version_name(name, 'deployment name', true)!
	return client.rpc_describe_worker_deployment(temporal.TemporalApiWorkflowserviceV1_DescribeWorkerDeploymentRequest{
		namespace:       client.options.namespace
		deployment_name: name
	})!
}

pub fn (client &Client) set_current_deployment(options DeploymentRoutingUpdate) ![]u8 {
	options.validate()!
	response := client.rpc_set_worker_deployment_current_version(temporal.TemporalApiWorkflowserviceV1_SetWorkerDeploymentCurrentVersionRequest{
		namespace:                  client.options.namespace
		deployment_name:            options.deployment_name
		build_id:                   options.build_id
		conflict_token:             options.conflict_token
		identity:                   client.options.transport.identity
		ignore_missing_task_queues: options.ignore_missing_task_queues
		allow_no_pollers:           options.allow_no_pollers
	})!
	return response.conflict_token
}

pub fn (client &Client) set_ramping_deployment(options DeploymentRoutingUpdate, percentage f32) ![]u8 {
	options.validate()!
	if math.is_nan(f64(percentage)) || math.is_inf(f64(percentage), 0) || percentage < 0
		|| percentage > 100 {
		return error('deployment ramp percentage must be finite and between zero and 100')
	}
	response := client.rpc_set_worker_deployment_ramping_version(temporal.TemporalApiWorkflowserviceV1_SetWorkerDeploymentRampingVersionRequest{
		namespace:                  client.options.namespace
		deployment_name:            options.deployment_name
		build_id:                   options.build_id
		percentage:                 percentage
		conflict_token:             options.conflict_token
		identity:                   client.options.transport.identity
		ignore_missing_task_queues: options.ignore_missing_task_queues
		allow_no_pollers:           options.allow_no_pollers
	})!
	return response.conflict_token
}

pub fn (client &Client) describe_worker_deployment_version(version WorkerDeploymentVersion) !temporal.TemporalApiWorkflowserviceV1_DescribeWorkerDeploymentVersionResponse {
	return client.rpc_describe_worker_deployment_version(temporal.TemporalApiWorkflowserviceV1_DescribeWorkerDeploymentVersionRequest{
		namespace:          client.options.namespace
		deployment_version: version.proto()!
	})!
}
