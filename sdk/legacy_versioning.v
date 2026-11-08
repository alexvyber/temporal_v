module sdk

import temporal_v.temporal

// Compatibility sets are the service's legacy build-ID versioning protocol.
// The namespace must explicitly enable the corresponding server APIs.
pub struct NewBuildIdSet {
pub:
	build_id string
}

pub struct CompatibleBuildId {
pub:
	build_id          string
	existing_build_id string
	make_set_default  bool
}

pub struct PromoteBuildIdSet {
pub:
	build_id string
}

pub struct PromoteBuildId {
pub:
	build_id string
}

pub struct MergeBuildIdSets {
pub:
	primary_build_id   string
	secondary_build_id string
}

pub type BuildIdCompatibilityUpdate = NewBuildIdSet
	| CompatibleBuildId
	| PromoteBuildIdSet
	| PromoteBuildId
	| MergeBuildIdSets

pub fn (client &Client) update_build_id_compatibility(task_queue string, update BuildIdCompatibilityUpdate) ! {
	validate_version_name(task_queue, 'task queue', true)!
	operation := match update {
		NewBuildIdSet {
			validate_version_name(update.build_id, 'build ID', true)!
			temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_Operation(temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet_Case{
				value: update.build_id
			})
		}
		CompatibleBuildId {
			validate_version_name(update.build_id, 'build ID', true)!
			validate_version_name(update.existing_build_id, 'existing build ID', true)!
			temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_Operation(temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId_Case{
				value: temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
					new_build_id:                 update.build_id
					existing_compatible_build_id: update.existing_build_id
					make_set_default:             update.make_set_default
				}
			})
		}
		PromoteBuildIdSet {
			validate_version_name(update.build_id, 'build ID', true)!
			temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_Operation(temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_PromoteSetByBuildId_Case{
				value: update.build_id
			})
		}
		PromoteBuildId {
			validate_version_name(update.build_id, 'build ID', true)!
			temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_Operation(temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_PromoteBuildIdWithinSet_Case{
				value: update.build_id
			})
		}
		MergeBuildIdSets {
			validate_version_name(update.primary_build_id, 'primary build ID', true)!
			validate_version_name(update.secondary_build_id, 'secondary build ID', true)!
			temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_Operation(temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_MergeSets_Case{
				value: temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest_MergeSets{
					primary_set_build_id:   update.primary_build_id
					secondary_set_build_id: update.secondary_build_id
				}
			})
		}
	}

	client.rpc_update_worker_build_id_compatibility(temporal.TemporalApiWorkflowserviceV1_UpdateWorkerBuildIdCompatibilityRequest{
		namespace:  client.options.namespace
		task_queue: task_queue
		operation:  operation
	})!
}

pub fn (client &Client) build_id_compatibility(task_queue string, max_sets int) !temporal.TemporalApiWorkflowserviceV1_GetWorkerBuildIdCompatibilityResponse {
	validate_version_name(task_queue, 'task queue', true)!
	if max_sets < 0 { return error('maximum compatibility sets cannot be negative') }
	return client.rpc_get_worker_build_id_compatibility(temporal.TemporalApiWorkflowserviceV1_GetWorkerBuildIdCompatibilityRequest{
		namespace:  client.options.namespace
		task_queue: task_queue
		max_sets:   max_sets
	})!
}

pub struct BuildIdReachabilityOptions {
pub:
	build_ids    []string
	task_queues  []string
	reachability temporal.TemporalApiEnumsV1_TaskReachability
}

pub fn (client &Client) build_id_reachability(options BuildIdReachabilityOptions) !temporal.TemporalApiWorkflowserviceV1_GetWorkerTaskReachabilityResponse {
	if options.build_ids.len == 0 { return error('at least one build ID is required') }
	for build in options.build_ids {
		validate_version_name(build, 'build ID', false)!
	}
	for queue in options.task_queues {
		validate_version_name(queue, 'task queue', true)!
	}
	return client.rpc_get_worker_task_reachability(temporal.TemporalApiWorkflowserviceV1_GetWorkerTaskReachabilityRequest{
		namespace:    client.options.namespace
		build_ids:    options.build_ids
		task_queues:  options.task_queues
		reachability: options.reachability
	})!
}
