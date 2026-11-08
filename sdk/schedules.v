module sdk

import temporal
import time

pub struct ScheduleWorkflow {
pub:
	workflow_id               string
	workflow_type             string
	task_queue                string
	arguments                 Arguments = Arguments([]string{})
	execution_timeout_seconds i64
	run_timeout_seconds       i64
	task_timeout_seconds      i64 = 10
	retry                     ?RetryPolicy
	headers                   map[string]PayloadValue
	memo                      map[string]PayloadValue
	search_attributes         []SearchAttribute
	versioning_override       ?VersioningOverride
	priority                  ?temporal.TemporalApiCommonV1_Priority
	static_summary            string
	static_details            string
}

pub struct ScheduleDefinition {
pub:
	spec     temporal.TemporalApiScheduleV1_ScheduleSpec
	workflow ScheduleWorkflow
	policies temporal.TemporalApiScheduleV1_SchedulePolicies
	state    temporal.TemporalApiScheduleV1_ScheduleState
}

pub fn (client &Client) encode_schedule(definition ScheduleDefinition) !temporal.TemporalApiScheduleV1_Schedule {
	if definition.state.remaining_actions < 0 {
		return error('remaining schedule actions cannot be negative')
	}
	options := definition.workflow
	start := client.start_request(WorkflowStart{
		workflow_id:               options.workflow_id
		workflow_type:             options.workflow_type
		task_queue:                options.task_queue
		arguments:                 options.arguments
		execution_timeout_seconds: options.execution_timeout_seconds
		run_timeout_seconds:       options.run_timeout_seconds
		task_timeout_seconds:      options.task_timeout_seconds
		retry:                     options.retry
		headers:                   options.headers
		memo:                      options.memo
		search_attributes:         options.search_attributes
		versioning_override:       options.versioning_override
		priority:                  options.priority
		static_summary:            options.static_summary
		static_details:            options.static_details
		id_reuse:                  .allow_duplicate
	})!
	return temporal.TemporalApiScheduleV1_Schedule{
		spec:     definition.spec
		policies: definition.policies
		state:    definition.state
		action:   temporal.TemporalApiScheduleV1_ScheduleAction{
			action: temporal.TemporalApiScheduleV1_ScheduleAction_StartWorkflow_Case{
				value: temporal.TemporalApiWorkflowV1_NewWorkflowExecutionInfo{
					workflow_id:                start.workflow_id
					workflow_type:              start.workflow_type
					task_queue:                 start.task_queue
					input:                      start.input
					workflow_execution_timeout: start.workflow_execution_timeout
					workflow_run_timeout:       start.workflow_run_timeout
					workflow_task_timeout:      start.workflow_task_timeout
					retry_policy:               start.retry_policy
					memo:                       start.memo
					search_attributes:          start.search_attributes
					header:                     start.header
					user_metadata:              start.user_metadata
					versioning_override:        start.versioning_override
					priority:                   start.priority
				}
			}
		}
	}
}

pub struct ScheduleCreateOptions {
pub:
	definition        ScheduleDefinition
	memo              map[string]PayloadValue
	search_attributes []SearchAttribute
	initial_patch     ?temporal.TemporalApiScheduleV1_SchedulePatch
	request_id        string
}

pub fn (client &Client) create_schedule(id string, options ScheduleCreateOptions) !temporal.TemporalApiWorkflowserviceV1_CreateScheduleResponse {
	validate_version_name(id, 'schedule ID', true)!
	return client.rpc_create_schedule(temporal.TemporalApiWorkflowserviceV1_CreateScheduleRequest{
		namespace:         client.options.namespace
		schedule_id:       id
		identity:          client.options.transport.identity
		request_id:        if options.request_id != '' {
			options.request_id
		} else {
			random_request_id()!
		}
		schedule:          client.encode_schedule(options.definition)!
		initial_patch:     options.initial_patch
		memo:              temporal.TemporalApiCommonV1_Memo{
			fields: client.options.data_converter.encode_map(options.memo)!
		}
		search_attributes: search_attributes_proto(options.search_attributes)!
	})!
}

pub fn (client &Client) describe_schedule(id string) !temporal.TemporalApiWorkflowserviceV1_DescribeScheduleResponse {
	validate_version_name(id, 'schedule ID', true)!
	return client.rpc_describe_schedule(temporal.TemporalApiWorkflowserviceV1_DescribeScheduleRequest{
		namespace:   client.options.namespace
		schedule_id: id
	})!
}

// Keep the described protobuf intact when changing a schedule: existing payloads
// and unknown fields must survive without another codec pass.
pub struct ScheduleUpdate {
pub:
	schedule          temporal.TemporalApiScheduleV1_Schedule
	conflict_token    []u8
	memo              ?map[string]PayloadValue
	search_attributes ?[]SearchAttribute
	request_id        string
}

pub fn (client &Client) update_schedule(id string, options ScheduleUpdate) ! {
	validate_version_name(id, 'schedule ID', true)!
	mut request := temporal.TemporalApiWorkflowserviceV1_UpdateScheduleRequest{
		namespace:      client.options.namespace
		schedule_id:    id
		identity:       client.options.transport.identity
		request_id:     if options.request_id != '' {
			options.request_id
		} else {
			random_request_id()!
		}
		schedule:       options.schedule
		conflict_token: options.conflict_token
	}
	if memo := options.memo {
		request.memo = temporal.TemporalApiCommonV1_Memo{
			fields: client.options.data_converter.encode_map(memo)!
		}
	}
	if attributes := options.search_attributes {
		request.search_attributes = search_attributes_proto(attributes)!
	}
	client.rpc_update_schedule(request)!
}

pub fn (client &Client) delete_schedule(id string) ! {
	validate_version_name(id, 'schedule ID', true)!
	client.rpc_delete_schedule(temporal.TemporalApiWorkflowserviceV1_DeleteScheduleRequest{
		namespace:   client.options.namespace
		schedule_id: id
		identity:    client.options.transport.identity
	})!
}

pub fn (client &Client) patch_schedule(id string, patch temporal.TemporalApiScheduleV1_SchedulePatch) ! {
	validate_version_name(id, 'schedule ID', true)!
	client.rpc_patch_schedule(temporal.TemporalApiWorkflowserviceV1_PatchScheduleRequest{
		namespace:   client.options.namespace
		schedule_id: id
		identity:    client.options.transport.identity
		request_id:  random_request_id()!
		patch:       patch
	})!
}

pub fn (client &Client) pause_schedule(id string, note string) ! {
	client.patch_schedule(id, temporal.TemporalApiScheduleV1_SchedulePatch{
		pause: if note != '' {
			note
		} else {
			'Paused by Temporal V client'
		}
	})!
}

pub fn (client &Client) unpause_schedule(id string, note string) ! {
	client.patch_schedule(id, temporal.TemporalApiScheduleV1_SchedulePatch{
		unpause: if note != '' {
			note
		} else {
			'Unpaused by Temporal V client'
		}
	})!
}

pub struct ScheduleTriggerOptions {
pub:
	overlap        temporal.TemporalApiEnumsV1_ScheduleOverlapPolicy
	scheduled_time ?time.Time
}

pub fn (client &Client) trigger_schedule(id string, options ScheduleTriggerOptions) ! {
	mut trigger := temporal.TemporalApiScheduleV1_TriggerImmediatelyRequest{
		overlap_policy: options.overlap
	}
	if scheduled := options.scheduled_time {
		trigger.scheduled_time = schedule_timestamp(scheduled)
	}
	client.patch_schedule(id, temporal.TemporalApiScheduleV1_SchedulePatch{
		trigger_immediately: trigger
	})!
}

pub struct ScheduleBackfill {
pub:
	start   time.Time
	end     time.Time
	overlap temporal.TemporalApiEnumsV1_ScheduleOverlapPolicy
}

fn schedule_timestamp(value time.Time) temporal.GoogleProtobuf_Timestamp {
	return temporal.GoogleProtobuf_Timestamp{
		seconds: value.unix()
		nanos:   value.nanosecond
	}
}

pub fn (client &Client) backfill_schedule(id string, periods []ScheduleBackfill) ! {
	if periods.len == 0 { return error('at least one backfill period is required') }
	mut backfills := []temporal.TemporalApiScheduleV1_BackfillRequest{}
	for period in periods {
		if period.start >= period.end { return error('backfill end must follow start') }
		backfills << temporal.TemporalApiScheduleV1_BackfillRequest{
			start_time:     schedule_timestamp(period.start)
			end_time:       schedule_timestamp(period.end)
			overlap_policy: period.overlap
		}
	}
	client.patch_schedule(id, temporal.TemporalApiScheduleV1_SchedulePatch{
		backfill_request: backfills
	})!
}

pub struct ScheduleListOptions {
pub:
	page_size  int = 100
	page_token []u8
	query      string
}

pub fn (client &Client) list_schedules(options ScheduleListOptions) !temporal.TemporalApiWorkflowserviceV1_ListSchedulesResponse {
	if options.page_size < 1 { return error('schedule page size must be positive') }
	return client.rpc_list_schedules(temporal.TemporalApiWorkflowserviceV1_ListSchedulesRequest{
		namespace:         client.options.namespace
		maximum_page_size: options.page_size
		next_page_token:   options.page_token
		query:             options.query
	})!
}

pub fn (client &Client) schedule_matching_times(id string, start time.Time, end time.Time) ![]temporal.GoogleProtobuf_Timestamp {
	validate_version_name(id, 'schedule ID', true)!
	if start >= end { return error('matching-time end must follow start') }
	return client.rpc_list_schedule_matching_times(temporal.TemporalApiWorkflowserviceV1_ListScheduleMatchingTimesRequest{
		namespace:   client.options.namespace
		schedule_id: id
		start_time:  schedule_timestamp(start)
		end_time:    schedule_timestamp(end)
	})!.start_time
}
