module sdk

import temporal_v.temporal
import time

pub struct ActivityInfo {
pub:
	namespace                    string
	task_token                   []u8 @[json: '-']
	deadline                     ?time.Time
	workflow_type                string
	task_queue                   string
	scheduled_at                 ?time.Time
	current_attempt_scheduled_at ?time.Time
	started_at                   ?time.Time
	schedule_to_close            time.Duration
	start_to_close               time.Duration
	heartbeat_timeout            time.Duration
	retry_policy                 ?temporal.TemporalApiCommonV1_RetryPolicy
	priority                     ?temporal.TemporalApiCommonV1_Priority
	workflow_id                  string
	run_id                       string
	activity_id                  string
	activity_type                string
	attempt                      u32
	is_local                     bool
	is_standalone                bool
	workflow_namespace           string
	raw_heartbeat_details        []RawValue          @[json: '-']
	raw_headers                  map[string]RawValue @[json: '-']
}

pub fn (context &ActivityContext) info() ActivityInfo {
	return ActivityInfo{
		...context.information
		task_token:            context.task_token.clone()
		retry_policy:          owned_proto_temporalapicommonv1_retrypolicy(context.information.retry_policy)
		priority:              owned_proto_temporalapicommonv1_priority(context.information.priority)
		raw_headers:           context.raw_headers()
		raw_heartbeat_details: context.information.raw_heartbeat_details.clone()
	}
}

fn activity_timestamp(value ?temporal.GoogleProtobuf_Timestamp) ?time.Time {
	if timestamp := value {
		return time.unix(timestamp.seconds).add(timestamp.nanos * time.nanosecond)
	}
	return none
}

fn activity_duration(value ?temporal.GoogleProtobuf_Duration) time.Duration {
	if duration := value {
		return duration.seconds * time.second + duration.nanos * time.nanosecond
	}
	return 0
}

// raw_heartbeat_details returns owned codec/storage-decoded progress from the
// previous attempt, without invoking the payload converter.
pub fn (context &ActivityContext) raw_heartbeat_details() []RawValue {
	return context.information.raw_heartbeat_details.clone()
}

// heartbeat_values explicitly converts progress in the Activity result scope.
// Conversion errors belong to the caller; an opaque checkpoint does not prevent
// the Activity handler from running or reading its raw details.
pub fn (context &ActivityContext) heartbeat_values() ![]PayloadValue {
	mut values := []PayloadValue{}
	converter := context.result_converter()
	for raw in context.information.raw_heartbeat_details {
		values << converter.decode_raw_value(raw)!
	}
	return values
}

pub fn (context &ActivityContext) heartbeat_details() ![]string {
	mut values := []string{}
	for value in context.heartbeat_values()! {
		values << value.logical_data()
	}
	return values
}

// Match the SDK deadline contract: the earlier start-to-close or original
// schedule-to-close deadline. A zero schedule-to-close timeout is unlimited.
fn activity_deadline(start temporal.CoresdkActivityTask_Start) ?time.Time {
	scheduled := start.scheduled_time or { return none }
	started := start.started_time or { return none }
	start_timeout := start.start_to_close_timeout or { return none }
	schedule_timeout := start.schedule_to_close_timeout or { return none }
	first := activity_deadline_sum(started, start_timeout) or { return none }
	second := activity_deadline_sum(scheduled, schedule_timeout) or { return none }
	if schedule_timeout.seconds == 0 && schedule_timeout.nanos == 0 { return first }
	if second.unix() < first.unix() || (second.unix() == first.unix()
		&& second.nanosecond < first.nanosecond) {
		return second
	}
	return first
}

fn activity_deadline_sum(timestamp temporal.GoogleProtobuf_Timestamp, duration temporal.GoogleProtobuf_Duration) ?time.Time {
	if timestamp.seconds < -62135596800 || timestamp.seconds > 253402300799 || timestamp.nanos < 0
		|| timestamp.nanos >= 1000000000 {
		return none
	}
	if duration.seconds < 0 || duration.seconds > 315576000000 || duration.nanos < 0
		|| duration.nanos >= 1000000000 {
		return none
	}
	// Add seconds/nanos separately; a nanosecond count would overflow for long
	// timeouts or dates outside the Unix-nanosecond range.
	nanos := i64(timestamp.nanos) + i64(duration.nanos)
	seconds := timestamp.seconds + duration.seconds + nanos / 1000000000
	return time.unix(seconds).add((nanos % 1000000000) * time.nanosecond)
}

// Application headers after codec/storage decoding, before payload conversion.
pub fn (context &ActivityContext) raw_headers() map[string]RawValue {
	return context.information.raw_headers.clone()
}

pub fn (context &ActivityContext) header_values() !map[string]PayloadValue {
	mut result := map[string]PayloadValue{}
	mut keys := context.information.raw_headers.keys()
	keys.sort()
	for key in keys {
		result[key] = context.converter.decode_raw_value(context.information.raw_headers[key])!
	}
	return result
}

// WorkflowHandle contains execution IDs; use client() for a client in the worker
// namespace. Standalone Activities have no parent workflow execution.
pub fn (context &ActivityContext) workflow_handle() ?WorkflowHandle {
	if context.information.is_standalone || context.information.workflow_id == '' { return none }
	return WorkflowHandle{
		workflow_id: context.information.workflow_id
		run_id:      context.information.run_id
	}
}
