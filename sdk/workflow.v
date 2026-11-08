module sdk

import temporal
import time

pub enum EventKind {
	started
	cancel_requested
	activity_completed
	activity_failed
	activity_cancelled
	timer_fired
	signal_received
	child_started
	child_completed
	child_failed
	child_cancelled
	update_received
	external_signal_completed
	external_signal_failed
	external_cancel_completed
	nexus_started
	nexus_completed
	nexus_failed
	nexus_cancelled
	nexus_timed_out
	external_cancel_failed
	condition_satisfied
	condition_cancelled
	condition_timed_out
}

pub struct Event {
pub:
	typed_result         ?PayloadValue  @[json: '-']
	typed_arguments      []PayloadValue @[json: '-']
	raw_result           ?RawValue      @[json: '-']
	raw_arguments        []RawValue
	sender_identity      string
	originating_event_id i64
	arguments            []string
	kind                 EventKind
	sequence             u32
	data                 string
	message              string
	name                 string
	id                   string
	failure              ?Failure
	headers              map[string]temporal.TemporalApiCommonV1_Payload
}

pub interface Workflow {
mut:
	on_event(event Event, mut context WorkflowContext) !
}

pub interface QueryableWorkflow {
	query(name string, arguments []string, view WorkflowContextView) !PayloadValue
}

pub interface UpdatableWorkflow {
	update_names() []string
	validate_update(name string, arguments []string, view WorkflowContextView) !
}

pub type WorkflowFactory = fn (WorkflowContextView, []PayloadValue) !Workflow

pub struct ActivityOptions {
pub:
	event_groups            []EventGroup
	activity_id             string
	summary                 string
	versioning_intent       temporal.CoresdkCommon_VersioningIntent
	priority                ?temporal.TemporalApiCommonV1_Priority
	disable_eager_execution bool
	headers                 map[string]PayloadValue
	start_to_close          time.Duration = 10 * time.second
	schedule_to_close       time.Duration
	schedule_to_start       time.Duration
	heartbeat_timeout       time.Duration
	task_queue              string
	retry                   RetryPolicy
	cancellation            ActivityCancellation
	scope                   u32
}

pub struct WorkflowContext {
	patch_activation_callback ?PatchActivationCallback
	event_groups_enabled      bool
	metrics                   &MetricMeter       = &MetricMeter{}
	metric_replay             &MetricReplayState = &MetricReplayState{}
mut:
	command_guard                            &WorkflowCommandGuard = &WorkflowCommandGuard{}
	next_condition                           u32
	conditions                               map[u32]PendingCondition
	condition_timers                         map[u32]u32
	condition_polling                        bool
	deferred_condition_events                []ConditionResolution
	context_values                           WorkflowContextValues
	command_context_values                   []WorkflowContextValues
	event_group_state                        EventGroupState
	local_activity_context_values            map[u32]WorkflowContextValues
	local_activity_event_groups              map[u32]EventGroupState
	tracing                                  ?TraceOptions
	trace_root                               ?&TraceSpan
	trace_baggage                            Baggage
	trace_current                            ?TraceContext
	trace_next_event                         u64
	trace_next_command                       u64
	recorded_value_error                     ?IError
	forced_task_error                        ?WorkflowTaskError
	next_side_effect_id                      i64
	last_workflow_marker_event_id            i64
	side_effect_markers                      map[i64]RecordedWorkflowMarker
	mutable_side_effect_calls                map[string]i64
	mutable_side_effect_markers              map[string]map[i64]RecordedWorkflowMarker
	mutable_side_effect_values               map[string][]temporal.TemporalApiCommonV1_Payload
	current_task_deployment_version          ?temporal.CoresdkCommon_WorkerDeploymentVersion
	last_sdk_version                         string
	target_worker_deployment_version_changed bool

	activity_contexts               map[u32]SerializationContext
	child_contexts                  map[u32]SerializationContext
	nexus_contexts                  map[u32]SerializationContext
	memo_values                     map[string]temporal.TemporalApiCommonV1_Payload
	search_attribute_values         map[string]temporal.TemporalApiCommonV1_Payload
	local_activity_metadata         map[u32]temporal.TemporalApiSdkV1_UserMetadata
	local_activity_sequences        map[u32]u32
	queue                           string
	next_activity                   u32
	next_timer                      u32
	activities                      map[u32]bool
	timers                          map[u32]bool
	commands                        []temporal.CoresdkWorkflowCommands_WorkflowCommand
	closed                          bool
	namespace                       string
	current_details                 string
	workflow_id                     string
	workflow_type                   string
	run_id                          string
	workflow_time                   time.Time = time.unix(0)
	history_length                  u32
	history_size_bytes              u64
	continue_as_new_suggested       bool
	suggest_continue_as_new_reasons []temporal.TemporalApiEnumsV1_SuggestContinueAsNewReason
	next_child                      u32
	children                        map[u32]bool
	child_cancellations             map[u32]bool
	initialization                  temporal.CoresdkWorkflowActivation_InitializeWorkflow
	updates                         map[string]PendingWorkflowUpdate
	known_patches                   map[string]bool
	recorded_versions               map[string]int
	retired_version_changes         map[string]bool
	patch_decisions                 map[string]bool
	deprecated_patches              map[string]bool
	replaying                       bool
	replaying_history               bool
	cancelled_timers                map[u32]bool
	next_external                   u32
	external_operations             map[u32]string
	random_state                    u64
	random_seed                     u64
	random_streams                  &WorkflowRandomStreams = unsafe { nil }
	local_activities                map[u32]temporal.CoresdkWorkflowCommands_ScheduleLocalActivity
	local_backoffs                  map[u32]u32
	activity_cancellations          map[u32]bool
	deferred_events                 []Event
	converter                       DataConverter
	logger                          Logger = ConsoleLogger{}
	next_nexus                      u32
	nexus_operations                map[u32]bool
	nexus_cancellations             map[u32]bool
	next_scope                      u32
	scopes                          map[u32]ScopeState
	activity_scopes                 map[u32]u32
	timer_scopes                    map[u32]u32
	child_scopes                    map[u32]u32
	nexus_scopes                    map[u32]u32
	external_scopes                 map[u32]u32
	cancellation_requested          bool
}

fn (context &WorkflowContext) ensure_open() ! {
	context.ensure_writable()!
	if context.closed { return error('workflow is already closed') }
}

pub fn (mut context WorkflowContext) activity(name string, arguments Arguments, options ActivityOptions) !u32 {
	context.ensure_open()!
	if name == '' || options.start_to_close < 0 || options.schedule_to_close < 0
		|| options.schedule_to_start < 0 || options.heartbeat_timeout < 0
		|| (options.start_to_close == 0 && options.schedule_to_close == 0) {
		return error('invalid Activity options')
	}
	context.ensure_scope(options.scope)!
	sequence := context.next_activity + 1
	activity_id := if options.activity_id == '' {
		'activity-${sequence}'
	} else {
		options.activity_id
	}
	queue := if options.task_queue == '' { context.queue } else { options.task_queue }
	converter := context.activity_converter(name, activity_id, queue, false)
	encoded := converter.encode_arguments(arguments)!
	metadata := converter.command_metadata(options.summary, '')!
	groups := context.command_event_groups(options.event_groups)!
	mut command := temporal.CoresdkWorkflowCommands_ScheduleActivity{
		activity_type:          name
		task_queue:             if options.task_queue == '' {
			context.queue
		} else {
			options.task_queue
		}
		arguments:              encoded
		headers:                converter.encode_headers(options.headers)!
		retry_policy:           options.retry.encode()!
		cancellation_type:      options.cancellation.encode()
		do_not_eagerly_execute: options.disable_eager_execution
		versioning_intent:      options.versioning_intent
		priority:               options.priority
	}
	if options.start_to_close > 0 {
		command.start_to_close_timeout = duration_proto(options.start_to_close)!
	}
	if options.schedule_to_close > 0 {
		command.schedule_to_close_timeout = duration_proto(options.schedule_to_close)!
	}
	if options.schedule_to_start > 0 {
		command.schedule_to_start_timeout = duration_proto(options.schedule_to_start)!
	}
	if options.heartbeat_timeout > 0 {
		command.heartbeat_timeout = duration_proto(options.heartbeat_timeout)!
	}
	context.next_activity = sequence
	context.activity_contexts[sequence] = converter.serialization_context
	context.activities[sequence] = true
	context.activity_scopes[sequence] = options.scope
	command.seq = sequence
	command.activity_id = activity_id
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		user_metadata:       metadata.value
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleActivity_Case{
			value: command
		}
	})
	return sequence
}

pub fn (mut context WorkflowContext) complete(value PayloadValue) ! {
	context.ensure_open()!
	result := context.result_converter().encode_payload_values([value])![0]
	context.stop_pending(false, true)
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_CompleteWorkflowExecution_Case{
			value: temporal.CoresdkWorkflowCommands_CompleteWorkflowExecution{
				result: result
			}
		}
	})
	context.closed = true
}

pub fn (mut context WorkflowContext) fail(message string, category string) ! {
	context.ensure_open()!
	encoded := context.result_converter().encode_failure(ApplicationError{
		message:       message
		category:      category
		non_retryable: true
	}, category)!
	context.finish_with_failure(encoded)
}

fn (mut context WorkflowContext) stop_pending(cancel_children bool, warn_unfinished_updates bool) {
	mut nexus := context.nexus_operations.keys()
	nexus.sort()
	for sequence in nexus {
		context.cancel_nexus(sequence) or {}
	}
	mut external_sequences := context.external_operations.keys()
	external_sequences.sort()
	for sequence in external_sequences {
		if context.external_operations[sequence] == 'signal' {
			context.cancel_signal(sequence) or {}
		}
	}
	mut children := context.children.keys()
	children.sort()
	if cancel_children {
		for sequence in children {
			context.cancel_child(sequence, 'parent cancelled') or {}
		}
	}
	context.abandon_pending_updates(warn_unfinished_updates)
	mut activities := context.activities.keys()
	activities.sort()
	for sequence in activities {
		context.cancel_activity(sequence) or {}
	}
	mut timers := context.timers.keys()
	timers.sort()
	for sequence in timers {
		context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
			variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelTimer_Case{
				value: temporal.CoresdkWorkflowCommands_CancelTimer{
					seq: sequence
				}
			}
		})
	}
}

@[params]
pub struct WorkflowCancelOptions {
pub:
	event_groups []EventGroup
	details      Arguments = Arguments([]string{})
}

pub fn (mut context WorkflowContext) cancel(options WorkflowCancelOptions) ! {
	context.ensure_writable()!
	if context.closed { return }
	groups := context.command_event_groups(options.event_groups)!
	encoded := context.result_converter().encode_arguments(options.details)!
	mut command := temporal.CoresdkWorkflowCommands_CancelWorkflowExecution{}
	if encoded.len > 0 {
		command.details = temporal.TemporalApiCommonV1_Payloads{
			payloads: encoded
		}
	}
	context.stop_pending(true, true)
	context.queue_command(temporal.CoresdkWorkflowCommands_WorkflowCommand{
		event_group_markers: groups.markers
		variant:             temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelWorkflowExecution_Case{
			value: command
		}
	})
	context.closed = true
}

pub fn (context &WorkflowContext) headers() !map[string]PayloadValue {
	return context.converter.decode_headers(context.initialization.headers)!
}

// Details are reconstructed by workflow code during replay and exposed to Temporal
// UI through the reserved metadata query. They must not contain secrets.
pub fn (mut context WorkflowContext) set_current_details(details string) ! {
	context.ensure_open()!
	context.current_details = details
}

pub fn (context &WorkflowContext) current_details() string {
	return context.current_details
}

// now returns the recorded workflow clock, including its nanoseconds. It is
// constant within an activation and reconstructed from history during replay.
pub fn (context &WorkflowContext) now() time.Time {
	return context.workflow_time
}

// Decode headers attached to a workflow event using this execution's converter.
pub fn (context &WorkflowContext) event_headers(event Event) !map[string]PayloadValue {
	return context.converter.decode_headers(event.headers)!
}

// Use only to suppress nondurable diagnostics. Durable commands must be the
// same during replay and live execution.
pub fn (context &WorkflowContext) is_replaying() bool {
	return context.replaying
}

// is_replaying_history_events excludes query-only activations from replay-aware
// observability suppression. is_replaying() retains Core's activation replay flag.
pub fn (context &WorkflowContext) is_replaying_history_events() bool {
	return context.replaying_history
}

fn (context &WorkflowContext) ensure_writable() ! {
	if context.command_guard.reason != '' {
		return WorkflowTaskError{
			message: 'workflow commands are forbidden in ' + context.command_guard.reason
		}
	}
}

struct WorkflowCommandGuard {
mut:
	reason string
}
