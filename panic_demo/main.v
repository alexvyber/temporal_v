module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import os
import x.json2
import time
import alexvyber.temporal_v.example_converter

struct PanicInput {
	mode     string
	endpoint string
}

struct PanicWorkflow {
mut:
	mode           string
	count          int
	nexus_sequence u32
}

fn fixed() bool {
	return os.getenv('V_PANIC_FIXED') == 'true'
}

@[noinline]
fn panic_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	if os.getenv('V_PANIC_FACTORY') == 'true' && !fixed() { panic('factory panic') }
	return PanicWorkflow{}
}

@[noinline]
fn (mut workflow PanicWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		input := json2.decode[PanicInput](event.data)!
		workflow.mode = input.mode
		if workflow.mode.starts_with('nexus-') {
			workflow.nexus_sequence = context.nexus_operation(sdk.PayloadValue('null'), sdk.NexusOptions{
				endpoint:  input.endpoint
				service:   'v.panic'
				operation: workflow.mode[6..]
			})!
		}
		if workflow.mode == 'start' && !fixed() { panic('start panic') }
		if workflow.mode == 'command' { context.timer(time.millisecond)! }
		if workflow.mode.starts_with('activity-') || workflow.mode.starts_with('local-') {
			if workflow.mode.starts_with('local-') {
				context.local_activity('v.panic.activity', json2.encode(workflow.mode), sdk.LocalActivityOptions{
					retry: sdk.RetryPolicy{
						maximum_attempts: 2
						initial_interval: 10 * time.millisecond
					}
				})!
			} else {
				context.activity('v.panic.activity', json2.encode(workflow.mode), sdk.ActivityOptions{
					retry: sdk.RetryPolicy{
						maximum_attempts: 2
						initial_interval: 10 * time.millisecond
					}
				})!
			}
		}
		if workflow.mode == 'start' || workflow.mode == 'factory' {
			context.complete('"repaired"')!
		}
	}
	if event.kind == .signal_received {
		if event.name == 'panic' {
			workflow.count++
			if !fixed() { panic('signal panic') }
		}
		if event.name == 'finish' { context.complete(workflow.count.str())! }
		if event.name == 'cancel-nexus' { context.cancel_nexus(workflow.nexus_sequence)! }
	}
	if event.kind == .update_received {
		workflow.count++
		if event.name == 'handler-panic' && !fixed() { panic('update handler panic') }
		context.complete_update(event.id, workflow.count.str())!
	}
	if event.kind in [.activity_completed, .nexus_completed] { context.complete(event.data)! }
	if event.kind == .nexus_cancelled { context.complete('"nexus-canceled"')! }
	if event.kind == .activity_failed {
		if failure := event.failure {
			context.fail_application(sdk.ApplicationError{
				message:  failure.root_message()
				category: failure.category
			})!
		}
	}
	if event.kind == .timer_fired { context.complete('"command-recovered"')! }
	if event.kind == .cancel_requested { context.cancel()! }
}

@[noinline]
fn (workflow PanicWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name == 'panic' { panic('query panic') }
	return sdk.PayloadValue(workflow.count.str())
}

fn (workflow PanicWorkflow) update_names() []string {
	return ['validator-panic', 'handler-panic', 'normal']
}

@[noinline]
fn (workflow PanicWorkflow) validate_update(name string, arguments []string, view sdk.WorkflowContextView) ! {
	if name == 'validator-panic' { panic('validator panic') }
}

@[noinline]
fn panic_activity(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	defer {
		if path := os.getenv_opt('V_PANIC_DEFER_LOG') {
			mut file := os.open_append(path) or { panic(err) }
			file.writeln(context.info().activity_id + ':' + context.info().attempt.str()) or {
				panic(err)
			}
			file.close()
		}
	}
	mode := json2.decode[string](sdk.single_argument(input)!)!
	if mode == 'activity-hook-drain' && os.getenv('V_WORKER_HOOK_DRAIN') == 'true' {
		return hook_draining_activity(mut context)!
	}
	if mode.ends_with('-error') {
		return sdk.ApplicationError{
			message: 'failure converter input'
		}
	}
	if mode.ends_with('-store') || mode.ends_with('-retrieve') {
		return sdk.PayloadValue('"storage-output"')
	}
	if mode.ends_with('-input') || mode.ends_with('-output') {
		return sdk.PayloadValue('"converter-output"')
	}
	if mode.ends_with('fail') || context.info().attempt == 1 { panic('Activity panic') }
	return sdk.PayloadValue('"activity-recovered"')
}

fn runtime() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register_dynamic(raw_start_factory)!
	runtime.register('v.raw-workflow.named', named_start_factory)!
	if os.getenv('V_RAW_WORKFLOW_REPAIR') != 'true' {
		runtime.register('v.raw-workflow.bad', named_start_factory)!
	}
	runtime.register('v.panic', panic_factory)!
	runtime.register('v.raw-activities', raw_activities_factory)!
	runtime.register('v.activity-client', activity_client_factory)!
	runtime.register('v.activity-deadline', activity_metadata_factory)!
	runtime.register('v.event-group-wire', event_group_wire_factory)!
	runtime.register('v.event-groups', event_groups_factory)!
	runtime.register('v.context-values', context_values_factory)!
	runtime.register('v.conditions', conditions_factory)!
	runtime.register('v.raw-headers', raw_headers_factory)!
	runtime.register('v.workflow-headers', workflow_headers_factory)!
	runtime.register('v.patch-policy', patch_policy_factory)!
	runtime.register('v.activity-wait', activity_wait_factory)!
	runtime.register('v.initializer', initializer_factory)!
	runtime.register_local_activity('v.activity-wait', activity_wait_handler)!
	runtime.register('v.activation-packed', packing_factory)!
	runtime.register('v.activation-packed.child', packing_child_factory)!
	runtime.register_local_activity('v.raw-headers.named', raw_header_handler)!
	runtime.register('v.event-group.child', event_group_child_factory)!
	runtime.register('v.event-group.receiver', event_group_receiver_factory)!
	runtime.register_local_activity('v.event-group.echo', event_group_echo)!
	runtime.register_local_activity('v.activity-deadline', activity_metadata_handler)!
	runtime.register('v.activity-client.child', activity_client_child_factory)!
	runtime.register_local_activity('v.activity-client', activity_client_handler)!
	runtime.register('v.raw-heartbeats', raw_heartbeat_factory)!
	runtime.register_local_activity('v.raw.named', raw_named_activity)!
	runtime.register_dynamic_local_activity(raw_dynamic_activity)!
	runtime.register('v.forced-task', forced_factory)!
	runtime.register('v.random-stream', random_stream_factory)!
	runtime.register('v.raw-interactions', raw_interactions_factory)!
	runtime.register('v.named-raw-interactions', named_raw_interactions_factory)!
	runtime.register('v.bad-raw-query', duplicate_raw_query_factory)!
	runtime.register('v.bad-raw-signal', empty_raw_signal_factory)!
	runtime.register('v.ambiguous-raw-query', ambiguous_raw_query_factory)!
	runtime.register('v.update-lifecycle', handler_lifecycle_factory)!
	runtime.register('v.numeric-version', numeric_version_factory)!
	runtime.register('v.dynamic-updates', named_dynamic_factory)!
	runtime.register('v.dynamic-only', dynamic_only_factory)!
	runtime.register_local_activity('v.version.echo', version_echo)!
	runtime.register_local_activity('v.panic.activity', panic_activity)!
	return runtime
}

fn run() ! {
	if os.args.len > 1 && os.args[1] == 'hook-unit' {
		check_worker_hooks()!
		return
	}
	if os.args[1] == 'version-validation' {
		validate_numeric_versions()!
		return
	}
	config := sdk.options_from_env()!
	original_converter := panic_converter()!
	base_converter := if os.getenv('V_WORKFLOW_HEADER_CONVERTER') == 'true' {
		sdk.DataConverter{
			...original_converter
			payload_converter: WorkflowHeaderConverter{}
		}
	} else if os.getenv('V_RAW_HEADER_CONVERTER') == 'true' {
		sdk.DataConverter{
			...original_converter
			payload_converter: RawHeaderConverter{}
		}
	} else {
		original_converter
	}
	before_packing := if os.getenv('V_GROUP_POISON') == 'true' {
		sdk.DataConverter{
			...base_converter
			payload_converter: GroupPoisonConverter{}
		}
	} else {
		base_converter
	}
	converter := activation_fixture_converter(before_packing)
	if os.args[1] == 'activity-wait-control' {
		serve_activity_wait_control(config, converter)!
		return
	}
	if os.args[1] == 'activation-packed' {
		activation_packed_client(config, converter)!
		return
	}
	if os.args[1].starts_with('hook-') {
		serve_hook_role(config, converter)!
		return
	}
	if os.args[1] == 'patch-policy-start' {
		start_patch_policy(config, converter)!
		return
	}
	if os.args[1] == 'initializer-start' {
		initializer_start(config, converter)!
		return
	}
	if os.args[1].starts_with('workflow-headers-') {
		workflow_header_client(config, converter)!
		return
	}
	if os.args[1] == 'parent-handle-standalone' {
		check_standalone_parent(config, converter)!
		return
	}
	if os.args[1] == 'activity-client-worker' {
		serve_activity_client(config, converter)!
		return
	}
	if os.args[1] == 'raw-workflow-start' {
		start_raw_workflows(config, converter)!
		return
	}
	if os.args[1] == 'raw-standalone' {
		raw_standalone_activity(config, converter)!
		return
	}
	if os.args[1] == 'interactions-opaque' {
		raw_interaction_client(config, converter)!
		return
	}
	if os.args[1] == 'random-reset' {
		reset_random_streams(config, converter)!
		return
	}
	if os.args[1] == 'panic-responses' {
		check_panic_responses(config, converter)!
		return
	}
	if os.args[1].starts_with('dynamic-opaque') {
		dynamic_opaque_client(config, converter)!
		return
	}
	if os.args[1] == 'replay' {
		runtime()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			worker_interceptors:           configured_worker_hooks()
			namespace:                     config.namespace
			experimental_event_groups:     os.getenv('V_EVENT_GROUPS') == 'true'
			patch_activation_callback:     configured_patch_policy()
			query_interceptors:            workflow_header_queries()
			update_validator_interceptors: workflow_header_validators()
			command_interceptors:          [
				sdk.CommandInterceptor(PanicCommandInterceptor{}),
				sdk.CommandInterceptor(EventGroupWireInterceptor{}),
				sdk.CommandInterceptor(ContextValuesCommandInterceptor{}),
			]
			workflow_panic_policy:         panic_policy()
			workflow_id:                   os.args[3]
			task_queue:                    os.args[4]
			data_converter:                converter
		})!
		println('OK')
		return
	}
	mut activities := sdk.Activities{}
	activities.register('v.raw-headers.named', raw_header_handler)!
	activities.register('v.raw.named', raw_named_activity)!
	activities.register('v.heartbeat.named', raw_named_heartbeat)!
	activities.register_dynamic(raw_dynamic_activity)!
	activities.register('v.panic.activity', panic_activity)!
	activities.register('v.activity-wait', activity_wait_handler)!
	activities.register('v.activity-deadline', activity_metadata_handler)!
	activities.register('v.event-group.echo', event_group_echo)!
	activities.register('v.version.echo', version_echo)!
	mut services := sdk.NexusServices{}
	for operation in ['sync', 'cancel'] {
		services.register('v.panic', operation, sdk.NexusOperation{
			start:  panic_nexus_start
			cancel: panic_nexus_cancel
		})!
	}
	sdk.run_worker(sdk.WorkerOptions{
		...config
		worker_interceptors:           configured_worker_hooks()
		experimental_event_groups:     os.getenv('V_EVENT_GROUPS') == 'true'
		patch_activation_callback:     configured_patch_policy()
		query_interceptors:            workflow_header_queries()
		update_validator_interceptors: workflow_header_validators()
		command_interceptors:          [
			sdk.CommandInterceptor(PanicCommandInterceptor{}),
			sdk.CommandInterceptor(EventGroupWireInterceptor{}),
			sdk.CommandInterceptor(ContextValuesCommandInterceptor{}),
		]
		workflow_panic_policy:         panic_policy()
		data_converter:                converter
		activity_concurrency:          4
		workflow_concurrency:          8
	}, runtime()!, activities, sdk.WorkerServices{
		nexus: services
	})!
}

fn main() {
	run() or {
		if os.getenv('V_ACTIVATION_EXPECT_FATAL') != '' {
			assert err is sdk.WorkerInterceptorError
			if err is sdk.WorkerInterceptorError {
				assert err.phase == 'activation'
				if os.getenv('V_ACTIVATION_EXPECT_FATAL') == 'panic' {
					assert err.cause is sdk.PanicError
					if err.cause is sdk.PanicError {
						assert err.cause.stack_trace.contains('on_workflow_activation')
					}
				} else {
					assert err.cause is sdk.ApplicationError
				}
				os.write_file(os.join_path(os.getenv('V_ACTIVATION_AUDIT_DIR'), 'fatal-' +
					os.getenv('V_ACTIVATION_EXPECT_FATAL') + '-' + os.getpid().str() + '.proof'),
					err.msg()) or { panic(err.msg()) }
			}
		}
		eprintln(err.msg())
		exit(1)
	}
}

fn panic_policy() sdk.WorkflowPanicPolicy {
	if os.getenv('V_PANIC_FAIL_WORKFLOW') == 'true' { return .fail_workflow }
	return .block_workflow
}

fn panic_nexus_start(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	if context.info().operation == 'sync' {
		marker := os.join_path(os.getenv('V_PANIC_NEXUS_DIR'), context.info().request_id)
		if !os.exists(marker) {
			os.write_file(marker, 'start panic')!
			panic('Nexus start panic')
		}
		return sdk.NexusResult{
			value: sdk.PayloadValue('"nexus-recovered"')
		}
	}
	options := sdk.options_from_env()!
	client := sdk.connect(sdk.ClientOptions{
		address:        options.address
		data_converter: example_converter.from_env()!
	})!
	defer { client.close() }
	return context.start_workflow(client, sdk.WorkflowStart{
		workflow_type: 'v.panic'
		task_queue:    options.task_queue
		arguments:     '{"mode":"wait"}'
	})!
}

fn panic_nexus_cancel(mut context sdk.NexusContext, token string) ! {
	marker := os.join_path(os.getenv('V_PANIC_NEXUS_DIR'), 'cancel-' + context.info().request_id)
	if !os.exists(marker) {
		os.write_file(marker, 'cancel panic')!
		panic('Nexus cancel panic')
	}
	options := sdk.options_from_env()!
	client := sdk.connect(sdk.ClientOptions{
		address:        options.address
		data_converter: example_converter.from_env()!
	})!
	defer { client.close() }
	context.cancel_workflow(client, token)!
}

struct PanicCommandInterceptor {}

@[noinline]
fn (interceptor PanicCommandInterceptor) intercept(command temporal.CoresdkWorkflowCommands_WorkflowCommand, information sdk.WorkflowInfo, next sdk.CommandNext) !temporal.CoresdkWorkflowCommands_WorkflowCommand {
	if information.workflow_id.ends_with('-command') && !fixed() {
		panic('command interceptor panic')
	}
	return next.execute(command)!
}
