module sdk

import alexvyber.temporal_v.temporal
import time
import x.json2

@[heap]
struct Run {
	command_interceptors          []CommandInterceptor
	workflow_interceptors         []WorkflowInterceptor
	query_interceptors            []QueryInterceptor
	update_validator_interceptors []UpdateValidatorInterceptor
mut:
	workflow Workflow
	context  WorkflowContext
}

fn (mut run Run) take_commands() ![]temporal.CoresdkWorkflowCommands_WorkflowCommand {
	if forced := run.context.forced_task_error { return forced }
	if retained := run.context.recorded_value_error { return retained }
	for run.context.deferred_events.len > 0 {
		events := run.context.deferred_events.clone()
		run.context.deferred_events.clear()
		for event in events {
			run.deliver(event)!
		}
	}
	mut commands := []temporal.CoresdkWorkflowCommands_WorkflowCommand{}
	information := run.context.info()
	assert run.context.command_context_values.len == run.context.commands.len
	for index, command in run.context.commands {
		next := CommandNext{
			interceptors: run.command_interceptors
			information:  WorkflowInfo{
				...information
				context_values: run.context.command_context_values[index]
			}
		}
		mut prepared := command
		if parent := run.context.trace_context() {
			if !trace_command_has_carrier(command) {
				prepared = run.context.trace_command(command, parent)!
			}
		}
		commands << next.execute(prepared)!
	}
	run.context.commands.clear()
	run.context.command_context_values.clear()
	run.context.finish_workflow_trace(commands)
	return commands
}

pub struct Runtime {
mut:
	metrics              &MetricMeter = &MetricMeter{}
	dynamic_factory      ?WorkflowFactory
	dynamic_registration WorkflowRegistrationOptions
	factories            map[string]WorkflowFactory
	registrations        map[string]WorkflowRegistrationOptions
	runs                 map[string]&Run
	local_handlers       Activities
}

@[params]
pub struct WorkflowRegistrationOptions {
pub:
	versioning_behavior VersioningBehavior
}

pub fn (mut runtime Runtime) register(name string, factory WorkflowFactory, options WorkflowRegistrationOptions) ! {
	if name == '' || name in runtime.factories {
		return error('invalid or duplicate workflow registration: ${name}')
	}
	runtime.factories[name] = factory
	runtime.registrations[name] = options
}

pub fn (mut runtime Runtime) register_local_activity(name string, handler ActivityHandler) ! {
	runtime.local_handlers.register(name, handler)!
}

fn (runtime &Runtime) cached_runs() int {
	return runtime.runs.len
}

fn (mut run Run) deliver(event Event) ! {
	if run.context.closed { return }
	run.invoke(event) or {
		if forced := run.context.forced_task_error { return forced }
		if err is WorkflowTaskError || err is NondeterminismError || err is PanicError {
			return err
		}
		if err is CancelledError {
			run.context.cancel(details: err.details)!
		} else if err is Failure {
			run.context.fail_captured(err)!
		} else if err is ApplicationError {
			run.context.fail_application(err)!
		} else {
			run.context.fail(err.msg(), 'VWorkflowError')!
		}
	}
	if forced := run.context.forced_task_error { return forced }
	if retained := run.context.recorded_value_error { return retained }
	run.poll_conditions()!
}

fn (mut runtime Runtime) activate_callback(activation temporal.CoresdkWorkflowActivation_WorkflowActivation, options WorkerOptions) ![]temporal.CoresdkWorkflowCommands_WorkflowCommand {
	mut query_only := true
	for job in activation.jobs {
		variant := job.variant or {
			query_only = false
			break
		}

		if variant !is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_QueryWorkflow_Case {
			query_only = false
			break
		}
	}
	replaying_history := activation.is_replaying && !query_only

	mut commands := []temporal.CoresdkWorkflowCommands_WorkflowCommand{}
	mut patches := map[string]bool{}
	mut workflow_markers := []temporal.CoresdkWorkflowActivation_NotifyWorkflowMarker{}
	for job in activation.jobs {
		variant := job.variant or { continue }
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_NotifyWorkflowMarker_Case {
			workflow_markers << variant.value
		}
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_NotifyHasPatch_Case {
			patches[variant.value.patch_id] = true
		}
	}
	if mut existing := runtime.runs[activation.run_id] {
		existing.context.remember_workflow_markers(workflow_markers)!
		existing.context.current_task_deployment_version = activation.deployment_version_for_current_task
		existing.context.last_sdk_version = activation.last_sdk_version
		existing.context.target_worker_deployment_version_changed = activation.target_worker_deployment_version_changed
		existing.context.replaying = activation.is_replaying
		existing.context.replaying_history = replaying_history
		mut metric_replay := existing.context.metric_replay
		metric_replay.set(replaying_history)
		existing.context.history_length = activation.history_length
		existing.context.history_size_bytes = activation.history_size_bytes
		existing.context.continue_as_new_suggested = activation.continue_as_new_suggested
		existing.context.suggest_continue_as_new_reasons =
			activation.suggest_continue_as_new_reasons.clone()
		for id, present in patches {
			existing.context.known_patches[id] = present
		}
		if timestamp := activation.timestamp {
			existing.context.workflow_time =
				time.unix(timestamp.seconds).add(timestamp.nanos * time.nanosecond)
		}
	}
	for job in activation.jobs {
		variant := job.variant or { return error('activation job has no variant') }
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_RemoveFromCache_Case {
			if variant.value.reason in [.nondeterminism, .fatal] {
				emit_log(options.logger, .error,
					'WORKFLOW_EVICTED run=${activation.run_id} reason=${variant.value.reason} message=${variant.value.message}')
			}
			runtime.forget(activation.run_id)
			continue
		}
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_NotifyHasPatch_Case {
			continue
		}
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_NotifyWorkflowMarker_Case {
			continue
		}
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_InitializeWorkflow_Case {
			if activation.run_id in runtime.runs {
				return error('duplicate workflow initialization')
			}
			factory := runtime.resolve_workflow(variant.value.workflow_type) or {
				mut context := WorkflowContext{
					initialization: variant.value
					queue:          options.task_queue
					namespace:      options.namespace
					workflow_id:    variant.value.workflow_id
					converter:      options.data_converter.with_context(workflow_serialization_context(options.namespace,
						variant.value.workflow_id))
				}
				context.fail('unknown workflow type: ${variant.value.workflow_type}',
					'UnknownWorkflowType')!
				commands << context.commands
				continue
			}
			meter := runtime.metrics.scope()!
			mut transferred := false
			defer {
				if !transferred { meter.close() }
			}
			mut context := WorkflowContext{
				patch_activation_callback:                options.patch_activation_callback
				event_groups_enabled:                     options.experimental_event_groups
				metrics:                                  meter
				metric_replay:                            &MetricReplayState{
					replaying: replaying_history
				}
				current_task_deployment_version:          activation.deployment_version_for_current_task
				last_sdk_version:                         activation.last_sdk_version
				target_worker_deployment_version_changed: activation.target_worker_deployment_version_changed
				queue:                                    options.task_queue
				namespace:                                options.namespace
				workflow_id:                              variant.value.workflow_id
				workflow_type:                            variant.value.workflow_type
				run_id:                                   activation.run_id
				history_length:                           activation.history_length
				history_size_bytes:                       activation.history_size_bytes
				continue_as_new_suggested:                activation.continue_as_new_suggested
				suggest_continue_as_new_reasons:          activation.suggest_continue_as_new_reasons.clone()
				replaying:                                activation.is_replaying
				replaying_history:                        replaying_history
				known_patches:                            patches
				random_state:                             variant.value.randomness_seed
				random_seed:                              variant.value.randomness_seed
				converter:                                options.data_converter.with_context(workflow_serialization_context(options.namespace,
					variant.value.workflow_id))
				logger:                                   options.logger
				initialization:                           variant.value
			}
			if timestamp := activation.timestamp {
				context.workflow_time =
					time.unix(timestamp.seconds).add(timestamp.nanos * time.nanosecond)
			}
			if memo := variant.value.memo { context.memo_values = memo.fields.clone() }
			if attributes := variant.value.search_attributes {
				context.search_attribute_values = attributes.indexed_fields.clone()
			}
			decoded := if variant.value.workflow_type in runtime.factories {
				context.converter.decode_arguments(variant.value.arguments)!
			} else {
				context.converter.decode_raw_arguments(variant.value.arguments)!
			}
			view := context.interaction_view(variant.value.headers, decoded.raw_values,
				context.capture_context_values())!
			workflow := invoke_workflow_factory(factory, view, decoded.typed_values)!
			validate_dynamic_interactions(workflow)!
			context.initialize_tracing(options.tracing)!
			mut run := &Run{
				workflow:                      workflow
				context:                       context
				command_interceptors:          options.command_interceptors
				workflow_interceptors:         options.workflow_interceptors
				query_interceptors:            options.query_interceptors
				update_validator_interceptors: options.update_validator_interceptors
			}
			runtime.runs[activation.run_id] = run
			transferred = true
			run.context.remember_workflow_markers(workflow_markers)!
			arguments := decoded.values
			run.deliver(Event{
				kind:            .started
				raw_arguments:   decoded.raw_values
				typed_arguments: decoded.typed_values
				data:            event_data(arguments)
				arguments:       arguments
				headers:         variant.value.headers.clone()
			})!
		} else {
			if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_QueryWorkflow_Case {
				run := runtime.runs[activation.run_id] or {
					commands << failed_query(variant.value.query_id,
						error('workflow state unavailable for query'), options.data_converter)!
					continue
				}
				commands << query_reply(run, variant.value)!
				continue
			}
			mut run := runtime.runs[activation.run_id] or {
				return error('activation references an uncached run')
			}
			if run.context.closed {
				continue
			}
			match variant {
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveNexusOperationStart_Case {
					sequence := variant.value.seq
					scope := run.context.nexus_contexts[sequence] or {
						return error('missing Nexus serialization scope')
					}
					converter := run.context.converter.with_context(scope)
					if sequence !in run.context.nexus_operations {
						return error('unknown Nexus start sequence')
					}
					status := variant.value.status or { return error('missing Nexus start status') }
					if status is temporal.CoresdkWorkflowActivation_ResolveNexusOperationStart_OperationToken_Case {
						run.deliver(Event{
							kind:     .nexus_started
							sequence: sequence
							id:       status.value
						})!
					} else if status is temporal.CoresdkWorkflowActivation_ResolveNexusOperationStart_StartedSync_Case {
						run.deliver(Event{ kind: .nexus_started, sequence: sequence })!
					} else if status is temporal.CoresdkWorkflowActivation_ResolveNexusOperationStart_Failed_Case {
						run.context.nexus_operations.delete(sequence)
						run.context.nexus_contexts.delete(sequence)
						run.context.nexus_scopes.delete(sequence)
						run.context.nexus_cancellations.delete(sequence)
						information := failure_from_proto(status.value, converter)
						kind := match information.root_kind() {
							.timeout { EventKind.nexus_timed_out }
							.canceled { EventKind.nexus_cancelled }
							else { EventKind.nexus_failed }
						}

						run.deliver(Event{
							kind:     kind
							sequence: sequence
							message:  status.value.message
							failure:  information
						})!
					}
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveNexusOperation_Case {
					sequence := variant.value.seq
					scope := run.context.nexus_contexts[sequence] or {
						return error('missing Nexus serialization scope')
					}
					converter := run.context.converter.with_context(scope)
					if sequence !in run.context.nexus_operations {
						return error('unknown Nexus resolution sequence')
					}
					run.context.nexus_operations.delete(sequence)
					run.context.nexus_contexts.delete(sequence)
					run.context.nexus_scopes.delete(sequence)
					run.context.nexus_cancellations.delete(sequence)
					result := variant.value.result or { return error('missing Nexus result') }
					status := result.status or { return error('missing Nexus result status') }
					match status {
						temporal.CoresdkNexus_NexusOperationResult_Completed_Case {
							decoded := converter.decode_single_argument([status.value])!
							run.deliver(Event{
								kind:         .nexus_completed
								sequence:     sequence
								data:         decoded.values[0]
								raw_result:   decoded.raw_values[0]
								typed_result: decoded.typed_values[0]
							})!
						}
						temporal.CoresdkNexus_NexusOperationResult_Cancelled_Case {
							run.deliver(Event{
								kind:     .nexus_cancelled
								sequence: sequence
								message:  status.value.message
								failure:  failure_from_proto(status.value, converter)
							})!
						}
						temporal.CoresdkNexus_NexusOperationResult_TimedOut_Case {
							run.deliver(Event{
								kind:     .nexus_timed_out
								sequence: sequence
								message:  status.value.message
								failure:  failure_from_proto(status.value, converter)
							})!
						}
						temporal.CoresdkNexus_NexusOperationResult_Failed_Case {
							run.deliver(Event{
								kind:     .nexus_failed
								sequence: sequence
								message:  status.value.message
								failure:  failure_from_proto(status.value, converter)
							})!
						}
					}
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_UpdateRandomSeed_Case {
					run.context.reseed_random(variant.value.randomness_seed)
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveSignalExternalWorkflow_Case {
					sequence := variant.value.seq
					if run.context.external_operations[sequence] != 'signal' {
						return error('unknown external signal sequence')
					}
					run.context.external_operations.delete(sequence)
					run.context.external_scopes.delete(sequence)
					if info := variant.value.failure {
						run.deliver(Event{
							kind:     .external_signal_failed
							sequence: sequence
							message:  info.message
							failure:  failure_from_proto(info, run.context.converter)
						})!
					} else {
						run.deliver(Event{ kind: .external_signal_completed, sequence: sequence })!
					}
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveRequestCancelExternalWorkflow_Case {
					sequence := variant.value.seq
					if run.context.external_operations[sequence] != 'cancel' {
						return error('unknown external cancel sequence')
					}
					run.context.external_operations.delete(sequence)
					run.context.external_scopes.delete(sequence)
					if info := variant.value.failure {
						run.deliver(Event{
							kind:     .external_cancel_failed
							sequence: sequence
							message:  info.message
							failure:  failure_from_proto(info, run.context.converter)
						})!
					} else {
						run.deliver(Event{ kind: .external_cancel_completed, sequence: sequence })!
					}
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_DoUpdate_Case {
					run.update(variant.value)!
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveChildWorkflowExecutionStart_Case {
					status := variant.value.status or { return error('missing child start status') }
					if variant.value.seq !in run.context.children {
						return error('unknown child sequence')
					}
					if status is temporal.CoresdkWorkflowActivation_ResolveChildWorkflowExecutionStart_Succeeded_Case {
						run.deliver(Event{
							kind:     .child_started
							sequence: variant.value.seq
							id:       status.value.run_id
						})!
					} else {
						run.context.children.delete(variant.value.seq)
						run.context.child_contexts.delete(variant.value.seq)
						run.context.child_scopes.delete(variant.value.seq)
						run.context.child_cancellations.delete(variant.value.seq)
						if status is temporal.CoresdkWorkflowActivation_ResolveChildWorkflowExecutionStart_Failed_Case {
							run.deliver(Event{
								kind:     .child_failed
								sequence: variant.value.seq
								message:  'child start rejected: ${status.value.cause}'
							})!
						} else {
							run.deliver(Event{ kind: .child_cancelled, sequence: variant.value.seq })!
						}
					}
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveChildWorkflowExecution_Case {
					sequence := variant.value.seq
					if sequence !in run.context.children { return error('unknown child sequence') }
					scope := run.context.child_contexts[sequence] or {
						return error('missing child serialization scope')
					}
					converter := run.context.converter.with_context(scope)
					run.context.child_contexts.delete(sequence)
					run.context.children.delete(sequence)
					run.context.child_scopes.delete(sequence)
					run.context.child_cancellations.delete(sequence)
					result := variant.value.result or { return error('missing child result') }
					status := result.status or { return error('missing child status') }
					match status {
						temporal.CoresdkChildWorkflow_ChildWorkflowResult_Completed_Case {
							value := status.value.result or {
								return error('missing child payload')
							}

							decoded := converter.decode_single_argument([value])!
							run.deliver(Event{
								kind:         .child_completed
								sequence:     sequence
								data:         decoded.values[0]
								raw_result:   decoded.raw_values[0]
								typed_result: decoded.typed_values[0]
							})!
						}
						temporal.CoresdkChildWorkflow_ChildWorkflowResult_Failed_Case {
							info := status.value.failure or {
								return error('missing child failure')
							}

							run.deliver(Event{
								kind:     .child_failed
								sequence: sequence
								message:  info.message
								failure:  failure_from_proto(info, converter)
							})!
						}
						else {
							run.deliver(Event{ kind: .child_cancelled, sequence: sequence })!
						}
					}
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_SignalWorkflow_Case {
					decoded := run.context.decode_signal_arguments(run.workflow,
						variant.value.signal_name, variant.value.input)!
					arguments := decoded.values
					run.deliver(Event{
						kind:                 .signal_received
						raw_arguments:        decoded.raw_values
						typed_arguments:      decoded.typed_values
						sender_identity:      variant.value.identity
						originating_event_id: variant.value.originating_event_id
						name:                 variant.value.signal_name
						data:                 event_data(arguments)
						arguments:            arguments
						headers:              variant.value.headers.clone()
					})!
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_FireTimer_Case {
					if variant.value.seq in run.context.cancelled_timers {
						run.context.cancelled_timers.delete(variant.value.seq)
						continue
					}
					if variant.value.seq !in run.context.timers {
						return error('unknown timer sequence')
					}
					run.context.timers.delete(variant.value.seq)
					run.context.timer_scopes.delete(variant.value.seq)
					if sequence := run.context.local_backoffs[variant.value.seq] {
						run.context.local_backoffs.delete(variant.value.seq)
						mut command := run.context.local_activities[sequence] or {
							return error('missing local retry command')
						}
						// Each timer-backed retry is a new Core state machine. Keep
						// the application's handle stable while assigning a fresh seq.
						run.context.next_activity++
						command.seq = run.context.next_activity
						run.context.local_activity_sequences[command.seq] = sequence
						run.context.local_activities[sequence] = command
						run.context.schedule_local(command)
						commands << run.take_commands()!
						continue
					}
					if condition_id := run.context.condition_timers[variant.value.seq] {
						run.context.condition_timers.delete(variant.value.seq)
						if pending := run.context.conditions[condition_id] {
							run.context.conditions[condition_id] = PendingCondition{
								...pending
								timed_out: true
							}
						}
						run.poll_conditions()!
						continue
					}
					run.deliver(Event{ kind: .timer_fired, sequence: variant.value.seq })!
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_ResolveActivity_Case {
					sequence := run.context.local_activity_sequences[variant.value.seq] or {
						variant.value.seq
					}
					run.context.local_activity_sequences.delete(variant.value.seq)
					if sequence !in run.context.activities {
						return error('unknown Activity sequence')
					}
					scope := run.context.activity_contexts[sequence] or {
						return error('missing Activity serialization scope')
					}
					converter := run.context.converter.with_context(scope)
					resolution := variant.value.result or {
						return error('missing Activity resolution')
					}

					status := resolution.status or { return error('missing Activity status') }
					if status is temporal.CoresdkActivityResult_ActivityResolution_Backoff_Case {
						run.context.local_backoff(sequence, status.value)!
						commands << run.take_commands()!
						continue
					}
					run.context.activities.delete(sequence)
					run.context.activity_contexts.delete(sequence)
					run.context.activity_scopes.delete(sequence)
					run.context.local_activities.delete(sequence)
					run.context.local_activity_metadata.delete(sequence)
					run.context.local_activity_event_groups.delete(sequence)
					run.context.local_activity_context_values.delete(sequence)
					run.context.activity_cancellations.delete(sequence)
					match status {
						temporal.CoresdkActivityResult_ActivityResolution_Completed_Case {
							value := status.value.result or { return error('missing result') }
							decoded := converter.decode_single_argument([value])!
							run.deliver(Event{
								kind:         .activity_completed
								sequence:     sequence
								data:         decoded.values[0]
								raw_result:   decoded.raw_values[0]
								typed_result: decoded.typed_values[0]
							})!
						}
						temporal.CoresdkActivityResult_ActivityResolution_Failed_Case {
							info := status.value.failure or { return error('missing failure') }
							run.deliver(Event{
								kind:     .activity_failed
								sequence: sequence
								message:  info.message
								failure:  failure_from_proto(info, converter)
							})!
						}
						temporal.CoresdkActivityResult_ActivityResolution_Cancelled_Case {
							mut information := ?Failure(none)
							if cause := status.value.failure {
								information = failure_from_proto(cause, converter)
							}
							run.deliver(Event{
								kind:     .activity_cancelled
								sequence: sequence
								failure:  information
							})!
						}
						else {
							return error('unsupported Activity resolution')
						}
					}
				}
				temporal.CoresdkWorkflowActivation_WorkflowActivationJob_CancelWorkflow_Case {
					if run.context.request_cancel(variant.value.reason)! {
						run.deliver(Event{ kind: .cancel_requested, message: variant.value.reason })!
					}
				}
				else {
					return error('unsupported workflow job: ${variant.type_name()}')
				}
			}
		}
		if mut run := runtime.runs[activation.run_id] { commands << run.take_commands()! }
	}
	return commands
}

fn (mut runtime Runtime) forget(run_id string) {
	if run := runtime.runs[run_id] { run.context.metrics.close() }
	runtime.runs.delete(run_id)
}

fn query_value(initial_run &Run, request temporal.CoresdkWorkflowActivation_QueryWorkflow) !temporal.TemporalApiCommonV1_Payload {
	mut run := initial_run
	mut guard := run.context.command_guard
	previous := guard.reason
	guard.reason = 'query handler'
	defer { guard.reason = previous }
	span := run.context.inbound_trace_span('QueryWorkflow:' + request.query_type, request.headers)
	defer {
		if value := span { value.finish() }
	}
	if request.query_type == '__temporal_workflow_metadata' {
		// Temporal UI expects plain JSON metadata, independent of application codecs.
		data := json2.encode(WorkflowMetadata{
			definition:      workflow_definition(run)!
			current_details: run.context.current_details
		})
		return JsonPayloadConverter{}.to_payload(data, SerializationContext{})!
	}
	decoded := run.context.decode_query_arguments(run.workflow, request.query_type,
		request.arguments)!
	input := decoded.values
	value := QueryNext{
		context_values:  run.context.capture_context_values()
		propagation:     run.context.inbound_propagation(request.headers, span)
		run:             run
		request_headers: request.headers
		argument_values: decoded.raw_values
		typed_arguments: decoded.typed_values
	}.execute(request.query_type, input) or {
		if failed_span := span {
			if tracing := run.context.tracing {
				if tracing.record_errors {
					failed_span.record_exception('QueryError', err) or {}
					failed_span.set_status(.error, err.msg()) or {}
				}
			}
		}
		return err
	}
	return run.context.converter.encode_value(value)!
}

fn query_reply(run &Run, request temporal.CoresdkWorkflowActivation_QueryWorkflow) !temporal.CoresdkWorkflowCommands_WorkflowCommand {
	value := query_value(run, request) or {
		return failed_query(request.query_id, err, run.context.converter)!
	}
	return temporal.CoresdkWorkflowCommands_WorkflowCommand{
		variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_RespondToQuery_Case{
			value: temporal.CoresdkWorkflowCommands_QueryResult{
				query_id: request.query_id
				variant:  temporal.CoresdkWorkflowCommands_QueryResult_Succeeded_Case{
					value: temporal.CoresdkWorkflowCommands_QuerySuccess{
						response: value
					}
				}
			}
		}
	}
}

fn failed_query(query_id string, err IError, converter DataConverter) !temporal.CoresdkWorkflowCommands_WorkflowCommand {
	return temporal.CoresdkWorkflowCommands_WorkflowCommand{
		variant: temporal.CoresdkWorkflowCommands_WorkflowCommand_RespondToQuery_Case{
			value: temporal.CoresdkWorkflowCommands_QueryResult{
				query_id: query_id
				variant:  temporal.CoresdkWorkflowCommands_QueryResult_Failed_Case{
					value: converter.encode_failure(err, 'VQueryError')!
				}
			}
		}
	}
}

fn (runtime &Runtime) activation_versioning(activation temporal.CoresdkWorkflowActivation_WorkflowActivation) temporal.TemporalApiEnumsV1_VersioningBehavior {
	mut workflow_type := ''
	if run := runtime.runs[activation.run_id] { workflow_type = run.context.workflow_type }
	for job in activation.jobs {
		if variant := job.variant {
			if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_InitializeWorkflow_Case {
				workflow_type = variant.value.workflow_type
			}
		}
	}
	options := runtime.registrations[workflow_type] or { runtime.dynamic_registration }
	return match options.versioning_behavior {
		.pinned { .versioning_behavior_pinned }
		.auto_upgrade { .versioning_behavior_auto_upgrade }
		.unspecified { .versioning_behavior_unspecified }
	}
}

fn (runtime &Runtime) validate_versioning(config WorkerOptions) ! {
	for name, options in runtime.registrations {
		validate_workflow_versioning(name, options, config)!
	}
	if _ := runtime.dynamic_factory {
		validate_workflow_versioning('<dynamic>', runtime.dynamic_registration, config)!
	}
}

fn validate_workflow_versioning(name string, options WorkflowRegistrationOptions, config WorkerOptions) ! {
	if options.versioning_behavior == .unspecified { return }
	deployment := config.deployment or {
		return error('workflow ${name} declares versioning behavior but worker deployment versioning is disabled')
	}

	if !deployment.use_versioning {
		return error('workflow ${name} declares versioning behavior but worker deployment versioning is disabled')
	}
}

pub fn (mut runtime Runtime) register_dynamic(factory WorkflowFactory, options WorkflowRegistrationOptions) ! {
	if _ := runtime.dynamic_factory { return error('dynamic workflow already registered') }
	runtime.dynamic_factory = factory
	runtime.dynamic_registration = options
}

pub fn (mut runtime Runtime) register_dynamic_local_activity(handler DynamicActivityHandler) ! {
	runtime.local_handlers.register_dynamic(handler)!
}

fn (runtime &Runtime) resolve_workflow(name string) !WorkflowFactory {
	if named := runtime.factories[name] { return named }
	if dynamic := runtime.dynamic_factory { return dynamic }
	return error('unknown workflow type: ' + name)
}
