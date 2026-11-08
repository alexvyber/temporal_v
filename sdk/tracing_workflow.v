module sdk

import crypto.sha256
import temporal_v.temporal

// Workflow identities use a separate domain and never consume workflow RNG state.
fn workflow_trace_child(parent TraceContext, run_id string, identity string) TraceContext {
	digest := sha256.sum(('temporal-v/tracing/v1\x00' + run_id + '\x00' + identity).bytes()).hex()
	return TraceContext{
		...parent
		span_id: digest[..16]
		remote:  false
	}
}

fn workflow_trace_nanos(context &WorkflowContext) u64 {
	stamp := context.workflow_time.unix_nano()
	return if stamp < 0 { u64(0) } else { u64(stamp) }
}

fn (mut context WorkflowContext) initialize_tracing(options ?TraceOptions) ! {
	tracing := options or { return }
	tracing.validate()!
	incoming := extract_temporal_propagation_context(context.initialization.headers)
	mut parent := incoming.span
	context.trace_baggage = inherited_baggage(incoming.baggage, tracing.baggage)
	if parent == none { parent = tracing.parent }
	root := if value := parent {
		value
	} else {
		digest := sha256.sum(('temporal-v/tracing/root/v1\x00' + context.run_id).bytes()).hex()
		TraceContext{
			trace_id:    digest[..32]
			span_id:     digest[32..48]
			trace_flags: 1
		}
	}
	identity := workflow_trace_child(root, context.run_id, 'workflow')
	context.tracing = tracing
	context.trace_root = tracing.provider.start_span('RunWorkflow:' + context.workflow_type, TraceSpanOptions{
		parent:           parent
		context:          identity
		baggage:          context.trace_baggage
		kind:             .server
		start_unix_nanos: workflow_trace_nanos(context)
		attributes:       context.workflow_trace_attributes()
	})!
}

fn (context &WorkflowContext) workflow_trace_attributes() map[string]TraceAttribute {
	return {
		'temporal.namespace':       TraceAttribute(context.namespace)
		'temporal.workflow.id':     TraceAttribute(context.workflow_id)
		'temporal.workflow.run_id': TraceAttribute(context.run_id)
		'temporal.workflow.type':   TraceAttribute(context.workflow_type)
	}
}

pub fn (context &WorkflowContext) trace_context() ?TraceContext {
	if current := context.trace_current { return current }
	if root := context.trace_root { return root.context() }
	return none
}

fn (mut run Run) invoke_traced(event Event) ! {
	tracing := run.context.tracing or {
		WorkflowNext{
			workflow:     run.workflow
			interceptors: run.workflow_interceptors
		}.execute(event, mut run.context)!
		return
	}

	root := run.context.trace_root or { return error('workflow trace root missing') }
	run.context.trace_next_event++
	mut parent := root.context()
	if event.kind in [.signal_received, .update_received] {
		if incoming := extract_temporal_trace_context(event.headers) { parent = incoming }
	}
	identity := workflow_trace_child(parent, run.context.run_id,
		'event:${run.context.trace_next_event}')
	selected_baggage := inherited_baggage(extract_temporal_propagation_context(event.headers).baggage,
		run.context.trace_baggage)
	span := tracing.provider.start_span('WorkflowEvent:' + event.kind.str(), TraceSpanOptions{
		parent:           parent
		context:          identity
		baggage:          selected_baggage
		kind:             .internal
		start_unix_nanos: workflow_trace_nanos(&run.context)
		suppress_export:  run.context.replaying_history
		attributes:       run.context.workflow_trace_attributes()
	})!
	previous_baggage := run.context.trace_baggage
	run.context.trace_baggage = selected_baggage
	previous := run.context.trace_current
	run.context.trace_current = span.context()
	first := run.context.commands.len
	defer {
		span.finish(TraceEndOptions{ unix_nanos: workflow_trace_nanos(&run.context) })
		run.context.trace_current = previous
		run.context.trace_baggage = previous_baggage
	}

	WorkflowNext{
		workflow:     run.workflow
		interceptors: run.workflow_interceptors
	}.execute(event, mut run.context) or {
		if tracing.record_errors {
			span.record_exception('WorkflowError', err) or {}
			span.set_status(.error, err.msg()) or {}
		}
		return err
	}
	for index in first .. run.context.commands.len {
		run.context.commands[index] = run.context.trace_command(run.context.commands[index],
			span.context())!
	}
}

fn (mut context WorkflowContext) trace_command(command temporal.CoresdkWorkflowCommands_WorkflowCommand, parent TraceContext) !temporal.CoresdkWorkflowCommands_WorkflowCommand {
	tracing := context.tracing or { return command }
	mut variant := command.variant or { return command }
	name := match variant {
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleActivity_Case {
			'StartActivity:' + variant.value.activity_type
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleLocalActivity_Case {
			'StartLocalActivity:' + variant.value.activity_type
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_StartChildWorkflowExecution_Case {
			'StartChildWorkflow:' + variant.value.workflow_type
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ContinueAsNewWorkflowExecution_Case {
			'ContinueAsNew'
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_SignalExternalWorkflowExecution_Case {
			'SignalWorkflow:' + variant.value.signal_name
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleNexusOperation_Case {
			'StartNexusOperation:' + variant.value.service + '/' + variant.value.operation
		}
		else {
			return command
		}
	}

	context.trace_next_command++
	identity := workflow_trace_child(parent, context.run_id,
		'command:${context.trace_next_command}')
	span := tracing.provider.start_span(name, TraceSpanOptions{
		parent:           parent
		context:          identity
		baggage:          context.trace_baggage
		kind:             .producer
		start_unix_nanos: workflow_trace_nanos(context)
		suppress_export:  context.replaying_history
		attributes:       context.workflow_trace_attributes()
	})!
	defer { span.finish(TraceEndOptions{ unix_nanos: workflow_trace_nanos(context) }) }
	propagation := PropagationContext{
		span:    span.context()
		baggage: context.trace_baggage
	}
	payload := propagation.temporal_header()!
	match mut variant {
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleActivity_Case {
			variant.value.headers[trace_header_key] = payload
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleLocalActivity_Case {
			variant.value.headers[trace_header_key] = payload
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_StartChildWorkflowExecution_Case {
			variant.value.headers[trace_header_key] = payload
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ContinueAsNewWorkflowExecution_Case {
			variant.value.headers[trace_header_key] = payload
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_SignalExternalWorkflowExecution_Case {
			variant.value.headers[trace_header_key] = payload
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleNexusOperation_Case {
			mut headers := variant.value.nexus_header.clone()
			for key in headers.keys() {
				if key.to_lower() in ['traceparent', 'tracestate', 'baggage'] {
					headers.delete(key)
				}
			}
			for key, value in propagation.headers()! {
				headers[key] = value
			}
			variant.value.nexus_header = headers.clone()
		}
		else {}
	}

	return temporal.CoresdkWorkflowCommands_WorkflowCommand{
		...command
		variant: variant
	}
}

fn (context &WorkflowContext) finish_workflow_trace(commands []temporal.CoresdkWorkflowCommands_WorkflowCommand) {
	if context.replaying_history { return }
	root := context.trace_root or { return }
	for command in commands {
		variant := command.variant or { continue }
		match variant {
			temporal.CoresdkWorkflowCommands_WorkflowCommand_CompleteWorkflowExecution_Case,
			temporal.CoresdkWorkflowCommands_WorkflowCommand_ContinueAsNewWorkflowExecution_Case {
				root.finish(TraceEndOptions{ unix_nanos: workflow_trace_nanos(context) })
			}
			temporal.CoresdkWorkflowCommands_WorkflowCommand_FailWorkflowExecution_Case {
				if tracing := context.tracing {
					if tracing.record_errors { root.set_status(.error, 'workflow failed') or {} }
				}
				root.finish(TraceEndOptions{ unix_nanos: workflow_trace_nanos(context) })
			}
			temporal.CoresdkWorkflowCommands_WorkflowCommand_CancelWorkflowExecution_Case {
				root.finish(TraceEndOptions{ unix_nanos: workflow_trace_nanos(context) })
			}
			else {}
		}
	}
}

// Query/validator handlers are read-only and never change workflow trace counters.
fn (context &WorkflowContext) inbound_trace_span(name string, headers map[string]temporal.TemporalApiCommonV1_Payload) ?&TraceSpan {
	tracing := context.tracing or { return none }
	mut parent := extract_temporal_trace_context(headers)
	if parent == none { parent = context.trace_context() }
	return tracing.provider.start_span(name, TraceSpanOptions{
		parent:          parent
		baggage:         inherited_baggage(extract_temporal_propagation_context(headers).baggage,
			context.trace_baggage)
		kind:            .server
		suppress_export: context.replaying_history
		attributes:      context.workflow_trace_attributes()
	}) or { return none }
}

fn trace_command_has_carrier(command temporal.CoresdkWorkflowCommands_WorkflowCommand) bool {
	variant := command.variant or { return true }
	return match variant {
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleActivity_Case {
			trace_header_key in variant.value.headers
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleLocalActivity_Case {
			trace_header_key in variant.value.headers
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_StartChildWorkflowExecution_Case {
			trace_header_key in variant.value.headers
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ContinueAsNewWorkflowExecution_Case {
			trace_header_key in variant.value.headers
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_SignalExternalWorkflowExecution_Case {
			trace_header_key in variant.value.headers
		}
		temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleNexusOperation_Case {
			'traceparent' in variant.value.nexus_header
		}
		else {
			true
		}
	}
}

fn (context &WorkflowContext) inbound_propagation(headers map[string]temporal.TemporalApiCommonV1_Payload, span ?&TraceSpan) PropagationContext {
	incoming := extract_temporal_propagation_context(headers)
	current := if value := span { ?TraceContext(value.context()) } else { incoming.span }
	return PropagationContext{
		span:    current
		baggage: inherited_baggage(incoming.baggage, context.trace_baggage)
	}
}
