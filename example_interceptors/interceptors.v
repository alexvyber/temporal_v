module example_interceptors

import temporal_v.sdk
import temporal_v.corebridge
import os
import temporal_v.temporal

// Fixture interceptors demonstrate immutable, thread-safe chain registration.
struct ClientTrace {
	name string
}

fn (trace ClientTrace) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	eprintln('INTERCEPT client ${trace.name} enter ${request.method}')
	defer { eprintln('INTERCEPT client ${trace.name} exit ${request.method}') }
	mut chain := ''
	mut metadata := []corebridge.RpcMetadataEntry{}
	for entry in request.metadata {
		if entry.name == 'x-v-chain' {
			chain = entry.value.bytestr()
		} else if entry.name != 'x-v-binary-bin' {
			metadata << entry
		}
	}
	if chain != if trace.name == 'outer' {
		''
	} else {
		'outer'
	} {
		return error('incorrect client interceptor order')
	}
	metadata << corebridge.RpcMetadataEntry{
		name:  'x-v-chain'
		value: trace.name.bytes()
	}
	metadata << corebridge.RpcMetadataEntry{
		name:  'x-v-binary-bin'
		value: [u8(0), 255, 128]
	}
	return next.execute(sdk.ClientRpcRequest{
		...request
		metadata: metadata
	}) or {
		if err is sdk.RpcError {
			if err.status == 6 {
				status := err.status_details()!
				if status.code != 6 { return error('incorrect rich gRPC status code') }
				mut found := false
				for detail in status.details {
					if detail.type_url.ends_with('/temporal.api.errordetails.v1.WorkflowExecutionAlreadyStartedFailure') {
						existing :=
							temporal.TemporalApiErrordetailsV1_WorkflowExecutionAlreadyStartedFailure.decode(detail.value)!
						if existing.run_id == '' {
							return error('missing existing run ID in RPC failure')
						}
						found = true
					}
				}
				if !found { return error('missing duplicate-start typed gRPC detail') }
				eprintln('INTERCEPT rich duplicate-start detail verified')
			}
		}
		return err
	}
}

struct ActivityTrace {
	name string
}

fn (trace ActivityTrace) intercept(mut context sdk.ActivityContext, input []string, next sdk.ActivityNext) !sdk.PayloadValue {
	if (context.header_values()!['x-trace'] or { return error('missing trace header') }).logical_data() != '"trace-propagated"' {
		return error('outbound trace header was not propagated')
	}
	eprintln('INTERCEPT activity ${trace.name} enter local=${context.info().is_local}')
	defer { eprintln('INTERCEPT activity ${trace.name} exit local=${context.info().is_local}') }
	return next.execute(mut context, input)!
}

struct WorkflowTrace {
	name string
}

fn (trace WorkflowTrace) intercept(event sdk.Event, mut context sdk.WorkflowContext, next sdk.WorkflowNext) ! {
	eprintln('INTERCEPT workflow ${trace.name} enter ${event.kind}')
	defer { eprintln('INTERCEPT workflow ${trace.name} exit ${event.kind}') }
	next.execute(event, mut context)!
}

struct QueryTrace {
	name string
}

fn (trace QueryTrace) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.QueryNext) !sdk.PayloadValue {
	if info.workflow_id == '' { return error('missing interceptor workflow info') }
	if name == 'interceptor_reject' { return error('query rejected by interceptor') }
	eprintln('INTERCEPT query ${trace.name} enter ${name}')
	defer { eprintln('INTERCEPT query ${trace.name} exit ${name}') }
	return next.execute(name, input)!
}

struct UpdateTrace {
	name string
}

fn (trace UpdateTrace) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.UpdateValidatorNext) ! {
	if info.workflow_id == '' { return error('missing interceptor workflow info') }
	if input == ['{"amount":777}'] { return error('update rejected by interceptor') }
	eprintln('INTERCEPT validator ${trace.name} enter ${name}')
	defer { eprintln('INTERCEPT validator ${trace.name} exit ${name}') }
	next.execute(name, input)!
}

pub fn client() []sdk.ClientInterceptor {
	if os.getenv('V_SDK_EXAMPLE_INTERCEPTORS') != 'true' { return []sdk.ClientInterceptor{} }
	return [sdk.ClientInterceptor(ClientTrace{'outer'}), ClientTrace{'inner'}]
}

pub fn worker(options sdk.WorkerOptions) sdk.WorkerOptions {
	if os.getenv('V_SDK_EXAMPLE_INTERCEPTORS') != 'true' { return options }
	return sdk.WorkerOptions{
		...options
		command_interceptors:          [sdk.CommandInterceptor(CommandTrace{'outer'}),
			CommandTrace{'inner'}]
		activity_interceptors:         [
			sdk.ActivityInterceptor(ActivityTrace{'outer'}),
			ActivityTrace{'inner'},
		]
		workflow_interceptors:         [
			sdk.WorkflowInterceptor(WorkflowTrace{'outer'}),
			WorkflowTrace{'inner'},
		]
		query_interceptors:            [sdk.QueryInterceptor(QueryTrace{'outer'}),
			QueryTrace{'inner'}]
		update_validator_interceptors: [
			sdk.UpdateValidatorInterceptor(UpdateTrace{'outer'}),
			UpdateTrace{'inner'},
		]
	}
}

struct CommandTrace {
	name string
}

fn (trace CommandTrace) intercept(command temporal.CoresdkWorkflowCommands_WorkflowCommand, info sdk.WorkflowInfo, next sdk.CommandNext) !temporal.CoresdkWorkflowCommands_WorkflowCommand {
	eprintln('INTERCEPT command ${trace.name} enter outbound')
	defer { eprintln('INTERCEPT command ${trace.name} exit outbound') }
	mut modified := command
	variant := command.variant or { return error('missing outbound command') }
	if trace.name == 'outer' {
		converter := sdk.DataConverter{}
		header := converter.encode_values(['"trace-propagated"'])![0]
		if variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleActivity_Case {
			mut value := variant.value
			value.headers['x-trace'] = header
			modified.variant = temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleActivity_Case{
				value: value
			}
		} else if variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleLocalActivity_Case {
			mut value := variant.value
			value.headers['x-trace'] = header
			modified.variant = temporal.CoresdkWorkflowCommands_WorkflowCommand_ScheduleLocalActivity_Case{
				value: value
			}
		}
	}
	return next.execute(modified)!
}
