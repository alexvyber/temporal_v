module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import os

struct NexusContextPlugin {}

fn (plugin NexusContextPlugin) name() string {
	return 'nexus-context-client'
}

fn (plugin NexusContextPlugin) configure_client(options sdk.ClientOptions) !sdk.ClientOptions {
	mut file := os.open_append(os.getenv('V_NEXUS_CLIENT_HOOKS'))!
	defer { file.close() }
	file.writeln('configured')!
	return options
}

fn nexus_context_client(mut context sdk.NexusContext) !&sdk.Client {
	closed := context.client()!
	client := context.client()!
	closed.close()
	mut rejected := false
	closed.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
		if !err.msg().contains('closed') {
			client.close()
			return err
		}
		rejected = true
	}
	if !rejected {
		client.close()
		return error('closed Nexus client was usable')
	}
	client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
		client.close()
		return err
	}
	expected := if os.getenv('V_NEXUS_CONTEXT_SHARED') == 'true' { 1 } else { 0 }
	if client.plugin_info().len != expected {
		client.close()
		return error('Nexus client plugin configuration changed')
	}
	return client
}

fn context_client_start(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	client := nexus_context_client(mut context)!
	defer { client.close() }
	config := sdk.options_from_env()!
	os.write_file(os.join_path(os.getenv('V_NEXUS_CONTEXT_CALLS'), 'start-' +
		context.info().request_id), 'client')!
	return context.start_workflow(client, sdk.WorkflowStart{
		workflow_type: 'v.nexus-target'
		task_queue:    config.task_queue
		arguments:     input
	})!
}

fn context_client_cancel(mut context sdk.NexusContext, token string) ! {
	client := nexus_context_client(mut context)!
	defer { client.close() }
	os.write_file(os.join_path(os.getenv('V_NEXUS_CONTEXT_CALLS'), 'cancel'), 'client')!
	context.cancel_workflow(client, token)!
}
