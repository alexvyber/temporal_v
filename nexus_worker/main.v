module main

import temporal_v.example_converter
import x.json2
import os
import crypto.sha256
import temporal_v.sdk
import time

struct Request {
	name     string
	delay_ms int
}

fn echo(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	request := json2.decode[Request](input)!
	if request.delay_ms < 0 || request.delay_ms > 10000 { return error('invalid Nexus delay') }
	context.sleep(request.delay_ms * time.millisecond)!
	return sdk.NexusResult{
		value: json2.encode('Hello from Nexus, ${request.name}!')
	}
}

fn handler_error(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	request := json2.decode[Request](input)!
	if request.name == 'permanent' {
		return sdk.NexusHandlerError{
			message: 'permanent request rejection'
			kind:    .bad_request
			retry:   .non_retryable
		}
	}
	// A test-only marker makes the first request fail and its server retry succeed.
	directory := os.getenv('V_SDK_EXAMPLE_NEXUS_RETRY_DIRECTORY')
	if directory == '' { return error('retry fixture directory is required') }
	marker := os.join_path(directory, sha256.hexhash(context.info().request_id))
	if !os.exists(marker) {
		os.write_file(marker, 'attempted')!
		return sdk.NexusHandlerError{
			message:  'transient upstream unavailable'
			kind:     .unavailable
			retry:    .retryable
			metadata: {
				'fixture': 'retry'
			}
		}
	}
	return sdk.NexusResult{
		value: '"handler retry succeeded"'
	}
}

fn fail(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	return sdk.ApplicationError{
		message:       'requested Nexus failure'
		category:      'NexusExampleFailure'
		non_retryable: true
	}
}

fn async_start(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	eprintln('NEXUS_INCOMING_LINKS count=${context.info().links.len}')
	config := sdk.options_from_env()!
	client := context.client()!
	defer { client.close() }
	return context.start_workflow(client, sdk.WorkflowStart{
		workflow_type: 'v.nexus-target'
		task_queue:    config.task_queue
		arguments:     input
	})!
}

fn async_cancel(mut context sdk.NexusContext, token string) ! {
	client := context.client()!
	defer { client.close() }
	context.cancel_workflow(client, token)!
}

fn main() {
	original := sdk.options_from_env() or {
		eprintln(err)
		exit(1)
	}
	converter := example_converter.from_env() or {
		eprintln(err)
		exit(1)
	}
	mut supplied := ?&sdk.Client(none)
	if os.getenv('V_NEXUS_CONTEXT_SHARED') == 'true' {
		supplied = sdk.connect(sdk.ClientOptions{
			address:        original.address
			namespace:      original.namespace
			transport:      original.transport
			data_converter: converter
			plugins:        [sdk.Plugin(NexusContextPlugin{})]
		}) or { panic(err) }
	}
	defer {
		if parent := supplied { parent.close() }
	}
	options := sdk.WorkerOptions{
		...original
		client:         supplied
		data_converter: converter
	}
	mut services := sdk.NexusServices{}
	for operation in ['activity', 'activity-fail', 'activity-heartbeat', 'activity-deferred',
		'activity-twice'] {
		services.register('v-service', operation, sdk.NexusOperation{
			start:  activity_start
			cancel: activity_cancel
		}) or { panic(err) }
	}
	for operation in ['query', 'query-reject', 'signal-retry', 'signal-start-retry'] {
		services.register('v-service', operation, sdk.NexusOperation{ start: interaction_start }) or {
			panic(err)
		}
	}
	for operation in ['update-hold', 'update-immediate', 'update-fail', 'update-reject'] {
		services.register('v-service', operation, sdk.NexusOperation{ start: update_start }) or {
			panic(err)
		}
	}
	services.register('v-service', 'echo', sdk.NexusOperation{ start: echo }) or { panic(err) }
	services.register('v-service', 'context-client', sdk.NexusOperation{
		start:  context_client_start
		cancel: context_client_cancel
	}) or { panic(err) }
	services.register('v-service', 'async', sdk.NexusOperation{
		start:  async_start
		cancel: async_cancel
	}) or { panic(err) }
	services.register('v-service', 'async-fail', sdk.NexusOperation{
		start:  async_start
		cancel: async_cancel
	}) or { panic(err) }
	services.register('v-service', 'handler-error', sdk.NexusOperation{ start: handler_error }) or {
		panic(err)
	}
	services.register('v-service', 'fail', sdk.NexusOperation{ start: fail }) or { panic(err) }
	sdk.run_nexus_worker(options, services) or {
		eprintln(err)
		exit(1)
	}
}
