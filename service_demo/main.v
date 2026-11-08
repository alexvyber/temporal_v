module main

import flag
import x.json2
import os
import rand
import temporal_v.sdk
import crypto.sha256
import temporal_v.example_converter
import temporal_v.example_tuning
import temporal_v.temporal
import time

fn argument(value string) temporal.TemporalApiCommonV1_Payloads {
	return temporal.TemporalApiCommonV1_Payloads{
		payloads: [
			temporal.TemporalApiCommonV1_Payload{
				metadata: {
					'encoding': 'json/plain'.bytes()
				}
				data:     value.bytes()
			},
		]
	}
}

fn service_calls(client &sdk.Client, queue string) ![]string {
	id := queue + '-service-' + rand.uuid_v4()
	mut cases := []string{}
	client.update_api_key('disposable-test-key')!
	client.update_metadata({
		'x-v-sdk-test': 'metadata'
	})!
	client.update_binary_metadata({
		'x-v-sdk-test-bin': [u8(0), 255, 128]
	})!
	client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
	health := client.rpc_health_check(temporal.GrpcHealthV1_HealthCheckRequest{
		service: 'temporal.api.workflowservice.v1.WorkflowService'
	})!
	if health.status != .serving { return error('workflow service is not serving') }
	client.update_api_key('')!
	client.update_metadata({})!
	client.update_binary_metadata({})!
	client.rpc_describe_namespace(temporal.TemporalApiWorkflowserviceV1_DescribeNamespaceRequest{
		namespace: 'default'
	})!
	started := client.rpc_signal_with_start_workflow_execution(temporal.TemporalApiWorkflowserviceV1_SignalWithStartWorkflowExecutionRequest{
		namespace:                   'default'
		workflow_id:                 id
		workflow_type:               temporal.TemporalApiCommonV1_WorkflowType{
			name: 'v.counter'
		}
		task_queue:                  temporal.TemporalApiTaskqueueV1_TaskQueue{
			name: queue
		}
		input:                       argument('{}')
		signal_name:                 'add'
		signal_input:                argument('{"amount":9}')
		request_id:                  rand.uuid_v4()
		workflow_id_conflict_policy: .workflow_id_conflict_policy_use_existing
	})!
	handle := sdk.WorkflowHandle{
		workflow_id: id
		run_id:      started.run_id
	}
	state := client.query(handle, 'state', 'null')!
	if !state.contains('9') { return error('signal-with-start did not deliver signal') }
	described := client.rpc_describe_workflow_execution(temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionRequest{
		namespace: 'default'
		execution: temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: id
			run_id:      started.run_id
		}
	})!
	info := described.workflow_execution_info or { return error('missing described execution') }
	if info.status != .workflow_execution_status_running {
		return error('expected running workflow')
	}
	client.signal(handle, 'finish', 'null')!
	if client.get_result(handle, 10000)!.status != .completed {
		return error('counter did not complete')
	}
	mut pages := 0
	mut token := []u8{}
	mut events := 0
	for {
		page :=
			client.history_page(handle, sdk.HistoryOptions{ page_size: 2, next_page_token: token })!
		pages++
		if history := page.history { events += history.events.len }
		token = page.next_page_token
		if token.len == 0 {
			break
		}
	}
	if pages < 2 || events < 5 { return error('history pagination was not exercised') }
	watch := time.new_stopwatch()
	for {
		query := "WorkflowId = '${id}'"
		count := client.rpc_count_workflow_executions(temporal.TemporalApiWorkflowserviceV1_CountWorkflowExecutionsRequest{
			namespace: 'default'
			query:     query
		})!
		listed := client.rpc_list_workflow_executions(temporal.TemporalApiWorkflowserviceV1_ListWorkflowExecutionsRequest{
			namespace: 'default'
			page_size: 10
			query:     query
		})!
		if count.count == 1 && listed.executions.len == 1 {
			break
		}
		if watch.elapsed() > 20 * time.second { return error('visibility did not converge') }
		time.sleep(50 * time.millisecond)
	}
	cases << 'Typed system/namespace calls, signal-with-start, describe, paginated history, count and visibility list'
	waiter := client.start(sdk.WorkflowStart{
		workflow_id:   id + '-terminated'
		workflow_type: 'v.wait'
		task_queue:    queue
		arguments:     '{}'
	})!
	client.rpc_terminate_workflow_execution(temporal.TemporalApiWorkflowserviceV1_TerminateWorkflowExecutionRequest{
		namespace:          'default'
		workflow_execution: temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: waiter.workflow_id
			run_id:      waiter.run_id
		}
		reason:             'typed V service test'
	})!
	if client.get_result(waiter, 10000)!.status != .terminated {
		return error('terminate result missing')
	}
	cases << 'Typed terminate call returned terminated workflow result'
	activity := client.rpc_start_activity_execution(temporal.TemporalApiWorkflowserviceV1_StartActivityExecutionRequest{
		namespace:              'default'
		activity_id:            id + '-activity'
		request_id:             rand.uuid_v4()
		activity_type:          temporal.TemporalApiCommonV1_ActivityType{
			name: 'v.echo'
		}
		task_queue:             temporal.TemporalApiTaskqueueV1_TaskQueue{
			name: queue
		}
		input:                  argument('"standalone"')
		start_to_close_timeout: temporal.GoogleProtobuf_Duration{
			seconds: 10
		}
		retry_policy:           temporal.TemporalApiCommonV1_RetryPolicy{
			maximum_attempts: 1
		}
	})!
	polled := client.rpc_poll_activity_execution(temporal.TemporalApiWorkflowserviceV1_PollActivityExecutionRequest{
		namespace:   'default'
		activity_id: id + '-activity'
		run_id:      activity.run_id
	})!
	outcome := polled.outcome or { return error('missing standalone Activity outcome') }
	value := outcome.value or { return error('missing standalone Activity value') }
	if value is temporal.TemporalApiActivityV1_ActivityExecutionOutcome_Result_Case {
		if value.value.payloads.len != 1
			|| value.value.payloads[0].data.bytestr() != '"Hello from V, standalone!"' {
			return error('unexpected standalone result')
		}
	} else {
		return error('standalone Activity failed')
	}
	cases << 'Typed standalone Activity start and result polling executed a registered V handler'
	return cases
}

fn operator_calls(client &sdk.Client, queue string) ![]string {
	client.rpc_operator_add_search_attributes(temporal.TemporalApiOperatorserviceV1_AddSearchAttributesRequest{
		namespace:         'default'
		search_attributes: {
			'VSDKOperatorKeyword': .indexed_value_type_keyword
		}
	})!
	listed := client.rpc_operator_list_search_attributes(temporal.TemporalApiOperatorserviceV1_ListSearchAttributesRequest{
		namespace: 'default'
	})!
	if 'VSDKOperatorKeyword' !in listed.custom_attributes {
		return error('operator search attribute not registered')
	}
	client.rpc_operator_remove_search_attributes(temporal.TemporalApiOperatorserviceV1_RemoveSearchAttributesRequest{
		namespace:         'default'
		search_attributes: ['VSDKOperatorKeyword']
	})!
	spec := temporal.TemporalApiNexusV1_EndpointSpec{
		name:   'operator-' + rand.uuid_v4()
		target: temporal.TemporalApiNexusV1_EndpointTarget{
			variant: temporal.TemporalApiNexusV1_EndpointTarget_Worker_Case{
				value: temporal.TemporalApiNexusV1_EndpointTarget_Worker{
					namespace:  'default'
					task_queue: queue
				}
			}
		}
	}
	created := client.rpc_operator_create_nexus_endpoint(temporal.TemporalApiOperatorserviceV1_CreateNexusEndpointRequest{
		spec: spec
	})!
	endpoint := created.endpoint or { return error('operator endpoint response missing endpoint') }
	client.rpc_operator_get_nexus_endpoint(temporal.TemporalApiOperatorserviceV1_GetNexusEndpointRequest{
		id: endpoint.id
	})!
	updated := client.rpc_operator_update_nexus_endpoint(temporal.TemporalApiOperatorserviceV1_UpdateNexusEndpointRequest{
		id:      endpoint.id
		version: endpoint.version
		spec:    spec
	})!
	current := updated.endpoint or { return error('updated endpoint missing') }
	endpoints := client.rpc_operator_list_nexus_endpoints(temporal.TemporalApiOperatorserviceV1_ListNexusEndpointsRequest{
		name:      spec.name
		page_size: 10
	})!
	if endpoints.endpoints.len != 1 || endpoints.endpoints[0].id != endpoint.id {
		return error('operator endpoint list mismatch')
	}
	client.rpc_operator_delete_nexus_endpoint(temporal.TemporalApiOperatorserviceV1_DeleteNexusEndpointRequest{
		id:      current.id
		version: current.version
	})!
	return [
		'Typed Health and Operator services: health check, search attribute add/list/remove, Nexus endpoint create/get/update/list/delete',
	]
}

fn schedule_calls(client &sdk.Client, queue string) ![]string {
	id := 'schedule-' + rand.uuid_v4()
	client.rpc_create_schedule(temporal.TemporalApiWorkflowserviceV1_CreateScheduleRequest{
		namespace:   'default'
		schedule_id: id
		request_id:  rand.uuid_v4()
		schedule:    temporal.TemporalApiScheduleV1_Schedule{
			spec:   temporal.TemporalApiScheduleV1_ScheduleSpec{}
			state:  temporal.TemporalApiScheduleV1_ScheduleState{
				paused: true
			}
			action: temporal.TemporalApiScheduleV1_ScheduleAction{
				action: temporal.TemporalApiScheduleV1_ScheduleAction_StartWorkflow_Case{
					value: temporal.TemporalApiWorkflowV1_NewWorkflowExecutionInfo{
						workflow_id:   id + '-workflow'
						workflow_type: temporal.TemporalApiCommonV1_WorkflowType{
							name: 'v.timer-cancel'
						}
						task_queue:    temporal.TemporalApiTaskqueueV1_TaskQueue{
							name: queue
						}
						input:         argument('{}')
					}
				}
			}
		}
	})!
	defer {
		client.rpc_delete_schedule(temporal.TemporalApiWorkflowserviceV1_DeleteScheduleRequest{
			namespace:   'default'
			schedule_id: id
		}) or { eprintln(err.msg()) }
	}
	client.rpc_describe_schedule(temporal.TemporalApiWorkflowserviceV1_DescribeScheduleRequest{
		namespace:   'default'
		schedule_id: id
	})!
	client.rpc_patch_schedule(temporal.TemporalApiWorkflowserviceV1_PatchScheduleRequest{
		namespace:   'default'
		schedule_id: id
		request_id:  rand.uuid_v4()
		patch:       temporal.TemporalApiScheduleV1_SchedulePatch{
			trigger_immediately: temporal.TemporalApiScheduleV1_TriggerImmediatelyRequest{}
		}
	})!
	watch := time.new_stopwatch()
	for {
		described := client.rpc_describe_schedule(temporal.TemporalApiWorkflowserviceV1_DescribeScheduleRequest{
			namespace:   'default'
			schedule_id: id
		})!
		if info := described.info {
			if info.recent_actions.len > 0 {
				execution := info.recent_actions[0].start_workflow_result or {
					return error('missing scheduled execution')
				}

				result := client.get_result(sdk.WorkflowHandle{
					workflow_id: execution.workflow_id
					run_id:      execution.run_id
				}, 10000)!
				if result.status != .completed {
					return error('scheduled workflow did not complete')
				}
				break
			}
		}
		if watch.elapsed() > 20 * time.second { return error('schedule trigger deadline') }
		time.sleep(50 * time.millisecond)
	}
	return [
		'Typed schedule create, describe, manual trigger, workflow result and deletion',
	]
}

fn concurrent_calls(client &sdk.Client, started chan bool, done chan string) {
	for index in 0 .. 50 {
		client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
			if err.msg().contains('closed') {
				done <- ''
				return
			}
			done <- err.msg()
			return
		}
		if index == 0 { started <- true }
	}
	done <- ''
}

fn concurrency(client &sdk.Client) ![]string {
	started := chan bool{cap: 12}
	done := chan string{cap: 12}
	for _ in 0 .. 12 {
		spawn concurrent_calls(client, started, done)
	}
	for _ in 0 .. 12 {
		_ := <-started
	}
	client.close()
	client.close()
	for _ in 0 .. 12 {
		message := <-done
		if message != '' { return error(message) }
	}
	client.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{}) or {
		if !err.msg().contains('closed') { return err }
		return [
			'Twelve concurrent RPC loops raced safely with idempotent client close; calls after close failed',
		]
	}
	return error('closed client accepted RPC')
}

fn update_calls(client &sdk.Client, queue string) ![]string {
	id := queue + '-update-with-start-' + rand.uuid_v4()
	start := sdk.WorkflowStart{
		workflow_id:     id
		workflow_type:   'v.counter'
		task_queue:      queue
		arguments:       '{}'
		request_id:      rand.uuid_v4()
		conflict_policy: .workflow_id_conflict_policy_use_existing
	}
	request := sdk.UpdateRequest{
		name:      'add'
		id:        'initial-update'
		arguments: '{"amount":4}'
		wait_for:  .completed
	}
	first := client.update_with_start(start, request)!
	if first.stage != .completed || client.update_result(first, 1000)!.failed {
		return error('Update-with-start did not complete')
	}
	repeated := client.update_with_start(start, request)!
	if client.update_result(repeated, 1000)!.data != client.update_result(first, 1000)!.data {
		return error('Update-with-start deduplication failed')
	}
	second := client.update_with_start(start, sdk.UpdateRequest{
		name:      'add'
		id:        'second-update'
		arguments: '{"amount":3}'
		wait_for:  .completed
	})!
	if !client.update_result(second, 1000)!.data.contains('7') {
		return error('existing workflow not reused')
	}
	client.start_update(first.workflow, sdk.UpdateRequest{
		name:                'delayed_add'
		id:                  'timed-update'
		arguments:           '{"amount":2}'
		wait_for:            .completed
		wait_timeout_millis: 50
	}) or {
		if err !is sdk.UpdateRpcError {
			return error('Update deadline did not preserve an UpdateRpcError')
		}
		// Tonic can surface the timeout stream reset as CANCELLED/h2. Preserve
		// that native status while checking the resumable Update identity.
		deadline_status := err.status == 4
			|| (err.status == 1 && err.message.contains('h2 protocol error'))
		if !deadline_status || err.update_id != 'timed-update' || err.workflow.workflow_id != id {
			return error('missing resumable Update identity: status=${err.status} update=${err.update_id} workflow=${err.workflow.workflow_id} expected=${id} message=${err.message}')
		}
	}
	delayed := client.update_result(sdk.UpdateHandle{ workflow: first.workflow, id: 'timed-update' },
		10000)!
	if delayed.failed || !delayed.data.contains('9') {
		return error('Update failed after caller deadline')
	}
	rejected := client.start_update(first.workflow, sdk.UpdateRequest{
		name:      'add'
		id:        'rejected-update'
		arguments: '{"amount":-1}'
		wait_for:  .completed
	})!
	if !client.update_result(rejected, 1000)!.failed {
		return error('Update validation rejection lost')
	}
	client.signal(first.workflow, 'finish', 'null')!
	if client.get_result(first.workflow, 10000)!.status != .completed {
		return error('Update-with-start counter did not complete')
	}
	return [
		'Update-with-start created/reused workflow, waited for completion and deduplicated stable IDs',
		'Update deadline preserved resumable identity; accepted Update completed after timeout; validation rejection retained its outcome',
	]
}

fn deployment_current(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	deployment := options.deployment or { return error('deployment options required') }
	client.rpc_set_worker_deployment_current_version(temporal.TemporalApiWorkflowserviceV1_SetWorkerDeploymentCurrentVersionRequest{
		namespace:       options.namespace
		deployment_name: deployment.name
		build_id:        options.build_id
	})!
	return ['current deployment set']
}

fn nexus_links() ![]string {
	original := temporal.TemporalApiCommonV1_Link_WorkflowEvent{
		namespace:   'namespace +/世界'
		workflow_id: 'workflow +/世界'
		run_id:      'run +/世界'
		reference:   temporal.TemporalApiCommonV1_Link_WorkflowEvent_EventRef_Case{
			value: temporal.TemporalApiCommonV1_Link_WorkflowEvent_EventReference{
				event_id:   27
				event_type: .event_type_nexus_operation_scheduled
			}
		}
	}
	link := sdk.nexus_link_from_common(temporal.TemporalApiCommonV1_Link{
		variant: temporal.TemporalApiCommonV1_Link_WorkflowEvent_Case{
			value: original
		}
	})!
	decoded := sdk.common_link_from_nexus(link)!
	variant := decoded.variant or { return error('missing converted workflow link') }
	if variant !is temporal.TemporalApiCommonV1_Link_WorkflowEvent_Case {
		return error('wrong workflow link case')
	}
	if variant.value.encode() != original.encode() {
		return error('workflow link did not round-trip escaped segments/reference')
	}
	camel := temporal.TemporalApiNexusV1_Link{
		...link
		url: link.url.replace('EVENT_TYPE_NEXUS_OPERATION_SCHEDULED', 'NexusOperationScheduled')
	}
	if sdk.common_link_from_nexus(camel)!.encode() != decoded.encode() {
		return error('CamelCase event type rejected')
	}
	for invalid in ['https://example.invalid/history',
		'temporal:///namespaces/default/workflows/id/run/history?eventID=-1',
		'temporal:///namespaces/default/workflows/id/run/history?eventID=bad'] {
		sdk.common_link_from_nexus(temporal.TemporalApiNexusV1_Link{ ...link, url: invalid }) or {
			continue
		}
		return error('invalid Nexus link accepted')
	}
	return [
		'Nexus workflow-event links round-tripped escaped Unicode/slash/space IDs and event references; malformed links rejected',
		nexus_resource_links()!,
	]
}

fn native_validation() ![]string {
	invalid_texts := ['bad\x00text', [u8(255), 254].bytestr()]
	for text in invalid_texts {
		sdk.connect(sdk.ClientOptions{ address: text }) or {
			if !err.msg().contains('UTF-8 without NUL') { return err }
			continue
		}
		return error('unsafe native address accepted')
	}
	for text in invalid_texts {
		sdk.connect(sdk.ClientOptions{
			transport: sdk.TransportOptions{
				identity: text
			}
		}) or {
			if !err.msg().contains('UTF-8 without NUL') { return err }
			continue
		}
		return error('unsafe native identity accepted')
	}
	return [
		'Native text validation rejected embedded NUL and invalid UTF-8 before entering Rust',
	]
}

fn cancellation_wait(client &sdk.Client, handle sdk.WorkflowHandle, done chan int) {
	client.get_result(handle, 30000) or {
		done <- err.code()
		return
	}
	done <- 0
}

fn raw_cancellation_wait(client &sdk.Client, handle sdk.WorkflowHandle, cancellation &sdk.RpcCancellation, done chan int) {
	request := temporal.TemporalApiWorkflowserviceV1_GetWorkflowExecutionHistoryRequest{
		namespace:                 'default'
		execution:                 temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: handle.workflow_id
			run_id:      handle.run_id
		}
		wait_new_event:            true
		history_event_filter_type: .history_event_filter_type_close_event
	}
	client.call_rpc(sdk.ClientRpcRequest{
		service:        .workflow
		method:         'GetWorkflowExecutionHistory'
		body:           request.encode()
		timeout_millis: 30000
		cancellation:   cancellation
		retry:          true
	}) or {
		done <- err.code()
		return
	}
	done <- 0
}

fn cancellation_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-rpc-cancellation'
		workflow_type: 'v.counter'
		task_queue:    options.task_queue
		arguments:     '{"initial":4}'
	})!
	cancellation := sdk.new_rpc_cancellation()!
	defer { cancellation.close() }
	scoped := sdk.connect(sdk.ClientOptions{
		address:            options.address
		namespace:          options.namespace
		rpc_timeout_millis: 30000
		cancellation:       cancellation
	})!
	defer { scoped.close() }
	done := chan int{cap: 1}
	spawn cancellation_wait(scoped, handle, done)
	time.sleep(100 * time.millisecond)
	cancellation.cancel()
	select {
		code := <-done {
			if code != 1 { return error('cancelled result wait returned status ${code}') }
		}
		3 * time.second {
			return error('cancelled result wait did not unblock')
		}
	}
	cancellation.close()
	cancellation.close()
	// A shared token owns independent concurrent native borrows. close cancels all
	// users and waits for them before freeing the native token.
	shared_token := sdk.new_rpc_cancellation()!
	results := chan int{cap: 4}
	for _ in 0 .. 4 {
		spawn raw_cancellation_wait(client, handle, shared_token, results)
	}
	time.sleep(100 * time.millisecond)
	watch := time.new_stopwatch()
	shared_token.close()
	if watch.elapsed() > 3 * time.second {
		return error('closing shared cancellation token did not drain')
	}
	for _ in 0 .. 4 {
		select {
			code := <-results {
				if code != 1 { return error('shared cancellation returned status ${code}') }
			}
			3 * time.second {
				return error('shared cancellation RPC still pending')
			}
		}
	}
	scoped.get_result(handle, 1000) or {
		if !err.msg().contains('token is closed') { return err }
		if client.query(handle, 'state', 'null')! != '{"total":4,"additions":[],"label":""}' {
			return error('RPC cancellation altered workflow state')
		}
		client.signal(handle, 'finish', 'null')!
		if client.get_result(handle, 10000)!.status != .completed {
			return error('workflow did not complete after caller cancellation')
		}
		return [
			'Client-wide result-wait cancellation returned gRPC CANCELLED without cancelling the workflow',
			'Closing a shared cancellation token drained four retry-enabled concurrent RPCs before freeing it; repeated close and use-after-close were safe',
		]
	}
	return error('RPC accepted a closed cancellation token')
}

fn binary_arguments(value string) sdk.Arguments {
	return sdk.Arguments(['alpha', value, 'omega'])
}

fn initial_binary_value() string {
	return [u8(0), 255, 128, 1, 2].bytestr()
}

fn updated_binary_value() string {
	return [u8(255), 0, 129, 8, 7].bytestr()
}

fn arguments_start(client &sdk.Client, options sdk.WorkerOptions) ![]sdk.WorkflowHandle {
	mut handles := []sdk.WorkflowHandle{}
	for mode in ['remote', 'local'] {
		handle := client.start(sdk.WorkflowStart{
			workflow_id:   options.task_queue + '-multi-' + mode
			workflow_type: 'v.arguments.' + mode
			task_queue:    options.task_queue
			arguments:     binary_arguments(initial_binary_value())
		})!
		watch := time.new_stopwatch()
		query_arguments := sdk.Arguments(sdk.TypedArguments{})
		for client.query(handle, 'ready', query_arguments)! != 'yes' {
			if watch.elapsed() > 10 * time.second {
				return error('argument Activity did not complete')
			}
			time.sleep(50 * time.millisecond)
		}
		if client.query(handle, 'echo', binary_arguments(initial_binary_value()))! != initial_binary_value() {
			return error('query lost binary argument boundary')
		}
		client.signal(handle, 'replace', binary_arguments('signalled'))!
		update := client.start_update(handle, sdk.UpdateRequest{
			name:      'replace'
			id:        'multi-update'
			arguments: binary_arguments(updated_binary_value())
			wait_for:  .completed
		})!
		result := client.update_result(update, 10000)!
		if result.failed || result.data != updated_binary_value() {
			return error('Update lost binary argument boundary')
		}
		if client.query(handle, 'state', sdk.Arguments(sdk.TypedArguments{}))! != updated_binary_value() {
			return error('signal/Update state incorrect')
		}
		handles << handle
	}
	return handles
}

fn arguments_finish(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	for mode in ['remote', 'local'] {
		handle := sdk.WorkflowHandle{
			workflow_id: options.task_queue + '-multi-' + mode
		}
		if client.query(handle, 'state', sdk.Arguments(sdk.TypedArguments{}))! != updated_binary_value() {
			return error('binary multi-argument state did not replay')
		}
		client.signal(handle, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
		result := client.get_result(handle, 10000)!
		if result.status != .completed || result.data != updated_binary_value() {
			return error('binary multi-argument result was corrupted')
		}
	}
	zero := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-multi-zero'
		workflow_type: 'v.arguments.zero'
		task_queue:    options.task_queue
	})!
	if client.get_result(zero, 10000)!.data != 'zero' { return error('zero-argument start failed') }
	child := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-multi-parent'
		workflow_type: 'v.arguments.parent'
		task_queue:    options.task_queue
		arguments:     binary_arguments(initial_binary_value())
	})!
	if client.get_result(child, 10000)!.data != initial_binary_value() {
		return error('child workflow lost multiple binary arguments')
	}
	chain := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-multi-chain'
		workflow_type: 'v.arguments.continue'
		task_queue:    options.task_queue
		arguments:     sdk.Arguments(['continue', initial_binary_value(), 'tail'])
	})!
	if client.get_chain_result(chain, 10000)!.data != initial_binary_value() {
		return error('continue-as-new lost multiple binary arguments')
	}
	return [
		'Zero/one/multiple payloads retained their boundaries through composed codecs and arbitrary binary bytes',
		'Multi-argument workflows, remote/local Activities, signals, queries and Updates survived SIGKILL/replay',
		'Default start passed zero arguments; child workflows and continue-as-new preserved argument lists',
	]
}

struct WrongCountCodec {}

fn (codec WrongCountCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	return []temporal.TemporalApiCommonV1_Payload{}
}

fn (codec WrongCountCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := values.clone()
	if values.len > 0 { result << values[0] }
	return result
}

fn codec_failure_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	converter := sdk.DataConverter{
		codecs: [sdk.PayloadCodec(WrongCountCodec{})]
	}
	mut encode_rejected := false
	converter.encode_value('null') or {
		if !err.msg().contains('exactly one') { return err }
		encode_rejected = true
	}
	if !encode_rejected { return error('codec silently dropped a payload') }
	mut decode_rejected := false
	converter.decode_value(argument('null').payloads[0]) or {
		if !err.msg().contains('exactly one') { return err }
		decode_rejected = true
	}
	if !decode_rejected { return error('codec silently inserted a payload') }
	directory := os.getenv('V_SDK_EXAMPLE_ASYNC_TOKEN_DIR')
	if directory == '' { return error('codec test requires a private token directory') }
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-poisoned-heartbeat'
		workflow_type: 'v.async-activity'
		task_queue:    options.task_queue
		arguments:     '{}'
	})!
	watch := time.new_stopwatch()
	for !os.exists(os.join_path(directory, handle.run_id + '.json')) {
		if watch.elapsed() > 10 * time.second { return error('deferred Activity did not start') }
		time.sleep(50 * time.millisecond)
	}
	client.rpc_respond_activity_task_failed_by_id(temporal.TemporalApiWorkflowserviceV1_RespondActivityTaskFailedByIdRequest{
		namespace:              options.namespace
		workflow_id:            handle.workflow_id
		run_id:                 handle.run_id
		activity_id:            'activity-1'
		identity:               'v-sdk-codec-fixture'
		failure:                temporal.TemporalApiFailureV1_Failure{
			message:      'retry with malformed stored heartbeat'
			failure_info: temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case{
				value: temporal.TemporalApiFailureV1_ApplicationFailureInfo{
					type_: 'CodecFixtureRetry'
				}
			}
		}
		last_heartbeat_details: temporal.TemporalApiCommonV1_Payloads{
			payloads: [
				temporal.TemporalApiCommonV1_Payload{
					metadata: {
						'encoding': 'json/plain'.bytes()
					}
					data:     'invalid-json-heartbeat'.bytes()
				},
			]
		}
	})!
	result := client.get_result(handle, 15000)!
	if result.status != .failed || !result.message.contains('heartbeat details decode failed') {
		return error('malformed stored heartbeat did not fail in isolation: ${result.status} ${result.message}')
	}
	healthy := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-healthy-after-poison'
		workflow_type: 'v.parallel'
		task_queue:    options.task_queue
		arguments:     '{"name":"healthy","delay_ms":50}'
	})!
	if client.get_result(healthy, 10000)!.status != .completed {
		return error('worker stopped serving after heartbeat decode failure')
	}
	for workflow_type in ['v.binary-error', 'v.binary-application-error'] {
		failed := client.start(sdk.WorkflowStart{
			workflow_id:   options.task_queue + '-' + workflow_type
			workflow_type: workflow_type
			task_queue:    options.task_queue
		})!
		diagnostic_result := client.get_result(failed, 10000)!
		if diagnostic_result.status != .failed || !diagnostic_result.message.contains('\\xff') {
			return error('binary diagnostic did not produce a valid recorded failure')
		}
	}
	return [
		'Binary diagnostic bytes were escaped in ordinary/application failures and structured logs without changing payload data',
		'Singular payload fields rejected missing/extra values in both codec directions',
		'Malformed heartbeat details on a retried deferred Activity failed in isolation; the same worker completed new Activities afterward',
	]
}

fn pending_activity(client &sdk.Client, handle sdk.WorkflowHandle, id string) !temporal.TemporalApiWorkflowV1_PendingActivityInfo {
	response := client.rpc_describe_workflow_execution(temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionRequest{
		namespace: 'default'
		execution: temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: handle.workflow_id
			run_id:      handle.run_id
		}
	})!
	for activity in response.pending_activities {
		if activity.activity_id == id { return activity }
	}
	return error('Activity no longer pending')
}

fn activity_management_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	for mode in ['pause', 'reset'] {
		handle := client.start(sdk.WorkflowStart{
			workflow_id:   options.task_queue + '-managed-' + mode
			workflow_type: 'v.activity-options'
			task_queue:    options.task_queue
			arguments:     '{"delay_ms":3000}'
		})!
		started := time.new_stopwatch()
		for {
			if activity := pending_activity(client, handle, 'activity-1') {
				if activity.state == .pending_activity_state_started {
					break
				}
			}
			if started.elapsed() > 10 * time.second {
				return error('managed Activity did not start')
			}
			time.sleep(50 * time.millisecond)
		}
		execution := temporal.TemporalApiCommonV1_WorkflowExecution{
			workflow_id: handle.workflow_id
			run_id:      handle.run_id
		}
		if mode == 'pause' {
			client.rpc_pause_activity(temporal.TemporalApiWorkflowserviceV1_PauseActivityRequest{
				namespace: options.namespace
				execution: execution
				activity:  temporal.TemporalApiWorkflowserviceV1_PauseActivityRequest_Id_Case{
					value: 'activity-1'
				}
				reason:    'SDK cancellation metadata test'
			})!
			paused := time.new_stopwatch()
			for {
				information := pending_activity(client, handle, 'activity-1')!
				if information.paused && information.state == .pending_activity_state_paused {
					break
				}
				if paused.elapsed() > 10 * time.second {
					return error('paused Activity did not cooperate with cancellation')
				}
				time.sleep(50 * time.millisecond)
			}
			client.rpc_unpause_activity(temporal.TemporalApiWorkflowserviceV1_UnpauseActivityRequest{
				namespace:       options.namespace
				execution:       execution
				activity:        temporal.TemporalApiWorkflowserviceV1_UnpauseActivityRequest_Id_Case{
					value: 'activity-1'
				}
				reset_attempts:  true
				reset_heartbeat: true
			})!
		} else {
			client.rpc_reset_activity(temporal.TemporalApiWorkflowserviceV1_ResetActivityRequest{
				namespace:       options.namespace
				execution:       execution
				activity:        temporal.TemporalApiWorkflowserviceV1_ResetActivityRequest_Id_Case{
					value: 'activity-1'
				}
				reset_heartbeat: true
			})!
		}
		result := client.get_result(handle, 20000)!
		if result.status != .completed {
			return error('managed Activity did not complete: ${mode} ${result.status} ${result.message}')
		}
	}
	return [
		'Pause/unpause and reset cooperatively interrupted running Activities; replacement attempts completed the original workflows',
	]
}

fn find_cause(failure &sdk.Failure, category string) !sdk.Failure {
	if failure.category == category { return *failure }
	if cause := failure.cause { return find_cause(cause, category)! }
	return error('missing failure category ${category}')
}

fn assert_failure_chain(failure sdk.Failure) ! {
	if failure.root_message() != 'root cause' { return error('missing root failure cause') }
	outer := find_cause(&failure, 'OuterFailure')!
	if os.getenv('V_SDK_EXAMPLE_FAILURE_CONVERTER') == 'tagged' {
		if failure.source != 'decoded-custom-v-failure'
			|| outer.source !in ['custom-v-failure', 'decoded-custom-v-failure'] {
			return error('custom failure converter was not used')
		}
	}
	inner := find_cause(&failure, 'InnerFailure')!
	if outer.details != ['{"step":1}', '"世界"'] || outer.stack_trace != 'outer_stack'
		|| outer.error_category != .benign || !outer.non_retryable {
		return error('outer failure metadata did not survive')
	}
	if inner.details != ['3'] || inner.stack_trace != 'inner_stack' {
		return error('inner failure metadata did not survive')
	}
}

fn failure_chain_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	policy := example_converter.from_env()!
	binary_policy := sdk.DataConverter{
		payload_converter: sdk.BinaryPayloadConverter{}
		codecs:            policy.codecs
	}
	encoded_attributes := sdk.DefaultFailureConverter{
		encode_common_attributes: true
	}.to_failure(error('attribute wire message'), binary_policy, 'VError')!
	decoded_attributes := binary_policy.decode_failure(encoded_attributes)
	if decoded_attributes.message != 'attribute wire message'
		|| decoded_attributes.protobuf != encoded_attributes.encode() {
		return error('standard failure attributes did not round-trip independently of the application value converter')
	}
	unavailable := sdk.DataConverter{}.decode_failure(encoded_attributes)
	if policy.codecs.len > 0 && (unavailable.attributes_error == ''
		|| unavailable.message != 'Encoded failure'
		|| unavailable.protobuf != encoded_attributes.encode()) {
		return error('unavailable attribute codec lost diagnostic or raw failure bytes')
	}

	for mode in ['remote', 'local'] {
		handle := client.start(sdk.WorkflowStart{
			workflow_id:   options.task_queue + '-failure-' + mode
			workflow_type: 'v.failure-chain.' + mode
			task_queue:    options.task_queue
		})!
		result := client.get_result(handle, 10000)!
		if result.status != .failed { return error('nested failure workflow did not fail') }
		failure := result.failure or { return error('missing workflow failure') }
		assert_failure_chain(failure)!
		description := client.rpc_describe_workflow_execution(temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionRequest{
			namespace: options.namespace
			execution: temporal.TemporalApiCommonV1_WorkflowExecution{
				workflow_id: handle.workflow_id
				run_id:      handle.run_id
			}
		})!
		info := description.workflow_execution_info or { return error('missing execution info') }
		memo := info.memo or { return error('missing captured failure memo') }
		fields := example_converter.from_env()!.decode_map(memo.fields)!
		expected := json2.decode[string]((fields['captured_failure_hash'] or {
			return error('missing captured failure hash')
		}).logical_data())!
		if expected != sha256.hexhash(failure.protobuf.bytestr()) {
			return error('rethrow changed captured failure protocol metadata')
		}
	}
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-failure-update'
		workflow_type: 'v.failure-updates'
		task_queue:    options.task_queue
	})!
	update :=
		client.start_update(handle, sdk.UpdateRequest{ name: 'nested', wait_for: .completed })!
	outcome := client.update_result(update, 10000)!
	if !outcome.failed { return error('Update did not report application failure') }
	assert_failure_chain(outcome.failure or { return error('missing Update failure') })!
	rejected := client.start_update(handle, sdk.UpdateRequest{
		name:     'rejected'
		wait_for: .completed
	})!
	rejection := client.update_result(rejected, 10000)!
	if !rejection.failed { return error('Update validator did not reject') }
	assert_failure_chain(rejection.failure or { return error('missing validator failure') })!
	client.signal(handle, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
	if client.get_result(handle, 10000)!.status != .completed {
		return error('failed Update closed the workflow')
	}
	return [
		'Nested application/generic causes, stack traces, benign category and multiple detail payloads survived remote/local Activity propagation',
		'Rethrowing captured failures preserved their exact protobuf bytes; failed Update retained its cause chain without closing the workflow',
	]
}

fn assert_failure_scope(value temporal.TemporalApiFailureV1_Failure, namespace string, workflow_id string) ! {
	if payload := value.encoded_attributes {
		context :=
			json2.decode[sdk.SerializationContext](payload.metadata['temporal-v/codec-context'].bytestr())!
		if context.kind != .workflow || context.namespace != namespace
			|| context.workflow_id != workflow_id {
			return error('failure common attributes kept the wrong actor scope')
		}
	}
	if info := value.failure_info {
		if info is temporal.TemporalApiFailureV1_Failure_ApplicationFailureInfo_Case {
			if details := info.value.details {
				for payload in details.payloads {
					for key in ['temporal-v/codec-context', 'temporal-v/value-context'] {
						context :=
							json2.decode[sdk.SerializationContext](payload.metadata[key].bytestr())!
						if context.kind != .workflow || context.namespace != namespace
							|| context.workflow_id != workflow_id {
							return error('failure details kept the wrong actor scope')
						}
					}
				}
			}
		}
	}
	if cause := value.cause { assert_failure_scope(*cause, namespace, workflow_id)! }
}

fn serialization_context_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	query_run := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-ctx-query-error'
		workflow_type: 'v.counter'
		task_queue:    options.task_queue
		arguments:     '{}'
	})!
	if _ := client.query(query_run, 'explode', sdk.Arguments(sdk.TypedArguments{})) {
		return error('query exception unexpectedly succeeded')
	} else {
		if err is sdk.QueryError {
			if err.msg() != 'requested query error' || err.failure.attributes_error != ''
				|| err.rpc_failure.details.len == 0 {
				return error('encrypted query failure lost details or RPC status')
			}
		} else {
			return error('query exception did not retain its structured encoded failure: ${err.msg()}')
		}
	}
	client.signal(query_run, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
	if client.get_result(query_run, 10000)!.status != .completed {
		return error('query exception closed its workflow')
	}

	for mode in ['remote', 'local'] {
		handle := client.start(sdk.WorkflowStart{
			workflow_id:   options.task_queue + '-ctx-' + mode + '-世界'
			workflow_type: 'v.failure-chain.' + mode
			task_queue:    options.task_queue
		})!
		result := client.get_result(handle, 10000)!
		if result.status != .failed { return error('contextual failure workflow did not fail') }
		failure := result.failure or { return error('missing contextual failure') }
		assert_failure_chain(failure)!
		assert_failure_scope(temporal.TemporalApiFailureV1_Failure.decode(failure.protobuf)!,
			options.namespace, handle.workflow_id)!
	}
	child_namespace := options.namespace + '-context-child'
	parent := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-ctx-parent'
		workflow_type: 'v.context-failure-parent'
		task_queue:    options.task_queue
		arguments:     json2.encode({
			'namespace': child_namespace
		})
	})!
	result := client.get_result(parent, 15000)!
	failure := result.failure or { return error('missing cross-namespace child failure') }
	if result.status != .failed || failure.kind != .child_workflow {
		return error('contextual child failure changed its protocol kind')
	}
	assert_failure_chain(failure)!
	raw_parent := temporal.TemporalApiFailureV1_Failure.decode(failure.protobuf)!
	information := raw_parent.failure_info or { return error('missing child identity') }
	if information is temporal.TemporalApiFailureV1_Failure_ChildWorkflowExecutionFailureInfo_Case {
		execution := information.value.workflow_execution or {
			return error('missing child execution')
		}

		if information.value.namespace != child_namespace
			|| execution.workflow_id != parent.workflow_id + '-child' {
			return error('failure rebinding changed child identity')
		}
	} else {
		return error('failure rebinding changed child protocol metadata')
	}
	assert_failure_scope(raw_parent, options.namespace, parent.workflow_id)!
	child := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-ctx-child-success'
		workflow_type: 'v.arguments.parent'
		task_queue:    options.task_queue
		arguments:     sdk.Arguments(['first', 'context child payload', 'third'])
	})!
	if client.get_result(child, 10000)!.data != 'context child payload' {
		return error('contextual child result did not decode')
	}
	continued := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-ctx-continue'
		workflow_type: 'v.arguments.continue'
		task_queue:    options.task_queue
		arguments:     sdk.Arguments(['continue', 'context continuation payload', 'third'])
	})!
	if client.get_chain_result(continued, 10000)!.data != 'context continuation payload' {
		return error('contextual continuation result did not decode')
	}
	return [
		'Structured query failures restored codec-protected messages while retaining gRPC status and leaving workflows open',
		'Actor context reached payload converters and composed codecs; remote/local failures retained causes and details across Activity-to-workflow rebinding',
		'Cross-namespace child failure retained its child identity while payloads rebound to the parent namespace/workflow ID',
		'Child success and continue-as-new preserved target-scoped argument and result conversion',
	]
}

fn run() ! {
	mut parser := flag.new_flag_parser(os.args)
	parser.skip_executable()
	mode := parser.string('mode', 0, 'service', 'service, schedules or concurrency')
	parser.finalize()!
	if mode == 'dynamic-validation' {
		println(json2.encode(dynamic_validation()!))
		return
	}
	if mode == 'versioning-validation' {
		println(json2.encode(versioning_validation()!))
		return
	}
	options := sdk.options_from_env()!
	client := sdk.connect(sdk.ClientOptions{
		address:        options.address
		namespace:      options.namespace
		transport:      options.transport
		data_converter: example_converter.from_env()!
	})!
	defer { client.close() }
	if mode == 'arguments-start' {
		println(json2.encode(arguments_start(client, options)!))
		return
	}
	cases := match mode {
		'native-validation' { native_validation()! }
		'nexus-links' { nexus_links()! }
		'service' { service_calls(client, options.task_queue)! }
		'arguments-finish' { arguments_finish(client, options)! }
		'codec-failures' { codec_failure_calls(client, options)! }
		'activity-management' { activity_management_calls(client, options)! }
		'serialization-contexts' { serialization_context_calls(client, options)! }
		'standalone-start' { standalone_restart_start(client, options)! }
		'client-interactions-start' { client_interactions_start(client, options)! }
		'client-interactions-finish' { client_interactions_finish(client, options)! }
		'nexus-activity-start' { nexus_activity_start(client, options)! }
		'nexus-activity-finish' { nexus_activity_finish(client, options)! }
		'update-context-start' { update_context_start(client, options)! }
		'update-context-finish' { update_context_finish(client, options)! }
		'nexus-interactions' { nexus_interaction_calls(client, options)! }
		'nexus-update-start' { nexus_update_start(client, options)! }
		'nexus-update-finish' { nexus_update_finish(client, options)! }
		'eager-workflows' { eager_workflow_calls(client, options)! }
		'request-identities' { request_identity_calls(client, options)! }
		'workflow-lifecycle' { lifecycle_calls(client, options)! }
		'result-contexts' { result_context_calls(client, options)! }
		'activity-result-start' { activity_result_context_start(client, options)! }
		'activity-result-finish' { activity_result_context_calls(client, options)! }
		'nexus-client-start' { nexus_client_start(client, options)! }
		'nexus-client-finish' { nexus_client_calls(client, options)! }
		'standalone-finish' { standalone_calls(client, options)! }
		'custom-tuners' { example_tuning.exercise(client, options)! }
		'failure-chains' { failure_chain_calls(client, options)! }
		'schedules' { schedule_calls(client, options.task_queue)! }
		'concurrency' { concurrency(client)! }
		'cancellation' { cancellation_calls(client, options)! }
		'deployment-current' { deployment_current(client, options)! }
		'operator' { operator_calls(client, options.task_queue)! }
		'updates' { update_calls(client, options.task_queue)! }
		else { return error('unknown service test mode') }
	}

	println(json2.encode(cases))
}

fn main() {
	run() or {
		eprintln(err.msg())
		exit(1)
	}
}
