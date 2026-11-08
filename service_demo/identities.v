module main

import sdk
import temporal
import json2

fn identity_signals(client &sdk.Client, workflow sdk.WorkflowHandle) ! {
	for _ in 0 .. 16 {
		client.signal(workflow, 'add', '{"amount":1}')!
	}
}

struct IdentityState {
	total     i64
	additions []i64
}

fn request_identity_calls(client &sdk.Client, options sdk.WorkerOptions) ![]string {
	handle := client.start(sdk.WorkflowStart{
		workflow_id:   options.task_queue + '-identities'
		workflow_type: 'v.counter'
		task_queue:    options.task_queue
		arguments:     '{}'
	})!
	mut tasks := []thread !{}
	for _ in 0 .. 16 {
		tasks << spawn identity_signals(client, handle)
	}
	for task in tasks {
		task.wait()!
	}
	for _ in 0 .. 2 {
		client.signal(handle, 'add', '{"amount":7}', request_id: 'explicit-deduplication-request')!
	}
	state := json2.decode[IdentityState](client.query(handle, 'state', 'null')!)!
	if state.total != 263 || state.additions.len != 257 {
		return error('automatic identity collision lost a signal, or explicit identity failed to deduplicate')
	}
	client.signal(handle, 'finish', 'null')!
	if client.get_result(handle, 10000)!.status != .completed {
		return error('identity fixture did not complete')
	}
	history := client.history(handle)!
	mut seen := map[string]bool{}
	for event in history.events {
		attributes := event.attributes or { continue }
		if attributes is temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowExecutionSignaledEventAttributes_Case {
			id := attributes.value.request_id
			if id in seen { return error('a duplicate signal request ID was persisted twice') }
			seen[id] = true
			if id != 'explicit-deduplication-request'
				&& (id.len != 36 || id[14] != `4` || id[19] !in [`8`, `9`, `a`, `b`]) {
				return error('automatic request ID is not a UUIDv4')
			}
		}
	}
	if seen.len != 258 { return error('independent history lost unique signal identities') }
	return [
		'256 signals from sixteen concurrent callers used unique OS-random UUIDs; an explicit repeated identity deduplicated exactly one signal, and independent history preserved every committed identity',
	]
}
