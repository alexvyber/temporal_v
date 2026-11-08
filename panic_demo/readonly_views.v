module main

import alexvyber.temporal_v.sdk
import os
import x.json2
import time

struct ReadonlyScopeValue {
	kind  string
	level int
}

fn readonly_views_enabled() bool {
	return os.getenv('V_READONLY_VIEWS') == 'true'
}

struct ReadonlyQueryScope {}

fn (interceptor ReadonlyQueryScope) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.QueryNext) !sdk.PayloadValue {
	if !readonly_views_enabled() { return next.execute(name, input)! }
	if info.workflow_type == 'v.workflow-headers' {
		prior := info.context_value[ReadonlyScopeValue]('readonly/scope') or {
			return error('missing outer query scope')
		}
		assert prior.kind == 'query' && prior.level == 1
	}
	scoped :=
		next.with_context_value('readonly/scope', ReadonlyScopeValue{ kind: 'query', level: 2 })!
	return scoped.execute(name, input)!
}

struct ReadonlyValidatorScope {}

fn (interceptor ReadonlyValidatorScope) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.UpdateValidatorNext) ! {
	if !readonly_views_enabled() {
		next.execute(name, input)!
		return
	}
	if info.workflow_type == 'v.workflow-headers' {
		prior := info.context_value[ReadonlyScopeValue]('readonly/scope') or {
			return error('missing outer validator scope')
		}
		assert prior.kind == 'validator' && prior.level == 1
	}
	next.with_context_value('readonly/scope', ReadonlyScopeValue{ kind: 'validator', level: 2 })!.execute(name,
		input)!
}

fn check_readonly_scope(view sdk.WorkflowContextView, kind string, count int) ! {
	info := view.info()
	assert info.workflow_id != '' && info.run_id != '' && info.namespace != ''
		&& info.task_queue != ''
	assert info.original_execution_run_id != ''
	assert view.now().unix_milli() == info.time_millis
	assert !view.is_replaying_history_events()
	scope := view.context_value[ReadonlyScopeValue]('readonly/scope') or {
		return error('handler has no read-only scope')
	}
	assert scope.kind == kind && scope.level == 2
	assert info.context_value[ReadonlyScopeValue]('readonly/scope') or {
		return error('information lost read-only scope')
	} == scope
	assert view.input_values().len == 1
	assert json2.encode(view) == '{}'
	check_workflow_headers(view.raw_headers()!)!
	check_workflow_headers(view.raw_request_headers()!)!
	mut conversion_failed := false
	view.request_headers() or { conversion_failed = true }
	assert conversion_failed
	raw := view.raw_request_headers()!
	view.decode_raw_value(raw['m-sentinel']) or {
		assert err.msg() == 'explicit workflow header sentinel'
		conversion_failed = false
	}
	assert !conversion_failed && view.decode_raw_value(raw['z-known'])!.logical_data() == 'true'
	memo := view.memo()!
	assert (memo['view-counter'] or { return error('missing current memo') }).logical_data() == count.str()
	assert (view.raw_memo()!['_tracer-data'] or { return error('lost application memo') }).data().bytestr() == '"application memo"'
	assert view.search_attributes()!.len == view.raw_search_attributes()!.len
	mut information := view.info()
	if policy := information.retry_policy {
		mut copies := [policy]
		assert copies[0].non_retryable_error_types == ['VIEW_SENTINEL']
		copies[0].non_retryable_error_types[0] = 'mutated'
	}
	assert (view.info().retry_policy or { return error('missing view retry policy') }).non_retryable_error_types == [
		'VIEW_SENTINEL',
	]
	initial := view.raw_initialization()!
	mut payloads := [initial.headers['z-known']]
	if payloads[0].data.len > 0 { payloads[0].data[0] ^= 0xff }
	check_workflow_headers(view.raw_headers()!)!
}

fn (workflow &WorkflowHeadersWorkflow) check_query_view(name string, view sdk.WorkflowContextView) ! {
	check_readonly_scope(view, 'query', workflow.signals + workflow.updates)!
	old := workflow.view_snapshot or { return error('missing retained read-only snapshot') }
	assert (old.memo()!['view-counter'] or { return error('retained memo missing') }).logical_data() == '0'
	assert old.info().context_value[ReadonlyScopeValue]('readonly/scope') == none
	mut captured := workflow.captured or { return error('missing captured context') }
	if name == 'view-force-fail' {
		captured.force_task_fail(error('forbidden query task failure'))
		return error('task failure guard did not run')
	}
	mut rejected := false
	captured.timer(time.millisecond) or {
		assert err.msg().contains('forbidden in query handler')
		rejected = true
	}
	assert rejected
	mut cancellation_rejected := false
	captured.cancel() or {
		assert err.msg().contains('forbidden in query handler')
		cancellation_rejected = true
	}
	assert cancellation_rejected
	mut random_rejected := false
	captured.random_u64() or {
		assert err.msg().contains('forbidden in query handler')
		random_rejected = true
	}
	assert random_rejected
	if name == 'view-panic' { panic('read-only query panic') }
}

fn (workflow &WorkflowHeadersWorkflow) check_validator_view(name string, view sdk.WorkflowContextView) ! {
	check_readonly_scope(view, 'validator', workflow.signals + workflow.updates)!
	mut captured := workflow.captured or { return error('missing captured context') }
	mut rejected := false
	captured.timer(time.millisecond) or {
		assert err.msg().contains('forbidden in Update validator')
		rejected = true
	}
	assert rejected
	mut cancellation_rejected := false
	captured.cancel() or {
		assert err.msg().contains('forbidden in Update validator')
		cancellation_rejected = true
	}
	assert cancellation_rejected
	if name == 'view-reject' {
		return sdk.ApplicationError{
			message:  'read-only validation rejection'
			category: 'ReadonlyValidation'
		}
	}
	if name == 'view-panic' { panic('read-only validator panic') }
}

fn check_dynamic_readonly_view(view sdk.WorkflowContextView, kind string, count int) ! {
	if !readonly_views_enabled() { return }
	assert json2.encode(view) == '{}'
	info := view.info()
	assert info.workflow_id != '' && info.original_execution_run_id != ''
	assert view.now().unix_milli() == info.time_millis
	assert view.input_values().len == count
	scope := view.context_value[ReadonlyScopeValue]('readonly/scope') or {
		return error('missing dynamic handler context')
	}
	assert scope.kind == kind && scope.level == 2
	assert info.context_value[ReadonlyScopeValue]('readonly/scope') or {
		return error('missing dynamic handler information')
	} == scope
}
