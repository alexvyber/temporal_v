module main

import temporal_v.sdk
import temporal_v.temporal
import x.json2
import os
import crypto.sha256

struct WorkflowHeaderConverter {}

fn (converter WorkflowHeaderConverter) to_payload(value sdk.PayloadValue, scope sdk.SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	return sdk.DefaultPayloadConverter{}.to_payload(value, scope)!
}

fn (converter WorkflowHeaderConverter) from_payload(value temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) !sdk.PayloadValue {
	if value.data.bytestr() == '"HEADER_SENTINEL"' {
		if scope.kind != .workflow || scope.workflow_id == '' {
			return error('wrong workflow header conversion scope')
		}
		return error('explicit workflow header sentinel')
	}
	return sdk.DefaultPayloadConverter{}.from_payload(value, scope)!
}

fn check_workflow_headers(headers map[string]sdk.RawValue) ! {
	if headers.len != 3 { return error('wrong raw workflow header count') }
	value := headers['a-opaque'] or { return error('missing raw workflow header') }
	if value.encoding() != 'application/x-opaque-header' || value.data().len != 300003
		|| sha256.sum(value.data()) != sha256.sum([u8(0), 255, 42].repeat(100001)) {
		return error('workflow opaque header changed')
	}
	if value.to_payload()!.metadata['private'] != [u8(111), 112, 97, 113, 117, 101, 0, 255, 0] {
		return error('workflow binary metadata changed')
	}
	mut bytes := value.data()
	bytes[0] = 42
	mut wire := value.to_payload()!
	wire.data[0] = 42
	if value.data()[0] != 0 { return error('raw workflow header ownership changed') }
}

fn check_workflow_header_conversion(mut context sdk.WorkflowContext, headers map[string]sdk.RawValue) ! {
	check_workflow_headers(headers)!
	mut rejected := false
	context.decode_raw_value(headers['m-sentinel']) or {
		if err.msg() != 'explicit workflow header sentinel' { return err }
		rejected = true
	}
	if !rejected || context.decode_raw_value(headers['z-known'])!.logical_data() != 'true' {
		return error('workflow scoped explicit header conversion changed')
	}
}

struct WorkflowHeadersWorkflow {
mut:
	view_snapshot ?sdk.WorkflowContextView
	captured      ?&sdk.WorkflowContext
	signals       int
	updates       int
}

fn workflow_headers_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return WorkflowHeadersWorkflow{}
}

fn (mut workflow WorkflowHeadersWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if readonly_views_enabled() {
		assert context.context_value[ReadonlyScopeValue]('readonly/scope') == none
	}
	if event.kind == .started {
		if readonly_views_enabled() {
			workflow.captured = &context
			workflow.view_snapshot = context.view()!
		}
		check_workflow_header_conversion(mut context, context.raw_headers()!)!
		check_workflow_headers(context.raw_event_headers(event)!)!
		mut rejected := false
		context.headers() or { rejected = true }
		if !rejected { return error('opaque start header converted silently') }
	}
	if event.kind in [.signal_received, .update_received] {
		check_workflow_header_conversion(mut context, context.raw_event_headers(event)!)!
		mut rejected := false
		context.event_headers(event) or { rejected = true }
		if !rejected { return error('opaque event header converted silently') }
	}
	if event.kind == .signal_received {
		if event.name == 'finish' {
			context.complete(json2.encode({
				'signals': workflow.signals
				'updates': workflow.updates
			}))!
		} else {
			workflow.signals++
			if readonly_views_enabled() {
				context.upsert_memo({
					'view-counter': sdk.PayloadValue((workflow.signals + workflow.updates).str())
				})!
			}
		}
	}
	if event.kind == .update_received {
		workflow.updates++
		if readonly_views_enabled() {
			context.upsert_memo({
				'view-counter': sdk.PayloadValue((workflow.signals + workflow.updates).str())
			})!
		}
		context.complete_update(event.id, sdk.PayloadValue(workflow.updates.str()))!
	}
}

fn (workflow &WorkflowHeadersWorkflow) query(name string, args []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if readonly_views_enabled() { workflow.check_query_view(name, view)! }
	return sdk.PayloadValue(json2.encode({
		'signals': workflow.signals
		'updates': workflow.updates
	}))
}

fn (workflow &WorkflowHeadersWorkflow) validate_update(name string, args []string, view sdk.WorkflowContextView) ! {
	if readonly_views_enabled() { workflow.check_validator_view(name, view)! }
	if os.getenv('V_WORKFLOW_HEADER_REPLAY') == 'true' {
		return error('accepted update validator reran during replay')
	}
}

struct WorkflowHeaderQuery {}

fn (interceptor WorkflowHeaderQuery) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.QueryNext) !sdk.PayloadValue {
	if info.workflow_type != 'v.workflow-headers' { return next.execute(name, input)! }
	raw := next.raw_headers()!
	check_workflow_headers(raw)!
	mut rejected := false
	next.headers() or { rejected = true }
	if !rejected { return error('opaque query header converted silently') }
	rejected = false
	next.decode_raw_value(raw['m-sentinel']) or {
		if err.msg() != 'explicit workflow header sentinel' { return err }
		rejected = true
	}
	if !rejected || next.decode_raw_value(raw['z-known'])!.logical_data() != 'true' {
		return error('query explicit conversion changed')
	}
	if readonly_views_enabled() {
		assert info.context_value[ReadonlyScopeValue]('readonly/scope') == none
		scoped := next.with_context_value('readonly/scope', ReadonlyScopeValue{
			kind:  'query'
			level: 1
		})!
		result := scoped.execute(name, input)!
		assert next.context_value[ReadonlyScopeValue]('readonly/scope') == none
		return result
	}
	return next.execute(name, input)!
}

struct WorkflowHeaderValidator {}

fn (interceptor WorkflowHeaderValidator) intercept(name string, input []string, info sdk.WorkflowInfo, next sdk.UpdateValidatorNext) ! {
	if info.workflow_type != 'v.workflow-headers' {
		next.execute(name, input)!
		return
	}
	if os.getenv('V_WORKFLOW_HEADER_REPLAY') == 'true' {
		return error('raw header validator reran during replay')
	}
	raw := next.raw_headers()!
	check_workflow_headers(raw)!
	mut rejected := false
	next.headers() or { rejected = true }
	if !rejected { return error('opaque validator header converted silently') }
	rejected = false
	next.decode_raw_value(raw['m-sentinel']) or {
		if err.msg() != 'explicit workflow header sentinel' { return err }
		rejected = true
	}
	if !rejected || next.decode_raw_value(raw['z-known'])!.logical_data() != 'true' {
		return error('validator explicit conversion changed')
	}
	if readonly_views_enabled() {
		assert info.context_value[ReadonlyScopeValue]('readonly/scope') == none
		scoped := next.with_context_value('readonly/scope', ReadonlyScopeValue{
			kind:  'validator'
			level: 1
		})!
		scoped.execute(name, input)!
		assert next.context_value[ReadonlyScopeValue]('readonly/scope') == none
		return
	}
	next.execute(name, input)!
}

fn workflow_header_queries() []sdk.QueryInterceptor {
	mut interceptors := []sdk.QueryInterceptor{}
	if os.getenv('V_WORKFLOW_HEADER_CONVERTER') == 'true' { interceptors << WorkflowHeaderQuery{} }
	if readonly_views_enabled() { interceptors << ReadonlyQueryScope{} }
	return interceptors
}

fn workflow_header_validators() []sdk.UpdateValidatorInterceptor {
	mut interceptors := []sdk.UpdateValidatorInterceptor{}
	if os.getenv('V_WORKFLOW_HEADER_CONVERTER') == 'true' {
		interceptors << WorkflowHeaderValidator{}
	}
	if readonly_views_enabled() { interceptors << ReadonlyValidatorScope{} }
	return interceptors
}

fn workflow_header_client(config sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{
		address:            config.address
		namespace:          config.namespace
		data_converter:     converter
		rpc_timeout_millis: 20000
	})!
	defer { client.close() }
	if os.args[1] == 'workflow-headers-start' {
		handle := client.start(sdk.WorkflowStart{
			workflow_id:   config.task_queue + '-' + os.args[2]
			workflow_type: 'v.workflow-headers'
			task_queue:    config.task_queue
			arguments:     'null'
			headers:       header_payloads()!
			memo:          if readonly_views_enabled() {
				{
					'view-counter': sdk.PayloadValue('0')
					'_tracer-data': sdk.PayloadValue('"application memo"')
				}
			} else {
				map[string]sdk.PayloadValue{}
			}
			retry:         if readonly_views_enabled() {
				?sdk.RetryPolicy(sdk.RetryPolicy{
					maximum_attempts:          1
					non_retryable_error_types: ['VIEW_SENTINEL']
				})
			} else {
				none
			}
		})!
		println(json2.encode(handle))
		return
	}
	handle := sdk.WorkflowHandle{
		workflow_id: os.args[2]
		run_id:      os.args[3]
	}
	match os.args[1] {
		'workflow-headers-query' {
			println(client.query(handle, if os.args.len > 4 { os.args[4] } else { 'state' },
				'null', sdk.QueryOptions{ headers: header_payloads()! })!)
		}
		'workflow-headers-signal' {
			client.signal(handle, os.args[4], 'null', sdk.MessageOptions{
				headers: header_payloads()!
			})!
			println('OK')
		}
		'workflow-headers-update' {
			update := client.start_update(handle, sdk.UpdateRequest{
				name:                if os.args.len > 4 {
					os.args[4]
				} else {
					'increment'
				}
				id:                  if os.args.len > 5 {
					os.args[5]
				} else {
					'workflow-header-update'
				}
				arguments:           'null'
				headers:             header_payloads()!
				wait_for:            .completed
				wait_timeout_millis: 20000
			})!
			result := client.update_result(update, 20000)!
			if result.failed { return error('raw header update failed: ' + result.message) }
			println(result.data)
		}
		else {
			return error('unknown workflow header client mode')
		}
	}
}

fn (workflow WorkflowHeadersWorkflow) update_names() []string {
	return if readonly_views_enabled() {
		['increment', 'view-reject', 'view-panic']
	} else {
		['increment']
	}
}
