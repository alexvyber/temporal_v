module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import x.json2
import os
import crypto.sha256
import time

struct PolicyContextCapture {
	context &sdk.WorkflowContext
}

fn fixture_patch_policy(input sdk.PatchActivationInput) bool {
	if input.patch_id.starts_with('__temporal_v_version') {
		panic('numeric version consulted public patch policy')
	}
	if input.workflow_info.workflow_type != 'v.patch-policy' { return true }
	stage := os.getenv('V_PATCH_POLICY_STAGE')
	if stage == 'poison' { panic('historical patch consulted rollout policy') }
	if input.patch_id == 'policy-panic' && stage != 'repaired' { panic('patch policy panic') }
	if input.patch_id.contains('retired') { panic('deprecation consulted rollout policy') }
	raw := input.raw_headers() or { panic(err) }
	check_workflow_headers(raw) or { panic(err) }
	if input.decode_raw_value(raw['z-known']) or { panic(err) }.logical_data() != 'true' {
		panic('policy header scope changed')
	}
	memo := input.raw_memo() or { panic(err) }
	cohort := input.decode_raw_value(memo['cohort']) or { panic(err) }
	if input.decode_raw_value(memo['_tracer-data']) or { panic(err) }.logical_data() != '"application memo"' {
		panic('memo application key was filtered')
	}
	if input.patch_id.starts_with('policy-late-') && input.decode_raw_value(memo['modified']) or {
		panic(err)
	}.logical_data() != '"yes"' {
		panic('policy memo snapshot was stale')
	}
	attributes := input.search_attributes() or { panic(err) }
	expected := if input.patch_id.starts_with('policy-late-') { '"late"' } else { '"initial"' }
	if attributes['VPatchPolicyKeyword'].data().bytestr() != expected {
		panic('policy search attributes were stale')
	}
	mut retry := input.workflow_info.retry_policy or { panic('policy retry metadata missing') }
	if retry.non_retryable_error_types != ['Original'] { panic('policy metadata already mutated') }
	retry.non_retryable_error_types[0] = 'Mutated'
	capture := input.workflow_info.context_value[&PolicyContextCapture]('policy-context') or {
		panic('policy context value missing')
	}
	mut context := capture.context
	mut denied := false
	context.timer(time.hour) or { denied = true }
	if !denied { panic('patch policy scheduled a workflow command') }
	if context.info().retry_policy or { panic('retry snapshot missing') }.non_retryable_error_types != [
		'Original',
	] {
		panic('policy snapshot mutated workflow retry state')
	}
	path := os.join_path(os.getenv('V_PATCH_POLICY_AUDIT'), input.workflow_info.run_id + '-' +
		sha256.hexhash(input.patch_id))
	mut file := os.open_append(path) or { panic(err) }
	file.writeln(input.patch_id) or { panic(err) }
	file.close()
	if input.patch_id == 'policy-panic' { return true }
	selected := cohort.logical_data() == '"allow"'
	return selected && if stage == 'inverse' {
		!input.patch_id.ends_with('-on')
	} else {
		input.patch_id.ends_with('-on')
	}
}

fn configured_patch_policy() ?sdk.PatchActivationCallback {
	if os.getenv('V_PATCH_POLICY') == 'true' {
		return sdk.PatchActivationCallback(fixture_patch_policy)
	}
	return none
}

struct PatchPolicyWorkflow {
mut:
	mode     string
	on       bool
	off      bool
	retired  bool
	late_on  bool
	late_off bool
	version  int
}

fn patch_policy_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return PatchPolicyWorkflow{}
}

fn policy_initial(mut context sdk.WorkflowContext) ! {
	context.patched('policy-on')!
	context.patched('policy-off')!
}

fn (mut workflow PatchPolicyWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		workflow.mode = json2.decode[map[string]string](event.data)!['mode']
		workflow.version = context.get_version('policy-numeric', sdk.default_version, 2)!
		if workflow.version != 2 { return error('numeric version affected by rollout policy') }
		if os.getenv('V_PATCH_POLICY_CODE') == 'legacy' { return }
		capture := &PolicyContextCapture{
			context: &context
		}
		if workflow.mode == 'panic' {
			context.with_context_value('policy-context', capture, policy_panic)!
			context.complete('"policy-repaired"')!
			return
		}
		context.with_context_value('policy-context', capture, policy_initial)!
		workflow.on = context.patched('policy-on')!
		workflow.off = context.patched('policy-off')!
		context.deprecate_patch('policy-on')!
		context.deprecate_patch('policy-off')!
		if workflow.on != context.patched('policy-on')!
			|| workflow.off != context.patched('policy-off')! {
			return error('deprecation changed memoized decision')
		}
		context.deprecate_patch('policy-retired')!
		workflow.retired = context.patched('policy-retired')!
	}
	if event.kind == .signal_received && event.name == 'late' {
		context.upsert_memo({
			'modified': sdk.PayloadValue('"yes"')
		})!
		context.upsert_search_attributes([
			sdk.SearchAttribute{ name: 'VPatchPolicyKeyword', type_: .keyword, value_json: '"late"' },
		])!
		capture := &PolicyContextCapture{
			context: &context
		}
		context.with_context_value('policy-context', capture, policy_late)!
		workflow.late_on = context.patched('policy-late-on')!
		workflow.late_off = context.patched('policy-late-off')!
	}
	if event.kind == .signal_received && event.name == 'finish' {
		context.complete(workflow.state())!
	}
}

fn policy_panic(mut context sdk.WorkflowContext) ! {
	context.patched('policy-panic')!
}

fn policy_late(mut context sdk.WorkflowContext) ! {
	context.patched('policy-late-on')!
	context.patched('policy-late-off')!
}

fn (workflow &PatchPolicyWorkflow) state() string {
	return json2.encode({
		'on':       workflow.on.str()
		'off':      workflow.off.str()
		'retired':  workflow.retired.str()
		'late_on':  workflow.late_on.str()
		'late_off': workflow.late_off.str()
		'version':  workflow.version.str()
	})
}

fn (workflow &PatchPolicyWorkflow) query(name string, args []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(workflow.state())
}

fn start_patch_policy(config sdk.WorkerOptions, converter sdk.DataConverter) ! {
	client := sdk.connect(sdk.ClientOptions{
		address:        config.address
		namespace:      config.namespace
		data_converter: converter
	})!
	defer { client.close() }
	handle := client.start(sdk.WorkflowStart{
		workflow_id:       config.task_queue + '-' + os.args[2]
		workflow_type:     'v.patch-policy'
		task_queue:        config.task_queue
		arguments:         json2.encode({
			'mode': os.args[4]
		})
		headers:           header_payloads()!
		memo:              {
			'cohort':       sdk.PayloadValue(json2.encode(os.args[3]))
			'_tracer-data': sdk.PayloadValue('"application memo"')
		}
		search_attributes: [
			sdk.SearchAttribute{
				name:       'VPatchPolicyKeyword'
				type_:      .keyword
				value_json: '"initial"'
			},
		]
		retry:             sdk.RetryPolicy{
			maximum_attempts:          1
			non_retryable_error_types: ['Original']
		}
	})!
	println(json2.encode(handle))
}
