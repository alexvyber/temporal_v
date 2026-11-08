module main

import temporal_v.sdk
import temporal_v.temporal
import temporal_v.corebridge
import os
import x.json2
import crypto.sha256

struct ActivationAudit {
	guard_failed      bool
	label             string
	run_id            string
	before            string
	after             string
	mutated           bool
	payloads          int
	initial_arguments int
}

struct ActivationTap {
	label string
}

fn (tap ActivationTap) name() string {
	return 'activation-' + tap.label
}

fn (tap ActivationTap) on_workflow_activation(activation temporal.CoresdkWorkflowActivation_WorkflowActivation) ! {
	before := sha256.sum(activation.encode()).hex()
	extracted :=
		corebridge.extract_payloads(.workflow_activation, activation.encode(), corebridge.PayloadMessageOptions{})!
	for payload in extracted.values {
		assert payload.external_payloads.len == 0
		assert 'temporal-v/codec-context' !in payload.metadata
		assert payload.data.len < 3 || payload.data[..3] != [u8(0), `B`, 255]
	}
	mut mutated := false
	mut initial_arguments := 0
	mut trigger := false
	for job in activation.jobs {
		variant := job.variant or { continue }
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_InitializeWorkflow_Case {
			initial_arguments = variant.value.arguments.len
			if tap.label == 'mutate' && variant.value.arguments.len > 0
				&& variant.value.arguments[0].data.len > 0 {
				mut initial := variant.value
				initial.arguments[0].data[0] ^= 0xff
				mutated = true
			}
		}
		if variant is temporal.CoresdkWorkflowActivation_WorkflowActivationJob_SignalWorkflow_Case {
			trigger = trigger || variant.value.signal_name == 'worker-fatal'
		}
	}
	after := sha256.sum(activation.encode()).hex()
	if mutated {
		assert before != after
	}
	audit := ActivationAudit{
		label:             tap.label
		run_id:            activation.run_id
		before:            before
		after:             after
		mutated:           mutated
		guard_failed:      tap.label == 'guard' && trigger
			&& os.getenv('V_ACTIVATION_HOOK_FAILURE') in ['error', 'panic']
		payloads:          extracted.values.len
		initial_arguments: initial_arguments
	}
	os.write_file(os.join_path(os.getenv('V_ACTIVATION_AUDIT_DIR'),
		'${activation.run_id}-${before}-${tap.label}.json'), json2.encode(audit))!
	if tap.label == 'guard' && trigger {
		if os.getenv('V_ACTIVATION_HOOK_FAILURE') == 'error' {
			return sdk.ApplicationError{
				message:  'activation guard error'
				category: 'ActivationGuard'
			}
		}
		if os.getenv('V_ACTIVATION_HOOK_FAILURE') == 'panic' { panic('activation guard panic') }
	}
}

fn configured_activation_hooks() []sdk.WorkerInterceptor {
	if os.getenv('V_ACTIVATION_AUDIT_DIR') == '' { return []sdk.WorkerInterceptor{} }
	return [sdk.WorkerInterceptor(ActivationTap{
		label: 'mutate'
	}), ActivationTap{
		label: 'guard'
	}, ActivationTap{ label: 'after' }]
}
