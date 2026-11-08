module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import os
import x.json2
import crypto.sha256

struct CompletionAudit {
	label          string
	run_id         string
	before         string
	after          string
	failed         bool
	commands       int
	mutated        bool
	external       bool
	encoded_prefix bool
	contextual     bool
}

struct CompletionTap {
	label string
}

fn (tap CompletionTap) name() string {
	return 'completion-' + tap.label
}

fn mutate_completion_payload(value temporal.TemporalApiCommonV1_Payload) bool {
	mut values := [value]
	if values[0].data.len > 0 {
		values[0].data[0] ^= 0xff
		return true
	}
	if values[0].external_payloads.len > 0 {
		values[0].external_payloads[0].size_bytes++
		return true
	}
	return false
}

fn (tap CompletionTap) on_workflow_activation_completion(completion temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion) {
	before := sha256.sum(completion.encode()).hex()
	mut failed := false
	mut commands := 0
	mut mutated := false
	mut external := false
	mut encoded_prefix := false
	mut contextual := false
	if status := completion.status {
		if status is temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion_Successful_Case {
			commands = status.value.commands.len
			for command in status.value.commands {
				variant := command.variant or { continue }
				if variant is temporal.CoresdkWorkflowCommands_WorkflowCommand_CompleteWorkflowExecution_Case {
					if payload := variant.value.result {
						external = payload.external_payloads.len > 0
						encoded_prefix = payload.data.len >= 3
							&& payload.data[..3] == [u8(0), `B`, 255]
						contextual = 'temporal-v/codec-context' in payload.metadata
						if tap.label == 'mutate' { mutated = mutate_completion_payload(payload) }
					}
				}
			}
		} else {
			failed = true
		}
	}
	after := sha256.sum(completion.encode()).hex()
	if tap.label == 'mutate' && mutated {
		assert before != after
	}
	if tap.label != 'mutate' {
		assert before == after
	}
	audit := CompletionAudit{
		label:          tap.label
		run_id:         completion.run_id
		before:         before
		after:          after
		failed:         failed
		commands:       commands
		mutated:        mutated
		external:       external
		encoded_prefix: encoded_prefix
		contextual:     contextual
	}
	file := os.join_path(os.getenv('V_WORKER_COMPLETION_DIR'),
		'${completion.run_id}-${before}-${tap.label}.json')
	os.write_file(file, json2.encode(audit)) or { panic(err) }
	if tap.label == 'panic' { panic('completion observer panic') }
}

fn configured_completion_hooks() []sdk.WorkerInterceptor {
	if os.getenv('V_WORKER_COMPLETION_DIR') == '' { return []sdk.WorkerInterceptor{} }
	return [sdk.WorkerInterceptor(CompletionTap{
		label: 'mutate'
	}), CompletionTap{
		label: 'panic'
	}, CompletionTap{ label: 'after' }]
}
