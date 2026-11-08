module main

import sdk
import json2
import time

struct DetailsInput {
	endpoint string
	index    int
	mode     string
}

struct DetailsWorkflow {
mut:
	cancel_remote            u32
	cancel_local             u32
	cancel_completions       int
	metadata_completions     int
	memo_preserves_null      bool
	received_metadata_signal bool
	received_metadata_update bool
	index                    int
	mode                     string
	ready                    bool
	activity                 u32
	captured                 ?sdk.Failure
}

fn make_details_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return DetailsWorkflow{}
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.details', make_details_workflow)!
	runtime.register_local_activity('v.details.headers', headers_activity)!
	runtime.register_local_activity('v.details.cancel', cancel_details_activity)!
	return runtime
}

fn (mut workflow DetailsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if workflow.mode == 'nexus-captured-error' && event.kind == .nexus_failed {
		failure := event.failure or { return error('captured Nexus failure missing') }
		check_nexus_failure(failure, workflow.index)!
		workflow.captured = failure
		workflow.ready = true
		return
	}
	if workflow.mode == 'nexus-cancel-error' && event.kind in [.nexus_cancelled, .nexus_failed] {
		failure := event.failure or { return error('Nexus cancellation failure missing') }
		if failure.root_kind() != .canceled || validate_failure(failure)! != 1 {
			return error('Nexus cancellation details changed')
		}
		context.complete(sdk.json_value(true)!)!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[DetailsInput](event.data)!
			workflow.mode = input.mode
			workflow.index = input.index
			if input.mode == 'cancel-error' {
				workflow.ready = true
				return
			}
			if input.mode == 'cancel-activities' {
				workflow.cancel_remote = context.activity('v.details.cancel', detail_arguments()!, sdk.ActivityOptions{
					start_to_close: 2 * time.minute
					retry:          sdk.RetryPolicy{
						maximum_attempts: 1
					}
					cancellation:   .wait_completed
				})!
				workflow.cancel_local = context.local_activity('v.details.cancel',
					detail_arguments()!, sdk.LocalActivityOptions{
					start_to_close: 2 * time.minute
					retry:          sdk.RetryPolicy{
						maximum_attempts: 1
					}
					cancellation:   .wait_completed
				})!
				return
			}
			if input.mode == 'nexus-captured-error' {
				context.nexus_operation(sdk.json_value(input.index)!, sdk.NexusOptions{
					endpoint:  input.endpoint
					service:   'typed.details'
					operation: 'captured'
				})!
				return
			}
			if input.mode == 'nexus-cancel-error' {
				context.nexus_operation(detail_arguments()!.values[1], sdk.NexusOptions{
					endpoint:  input.endpoint
					service:   'typed.details'
					operation: 'cancel'
				})!
				return
			}
			if input.mode == 'metadata' {
				workflow.start_metadata(mut context)!
				return
			}
			if input.mode == 'nexus' {
				context.nexus_operation(detail_arguments()!.values[input.index], sdk.NexusOptions{
					endpoint:  input.endpoint
					service:   'typed.details'
					operation: 'echo'
				})!
				return
			}
			if workflow.mode in ['wf-cancel', 'terminate'] {
				workflow.ready = true
				return
			}
			name := if workflow.mode == 'heartbeat' {
				'v.details.heartbeat'
			} else if workflow.mode.starts_with('defer-') {
				'v.details.defer'
			} else {
				'v.details.error'
			}
			workflow.activity = context.activity(name, detail_arguments()!, sdk.ActivityOptions{
				start_to_close:    2 * time.minute
				heartbeat_timeout: if workflow.mode == 'heartbeat' {
					10 * time.second
				} else {
					0
				}
				retry:             sdk.RetryPolicy{
					maximum_attempts: if workflow.mode == 'heartbeat' {
						2
					} else {
						1
					}
					initial_interval: 100 * time.millisecond
				}
				cancellation:      .wait_completed
			})!
		}
		.nexus_completed {
			value := event.typed_result or { return error('typed Nexus event missing') }
			validate_one_value(value, workflow.index)!
			context.complete(value)!
		}
		.nexus_failed {
			return error('typed Nexus operation failed: ' + event.message)
		}
		.activity_failed {
			value := event.failure or { return error('missing typed Activity failure') }
			if workflow.mode == 'metadata' { return value }
			if validate_failure(value)! < 1 { return error('missing typed failure details') }
			if workflow.mode == 'defer-fail' {
				context.complete(sdk.json_value(true)!)!
				return
			}
			workflow.captured = value
			workflow.ready = true
		}
		.activity_completed {
			if workflow.mode == 'metadata' {
				if event.data != 'true' { return error('metadata Activity failed') }
				workflow.metadata_completions++
				workflow.ready = workflow.metadata_completions == 2
				return
			}
			value := event.typed_result or { return error('missing typed Activity result') }
			if workflow.mode == 'defer-complete' {
				if value !is sdk.BinaryValue { return error('deferred result was not binary') }
				if value.data != 'null'.bytes() {
					return error('deferred binary null result changed')
				}
			} else if value.logical_data() != '"heartbeat-typed-ok"' {
				return error('wrong typed heartbeat result')
			}
			context.complete(sdk.json_value(true)!)!
		}
		.activity_cancelled {
			if workflow.mode == 'cancel-activities' {
				failure := event.failure or {
					return error('Activity cancellation failure missing')
				}

				if failure.root_kind() != .canceled || validate_failure(failure)! != 1 {
					return error('Activity cancellation details changed')
				}
				workflow.cancel_completions++
				workflow.captured = failure
				workflow.ready = workflow.cancel_completions == 2
				return
			}
			value := event.failure or { return error('missing typed cancellation') }
			if validate_failure(value)! != 1 { return error('cancellation details missing') }
			context.complete(sdk.json_value(true)!)!
		}
		.signal_received {
			if workflow.mode == 'cancel-activities' && event.name == 'cancel-all' {
				context.cancel_activity(workflow.cancel_remote)!
				context.cancel_activity(workflow.cancel_local)!
				return
			}
			if workflow.mode == 'metadata' && event.name == 'meta' {
				workflow.metadata_signal(event, mut context)!
				return
			}
			if event.name == 'cancel-activity' {
				context.cancel_activity(workflow.activity)!
				return
			}
			if event.name == 'finish' {
				if workflow.mode == 'cancel-error' {
					return sdk.CancelledError{
						scope:   3
						details: detail_arguments()!
					}
				}
				if workflow.mode == 'metadata' { workflow.finish_metadata(context)! }
				if workflow.mode == 'rethrow' {
					captured := workflow.captured or { return error('missing captured failure') }
					return captured
				}
				context.complete(sdk.json_value(true)!)!
			}
		}
		.update_received {
			if workflow.mode == 'metadata' {
				workflow.metadata_update(event, mut context)!
				return
			}
			captured := workflow.captured or { return error('missing captured update failure') }
			context.fail_update(event.id, captured)!
		}
		.cancel_requested {
			context.cancel(details: detail_arguments()!)!
		}
		else {}
	}
}

fn (workflow &DetailsWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	if name == 'fail' {
		captured := workflow.captured or { return error('failure not ready') }
		return captured
	}
	return sdk.json_value({
		'ready': workflow.ready
	})!
}

fn (workflow &DetailsWorkflow) update_names() []string {
	return ['fail', 'meta']
}

fn (workflow &DetailsWorkflow) validate_update(name string, input []string, view sdk.WorkflowContextView) ! {}
