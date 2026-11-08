module main

import sdk
import os
import json2
import time

struct ActivityWaitInput {
	shutdown bool
	local    bool
	timeout  bool
}

struct ActivityWaitWorkflow {
mut:
	sequence       u32
	expect_timeout bool
}

fn activity_wait_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ActivityWaitWorkflow{}
}

fn (mut workflow ActivityWaitWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		input := json2.decode[ActivityWaitInput](event.data)!
		workflow.expect_timeout = input.timeout
		if input.local {
			workflow.sequence = context.local_activity('v.activity-wait', '"wait"', sdk.LocalActivityOptions{
				start_to_close: if input.timeout {
					1500 * time.millisecond
				} else {
					30 * time.second
				}
				cancellation:   .wait_completed
				retry:          sdk.RetryPolicy{
					maximum_attempts: if input.shutdown {
						2
					} else {
						1
					}
					initial_interval: 10 * time.millisecond
				}
			})!
		} else {
			workflow.sequence = context.activity('v.activity-wait', '"wait"', sdk.ActivityOptions{
				start_to_close:    if input.timeout {
					1500 * time.millisecond
				} else {
					30 * time.second
				}
				heartbeat_timeout: if input.shutdown {
					5 * time.second
				} else {
					500 * time.millisecond
				}
				cancellation:      .wait_completed
				retry:             sdk.RetryPolicy{
					maximum_attempts: if input.shutdown {
						2
					} else {
						1
					}
					initial_interval: 10 * time.millisecond
				}
			})!
		}
	}
	if event.kind == .signal_received || event.kind == .cancel_requested {
		context.cancel_activity(workflow.sequence)!
	}
	if event.kind == .activity_cancelled { context.complete('"cancelled"')! }
	if event.kind == .activity_completed { context.complete(event.data)! }
	if event.kind == .activity_failed {
		failure := event.failure or { return error('missing Activity failure') }
		assert workflow.expect_timeout && failure.root_kind() == .timeout
		context.complete('"timedout"')!
	}
}

struct ActivityWaitAudit {
	reason  string
	details string
	local   bool
	stable  bool
}

fn activity_wait_handler(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	if os.getenv('V_WAIT_REPAIRED') == 'true' {
		if !context.info().is_local {
			assert context.info().attempt == 2
			progress := context.heartbeat_values()!
			assert progress.len == 2 && progress[0].logical_data() == '"progress"'
				&& progress[1].logical_data() == '7'
		}
		return sdk.PayloadValue('"repaired"')
	}
	assert !context.is_cancelled()
	assert context.cancellation_info() == none
	context.heartbeat(sdk.Arguments(['"progress"', '7']))!
	key := context.info().workflow_id
	directory := os.getenv('V_ACTIVITY_WAIT_DIR')
	os.write_file(os.join_path(directory, key + '-begin'), os.getpid().str())!
	received := context.wait_cancelled()!
	assert context.is_cancelled()
	info := context.cancellation_info() or {
		return error('missing observed Activity cancellation')
	}
	assert info.reason == received.reason
	encoded := json2.encode(info)
	if details := received.details {
		mut changed := details
		changed.is_worker_shutdown = !changed.is_worker_shutdown
	}
	// A repeated wait is immediate and the throwing API retains the same cause.
	assert json2.encode(context.wait_cancelled()!) == encoded
	mut checked := false
	context.check_cancelled() or {
		assert err is sdk.ActivityCancelledError
		if err is sdk.ActivityCancelledError {
			assert json2.encode(err.information) == encoded
		}
		checked = true
	}
	assert checked
	audit := ActivityWaitAudit{
		reason:  received.reason.str()
		details: if details := received.details {
			json2.encode(details)
		} else {
			''
		}
		local:   context.info().is_local
		stable:  json2.encode(context.cancellation_info() or {
			return error('missing retained cancellation')
		}) == encoded
	}
	os.write_file(os.join_path(directory, key + '-cancel.json'), json2.encode(audit))!
	return sdk.ActivityCancelledError{
		information: info
	}
}

fn wait_activity_control(control &sdk.WorkerControl, path string, done chan bool) {
	for {
		select {
			_ := <-done {
				return
			}
			else {}
		}
		if os.exists(path) {
			control.shutdown()
			return
		}
		time.sleep(10 * time.millisecond)
	}
}

fn serve_activity_wait_control(config sdk.WorkerOptions, converter sdk.DataConverter) ! {
	control := &sdk.WorkerControl{}
	done := chan bool{cap: 1}
	watcher := spawn wait_activity_control(control, os.join_path(os.getenv('V_ACTIVITY_WAIT_DIR'),

		'stop-' + os.getpid().str()), done)
	defer {
		done <- true
		watcher.wait()
	}
	mut activities := sdk.Activities{}
	activities.register('v.activity-wait', activity_wait_handler)!
	sdk.run_worker(sdk.WorkerOptions{ ...config, control: control, data_converter: converter },
		runtime()!, activities)!
}
