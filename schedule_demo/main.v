module main

import temporal_v.sdk
import temporal_v.temporal
import os
import x.json2
import time
import temporal_v.example_converter

struct ScheduledWorkflow {
mut:
	value string
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return ScheduledWorkflow{}
}

fn (mut workflow ScheduledWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			assert event.arguments.len == 2
			assert event.arguments[1] == '7'
			workflow.value = event.arguments[0]
		}
		.signal_received {
			if event.name == 'finish' { context.complete(workflow.value)! }
		}
		.cancel_requested {
			context.cancel()!
		}
		else {}
	}
}

fn (workflow &ScheduledWorkflow) query(name string, args []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return workflow.value
}

fn converter() !sdk.DataConverter {
	return example_converter.from_env()!
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.scheduled', make_workflow)!
	return runtime
}

fn run() ! {
	mode := os.args[1]
	if mode == 'validation' {
		validate_options()!
		return
	}
	if mode == 'worker' {
		sdk.run_workflows(sdk.WorkerOptions{
			...sdk.options_from_env()!
			data_converter: converter()!
		}, definitions()!)!
		return
	}
	if mode == 'replay' {
		definitions()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			data_converter: converter()!
			workflow_id:    os.args[3]
			task_queue:     os.args[4]
		})!
		println('REPLAY_OK')
		return
	}
	client := sdk.connect(sdk.ClientOptions{
		address:        os.getenv('TEMPORAL_ADDRESS')
		data_converter: converter()!
	})!
	defer { client.close() }
	id := if os.args.len > 2 { os.args[2] } else { '' }
	match mode {
		'create' {
			println(json2.encode(client.create_schedule(id, sdk.ScheduleCreateOptions{
				definition: sdk.ScheduleDefinition{
					spec:     temporal.TemporalApiScheduleV1_ScheduleSpec{
						interval: [
							temporal.TemporalApiScheduleV1_IntervalSpec{
								interval: temporal.GoogleProtobuf_Duration{
									seconds: 60
								}
							},
						]
					}
					workflow: sdk.ScheduleWorkflow{
						workflow_id:    id + '-workflow'
						workflow_type:  'v.scheduled'
						task_queue:     os.getenv('TEMPORAL_TASK_QUEUE')
						arguments:      sdk.Arguments([
							'"scheduled"',
							'7',
						])
						headers:        {
							'flow': '"header"'
						}
						memo:           {
							'action': '"memo"'
						}
						priority:       temporal.TemporalApiCommonV1_Priority{
							priority_key: 2
						}
						static_summary: 'Scheduled V'
					}
					policies: temporal.TemporalApiScheduleV1_SchedulePolicies{
						overlap_policy:            .schedule_overlap_policy_skip
						keep_original_workflow_id: os.getenv('V_SDK_SCHEDULE_CODEC') == 'true'
					}
					state:    temporal.TemporalApiScheduleV1_ScheduleState{
						paused:            true
						limited_actions:   true
						remaining_actions: 0
					}
				}
				memo:       {
					'schedule': '"memo"'
				}
			})!))
		}
		'describe' {
			println(json2.encode(client.describe_schedule(id)!))
		}
		'list' {
			println(json2.encode(client.list_schedules(sdk.ScheduleListOptions{
				page_size:  1
				page_token: if id != '' {
					json2.decode[[]u8](id)!
				} else {
					[]u8{}
				}
			})!))
		}
		'trigger' {
			client.trigger_schedule(id, sdk.ScheduleTriggerOptions{
				overlap: if os.args.len > 3 && os.args[3] == 'all' {
					.schedule_overlap_policy_allow_all
				} else {
					.schedule_overlap_policy_skip
				}
			})!
			println('OK')
		}
		'pause' {
			client.pause_schedule(id, '')!
			println('OK')
		}
		'unpause' {
			client.unpause_schedule(id, '')!
			println('OK')
		}
		'update' {
			described := client.describe_schedule(id)!
			mut schedule := described.schedule or { return error('no schedule') }
			state := schedule.state or { return error('missing schedule state') }
			schedule.state = temporal.TemporalApiScheduleV1_ScheduleState{
				...state
				paused: true
				notes:  'updated'
			}
			client.update_schedule(id, sdk.ScheduleUpdate{
				schedule:       schedule
				conflict_token: if os.args.len > 3 {
					json2.decode[[]u8](os.args[3])!
				} else {
					described.conflict_token
				}
			})!
			println('OK')
		}
		'clear-memo' {
			described := client.describe_schedule(id)!
			client.update_schedule(id, sdk.ScheduleUpdate{
				schedule:       described.schedule or { return error('no schedule') }
				conflict_token: described.conflict_token
				memo:           map[string]sdk.PayloadValue{}
			})!
			println('OK')
		}
		'matching' {
			println(json2.encode(client.schedule_matching_times(id, time.unix(os.args[3].i64()),
				time.unix(os.args[4].i64()))!))
		}
		'backfill' {
			client.backfill_schedule(id, [
				sdk.ScheduleBackfill{
					start:   time.unix(os.args[3].i64())
					end:     time.unix(os.args[4].i64())
					overlap: .schedule_overlap_policy_allow_all
				},
			])!
			println('OK')
		}
		'delete' {
			client.delete_schedule(id)!
			println('OK')
		}
		else {
			return error('unknown schedule mode')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
