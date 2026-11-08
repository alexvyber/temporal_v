module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import os
import x.json2
import sync

struct LegacyWorkflow {
mut:
	count i64
}

fn make_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return LegacyWorkflow{}
}

fn (mut workflow LegacyWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {}
		.signal_received {
			if event.name == 'add' { workflow.count += json2.decode[i64](event.data)! }
			if event.name == 'finish' { context.complete(json2.encode(workflow.count))! }
		}
		else {}
	}
}

fn (workflow &LegacyWorkflow) query(name string, args []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return json2.encode(workflow.count)
}

fn definitions() !&sdk.Runtime {
	mut runtime := &sdk.Runtime{}
	runtime.register('v.legacy', make_workflow)!
	return runtime
}

fn serve(options sdk.WorkerOptions, done chan string) {
	sdk.run_workflows(options, definitions() or {
		done <- err.msg()
		return
	}) or {
		done <- err.msg()
		return
	}
	done <- ''
}

fn compatibility(client &sdk.Client, mode string, build string, other string) ! {
	update := match mode {
		'new' {
			sdk.BuildIdCompatibilityUpdate(sdk.NewBuildIdSet{
				build_id: build
			})
		}
		'compatible' {
			sdk.BuildIdCompatibilityUpdate(sdk.CompatibleBuildId{
				build_id:          build
				existing_build_id: other
			})
		}
		'promote-set' {
			sdk.BuildIdCompatibilityUpdate(sdk.PromoteBuildIdSet{
				build_id: build
			})
		}
		'promote-build' {
			sdk.BuildIdCompatibilityUpdate(sdk.PromoteBuildId{
				build_id: build
			})
		}
		'merge' {
			sdk.BuildIdCompatibilityUpdate(sdk.MergeBuildIdSets{
				primary_build_id:   build
				secondary_build_id: other
			})
		}
		else {
			return error('unknown compatibility operation')
		}
	}

	client.update_build_id_compatibility(os.getenv('TEMPORAL_TASK_QUEUE'), update)!
}

@[heap]
struct Observation {
	mutex &sync.Mutex = sync.new_mutex()
mut:
	inline bool
}

struct Observe {
	state &Observation
}

fn (interceptor Observe) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	reply := next.execute(request)!
	if request.method == 'StartWorkflowExecution' {
		response :=
			temporal.TemporalApiWorkflowserviceV1_StartWorkflowExecutionResponse.decode(reply)!
		mut state := interceptor.state
		state.mutex.lock()
		state.inline = response.eager_workflow_task != none
		state.mutex.unlock()
	}
	return reply
}

fn eager(client &sdk.Client, state &Observation) ! {
	control := &sdk.WorkerControl{}
	done := chan string{cap: 1}
	worker := spawn serve(sdk.WorkerOptions{
		...sdk.options_from_env()!
		client:                     client
		deployment:                 none
		legacy_build_id_versioning: true
		build_id:                   'legacy-a'
		control:                    control
	}, done)
	defer {
		control.shutdown()
		message := <-done
		worker.wait()
		assert message == '', message
	}
	warm := client.start(sdk.WorkflowStart{
		workflow_id:   os.getenv('TEMPORAL_TASK_QUEUE') + '-warm'
		workflow_type: 'v.legacy'
		task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
	})!
	client.query(warm, 'state', 'null')!
	client.signal(warm, 'finish', 'null')!
	client.get_result(warm, 10000)!
	compatibility(client, 'new', 'legacy-c', '')!
	probe := client.start(sdk.WorkflowStart{
		workflow_id:   os.getenv('TEMPORAL_TASK_QUEUE') + '-ordinary'
		workflow_type: 'v.legacy'
		task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
	})!
	client.signal(probe, 'finish', 'null')!
	client.get_result(probe, 60000)!
	history := client.history(probe)!
	mut ordinary_identity := ''
	for event in history.events {
		if attributes := event.attributes {
			if attributes is temporal.TemporalApiHistoryV1_HistoryEvent_WorkflowTaskCompletedEventAttributes_Case {
				ordinary_identity = attributes.value.identity
			}
		}
	}
	assert ordinary_identity == 'remote-c', ordinary_identity
	println('ORDINARY_IDENTITY=' + ordinary_identity)
	handle := client.start(sdk.WorkflowStart{
		workflow_id:             os.getenv('TEMPORAL_TASK_QUEUE') + '-eager'
		workflow_type:           'v.legacy'
		task_queue:              os.getenv('TEMPORAL_TASK_QUEUE')
		request_eager_execution: true
	})!
	state.mutex.lock()
	inline := state.inline
	state.mutex.unlock()
	assert !inline, 'legacy worker received an eager task outside server matching'
	println(json2.encode(handle) + '\nINLINE=' + inline.str())
	client.signal(handle, 'finish', 'null')!
	client.get_result(handle, 60000)!
}

fn run() ! {
	if os.args[1] == 'validation' {
		validate_options()!
		return
	}
	if os.args[1] == 'worker' {
		sdk.run_workflows(sdk.WorkerOptions{
			...sdk.options_from_env()!
			deployment:                 none
			legacy_build_id_versioning: true
		}, definitions()!)!
		return
	}
	if os.args[1] == 'replay' {
		definitions()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id: os.args[3]
			task_queue:  os.args[4]
		})!
		println('REPLAY_OK')
		return
	}
	state := &Observation{}
	client := sdk.connect(sdk.ClientOptions{
		address:      os.getenv('TEMPORAL_ADDRESS')
		transport:    sdk.TransportOptions{
			identity: if os.args[1] == 'eager' {
				'local-a'
			} else {
				'controller'
			}
		}
		interceptors: [sdk.ClientInterceptor(Observe{
			state: state
		})]
	})!
	defer { client.close() }
	match os.args[1] {
		'new', 'compatible', 'promote-set', 'promote-build', 'merge' {
			compatibility(client, os.args[1], os.args[2], if os.args.len > 3 {
				os.args[3]
			} else {
				''
			})!
			println('OK')
		}
		'eager' {
			eager(client, state)!
		}
		'reachability' {
			println(json2.encode(client.build_id_reachability(sdk.BuildIdReachabilityOptions{
				build_ids:   [
					'legacy-a',
				]
				task_queues: [os.getenv('TEMPORAL_TASK_QUEUE')]
			})!))
		}
		'describe' {
			println(json2.encode(client.build_id_compatibility(os.getenv('TEMPORAL_TASK_QUEUE'), 0)!))
		}
		else {
			return error('unknown role')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
