module main

import sdk
import temporal
import os
import json2

fn version(build string) sdk.WorkerDeploymentVersion {
	return sdk.WorkerDeploymentVersion{
		deployment_name: os.getenv_opt('TEMPORAL_DEPLOYMENT_NAME') or {
			os.getenv('TEMPORAL_TASK_QUEUE')
		}
		build_id:        build
	}
}

fn override(name string, build string) !sdk.VersioningOverride {
	return match name {
		'pinned' {
			sdk.VersioningOverride(sdk.PinnedVersioningOverride{
				version: version(build)
			})
		}
		'auto' {
			sdk.VersioningOverride(sdk.AutoUpgradeVersioningOverride{})
		}
		'once' {
			sdk.VersioningOverride(sdk.OneTimeVersioningOverride{
				version: version(build)
			})
		}
		else {
			return error('unknown override')
		}
	}
}

fn run() ! {
	if os.args[1] == 'worker' { return serve_control() }
	if os.args[1] == 'validation' { return validate_options() }
	if os.args[1] == 'replay' {
		definitions()!.replay(os.read_bytes(os.args[2])!, sdk.ReplayOptions{
			workflow_id: os.args[3]
			task_queue:  os.args[4]
		})!
		println('REPLAY_OK')
		return
	}

	client := sdk.connect(sdk.ClientOptions{ address: os.getenv('TEMPORAL_ADDRESS') })!
	defer { client.close() }
	queue := os.getenv('TEMPORAL_TASK_QUEUE')
	match os.args[1] {
		'current' {
			token := client.set_current_deployment(sdk.DeploymentRoutingUpdate{
				deployment_name: queue
				build_id:        os.args[2]
			})!
			println(json2.encode(token))
		}
		'ramp' {
			token := client.set_ramping_deployment(sdk.DeploymentRoutingUpdate{
				deployment_name: queue
				build_id:        os.args[2]
				conflict_token:  if os.args.len > 4 {
					json2.decode[[]u8](os.args[4])!
				} else {
					[]u8{}
				}
			}, os.args[3].f32())!
			println(json2.encode(token))
		}
		'describe' {
			println(json2.encode(client.describe_worker_deployment(queue)!))
		}
		'describe-version' {
			println(json2.encode(client.describe_worker_deployment_version(version(os.args[2]))!))
		}
		'describe-workflow' {
			handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
			println(json2.encode(client.rpc_describe_workflow_execution(temporal.TemporalApiWorkflowserviceV1_DescribeWorkflowExecutionRequest{
				namespace: 'default'
				execution: temporal.TemporalApiCommonV1_WorkflowExecution{
					workflow_id: handle.workflow_id
					run_id:      handle.run_id
				}
			})!))
		}
		'start', 'start-control' {
			handle := client.start(sdk.WorkflowStart{
				workflow_id:         os.args[2]
				workflow_type:       os.args[3]
				task_queue:          if os.args[1] == 'start-control' {
					queue + '-control'
				} else {
					queue
				}
				arguments:           if os.args.len > 6 { os.args[6] } else { '{}' }
				versioning_override: override(os.args[4], os.args[5])!
				priority:            temporal.TemporalApiCommonV1_Priority{
					priority_key: 2
				}
			})!
			println(json2.encode(handle))
		}
		'override' {
			handle := json2.decode[sdk.WorkflowHandle](os.args[2])!
			println(json2.encode(client.set_versioning_override(handle, override(os.args[3],
				os.args[4])!)!))
		}
		'clear' {
			println(json2.encode(client.clear_versioning_override(json2.decode[sdk.WorkflowHandle](os.args[2])!)!))
		}
		else {
			return error('unknown mode')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
