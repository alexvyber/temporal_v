module main

import sdk
import os
import json2

const version_change = 'numeric/change 🌍/one'

struct VersionInput {
	padding int
}

struct VersionSnapshot {
	padding string
	before  string
	after   string
	version int
}

struct NumericVersionWorkflow {
mut:
	snapshot VersionSnapshot
	ready    bool
}

fn numeric_version_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return NumericVersionWorkflow{}
}

fn numeric_maximum() int {
	return match os.getenv('V_NUMERIC_VERSION_STAGE') {
		'v0' { 0 }
		'v1' { 1 }
		else { 2 }
	}
}

fn (mut workflow NumericVersionWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	stage := os.getenv('V_NUMERIC_VERSION_STAGE')
	if event.kind == .started {
		input := json2.decode[VersionInput](event.data)!
		if input.padding < 0 || input.padding > 1024 * 1024 {
			return error('invalid version fixture padding')
		}
		before := context.random_u64()!.str()
		selected := if stage == 'baseline' {
			sdk.default_version
		} else if stage == 'removed' {
			1
		} else if stage == 'deprecate' {
			context.deprecate_version(version_change, 1)!
			1
		} else {
			context.get_version(version_change, if stage == 'require2' {
				2
			} else {
				sdk.default_version
			}, numeric_maximum()) or {
				if os.getenv('V_NUMERIC_VERSION_SWALLOW') == 'true' {
					sdk.default_version
				} else {
					return err
				}
			}
		}
		after := context.random_u64()!.str()
		workflow.snapshot = VersionSnapshot{
			before:  before
			after:   after
			version: selected
			padding: 'x'.repeat(input.padding)
		}
		context.activity('v.version.echo', sdk.Arguments(json2.encode(workflow.snapshot)), sdk.ActivityOptions{
			retry: sdk.RetryPolicy{
				maximum_attempts: 1
			}
		})!
	}
	if event.kind == .activity_completed {
		if json2.decode[VersionSnapshot](event.data)! != workflow.snapshot {
			return error('versioned Activity result changed')
		}
		if stage !in ['baseline', 'removed', 'deprecate'] {
			if context.get_version(version_change, sdk.default_version, 2)! != workflow.snapshot.version {
				return error('repeated version choice changed')
			}
		}
		workflow.ready = true
	}
	if event.kind == .activity_failed {
		return error('versioned Activity failed: ' + event.message)
	}
	if event.kind == .signal_received && event.name == 'finish' {
		context.complete(sdk.PayloadValue(json2.encode(workflow.snapshot)))!
	}
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (workflow NumericVersionWorkflow) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode({
		'ready':   json2.Any(workflow.ready)
		'version': json2.Any(workflow.snapshot.version)
		'before':  json2.Any(workflow.snapshot.before)
		'after':   json2.Any(workflow.snapshot.after)
	}))
}

fn version_echo(mut context sdk.ActivityContext, input []string) !sdk.PayloadValue {
	return sdk.PayloadValue(sdk.single_argument(input)!)
}

fn validate_numeric_versions() ! {
	mut zero := sdk.WorkflowContext{}
	if zero.get_version('zero', sdk.default_version, 0)! != 0
		|| zero.get_version('zero', sdk.default_version, 2)! != 0 {
		return error('zero version choice changed')
	}
	mut maximum := sdk.WorkflowContext{}
	if maximum.get_version('maximum', sdk.default_version, 2147483647)! != 2147483647 {
		return error('maximum version truncated')
	}
	for bounds in [[-2, 1], [1, 0]] {
		mut context := sdk.WorkflowContext{}
		context.get_version('invalid', bounds[0], bounds[1]) or {
			context.get_version('valid-after-error', sdk.default_version, 1) or { continue }
			return error('invalid version error was not retained')
		}
		return error('invalid version range accepted')
	}
	mut retired := sdk.WorkflowContext{}
	retired.deprecate_version('retired', 1)!
	retired.get_version('retired', sdk.default_version, 2) or {
		mut namespace := sdk.WorkflowContext{}
		namespace.patched('__temporal_v_version/v1/reserved') or {
			namespace.deprecate_patch('__temporal_v_version/v1/reserved') or {
				println('VERSIONS_OK')
				return
			}
			return error('reserved numeric version namespace could be deprecated directly')
		}
		return error('reserved numeric version namespace could be patched directly')
	}
	return error('deprecated version change was reused')
}
