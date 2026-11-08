module workflows

import temporal_v.sdk
import os
import time

struct NondeterminismFixture {
	changed bool
}

fn new_nondeterminism_fixture(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return NondeterminismFixture{
		changed: os.getenv('V_SDK_EXAMPLE_NONDETERMINISM_CHANGED') == 'true'
	}
}

fn (mut workflow NondeterminismFixture) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	match event.kind {
		.started {
			if workflow.changed {
				context.activity('v.echo', '"changed"', sdk.ActivityOptions{})!
			} else {
				context.timer(60 * time.second)!
			}
		}
		.signal_received {
			if event.name == 'finish' { context.complete('"repaired"')! }
		}
		.cancel_requested {
			context.cancel()!
		}
		else {}
	}
}
