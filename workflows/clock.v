module workflows

import sdk
import time
import json2

struct ClockSnapshot {
pub:
	seconds     []i64
	nanoseconds []int
	millis      []i64
}

struct RecordedClock {
mut:
	snapshot ClockSnapshot
}

fn new_recorded_clock(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return RecordedClock{}
}

fn (mut workflow RecordedClock) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started || event.kind == .timer_fired || event.kind == .signal_received {
		first := context.now()
		assert first == context.now()
		mut seconds := workflow.snapshot.seconds.clone()
		mut nanoseconds := workflow.snapshot.nanoseconds.clone()
		mut millis := workflow.snapshot.millis.clone()
		seconds << first.unix()
		nanoseconds << first.nanosecond
		millis << context.info().time_millis
		workflow.snapshot = ClockSnapshot{
			seconds:     seconds
			nanoseconds: nanoseconds
			millis:      millis
		}
	}
	match event.kind {
		.started { context.timer(2 * time.second)! }
		.timer_fired { context.timer(60 * time.second)! }
		.signal_received { context.complete(json2.encode(workflow.snapshot))! }
		.cancel_requested { context.cancel()! }
		else {}
	}
}

fn (workflow &RecordedClock) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return json2.encode(workflow.snapshot)
}
