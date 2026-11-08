module sdk

pub struct ReplayInput {
pub:
	history []u8
	options ReplayOptions
}

pub enum ReplayOutcomeKind {
	skipped
	succeeded
	failed
}

pub struct ReplayOutcome {
pub:
	index       int
	workflow_id string
	kind        ReplayOutcomeKind
	result      ?ReplayResult
	message     string
	error_code  int
}

@[params]
pub struct ReplayBatchOptions {
pub:
	concurrency int = 1
	// On failure stop starting new cases, then drain cases already running.
	fail_fast bool
}

fn replay_one(runtime &Runtime, input ReplayInput, index int, completed chan ReplayOutcome) {
	result := runtime.replay(input.history, input.options) or {
		completed <- ReplayOutcome{
			index:       index
			workflow_id: input.options.workflow_id
			kind:        .failed
			message:     err.msg()
			error_code:  err.code()
		}
		return
	}
	completed <- ReplayOutcome{
		index:       index
		workflow_id: input.options.workflow_id
		kind:        .succeeded
		result:      result
	}
}

// Each history uses an isolated workflow cache and its own replay configuration.
// Results retain input order, including explicit entries for unstarted cases.
// Per-history failures are outcomes; invalid batch configuration is an API error.
pub fn (runtime &Runtime) replay_many(inputs []ReplayInput, options ReplayBatchOptions) ![]ReplayOutcome {
	if options.concurrency < 1 || options.concurrency > 32 {
		return error('replay concurrency must be between 1 and 32')
	}
	mut outcomes := []ReplayOutcome{cap: inputs.len}
	for index, input in inputs {
		outcomes << ReplayOutcome{
			index:       index
			workflow_id: input.options.workflow_id
		}
	}
	snapshot := &Runtime{
		...runtime.definitions_copy()
	}
	completed := chan ReplayOutcome{cap: options.concurrency}
	mut next := 0
	mut running := 0
	mut stopped := false
	for next < inputs.len || running > 0 {
		for !stopped && next < inputs.len && running < options.concurrency {
			spawn replay_one(snapshot, inputs[next], next, completed)
			next++
			running++
		}
		if running == 0 {
			break
		}
		outcome := <-completed
		running--
		outcomes[outcome.index] = outcome
		if options.fail_fast && outcome.kind == .failed { stopped = true }
	}
	return outcomes
}
