module workflows

import x.json2
import temporal_v.sdk
import time

struct CounterInput {
	initial i64
	label   string
}

struct Addition {
	amount i64
}

struct CounterState {
pub:
	total     i64
	additions []i64
	label     string
}

struct Counter {
mut:
	total     i64
	additions []i64
	label     string
	pending   map[u32]PendingAddition
}

struct PendingAddition {
	id     string
	amount i64
}

fn new_counter(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return Counter{}
}

fn (counter &Counter) snapshot() string {
	return json2.encode(CounterState{
		total:     counter.total
		additions: counter.additions
		label:     counter.label
	})
}

fn (mut counter Counter) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	match event.kind {
		.started {
			input := json2.decode[CounterInput](event.data)!
			counter.total = input.initial
			counter.label = input.label
			context.set_current_details('Counter: ${counter.label}')!
		}
		.signal_received {
			match event.name {
				'add' {
					addition := json2.decode[Addition](event.data)!
					counter.total += addition.amount
					counter.additions << addition.amount
				}
				'finish' {
					context.complete(counter.snapshot())!
				}
				else {
					return error('unknown signal: ${event.name}')
				}
			}
		}
		.update_received {
			if event.name == 'fail' { return error('requested update failure') }
			addition := json2.decode[Addition](event.data)!
			if event.name == 'delayed_add' {
				sequence := context.timer(2 * time.second)!
				counter.pending[sequence] = PendingAddition{
					id:     event.id
					amount: addition.amount
				}
			} else {
				counter.total += addition.amount
				counter.additions << addition.amount
				context.complete_update(event.id, counter.snapshot())!
			}
		}
		.timer_fired {
			addition := counter.pending[event.sequence] or { return error('unknown update timer') }
			counter.pending.delete(event.sequence)
			counter.total += addition.amount
			counter.additions << addition.amount
			context.complete_update(addition.id, counter.snapshot())!
		}
		else {
			return error('unexpected counter event')
		}
	}
}

fn (counter &Counter) update_names() []string {
	return ['add', 'delayed_add', 'fail']
}

fn (counter &Counter) validate_update(name string, arguments []string, view sdk.WorkflowContextView) ! {
	if name == 'fail' { return }
	input_json := sdk.single_argument(arguments)!
	addition := json2.decode[Addition](input_json)!
	if addition.amount < 0 { return error('update amount must be non-negative') }
}

fn (counter &Counter) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	match name {
		'state' {
			return counter.snapshot()
		}
		'offset' {
			input_json := sdk.single_argument(arguments)!
			addition := json2.decode[Addition](input_json)!
			return json2.encode(counter.total + addition.amount)
		}
		'explode' {
			return error('requested query error')
		}
		'invalid-result' {
			return 'not JSON'
		}
		else {
			return error('unknown query: ${name}')
		}
	}
}

fn (counter &Counter) interaction_definitions() sdk.WorkflowInteractions {
	return sdk.WorkflowInteractions{
		queries: [
			sdk.WorkflowInteractionDefinition{
				name:        'state'
				description: 'Current counter state 世界'
			},
			sdk.WorkflowInteractionDefinition{
				name: 'offset'
			},
			sdk.WorkflowInteractionDefinition{
				name: 'invalid-result'
			},
			sdk.WorkflowInteractionDefinition{
				name: 'explode'
			},
		]
		signals: [sdk.WorkflowInteractionDefinition{ name: 'finish' },
			sdk.WorkflowInteractionDefinition{ name: 'add', description: 'Add an amount' }]
		updates: [
			sdk.WorkflowInteractionDefinition{
				name:        'delayed_add'
				description: 'Add after a durable delay'
			},
			sdk.WorkflowInteractionDefinition{
				name:        'add'
				description: 'Add and return state'
			},
		]
	}
}
