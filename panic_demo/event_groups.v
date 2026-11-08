module main

import temporal_v.sdk
import temporal_v.temporal
import x.json2
import os
import time

struct GroupPoisonConverter {}

fn (converter GroupPoisonConverter) to_payload(value sdk.PayloadValue, scope sdk.SerializationContext) !temporal.TemporalApiCommonV1_Payload {
	if value.logical_data().contains('LABEL_SENTINEL') {
		panic('application converter received an Event Group label')
	}
	return sdk.DefaultPayloadConverter{}.to_payload(value, scope)!
}

fn (converter GroupPoisonConverter) from_payload(value temporal.TemporalApiCommonV1_Payload, scope sdk.SerializationContext) !sdk.PayloadValue {
	return sdk.DefaultPayloadConverter{}.from_payload(value, scope)!
}

fn labeled_group(id string) !sdk.EventGroup {
	return sdk.new_event_group(id)!.with_label('LABEL_SENTINEL ' + id)!
}

fn group_ids(context &sdk.WorkflowContext) []string {
	return context.current_event_groups().map(it.id())
}

fn group_error_scope(mut context sdk.WorkflowContext) ! {
	context.timer(time.hour, sdk.TimerOptions{ summary: 'caught-error' })!
	return error('expected scope error')
}

fn group_inner_scope(mut context sdk.WorkflowContext) ! {
	if group_ids(context) != ['outer', 'inner'] { return error('nested groups did not compose') }
	context.timer(time.hour, sdk.TimerOptions{ summary: 'nested' })!
	context.with_event_group(labeled_group('error')!, group_error_scope) or {
		if err.msg() != 'expected scope error' { return err }
	}
	if group_ids(context) != ['outer', 'inner'] { return error('failed scope leaked groups') }
	context.timer(time.hour, sdk.TimerOptions{ summary: 'nested-restored' })!
}

fn group_outer_scope(mut context sdk.WorkflowContext) ! {
	if group_ids(context) != ['outer'] { return error('outer group missing') }
	context.timer(time.hour, sdk.TimerOptions{
		summary:      'direct'
		event_groups: [
			labeled_group('inner')!,
			sdk.new_event_group('outer')!.with_label('LABEL_SENTINEL direct-override')!,
		]
	})!
	context.with_event_group(labeled_group('inner')!, group_inner_scope)!
	if group_ids(context) != ['outer'] { return error('nested scope leaked groups') }
	context.timer(time.hour, sdk.TimerOptions{ summary: 'outer-restored' })!
}

fn group_signal_scope(mut context sdk.WorkflowContext) ! {
	if context.current_event_groups().len != 2 {
		return error('signal and explicit groups did not compose')
	}
	context.timer(time.hour, sdk.TimerOptions{ summary: 'signal-explicit' })!
}

fn group_deferred_scope(mut context sdk.WorkflowContext) ! {
	context.timer(time.hour, sdk.TimerOptions{
		summary:      'after-local-update'
		event_groups: [
			labeled_group('after-local')!,
		]
	})!
}

fn group_never_scope(mut context sdk.WorkflowContext) ! {
	panic('invalid or disabled group callback ran')
}

fn group_complete_scope(mut context sdk.WorkflowContext) ! {
	context.complete('"chain-ok"')!
}

fn group_fail_scope(mut context sdk.WorkflowContext) ! {
	context.fail('group-failed', 'GroupFailure')!
}

fn group_cancel_scope(mut context sdk.WorkflowContext) ! {
	context.cancel(sdk.WorkflowCancelOptions{
		event_groups: [
			labeled_group('cancel-direct')!,
		]
	})!
}

fn group_panic_scope(mut context sdk.WorkflowContext) ! {
	context.timer(time.hour, sdk.TimerOptions{ summary: 'panic-before' })!
	if os.getenv('V_GROUP_PANIC_FIXED') != 'true' { panic('Event Group scope panic') }
	context.complete('"panic-repaired"')!
}

struct EventGroupsWorkflow {
mut:
	mode          string
	target        string
	initial       int
	child         bool
	nexus         bool
	tick          bool
	signals       int
	deferred      string
	local         u32
	captured      []sdk.EventGroup
	deferred_done bool
}

fn event_groups_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return EventGroupsWorkflow{}
}

fn (mut workflow EventGroupsWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		input := json2.decode[map[string]string](event.data)!
		workflow.mode = input['mode']
		workflow.target = input['target']
		if workflow.mode == 'chain' {
			if input['next'] == 'true' {
				context.with_event_group(labeled_group('chain-complete')!, group_complete_scope)!
			} else {
				context.continue_as_new(sdk.Arguments(json2.encode({
					'mode': 'chain'
					'next': 'true'
				})), sdk.ContinueOptions{ event_groups: [labeled_group('continue')!] })!
			}
			return
		}
		if workflow.mode == 'fail' {
			context.with_event_group(labeled_group('failure')!, group_fail_scope)!
			return
		}
		if workflow.mode == 'cancel' {
			context.with_event_group(labeled_group('cancel-scope')!, group_cancel_scope)!
			return
		}
		if workflow.mode == 'panic' {
			context.with_event_group(labeled_group('panic')!, group_panic_scope)!
			return
		}
		if workflow.mode == 'external' {
			context.signal_external(sdk.ExternalWorkflow{ workflow_id: workflow.target }, 'poke',
				'null', sdk.WorkflowSignalOptions{
				summary:      'external-signal'
				event_groups: [labeled_group('external-signal')!]
			})!
			return
		}
		if input['mode'] == 'disabled' {
			context.with_event_group(labeled_group('disabled')!, group_never_scope) or {
				if !err.msg().contains('experimental_event_groups') { return err }
				context.complete('"disabled-ok"')!
				return
			}
			return error('disabled Event Groups were accepted')
		}
		mut invalid := false
		context.with_event_group(sdk.EventGroup{}, group_never_scope) or { invalid = true }
		if !invalid { return error('invalid Event Group was accepted') }
		context.with_event_group(labeled_group('outer')!, group_outer_scope)!
		if context.current_event_groups().len != 0 { return error('outer scope leaked groups') }
		context.timer(time.millisecond, sdk.TimerOptions{ summary: 'outside' })!
		context.upsert_memo({
			'grouped': sdk.PayloadValue('true')
		}, sdk.EventGroupOptions{ event_groups: [labeled_group('memo')!] })!
		context.upsert_search_attributes([
			sdk.SearchAttribute{
				name:       'VEventGroupKeyword'
				type_:      .keyword
				value_json: '"grouped"'
			},
		], sdk.EventGroupOptions{ event_groups: [labeled_group('search')!] })!
		context.patched('event-group-patch', sdk.EventGroupOptions{
			event_groups: [
				labeled_group('patch')!,
			]
		})!
		context.deprecate_patch('event-group-retired', sdk.EventGroupOptions{
			event_groups: [
				labeled_group('retired')!,
			]
		})!
		context.activity('v.event-group.echo', sdk.Arguments(json2.encode('x'.repeat(300000))), sdk.ActivityOptions{
			event_groups:   [
				labeled_group('remote')!,
			]
			start_to_close: 30 * time.second
		})!
		context.local_activity('v.event-group.echo', '"retry"', sdk.LocalActivityOptions{
			event_groups:          [labeled_group('local')!]
			summary:               'group-local'
			local_retry_threshold: time.millisecond
			retry:                 sdk.RetryPolicy{
				maximum_attempts: 2
				initial_interval: 150 * time.millisecond
			}
			start_to_close:        30 * time.second
		})!
		context.child('v.event-group.child', context.info().workflow_id + '-child', 'null', sdk.ChildOptions{
			event_groups: [
				labeled_group('child')!,
			]
		})!
		context.nexus_operation('null', sdk.NexusOptions{
			endpoint:     input['endpoint']
			service:      'v.panic'
			operation:    'sync'
			event_groups: [labeled_group('nexus')!]
		})!
	}
	if workflow.mode == 'external' {
		if event.kind == .external_signal_completed {
			context.cancel_external(sdk.ExternalWorkflow{ workflow_id: workflow.target },
				'grouped external cancel', sdk.EventGroupOptions{
				event_groups: [labeled_group('external-cancel')!]
			})!
		}
		if event.kind == .external_cancel_completed { context.complete('"external-ok"')! }
		if event.kind in [.external_signal_failed, .external_cancel_failed] {
			return error('grouped external operation failed: ' + event.message)
		}
		return
	}
	if event.kind == .timer_fired {
		if context.current_event_groups().len != 0 {
			return error('handler groups leaked into timer resolution')
		}
		workflow.tick = true
	}
	if event.kind == .activity_completed {
		if event.sequence == workflow.local && workflow.deferred != '' {
			context.with_event_groups(workflow.captured, group_deferred_scope)!
			context.complete_update(workflow.deferred, '"deferred-ok"')!
			workflow.deferred_done = true
		} else {
			workflow.initial++
		}
	}
	if event.kind == .child_completed { workflow.child = true }
	if event.kind == .nexus_completed { workflow.nexus = true }
	if event.kind in [.activity_failed, .child_failed, .nexus_failed] {
		return error('grouped operation failed: ' + event.message)
	}
	if event.kind == .signal_received {
		if context.current_event_groups().len != 1 {
			return error('implicit signal Event Group missing')
		}
		if event.name == 'finish' {
			context.complete('"groups-ok"')!
			return
		}
		workflow.signals++
		context.timer(time.hour, sdk.TimerOptions{ summary: 'signal-timer' })!
		context.with_event_group(labeled_group('signal')!, group_signal_scope)!
	}
	if event.kind == .update_received {
		if context.current_event_groups().len != 1 {
			return error('implicit Update Event Group missing')
		}
		if event.name == 'deferred' {
			workflow.deferred = event.id
			workflow.captured = context.current_event_groups()
			workflow.local = context.local_activity('v.event-group.echo', '"retry"', sdk.LocalActivityOptions{
				summary:               'deferred-local'
				event_groups:          [labeled_group('deferred-local')!]
				local_retry_threshold: time.millisecond
				retry:                 sdk.RetryPolicy{
					maximum_attempts: 2
					initial_interval: 500 * time.millisecond
				}
				start_to_close:        30 * time.second
			})!
		} else {
			context.timer(time.hour, sdk.TimerOptions{
				summary:      'update-timer'
				event_groups: [
					labeled_group('update')!,
				]
			})!
			context.complete_update(event.id, '"immediate-ok"')!
		}
	}
}

fn (workflow EventGroupsWorkflow) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(json2.encode({
		'initial':  workflow.initial.str()
		'ready':    (workflow.initial == 2 && workflow.child && workflow.nexus && workflow.tick).str()
		'signals':  workflow.signals.str()
		'deferred': workflow.deferred_done.str()
	}))
}

fn (workflow EventGroupsWorkflow) update_names() []string {
	return ['immediate', 'deferred']
}

fn (workflow EventGroupsWorkflow) validate_update(name string, arguments []string, view sdk.WorkflowContextView) ! {
	if os.getenv('V_GROUP_REPLAY') == 'true' {
		return error('Update validator ran during grouped replay')
	}
}

struct EventGroupChild {}

fn event_group_child_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return EventGroupChild{}
}

fn (mut workflow EventGroupChild) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started { context.complete('"child-ok"')! }
}

fn event_group_echo(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue {
	if arguments[0] == '"retry"' && context.info().attempt == 1 {
		return sdk.ApplicationError{
			message: 'grouped local retry'
		}
	}
	return sdk.PayloadValue('"group-echo"')
}

struct EventGroupReceiver {
mut:
	signals int
}

fn event_group_receiver_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return EventGroupReceiver{}
}

fn (mut workflow EventGroupReceiver) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started {
		context.timer(time.hour, sdk.TimerOptions{ summary: 'peer-wait' })!
	}
	if event.kind == .signal_received { workflow.signals++ }
	if event.kind == .cancel_requested { context.cancel()! }
}

fn (workflow EventGroupReceiver) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue(workflow.signals.str())
}
