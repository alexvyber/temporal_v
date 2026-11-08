module workflows

import alexvyber.temporal_v.sdk
import time

struct MetadataWorkflow {
mut:
	updated bool
}

fn new_metadata_workflow(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	return MetadataWorkflow{}
}

fn (mut workflow MetadataWorkflow) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .cancel_requested {
		context.cancel()!
		return
	}
	if event.kind == .started {
		initial := context.memo()!
		if (initial['origin'] or { return error('missing memo entry') }).logical_data() != '"initial"' {
			return error('initial memo was not exposed')
		}
		info := context.info()
		if (info.task_timeout or { return error('missing task timeout') }).seconds != 7 || (info.run_timeout or {
			return error('missing run timeout')
		}).seconds != 30 {
			return error('workflow timeouts were not exposed')
		}
		if _ := info.start_time {
		} else {
			return error('missing workflow start time')
		}
		if context.last_completion_result()!.values.len != 0 {
			return error('unexpected prior completion result')
		}
		if _ := context.continued_failure() { return error('unexpected continued failure') }

		context.upsert_memo({
			'old':  '1'
			'keep': '"initial"'
		})!
		if (context.memo()!['keep'] or { return error('missing memo entry') }).logical_data() != '"initial"' {
			return error('memo read did not reflect current activation upsert')
		}
		context.timer(100 * time.millisecond)!
	} else if event.kind == .timer_fired {
		if workflow.updated {
			memo := context.memo()!
			if 'old' in memo || (memo['origin'] or { return error('missing memo entry') }).logical_data() != '"initial"' || (memo['keep'] or {
				return error('missing memo entry')
			}).logical_data() != '"updated"' {
				return error('memo state did not replay')
			}
			attributes := context.search_attributes()!
			if attributes.len != 3 || attributes[0].name != 'VSDKInt'
				|| attributes[0].value_json != '42' {
				return error('typed search attributes did not replay')
			}
			context.complete('"metadata complete"')!
			return
		}
		context.delete_memo(['old'])!
		context.upsert_memo({
			'keep': '"updated"'
		})!
		context.upsert_search_attributes([
			sdk.SearchAttribute{ name: 'VSDKKeyword', type_: .keyword, value_json: '"ready"' },
			sdk.SearchAttribute{ name: 'VSDKInt', type_: .integer, value_json: '42' },
			sdk.SearchAttribute{ name: 'VSDKKeywords', type_: .keyword_list, value_json: '["a","b"]' },
		])!
		workflow.updated = true
		context.timer(2 * time.second)!
	} else {
		return error('unexpected metadata event')
	}
}
