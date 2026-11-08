module main

import sdk
import temporal
import os
import sync

@[heap]
struct VisibilityFaults {
	mutex   &sync.Mutex = sync.new_mutex()
	mode    string
	method  string    = 'ListWorkflowExecutions'
	started chan bool = chan bool{cap: 1}
	release chan bool = chan bool{cap: 1}
mut:
	calls int
}

fn (faults &VisibilityFaults) intercept(request sdk.ClientRpcRequest, next sdk.ClientNext) ![]u8 {
	mut state := unsafe { faults }
	state.mutex.lock()
	state.calls++
	call := state.calls
	state.mutex.unlock()
	if request.method != faults.method { return error('visibility method changed') }
	query, namespace, page_size := match request.method {
		'ListWorkflowExecutions' {
			value :=
				temporal.TemporalApiWorkflowserviceV1_ListWorkflowExecutionsRequest.decode(request.body)!
			value.query, value.namespace, value.page_size
		}
		'ListArchivedWorkflowExecutions' {
			value :=
				temporal.TemporalApiWorkflowserviceV1_ListArchivedWorkflowExecutionsRequest.decode(request.body)!
			value.query, value.namespace, value.page_size
		}
		else {
			value :=
				temporal.TemporalApiWorkflowserviceV1_ScanWorkflowExecutionsRequest.decode(request.body)!
			value.query, value.namespace, value.page_size
		}
	}

	if query != 'fixture' || namespace != 'visibility-test' || page_size != 2 {
		return error('visibility options lost')
	}
	if faults.mode == 'busy' {
		faults.started <- true
		_ := <-faults.release
	}
	if faults.mode == 'retry' && call == 1 {
		return sdk.RpcError{
			status:  14
			message: 'visibility temporary failure'
		}
	}
	token := if faults.mode == 'cycle' {
		if call % 2 == 1 { 'a'.bytes() } else { 'b'.bytes() }
	} else if faults.mode == 'empty' && call == 1 {
		'a'.bytes()
	} else {
		[]u8{}
	}
	entries := if faults.mode == 'cycle' || (faults.mode == 'empty' && call == 1) {
		[]temporal.TemporalApiWorkflowV1_WorkflowExecutionInfo{}
	} else {
		[
			temporal.TemporalApiWorkflowV1_WorkflowExecutionInfo{
				execution:  temporal.TemporalApiCommonV1_WorkflowExecution{
					workflow_id: 'synthetic'
					run_id:      'run'
				}
				pb_unknown: [
					u8(160),
					6,
					7,
				]
			},
		]
	}
	return match request.method {
		'ListWorkflowExecutions' {
			temporal.TemporalApiWorkflowserviceV1_ListWorkflowExecutionsResponse{
				executions:      entries
				next_page_token: token
			}.encode()
		}
		'ListArchivedWorkflowExecutions' {
			temporal.TemporalApiWorkflowserviceV1_ListArchivedWorkflowExecutionsResponse{
				executions:      entries
				next_page_token: token
			}.encode()
		}
		else {
			temporal.TemporalApiWorkflowserviceV1_ScanWorkflowExecutionsResponse{
				executions:      entries
				next_page_token: token
			}.encode()
		}
	}
}

fn validate_pagination() ! {
	for kind in [sdk.WorkflowListKind.current, .archived, .scan] {
		method := match kind {
			.current { 'ListWorkflowExecutions' }
			.archived { 'ListArchivedWorkflowExecutions' }
			.scan { 'ScanWorkflowExecutions' }
		}

		for mode in ['empty', 'cycle', 'retry'] {
			faults := &VisibilityFaults{
				mode:   mode
				method: method
			}
			client := sdk.connect(sdk.ClientOptions{
				address:      os.getenv('TEMPORAL_ADDRESS')
				namespace:    'visibility-test'
				interceptors: [sdk.ClientInterceptor(faults)]
			})!
			defer { client.close() }
			if _ := client.workflow_pages(sdk.WorkflowListOptions{ page_size: 0 }) {
				return error('invalid visibility size accepted')
			}
			mut pages := client.workflow_pages(sdk.WorkflowListOptions{
				query:     'fixture'
				page_size: 2
				kind:      kind
			})!
			if faults.calls != 0 { return error('visibility iterator was not lazy') }
			if mode == 'retry' {
				if _ := pages.next_page() {
					return error('injected visibility failure lost')
				} else {
					if err.code() != 14 { return err }
				}
				if pages.exhausted() { return error('RPC failure exhausted iterator') }
			}
			first := pages.next_page()!
			if mode == 'cycle' {
				pages.next_page()!
				if _ := pages.next_page() { return error('visibility token cycle accepted') }
			} else {
				page := if mode == 'empty' { pages.next_page()! } else { first }
				if !pages.exhausted() || page.executions.len != 1
					|| page.executions[0].raw.pb_unknown != [u8(160), 6, 7] {
					return error('visibility page content/EOF changed')
				}
				if pages.next_page()!.executions.len != 0 || faults.calls != 2 {
					return error('visibility EOF performed an RPC')
				}
			}
		}
	}
	validate_iterator_lifetime()!
	println('VISIBILITY_VALIDATION_OK')
}

fn advance_page(pages &sdk.WorkflowPages, result chan string) {
	mut cursor := unsafe { pages }
	cursor.next_page() or {
		result <- err.msg()
		return
	}
	result <- ''
}

fn validate_iterator_lifetime() ! {
	faults := &VisibilityFaults{
		mode: 'busy'
	}
	client := sdk.connect(sdk.ClientOptions{
		address:      os.getenv('TEMPORAL_ADDRESS')
		namespace:    'visibility-test'
		interceptors: [sdk.ClientInterceptor(faults)]
	})!
	defer { client.close() }
	mut pages := client.workflow_pages(sdk.WorkflowListOptions{ query: 'fixture', page_size: 2 })!
	result := chan string{cap: 1}
	spawn advance_page(pages, result)
	_ := <-faults.started
	if pages.exhausted() { return error('active iterator reported EOF') }
	if _ := pages.next_page() {
		return error('overlapping page request accepted')
	} else {
		if !err.msg().contains('active page request') { return err }
	}
	faults.release <- true
	if <-result != '' || !pages.exhausted() || faults.calls != 1 {
		return error('iterator state did not commit after overlap rejection')
	}
	closed_faults := &VisibilityFaults{
		mode: 'empty'
	}
	closed := sdk.connect(sdk.ClientOptions{
		address:      os.getenv('TEMPORAL_ADDRESS')
		namespace:    'visibility-test'
		interceptors: [sdk.ClientInterceptor(closed_faults)]
	})!
	mut cursor := closed.workflow_pages(sdk.WorkflowListOptions{ query: 'fixture', page_size: 2 })!
	closed.close()
	if _ := cursor.next_page() {
		return error('closed visibility client accepted a request')
	} else {
		if !err.msg().contains('closed') { return err }
	}
	if closed_faults.calls != 0 { return error('closed client reached its interceptor') }
}
