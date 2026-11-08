module sdk

import alexvyber.temporal_v.temporal
import sync

pub enum WorkflowListKind {
	current
	archived
	scan
}

pub struct WorkflowListOptions {
pub:
	query           string
	page_size       int = 100
	next_page_token []u8
	kind            WorkflowListKind
	// Disabling decoding leaves memo available as wire payloads in raw.
	decode_memo bool = true
}

pub struct WorkflowListing {
pub:
	handle WorkflowHandle
	memo   map[string]PayloadValue @[json: '-']
	raw    temporal.TemporalApiWorkflowV1_WorkflowExecutionInfo
}

pub struct WorkflowListPage {
pub:
	executions      []WorkflowListing
	next_page_token []u8
}

fn (options WorkflowListOptions) validate() ! {
	if options.page_size < 1 { return error('workflow list page size must be positive') }
	validate_version_name(options.query, 'visibility query', false)!
}

pub fn (client &Client) workflow_page(options WorkflowListOptions) !WorkflowListPage {
	options.validate()!
	method, body := match options.kind {
		.current {
			'ListWorkflowExecutions', temporal.TemporalApiWorkflowserviceV1_ListWorkflowExecutionsRequest{
				namespace:       client.options.namespace
				query:           options.query
				page_size:       options.page_size
				next_page_token: options.next_page_token
			}.encode()
		}
		.archived {
			'ListArchivedWorkflowExecutions', temporal.TemporalApiWorkflowserviceV1_ListArchivedWorkflowExecutionsRequest{
				namespace:       client.options.namespace
				query:           options.query
				page_size:       options.page_size
				next_page_token: options.next_page_token
			}.encode()
		}
		.scan {
			'ScanWorkflowExecutions', temporal.TemporalApiWorkflowserviceV1_ScanWorkflowExecutionsRequest{
				namespace:       client.options.namespace
				query:           options.query
				page_size:       options.page_size
				next_page_token: options.next_page_token
			}.encode()
		}
	}

	// A page spans multiple workflows. Retrieve each memo after selecting that
	// execution's scope instead of assigning the entire response one identity.
	bytes := client.call_rpc(ClientRpcRequest{
		service:        .workflow
		method:         method
		body:           body
		raw_payloads:   true
		timeout_millis: client.options.rpc_timeout_millis
		retry:          client.options.rpc_retry
		cancellation:   client.options.cancellation
	})!
	return match options.kind {
		.current {
			page :=
				temporal.TemporalApiWorkflowserviceV1_ListWorkflowExecutionsResponse.decode(bytes)!
			client.workflow_listing_page(page.executions, page.next_page_token, options.decode_memo)!
		}
		.archived {
			page :=
				temporal.TemporalApiWorkflowserviceV1_ListArchivedWorkflowExecutionsResponse.decode(bytes)!
			client.workflow_listing_page(page.executions, page.next_page_token, options.decode_memo)!
		}
		.scan {
			page :=
				temporal.TemporalApiWorkflowserviceV1_ScanWorkflowExecutionsResponse.decode(bytes)!
			client.workflow_listing_page(page.executions, page.next_page_token, options.decode_memo)!
		}
	}
}

fn (client &Client) workflow_listing_page(executions []temporal.TemporalApiWorkflowV1_WorkflowExecutionInfo, token []u8, decode_memo bool) !WorkflowListPage {
	mut entries := []WorkflowListing{cap: executions.len}
	for info in executions {
		execution := info.execution or {
			return error('workflow listing has no execution identity')
		}

		if execution.workflow_id == '' || execution.run_id == '' {
			return error('workflow listing has an incomplete execution identity')
		}
		mut memo := map[string]PayloadValue{}
		if decode_memo {
			if fields := info.memo {
				memo = client.workflow_converter(execution.workflow_id).decode_map(fields.fields)!
			}
		}
		entries << WorkflowListing{
			handle: WorkflowHandle{
				workflow_id: execution.workflow_id
				run_id:      execution.run_id
			}
			raw:    info
			memo:   memo
		}
	}
	return WorkflowListPage{
		executions:      entries
		next_page_token: token.clone()
	}
}

pub fn (client &Client) count_workflows(query string) !temporal.TemporalApiWorkflowserviceV1_CountWorkflowExecutionsResponse {
	validate_version_name(query, 'visibility query', false)!
	return client.rpc_count_workflow_executions(temporal.TemporalApiWorkflowserviceV1_CountWorkflowExecutionsRequest{
		namespace: client.options.namespace
		query:     query
	})!
}

@[heap]
pub struct WorkflowPages {
	mutex   &sync.Mutex = sync.new_mutex()
	client  &Client
	options WorkflowListOptions
mut:
	token       []u8
	seen_tokens map[string]bool
	finished    bool
	advancing   bool
}

// Iteration is lazy. RPC errors leave the current token available for retry.
pub fn (client &Client) workflow_pages(options WorkflowListOptions) !&WorkflowPages {
	options.validate()!
	token := options.next_page_token.clone()
	mut seen := map[string]bool{}
	if token.len > 0 { seen[token.bytestr().clone()] = true }
	return &WorkflowPages{
		client:      client
		options:     options
		token:       token
		seen_tokens: seen
	}
}

pub fn (pages &WorkflowPages) exhausted() bool {
	pages.mutex.lock()
	defer { pages.mutex.unlock() }
	return pages.finished
}

pub fn (mut pages WorkflowPages) next_page() !WorkflowListPage {
	pages.mutex.lock()
	if pages.finished {
		pages.mutex.unlock()
		return WorkflowListPage{}
	}
	if pages.advancing {
		pages.mutex.unlock()
		return error('workflow iterator already has an active page request')
	}
	pages.advancing = true
	token := pages.token.clone()
	pages.mutex.unlock()
	defer {
		pages.mutex.lock()
		pages.advancing = false
		pages.mutex.unlock()
	}
	// Interceptors and storage drivers run outside the state lock, allowing
	// them to inspect the iterator without deadlocking its active request.
	page :=
		pages.client.workflow_page(WorkflowListOptions{ ...pages.options, next_page_token: token })!
	pages.mutex.lock()
	defer { pages.mutex.unlock() }
	if page.next_page_token.len == 0 {
		pages.finished = true
	} else {
		key := page.next_page_token.bytestr().clone()
		if key in pages.seen_tokens {
			return error('server returned a repeated workflow list page token')
		}
		pages.seen_tokens[key] = true
	}
	pages.token = page.next_page_token.clone()
	return page
}
