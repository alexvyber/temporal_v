module sdk

import temporal

pub fn (mut context NexusContext) query_workflow(original &Client, workflow WorkflowHandle, name string, arguments Arguments, options QueryOptions) !NexusResult {
	client := context.trace_client(original)!
	defer { client.close() }
	context.check_cancelled()!
	reply := client.query_response(workflow, name, arguments, options) or {
		if err is QueryError { return err.failure }
		if err is QueryRejectedError {
			return ApplicationError{
				message:       err.msg()
				category:      'NexusQueryRejected'
				non_retryable: true
			}
		}
		return err
	}
	mut links := []temporal.TemporalApiNexusV1_Link{}
	if link := reply.link { links << nexus_link_from_common(link)! }
	return NexusResult{
		value: reply.value
		links: links
	}
}

fn (context &NexusContext) signal_links(links []temporal.TemporalApiCommonV1_Link) ![]temporal.TemporalApiCommonV1_Link {
	mut result := links.clone()
	for link in context.info().links {
		if link.type_.starts_with(nexus_link_prefix) { result << common_link_from_nexus(link)! }
	}
	return result
}

fn (mut context NexusContext) check_signal() ! {
	context.check_cancelled()!
	if context.signaled { return error('only one signal can be sent per Nexus invocation') }
	if context.info().request_id == '' { return error('Nexus signal requires a request ID') }
}

pub fn (mut context NexusContext) signal_workflow(original &Client, workflow WorkflowHandle, name string, arguments Arguments, options MessageOptions) ! {
	client := context.trace_client(original)!
	defer { client.close() }
	context.check_signal()!
	client.signal(workflow, name, arguments, MessageOptions{
		...options
		request_id: context.info().request_id
		links:      context.signal_links(options.links)!
	})!
	context.signaled = true
}

pub fn (mut context NexusContext) signal_with_start(original &Client, options WorkflowStart, name string, arguments Arguments) !SignalWithStartResult {
	client := context.trace_client(original)!
	defer { client.close() }
	context.check_signal()!
	started := client.signal_with_start(WorkflowStart{
		...options
		request_id: context.info().request_id
		links:      context.signal_links(options.links)!
	}, name, arguments)!
	context.signaled = true
	return started
}
