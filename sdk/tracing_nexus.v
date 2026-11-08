module sdk

import temporal_v.temporal

pub fn (context &NexusContext) trace_context() ?TraceContext {
	return context.trace_current
}

fn (context &NexusContext) trace_client(original &Client) !&Client {
	mut tracing := original.options.tracing
	if tracing == none { tracing = context.tracing }
	if current := context.trace_context() {
		if configured := tracing {
			tracing = TraceOptions{
				...configured
				parent:  current
				baggage: context.trace_baggage
			}
		}
	}
	return &Client{
		connection: original.connection.retain_client()!
		plugins:    original.plugins.clone()
		options:    ClientOptions{
			...original.options
			tracing: tracing
		}
	}
}

fn start_nexus_trace(options ?TraceOptions, endpoint string, request temporal.TemporalApiNexusV1_Request) ?&TraceSpan {
	tracing := options or { return none }
	operation := request.variant or { return none }
	service, name, action := match operation {
		temporal.TemporalApiNexusV1_Request_StartOperation_Case {
			operation.value.service, operation.value.operation, 'StartNexusOperation'
		}
		temporal.TemporalApiNexusV1_Request_CancelOperation_Case {
			operation.value.service, operation.value.operation, 'CancelNexusOperation'
		}
	}

	mut parent := extract_trace_context(request.header)
	if parent == none { parent = tracing.parent }
	return tracing.provider.start_span(action + ':' + service + '/' + name, TraceSpanOptions{
		parent:     parent
		baggage:    nexus_trace_baggage(options, request.header)
		kind:       .server
		attributes: {
			'temporal.nexus.endpoint':  TraceAttribute(endpoint)
			'temporal.nexus.service':   TraceAttribute(service)
			'temporal.nexus.operation': TraceAttribute(name)
		}
	}) or { return none }
}

fn finish_nexus_trace_error(span ?&TraceSpan, options ?TraceOptions, failure IError) {
	value := span or { return }
	tracing := options or { return }
	if failure.code() == 499 {
		value.set_attribute('temporal.nexus.cancelled', TraceAttribute(true)) or {}
		return
	}
	if tracing.record_errors {
		value.record_exception('NexusError', failure) or {}
		value.set_status(.error, failure.msg()) or {}
	}
}

fn nexus_trace_baggage(options ?TraceOptions, headers map[string]string) Baggage {
	parent := if tracing := options { tracing.baggage } else { Baggage{} }
	return inherited_baggage(extract_baggage(headers), parent)
}

// client shares the worker connection with independent ownership. Close the
// returned handle; it may outlive this Nexus request and its worker.
pub fn (context &NexusContext) client() !&Client {
	if isnil(context.connection) { return error('Nexus context has no worker connection') }
	mut tracing := context.client_configuration.tracing
	if tracing == none { tracing = context.tracing }
	if current := context.trace_context() {
		if configured := tracing {
			tracing = TraceOptions{
				...configured
				parent:  current
				baggage: context.trace_baggage
			}
		}
	}
	return &Client{
		connection: context.connection.retain_client()!
		options:    ClientOptions{
			...context.client_configuration
			tracing: tracing
		}
		plugins:    context.client_plugins.clone()
	}
}
