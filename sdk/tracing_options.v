module sdk

pub struct TraceOptions {
pub:
	provider      &TraceProvider
	parent        ?TraceContext
	baggage       Baggage
	record_errors bool = true
}

fn (options TraceOptions) validate() ! {
	if isnil(options.provider) { return error('trace provider is required') }
	if parent := options.parent { parent.validate()! }
}

pub fn (client &Client) with_trace_parent(parent TraceContext) !&Client {
	parent.validate()!
	tracing := client.options.tracing or { return error('client tracing is not configured') }
	connection := client.connection.retain_client()!
	return &Client{
		connection: connection
		plugins:    client.plugins.clone()
		options:    ClientOptions{
			...client.options
			tracing: TraceOptions{
				...tracing
				parent: parent
			}
		}
	}
}
