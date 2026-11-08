module sdk

fn worker_client_configuration(options WorkerOptions) (ClientOptions, []PluginRegistration) {
	if client := options.client { return client.options, client.plugins.clone() }
	return ClientOptions{
		address:        options.address
		namespace:      options.namespace
		transport:      options.transport
		data_converter: options.data_converter
		telemetry:      options.telemetry
		tracing:        options.tracing
	}, []PluginRegistration{}
}

// client retains the worker's existing connection. The caller owns this client
// and must close it. Closing it does not close the worker; it may outlive the
// Activity and worker. No connection or plugin hooks are run again.
pub fn (context &ActivityContext) client() !&Client {
	if isnil(context.connection) { return error('Activity context has no worker connection') }
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
