module sdk

import x.json2
import temporal_v.temporal

// Baggage travels alongside SpanContext and is never exported as span attributes.
pub struct PropagationContext {
pub:
	span    ?TraceContext
	baggage Baggage
}

pub fn (context PropagationContext) headers() !map[string]string {
	mut headers := map[string]string{}
	if span := context.span { headers = span.propagation_headers()! }
	if context.baggage.entries.len > 0 { headers['baggage'] = context.baggage.header() }
	return headers
}

pub fn extract_propagation_context(headers map[string]string) PropagationContext {
	return PropagationContext{
		span:    extract_trace_context(headers)
		baggage: extract_baggage(headers)
	}
}

pub fn (context PropagationContext) temporal_header() !temporal.TemporalApiCommonV1_Payload {
	mut payload := JsonPayloadConverter{}.to_payload(PayloadValue(JsonValue{
		data: json2.encode(context.headers()!)
	}), SerializationContext{})!
	payload.metadata['temporal-v/propagation'] = '1'.bytes()
	return payload
}

pub fn extract_temporal_propagation_context(headers map[string]temporal.TemporalApiCommonV1_Payload) PropagationContext {
	payload := headers[trace_header_key] or { return PropagationContext{} }
	value := JsonPayloadConverter{}.from_payload(payload, SerializationContext{}) or {
		return PropagationContext{}
	}
	carrier := json2.decode[map[string]string](value.logical_data()) or {
		return PropagationContext{}
	}
	return extract_propagation_context(carrier)
}

fn inherited_baggage(incoming Baggage, parent Baggage) Baggage {
	return if incoming.entries.len > 0 { incoming } else { parent }
}

pub fn (client &Client) with_propagation_context(context PropagationContext) !&Client {
	if span := context.span { span.validate()! }
	tracing := client.options.tracing or { return error('client tracing is not configured') }
	return &Client{
		connection: client.connection.retain_client()!
		plugins:    client.plugins.clone()
		options:    ClientOptions{
			...client.options
			tracing: TraceOptions{
				...tracing
				parent:  context.span
				baggage: context.baggage
			}
		}
	}
}

pub fn (context &WorkflowContext) propagation_context() PropagationContext {
	return PropagationContext{
		span:    context.trace_context()
		baggage: context.trace_baggage
	}
}

pub fn (context &ActivityContext) propagation_context() PropagationContext {
	return PropagationContext{
		span:    context.trace_context()
		baggage: context.trace_baggage
	}
}

pub fn (context &NexusContext) propagation_context() PropagationContext {
	return PropagationContext{
		span:    context.trace_context()
		baggage: context.trace_baggage
	}
}
