module sdk

pub fn (context &ActivityContext) trace_context() ?TraceContext {
	return context.trace_current
}

fn execute_traced_activity(handler ActivityDispatch, mut context ActivityContext, arguments []string) !PayloadValue {
	tracing := context.tracing or {
		context.check_cancelled()!
		return (ActivityNext{
			handler:      handler
			interceptors: context.interceptors
		}).execute(mut context, arguments)!
	}

	incoming := extract_temporal_propagation_context(context.trace_headers)
	context.trace_baggage = inherited_baggage(incoming.baggage, tracing.baggage)
	mut parent := incoming.span
	if parent == none { parent = tracing.parent }
	information := context.information
	span := tracing.provider.start_span('RunActivity:' + information.activity_type, TraceSpanOptions{
		parent:     parent
		baggage:    context.trace_baggage
		kind:       .server
		attributes: {
			'temporal.namespace':        TraceAttribute(information.workflow_namespace)
			'temporal.workflow.id':      TraceAttribute(information.workflow_id)
			'temporal.workflow.run_id':  TraceAttribute(information.run_id)
			'temporal.activity.id':      TraceAttribute(information.activity_id)
			'temporal.activity.type':    TraceAttribute(information.activity_type)
			'temporal.activity.attempt': TraceAttribute(i64(information.attempt))
			'temporal.activity.local':   TraceAttribute(information.is_local)
		}
	})!
	context.trace_current = span.context()
	defer { span.finish() }
	context.check_cancelled() or {
		finish_activity_trace_error(span, err, tracing)
		return err
	}
	return (ActivityNext{
		handler:      handler
		interceptors: context.interceptors
	}).execute(mut context, arguments) or {
		finish_activity_trace_error(span, err, tracing)
		return err
	}
}

fn finish_activity_trace_error(span &TraceSpan, failure IError, tracing TraceOptions) {
	if failure is CancelledError || failure is ActivityCancelledError {
		span.set_attribute('temporal.activity.cancelled', TraceAttribute(true)) or {}
		return
	}
	if tracing.record_errors {
		span.record_exception('ActivityError', failure) or {}
		span.set_status(.error, failure.msg()) or {}
	}
}
