module sdk

import corebridge

struct GrpcCallbackOutcome {
mut:
	failure ?IError
	reply   corebridge.GrpcReply
}

fn invoke_grpc_transport(transport GrpcTransport, context &GrpcTransportContext, request corebridge.GrpcRequestSnapshot) !corebridge.GrpcReply {
	mut outcome := GrpcCallbackOutcome{}
	recover_grpc_transport(transport, context, request, mut outcome)
	if failure := outcome.failure { return failure }
	return outcome.reply
}

fn recover_grpc_transport(transport GrpcTransport, context &GrpcTransportContext, request corebridge.GrpcRequestSnapshot, mut outcome GrpcCallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	outcome.reply = transport.call(context, request) or {
		outcome.failure = err
		return
	}
}

fn invoke_metric_export(exporter MetricExporter, context &MetricExportContext, values []MetricSample) ! {
	mut outcome := CallbackOutcome{}
	recover_metric_export(exporter, context, values, mut outcome)
	if failure := outcome.failure { return failure }
}

fn recover_metric_export(exporter MetricExporter, context &MetricExportContext, values []MetricSample, mut outcome CallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	exporter.export(context, values) or {
		outcome.failure = err
		return
	}
}

fn invoke_metric_shutdown(exporter MetricExporterShutdown, context &MetricExportContext) ! {
	mut outcome := CallbackOutcome{}
	recover_metric_shutdown(exporter, context, mut outcome)
	if failure := outcome.failure { return failure }
}

fn recover_metric_shutdown(exporter MetricExporterShutdown, context &MetricExportContext, mut outcome CallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	exporter.shutdown(context) or {
		outcome.failure = err
		return
	}
}

fn invoke_trace_export(exporter TraceExporter, context &TraceExportContext, values []TraceSpanData) ! {
	mut outcome := CallbackOutcome{}
	recover_trace_export(exporter, context, values, mut outcome)
	if failure := outcome.failure { return failure }
}

fn recover_trace_export(exporter TraceExporter, context &TraceExportContext, values []TraceSpanData, mut outcome CallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	exporter.export(context, values) or {
		outcome.failure = err
		return
	}
}

fn invoke_trace_shutdown(exporter TraceExporterShutdown, context &TraceExportContext) ! {
	mut outcome := CallbackOutcome{}
	recover_trace_shutdown(exporter, context, mut outcome)
	if failure := outcome.failure { return failure }
}

fn recover_trace_shutdown(exporter TraceExporterShutdown, context &TraceExportContext, mut outcome CallbackOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	exporter.shutdown(context) or {
		outcome.failure = err
		return
	}
}
