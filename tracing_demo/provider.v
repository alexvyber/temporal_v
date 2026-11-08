module main

import alexvyber.temporal_v.sdk
import sync
import time

@[heap]
struct ExportAudit {
	mutex            &sync.RwMutex = sync.new_rwmutex()
	entered          chan bool     = chan bool{cap: 1024}
	release          chan bool     = chan bool{cap: 1024}
	shutdown_entered chan bool     = chan bool{cap: 1}
mut:
	records               []sdk.TraceSpanData
	wait_export           bool
	fail_export           bool
	fail_shutdown         bool
	wait_shutdown         bool
	shutdowns             int
	close_from_export     bool
	provider              ?&sdk.TraceProvider
	panic_export          bool
	panic_shutdown        bool
	export_defers         int
	shutdown_defers       int
	last_context          ?&sdk.TraceExportContext
	last_shutdown_context ?&sdk.TraceExportContext
}

struct RecordingTraceExporter {
	audit &ExportAudit
}

fn (exporter RecordingTraceExporter) export(context &sdk.TraceExportContext, spans []sdk.TraceSpanData) ! {
	defer {
		mut audit := exporter.audit
		audit.mutex.lock()
		audit.export_defers++
		audit.mutex.unlock()
	}
	{
		mut audit := exporter.audit
		audit.mutex.lock()
		audit.last_context = context
		audit.mutex.unlock()
	}
	mut state := exporter.audit
	state.mutex.rlock()
	wait := state.wait_export
	fail := state.fail_export
	panics := state.panic_export
	close_from_export := state.close_from_export
	provider := state.provider
	state.mutex.runlock()
	if close_from_export {
		if owner := provider { owner.shutdown(0)! }
	}
	state.entered <- true
	if wait {
		for {
			if context.cancelled() { return error('private-export-deadline') }
			select {
				_ := <-state.release {
					break
				}
				2 * time.millisecond {}
			}
		}
	}
	if panics { panic('private-trace-export-panic') }
	if fail { return error('private-export-credential') }
	state.mutex.lock()
	state.records << spans
	state.mutex.unlock()
}

fn (exporter RecordingTraceExporter) shutdown(context &sdk.TraceExportContext) ! {
	defer {
		mut audit := exporter.audit
		audit.mutex.lock()
		audit.shutdown_defers++
		audit.mutex.unlock()
	}
	mut state := exporter.audit
	state.mutex.lock()
	state.shutdowns++
	wait := state.wait_shutdown
	fail := state.fail_shutdown
	panics := state.panic_shutdown
	state.last_shutdown_context = context
	state.mutex.unlock()
	state.shutdown_entered <- true
	if wait {
		for {
			if context.cancelled() { return error('private-shutdown-deadline') }
			select {
				_ := <-state.release {
					break
				}
				2 * time.millisecond {}
			}
		}
	}
	if panics { panic('private-trace-shutdown-panic') }
	if fail { return error('private-shutdown-credential') }
}

fn (exporter RecordingTraceExporter) set_failure(enabled bool) {
	mut state := exporter.audit
	state.mutex.lock()
	state.fail_export = enabled
	state.mutex.unlock()
}

fn (exporter RecordingTraceExporter) attach(provider &sdk.TraceProvider) {
	mut state := exporter.audit
	state.mutex.lock()
	state.provider = provider
	state.mutex.unlock()
}

fn fixture_trace_data(name string, attributes map[string]sdk.TraceAttribute) !sdk.TraceSpanData {
	return sdk.TraceSpanData{
		context:          sdk.new_trace_context(true)!
		name:             name
		start_unix_nanos: 1
		end_unix_nanos:   2
		attributes:       attributes
	}
}

fn audit_records(state &ExportAudit) []sdk.TraceSpanData {
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	return state.records.clone()
}

@[heap]
struct TraceInputOwner {
mut:
	values []i64
}

fn check_trace_snapshot_bounds() ! {
	audit := &ExportAudit{
		wait_export: true
	}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:          RecordingTraceExporter{
			audit: audit
		}
		max_pending_spans: 2
		max_pending_bytes: 1024 * 1024
		max_batch:         1
		flush_interval:    time.millisecond
	})!
	defer { provider.shutdown(time.second) or {} }
	mut source := &TraceInputOwner{
		values: [i64(7), 8]
	}
	first := fixture_trace_data('first', {
		'array': sdk.TraceAttribute(sdk.TraceIntegerArray{
			values: source.values
		})
	})!
	if !provider.record(first)! { return error('first trace snapshot rejected') }
	_ := <-audit.entered
	source.values[0] = 99
	if !provider.record(fixture_trace_data('second', {})!)! {
		return error('second trace snapshot rejected')
	}
	if provider.record(fixture_trace_data('overflow', {})!)! {
		return error('in-flight span was excluded from queue capacity')
	}
	audit.release <- true
	audit.release <- true
	provider.force_flush(time.second)!
	records := audit_records(audit)
	if records.len != 2 || records[0].name != 'first' || records[1].name != 'second' {
		return error('trace queue ordering changed')
	}
	value := records[0].attributes['array'] or { return error('trace array snapshot missing') }
	if value is sdk.TraceIntegerArray {
		if value.values != [i64(7), 8] {
			return error('trace snapshots retained caller array storage')
		}
	} else {
		return error('trace snapshot array type changed')
	}
	provider.shutdown(time.second)!
	provider.shutdown(time.second)!
	stats := provider.stats()
	if stats.accepted_spans != 2 || stats.sent_spans != 2 || stats.dropped_spans != 1
		|| stats.pending_spans != 0 || stats.pending_bytes != 0 || !stats.closed {
		return error('trace queue accounting changed')
	}
	audit.mutex.rlock()
	closed := audit.shutdowns
	audit.mutex.runlock()
	if closed != 1 { return error('trace exporter shutdown was repeated') }
	if provider.record(fixture_trace_data('late', {})!)! {
		return error('closed provider accepted a trace')
	}
	tiny := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:          RecordingTraceExporter{
			audit: &ExportAudit{}
		}
		max_pending_bytes: 1
	})!
	if tiny.record(fixture_trace_data('bytes', {})!)! {
		return error('trace byte capacity was ignored')
	}
	tiny.shutdown(time.second)!
}

fn check_trace_failures() ! {
	audit := &ExportAudit{
		fail_export: true
	}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:       RecordingTraceExporter{
			audit: audit
		}
		max_batch:      1
		flush_interval: time.millisecond
	})!
	provider.record(fixture_trace_data('failure', {})!)!
	mut failed := false
	provider.force_flush(time.second) or { failed = true }
	if !failed || provider.stats().failed_spans != 1
		|| !provider.stats().last_error.contains('private-export-credential') {
		return error('trace export failure was not observable privately')
	}

	RecordingTraceExporter{
		audit: audit
	}.set_failure(false)
	provider.record(fixture_trace_data('repaired', {})!)!
	provider.force_flush(time.second)!
	provider.shutdown(time.second)!
	if provider.stats().sent_spans != 1 || provider.stats().pending_spans != 0 {
		return error('trace exporter did not recover')
	}
	shutdown_audit := &ExportAudit{
		fail_shutdown: true
	}
	shutdown_provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter: RecordingTraceExporter{
			audit: shutdown_audit
		}
	})!
	failed = false
	shutdown_provider.shutdown(time.second) or { failed = true }
	if !failed || !shutdown_provider.stats().closed || !shutdown_provider.stats().shutdown_failed
		|| !shutdown_provider.stats().last_error.contains('private-shutdown-credential') {
		return error('trace shutdown failure was hidden')
	}
}

fn check_trace_span_limits() ! {
	audit := &ExportAudit{}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:       RecordingTraceExporter{
			audit: audit
		}
		limits:         sdk.TraceLimits{
			attributes: 1
			events:     1
			links:      1
		}
		flush_interval: time.millisecond
	})!
	parent := sdk.new_trace_context(true)!
	span := provider.start_span('limits', sdk.TraceSpanOptions{
		parent:           parent
		start_unix_nanos: u64(0)
	})!
	if !span.is_recording() || span.context().trace_id != parent.trace_id
		|| span.context().span_id == parent.span_id {
		return error('recording trace child context changed')
	}
	span.set_attribute('n', sdk.TraceAttribute(i64(1)))!
	span.set_attribute('dropped', sdk.TraceAttribute(i64(2)))!
	span.set_attribute('n', sdk.TraceAttribute(i64(3)))!
	event := sdk.TraceEvent{
		name:       'event'
		unix_nanos: 1
	}
	span.add_event(event)!
	span.add_event(event)!
	link := sdk.TraceLink{
		context: parent
	}
	span.add_link(link)!
	span.add_link(link)!
	span.set_status(.error, 'old failure')!
	span.set_status(.ok, '')!
	span.set_status(.error, 'must not override OK')!
	span.finish(sdk.TraceEndOptions{ unix_nanos: u64(10) })
	span.finish()
	span.set_attribute('late', sdk.TraceAttribute(true))!
	if span.is_recording() { return error('completed span remained recording') }
	provider.force_flush(time.second)!
	provider.shutdown(time.second)!
	records := audit_records(audit)
	if records.len != 1 { return error('span finish was not idempotent') }
	data := records[0]
	if data.start_unix_nanos != 0 || data.end_unix_nanos != 10 || data.status != .ok
		|| data.status_message != '' || data.dropped_attributes != 1 || data.dropped_events != 1
		|| data.dropped_links != 1 {
		return error('trace limits, epoch timestamps or status precedence changed')
	}
	value := data.attributes['n'] or { return error('trace updated attribute missing') }
	if value is i64 {
		if value != 3 { return error('trace attribute replacement changed') }
	} else {
		return error('trace integer attribute type changed')
	}
}

fn emit_trace_spans(provider &sdk.TraceProvider, index int, done chan string) {
	for sequence in 0 .. 8 {
		span := provider.start_span('concurrent-' + index.str() + '-' + sequence.str()) or {
			done <- err.msg()
			return
		}
		span.set_attribute('worker', sdk.TraceAttribute(i64(index))) or {
			done <- err.msg()
			return
		}
		span.finish()
	}
	done <- ''
}

fn check_trace_concurrency() ! {
	audit := &ExportAudit{}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:          RecordingTraceExporter{
			audit: audit
		}
		max_pending_spans: 512
		max_pending_bytes: 4 * 1024 * 1024
		max_batch:         16
		flush_interval:    time.millisecond
	})!
	done := chan string{cap: 8}
	for index in 0 .. 8 {
		spawn emit_trace_spans(provider, index, done)
	}
	for _ in 0 .. 8 {
		message := <-done
		if message != '' { return error(message) }
	}
	provider.force_flush(time.second)!
	provider.shutdown(time.second)!
	records := audit_records(audit)
	mut names := map[string]bool{}
	mut identities := map[string]bool{}
	for data in records {
		if data.name in names || data.context.span_id in identities {
			return error('concurrent trace identity was reused')
		}
		names[data.name] = true
		identities[data.context.span_id] = true
	}
	if records.len != 64 || provider.stats().sent_spans != 64 || provider.stats().pending_bytes != 0 {
		return error('concurrent trace spans were lost')
	}
}

fn check_trace_shutdown() ! {
	audit := &ExportAudit{
		wait_export: true
	}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:       RecordingTraceExporter{
			audit: audit
		}
		export_timeout: 300 * time.second
	})!
	provider.record(fixture_trace_data('blocked', {})!)!
	_ := <-audit.entered
	mut expired := false
	provider.shutdown(time.millisecond) or { expired = true }
	if !expired { return error('blocked trace export did not respect caller shutdown budget') }
	provider.shutdown(time.second)!
	if !provider.stats().closed || provider.stats().failed_spans != 1
		|| provider.stats().pending_spans != 0 {
		return error('cooperative trace cancellation did not drain')
	}
	self_audit := &ExportAudit{
		close_from_export: true
	}
	self_provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter: RecordingTraceExporter{
			audit: self_audit
		}
	})!

	RecordingTraceExporter{
		audit: self_audit
	}.attach(self_provider)
	self_provider.record(fixture_trace_data('self-close', {})!)!
	self_provider.shutdown(time.second)!
	if !self_provider.stats().closed || self_provider.stats().sent_spans != 1 {
		return error('trace callback closure did not drain safely')
	}
}

fn provider_validation() ! {
	check_trace_snapshot_bounds()!
	check_trace_failures()!
	check_trace_span_limits()!
	check_trace_concurrency()!
	check_trace_shutdown()!
	println('PROVIDER_OK')
}

fn check_trace_panic_recovery() ! {
	mut audit := &ExportAudit{
		panic_export: true
	}
	provider := sdk.new_trace_provider(sdk.TraceProviderOptions{
		exporter:       RecordingTraceExporter{
			audit: audit
		}
		max_batch:      1
		flush_interval: time.millisecond
	})!
	provider.record(fixture_trace_data('panic', {})!)!
	mut failed := false
	provider.force_flush(time.second) or { failed = true }
	assert failed && provider.stats().failed_spans == 1 && provider.stats().pending_spans == 0
		&& provider.stats().last_error.contains('private-trace-export-panic')
	mut saved := audit
	saved.mutex.lock()
	saved.panic_export = false
	saved.panic_shutdown = true
	captured := saved.last_context
	saved.mutex.unlock()
	if context := captured {
		assert context.cancelled()
	} else {
		return error('panic exporter lost its context')
	}
	provider.record(fixture_trace_data('repaired', {})!)!
	provider.force_flush(time.second)!
	failed = false
	provider.shutdown(time.second) or { failed = true }
	assert failed && provider.stats().closed && provider.stats().shutdown_failed
		&& provider.stats().sent_spans == 1 && provider.stats().pending_spans == 0
		&& provider.stats().last_error.contains('private-trace-shutdown-panic')
	saved.mutex.rlock()
	export_defers := saved.export_defers
	shutdowns := saved.shutdowns
	shutdown_defers := saved.shutdown_defers
	shutdown_context := saved.last_shutdown_context
	saved.mutex.runlock()
	assert export_defers == 2 && shutdowns == 1 && shutdown_defers == 1
	if context := shutdown_context {
		assert context.cancelled()
	} else {
		return error('panic shutdown lost its context')
	}
	println('TRACE_PANIC_OK')
}
