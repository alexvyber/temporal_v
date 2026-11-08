module sdk

import sync
import time

pub struct TraceLimits {
pub:
	attributes int = 128
	events     int = 128
	links      int = 128
}

pub struct TraceProviderOptions {
pub:
	sampler           TraceSampler = ParentBasedTraceSampler{}
	exporter          TraceExporter
	max_pending_spans int           = 2048
	max_pending_bytes u64           = 16 * 1024 * 1024
	max_batch         int           = 256
	flush_interval    time.Duration = 100 * time.millisecond
	export_timeout    time.Duration = 5 * time.second
	limits            TraceLimits
}

pub struct TraceProviderStats {
pub mut:
	accepted_spans   u64
	pending_spans    int
	pending_bytes    u64
	sent_spans       u64
	failed_spans     u64
	dropped_spans    u64
	suppressed_spans u64
	batches          u64
	last_error       string
	closing          bool
	closed           bool
	shutdown_failed  bool
}

struct TraceQueueItem {
	data  TraceSpanData
	bytes u64
}

@[heap]
struct TraceProviderState {
	mutex    &sync.RwMutex = sync.new_rwmutex()
	options  TraceProviderOptions
	wake     chan bool = chan bool{cap: 1}
	progress chan bool = chan bool{cap: 1}
mut:
	queue             []TraceQueueItem
	stats             TraceProviderStats
	completed         u64
	flush_seen_failed u64
	active            ?&TraceExportContext
	dispatcher_thread u64
}

@[heap]
pub struct TraceProvider {
	state &TraceProviderState
}

fn (options TraceProviderOptions) validate() ! {
	if options.max_pending_spans < 1 || options.max_pending_spans > 65536
		|| options.max_pending_bytes < 1 || options.max_pending_bytes > 256 * 1024 * 1024
		|| options.max_batch < 1 || options.max_batch > 1024
		|| options.max_batch > options.max_pending_spans {
		return error('invalid trace queue or batch limits')
	}
	if options.flush_interval < time.millisecond || options.flush_interval > time.minute
		|| options.export_timeout < time.millisecond || options.export_timeout > 300 * time.second {
		return error('invalid trace flush interval or export timeout')
	}
	for limit in [options.limits.attributes, options.limits.events, options.limits.links] {
		if limit < 0 || limit > 1024 { return error('invalid trace span collection limit') }
	}
}

pub fn new_trace_provider(options TraceProviderOptions) !&TraceProvider {
	options.validate()!
	provider := &TraceProvider{
		state: &TraceProviderState{
			options: options
		}
	}
	spawn drive_trace_export(provider)
	return provider
}

fn notify_trace(channel chan bool) {
	select {
		channel <- true {}
		else {}
	}
}

pub fn (provider &TraceProvider) stats() TraceProviderStats {
	provider.state.mutex.rlock()
	defer { provider.state.mutex.runlock() }
	return provider.state.stats
}

pub fn (provider &TraceProvider) record(data TraceSpanData) !bool {
	size := trace_data_size(data)!
	snapshot := clone_trace_span(data)
	mut state := provider.state
	state.mutex.lock()
	if state.stats.closing || state.stats.pending_spans >= state.options.max_pending_spans
		|| size > state.options.max_pending_bytes - state.stats.pending_bytes {
		state.stats.dropped_spans++
		state.mutex.unlock()
		return false
	}
	state.queue << TraceQueueItem{
		data:  snapshot
		bytes: size
	}
	state.stats.accepted_spans++
	state.stats.pending_spans++
	state.stats.pending_bytes += size
	state.mutex.unlock()
	notify_trace(state.wake)
	return true
}

fn trace_export_error_message(message string) string {
	return protocol_text(if message.len > 4096 { message[..4096] } else { message })
}

fn drive_trace_export(provider &TraceProvider) {
	mut state := provider.state
	state.mutex.lock()
	state.dispatcher_thread = u64(sync.thread_id())
	state.mutex.unlock()
	for {
		state.mutex.lock()
		amount := if state.queue.len < state.options.max_batch {
			state.queue.len
		} else {
			state.options.max_batch
		}
		batch := state.queue[..amount].clone()
		state.queue = state.queue[amount..].clone()
		stopping := state.stats.closing && amount == 0
		state.mutex.unlock()
		if stopping {
			exporter := state.options.exporter
			if exporter is TraceExporterShutdown {
				context := trace_export_context(state.options.export_timeout)
				state.mutex.lock()
				state.active = context
				state.mutex.unlock()
				invoke_trace_shutdown(exporter, context) or {
					state.mutex.lock()
					state.stats.last_error = trace_export_error_message(err.msg())
					state.stats.shutdown_failed = true
					state.mutex.unlock()
				}
				context.close()
				state.mutex.lock()
				state.active = none
				state.mutex.unlock()
			}
			state.mutex.lock()
			state.stats.closed = true
			state.mutex.unlock()
			notify_trace(state.progress)
			return
		}
		if batch.len > 0 {
			mut values := []TraceSpanData{}
			mut bytes := u64(0)
			for item in batch {
				values << item.data
				bytes += item.bytes
			}
			context := trace_export_context(state.options.export_timeout)
			state.mutex.lock()
			state.active = context
			state.mutex.unlock()
			mut message := ''
			invoke_trace_export(state.options.exporter, context, values) or {
				message = trace_export_error_message(err.msg())
			}
			if message == '' && context.cancelled() {
				message = 'trace exporter exceeded its cooperative deadline'
			}
			context.close()
			state.mutex.lock()
			state.active = none
			state.stats.batches++
			state.stats.pending_spans -= batch.len
			state.stats.pending_bytes -= bytes
			state.completed += u64(batch.len)
			if message == '' {
				state.stats.sent_spans += u64(batch.len)
			} else {
				state.stats.failed_spans += u64(batch.len)
				state.stats.last_error = message
			}
			state.mutex.unlock()
			notify_trace(state.progress)
			continue
		}
		select {
			_ := <-state.wake {}
			state.options.flush_interval {}
		}
	}
}

fn valid_trace_wait(timeout time.Duration) ! {
	if timeout < 0 || timeout > 300 * time.second { return error('invalid trace drain timeout') }
}

pub fn (provider &TraceProvider) force_flush(timeout time.Duration) ! {
	valid_trace_wait(timeout)!
	mut state := provider.state
	state.mutex.rlock()
	target := state.stats.accepted_spans
	failed := state.flush_seen_failed
	own_dispatcher := state.dispatcher_thread == u64(sync.thread_id())
	state.mutex.runlock()
	if own_dispatcher {
		return error('trace exporter cannot synchronously flush its own dispatcher')
	}
	deadline := time.sys_mono_now() + i64(timeout)
	notify_trace(state.wake)
	for {
		state.mutex.lock()
		done := state.completed >= target
		failures := state.stats.failed_spans
		if done && failures > state.flush_seen_failed { state.flush_seen_failed = failures }
		state.mutex.unlock()
		if done {
			if failures > failed {
				return error('trace exports failed while flushing; inspect provider statistics')
			}
			return
		}
		if time.sys_mono_now() >= deadline { return error('trace flush deadline exceeded') }
		select {
			_ := <-state.progress {}
			5 * time.millisecond {}
		}
	}
}

pub fn (provider &TraceProvider) shutdown(timeout time.Duration) ! {
	valid_trace_wait(timeout)!
	mut state := provider.state
	state.mutex.lock()
	state.stats.closing = true
	own_dispatcher := state.dispatcher_thread == u64(sync.thread_id())
	state.mutex.unlock()
	notify_trace(state.wake)
	// A callback can request closure, but its dispatcher must finish the callback
	// before it can drain or destroy the exporter.
	if own_dispatcher { return }
	deadline := time.sys_mono_now() + i64(timeout)
	for {
		state.mutex.rlock()
		done := state.stats.closed
		failed := state.stats.shutdown_failed
		state.mutex.runlock()
		if done {
			if failed {
				return error('trace exporter shutdown failed; inspect provider statistics')
			}
			return
		}
		if time.sys_mono_now() >= deadline {
			state.mutex.rlock()
			active := state.active
			state.mutex.runlock()
			if context := active { context.close() }
			return error('trace shutdown deadline exceeded; dispatcher is still draining')
		}
		select {
			_ := <-state.progress {}
			5 * time.millisecond {}
		}
	}
}
