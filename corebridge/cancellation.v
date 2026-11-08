module corebridge

import sync

@[typedef]
struct C.TemporalCoreCancellationToken {}

fn C.temporal_core_cancellation_token_new() &C.TemporalCoreCancellationToken
fn C.temporal_core_cancellation_token_cancel(&C.TemporalCoreCancellationToken)
fn C.temporal_core_cancellation_token_free(&C.TemporalCoreCancellationToken)

struct RpcCancellationState {
	token &C.TemporalCoreCancellationToken
mut:
	mutex  &sync.RwMutex = sync.new_rwmutex()
	closed bool
}

@[heap]
pub struct RpcCancellation {
	state &RpcCancellationState
}

pub fn new_rpc_cancellation() !&RpcCancellation {
	token := C.temporal_core_cancellation_token_new()
	if isnil(token) { return error('cannot allocate RPC cancellation token') }
	return &RpcCancellation{
		state: &RpcCancellationState{
			token: token
		}
	}
}

pub fn (cancellation &RpcCancellation) cancel() {
	mut state := cancellation.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if !state.closed { C.temporal_core_cancellation_token_cancel(state.token) }
}

pub fn (cancellation &RpcCancellation) close() {
	// Cancellation releases in-flight users before the exclusive lifetime lock.
	cancellation.cancel()
	mut state := cancellation.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.closed { return }
	state.closed = true
	C.temporal_core_cancellation_token_free(state.token)
}
