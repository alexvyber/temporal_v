module sdk

pub struct CancelledError {
pub:
	scope   u32
	details Arguments = Arguments([]string{})
}

pub fn (err CancelledError) msg() string {
	return 'workflow cancellation scope ${err.scope} is cancelled'
}

pub fn (err CancelledError) code() int {
	return 499
}

struct ScopeState {
	parent   u32
	shielded bool
mut:
	cancelled bool
	reason    string
}

@[params]
pub struct ScopeOptions {
pub:
	parent   u32
	shielded bool
}

pub fn (mut context WorkflowContext) cancellation_scope(options ScopeOptions) !u32 {
	context.ensure_open()!
	if options.parent != 0 && options.parent !in context.scopes {
		return error('unknown parent cancellation scope')
	}
	context.next_scope++
	id := context.next_scope
	context.scopes[id] = ScopeState{
		parent:    options.parent
		shielded:  options.shielded
		cancelled: !options.shielded && context.scopes[options.parent].cancelled
		reason:    if !options.shielded { context.scopes[options.parent].reason } else { '' }
	}
	return id
}

fn (context &WorkflowContext) ensure_scope(id u32) ! {
	if id != 0 && id !in context.scopes { return error('unknown cancellation scope') }
	if context.scopes[id].cancelled {
		return CancelledError{
			scope: id
		}
	}
}

pub fn (context &WorkflowContext) scope_cancelled(id u32) !bool {
	if id != 0 && id !in context.scopes { return error('unknown cancellation scope') }
	return context.scopes[id].cancelled
}

fn (context &WorkflowContext) scope_descends_from(candidate u32, target u32) bool {
	mut current := candidate
	for {
		if current == target { return true }
		if current == 0 { return false }
		state := context.scopes[current]
		if state.shielded { return false }
		current = state.parent
	}
	return false
}

fn scope_sequences(scopes map[u32]u32, affected map[u32]bool) []u32 {
	mut result := []u32{}
	for sequence, scope in scopes {
		if affected[scope] { result << sequence }
	}
	result.sort()
	return result
}

@[params]
pub struct ScopeCancellationOptions {
pub:
	reason string
}

pub fn (context &WorkflowContext) scope_cancel_reason(id u32) !string {
	if id != 0 && id !in context.scopes { return error('unknown cancellation scope') }
	return context.scopes[id].reason
}

pub fn (mut context WorkflowContext) cancel_scope(id u32, options ScopeCancellationOptions) !bool {
	context.ensure_open()!
	if id != 0 && id !in context.scopes { return error('unknown cancellation scope') }
	if context.scopes[id].cancelled { return false }
	mut affected := map[u32]bool{}
	affected[id] = true
	for candidate, _ in context.scopes {
		if context.scope_descends_from(candidate, id) { affected[candidate] = true }
	}
	for scope, _ in affected {
		state := context.scopes[scope]
		if !state.cancelled {
			context.scopes[scope] = ScopeState{
				...state
				cancelled: true
				reason:    options.reason
			}
		}
	}
	for sequence in scope_sequences(context.nexus_scopes, affected) {
		context.cancel_nexus(sequence)!
	}
	for sequence in scope_sequences(context.external_scopes, affected) {
		context.cancel_signal(sequence)!
	}
	for sequence in scope_sequences(context.child_scopes, affected) {
		context.cancel_child(sequence, 'cancellation scope cancelled')!
	}
	for sequence in scope_sequences(context.activity_scopes, affected) {
		context.cancel_activity(sequence)!
	}
	for sequence in scope_sequences(context.timer_scopes, affected) {
		context.cancel_timer(sequence)!
	}
	return true
}

// Release deterministic scope state once no operations/subscopes depend on it.
pub fn (mut context WorkflowContext) release_scope(id u32) ! {
	context.ensure_open()!
	if id == 0 || id !in context.scopes { return error('cannot release root/unknown scope') }
	for _, state in context.scopes {
		if state.parent == id { return error('scope still has child scopes') }
	}
	for scopes in [context.activity_scopes, context.timer_scopes, context.child_scopes, context.nexus_scopes,
		context.external_scopes] {
		for _, scope in scopes {
			if scope == id { return error('scope still has pending operations') }
		}
	}
	for _, condition in context.conditions {
		if condition.options.scope == id { return error('scope still has pending conditions') }
	}
	context.scopes.delete(id)
}

fn (mut context WorkflowContext) request_cancel(reason string) !bool {
	if context.cancellation_requested { return false }
	context.cancellation_requested = true
	context.cancel_scope(0, reason: reason)!
	return true
}
