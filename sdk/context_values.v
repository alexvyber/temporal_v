module sdk

// Values are runtime-only. Store deterministic workflow data, never process state.
// Namespaced keys distinguish values of the same type. Interface objects retain
// their V ownership semantics; snapshot isolation applies to key bindings.
pub interface WorkflowContextValue {}

pub struct WorkflowContextValues {
	entries map[string]WorkflowContextValue
}

pub type WorkflowContextCallback = fn (mut WorkflowContext) !

pub fn (values WorkflowContextValues) get[T](key string) ?T {
	value := values.entries[key] or { return none }
	if value is T { return value }
	return none
}

pub fn (context &WorkflowContext) context_value[T](key string) ?T {
	return context.context_values.get[T](key)
}

pub fn (context &WorkflowContext) capture_context_values() WorkflowContextValues {
	return WorkflowContextValues{
		entries: context.context_values.entries.clone()
	}
}

// Capture before suspension, then re-enter when the event-driven handler resumes.
pub fn (mut context WorkflowContext) with_context_values(values WorkflowContextValues, callback WorkflowContextCallback) ! {
	context.ensure_open()!
	previous := context.context_values
	context.context_values = WorkflowContextValues{
		entries: values.entries.clone()
	}
	defer { context.context_values = previous }
	callback(mut context)!
}

pub fn (mut context WorkflowContext) with_context_value(key string, value WorkflowContextValue, callback WorkflowContextCallback) ! {
	context.ensure_open()!
	if key == '' { return error('workflow context key must be nonempty') }
	mut entries := context.context_values.entries.clone()
	entries[key] = value
	context.with_context_values(WorkflowContextValues{ entries: entries }, callback)!
}

// Outbound interceptors receive the scope captured when their command was queued.
pub fn (info WorkflowInfo) context_value[T](key string) ?T {
	return info.context_values.get[T](key)
}

pub fn (next QueryNext) context_value[T](key string) ?T {
	return next.context_values.get[T](key)
}

pub fn (next UpdateValidatorNext) context_value[T](key string) ?T {
	return next.context_values.get[T](key)
}

pub fn (values WorkflowContextValues) with_value(key string, value WorkflowContextValue) !WorkflowContextValues {
	if key == '' { return error('workflow context key must be nonempty') }
	mut entries := values.entries.clone()
	entries[key] = value
	return WorkflowContextValues{
		entries: entries
	}
}

pub fn (next QueryNext) with_context_values(values WorkflowContextValues) QueryNext {
	return QueryNext{
		...next
		context_values: WorkflowContextValues{
			entries: values.entries.clone()
		}
	}
}

pub fn (next QueryNext) with_context_value(key string, value WorkflowContextValue) !QueryNext {
	return next.with_context_values(next.context_values.with_value(key, value)!)
}

pub fn (next UpdateValidatorNext) with_context_values(values WorkflowContextValues) UpdateValidatorNext {
	return UpdateValidatorNext{
		...next
		context_values: WorkflowContextValues{
			entries: values.entries.clone()
		}
	}
}

pub fn (next UpdateValidatorNext) with_context_value(key string, value WorkflowContextValue) !UpdateValidatorNext {
	return next.with_context_values(next.context_values.with_value(key, value)!)
}
