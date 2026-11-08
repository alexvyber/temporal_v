module sdk

// force_task_fail discards this activation's commands and retries its workflow
// task. The first request wins even if the callback later returns another error,
// panics, or queues workflow completion. This does not fail the execution.
pub fn (mut context WorkflowContext) force_task_fail(cause IError) {
	if context.command_guard.reason != '' {
		panic('workflow task failure is forbidden in ' + context.command_guard.reason)
	}
	if _ := context.forced_task_error { return }
	context.forced_task_error = WorkflowTaskError{
		message: cause.msg()
	}
}
