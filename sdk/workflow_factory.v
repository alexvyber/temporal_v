module sdk

struct WorkflowFactoryOutcome {
mut:
	workflow ?Workflow
	failure  ?IError
}

fn invoke_workflow_factory(factory WorkflowFactory, view WorkflowContextView, inputs []PayloadValue) !Workflow {
	mut outcome := WorkflowFactoryOutcome{}
	mut owned_inputs := []PayloadValue{cap: inputs.len}
	for value in inputs {
		owned_inputs << clone_payload_value(value)
	}
	recover_workflow_factory(factory, view, owned_inputs, mut outcome)
	if failure := outcome.failure { return failure }
	return outcome.workflow or { return error('workflow factory returned no workflow') }
}

fn recover_workflow_factory(factory WorkflowFactory, view WorkflowContextView, inputs []PayloadValue, mut outcome WorkflowFactoryOutcome) {
	defer {
		if message := recover() {
			outcome.failure = PanicError{
				message:     message
				stack_trace: recovered_panic_stack_trace()
			}
		}
	}
	outcome.workflow = factory(view, inputs) or {
		outcome.failure = err
		return
	}
}
