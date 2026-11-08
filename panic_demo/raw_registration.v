module main

import alexvyber.temporal_v.sdk
import os

struct ValidRawRegistration {}

struct DuplicateRawQueryRegistration {}

struct EmptyRawSignalRegistration {}

struct AmbiguousRawQueryRegistration {}

fn finish_raw_registration(event sdk.Event, mut context sdk.WorkflowContext) ! {
	if event.kind == .started { context.complete(sdk.PayloadValue('"validated"'))! }
}

fn (mut workflow ValidRawRegistration) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	finish_raw_registration(event, mut context)!
}

fn (mut workflow DuplicateRawQueryRegistration) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	finish_raw_registration(event, mut context)!
}

fn (mut workflow EmptyRawSignalRegistration) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	finish_raw_registration(event, mut context)!
}

fn (mut workflow AmbiguousRawQueryRegistration) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
	finish_raw_registration(event, mut context)!
}

fn (workflow DuplicateRawQueryRegistration) query_names() []string {
	return ['fixed', 'fixed']
}

fn (workflow DuplicateRawQueryRegistration) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue('null')
}

fn (workflow EmptyRawSignalRegistration) signal_names() []string {
	return ['']
}

fn (workflow AmbiguousRawQueryRegistration) query(name string, input []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue('null')
}

fn (workflow AmbiguousRawQueryRegistration) query_dynamic(name string, input []sdk.RawValue, view sdk.WorkflowContextView) !sdk.PayloadValue {
	return sdk.PayloadValue('null')
}

fn duplicate_raw_query_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	if os.getenv('V_RAW_DECL_FIXED') == 'true' { return ValidRawRegistration{} }
	return DuplicateRawQueryRegistration{}
}

fn empty_raw_signal_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	if os.getenv('V_RAW_DECL_FIXED') == 'true' { return ValidRawRegistration{} }
	return EmptyRawSignalRegistration{}
}

fn ambiguous_raw_query_factory(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow {
	if os.getenv('V_RAW_DECL_FIXED') == 'true' { return ValidRawRegistration{} }
	return AmbiguousRawQueryRegistration{}
}
