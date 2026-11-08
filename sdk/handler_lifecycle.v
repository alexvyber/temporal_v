module sdk

pub enum HandlerUnfinishedPolicy {
	warn_and_abandon
	abandon
}

pub struct UpdateHandlerOptions {
pub:
	unfinished_policy HandlerUnfinishedPolicy
}

// This optional interface configures each registered Update handler.
pub interface ConfiguredUpdatesWorkflow {
	update_handler_options(name string) UpdateHandlerOptions
}

struct PendingWorkflowUpdate {
	result_context SerializationContext
	name           string
	id             string
	policy         HandlerUnfinishedPolicy
}

pub struct PendingUpdateInfo {
pub:
	id                   string
	name                 string
	protocol_instance_id string
	unfinished_policy    HandlerUnfinishedPolicy
}

fn update_unfinished_policy(workflow Workflow, name string) HandlerUnfinishedPolicy {
	if workflow is ConfiguredUpdatesWorkflow {
		return workflow.update_handler_options(name).unfinished_policy
	}
	return .warn_and_abandon
}

// V signal callbacks return synchronously. Accepted Updates remain unfinished
// until complete_update/fail_update is called, even after their event returns.
pub fn (context &WorkflowContext) all_handlers_finished() bool {
	return context.updates.len == 0
}

pub fn (context &WorkflowContext) pending_updates() []PendingUpdateInfo {
	mut ids := context.updates.keys()
	ids.sort()
	mut result := []PendingUpdateInfo{}
	for protocol_id in ids {
		pending := context.updates[protocol_id]
		result << PendingUpdateInfo{
			id:                   pending.id
			name:                 pending.name
			protocol_instance_id: protocol_id
			unfinished_policy:    pending.policy
		}
	}
	return result
}

fn (mut context WorkflowContext) abandon_pending_updates(warn bool) {
	for pending in context.pending_updates() {
		if warn && pending.unfinished_policy == .warn_and_abandon {
			context.log(.warn, 'Workflow closed with an unfinished Update handler', {
				'update_id':   pending.id
				'update_name': pending.name
			})
		}
	}
	context.updates.clear()
}
