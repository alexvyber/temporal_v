module sdk

import temporal_v.temporal

pub enum SerializationKind {
	unspecified
	workflow
	activity
	nexus
}

// Namespace/workflow ID, Activity ID/locality, and Nexus endpoint/service/name
// identify the payload's target scope. Type/queue fields are informational and
// may be absent when an execution is addressed without its original handle.
pub struct SerializationContext {
pub:
	kind          SerializationKind
	namespace     string
	workflow_id   string
	workflow_type string
	activity_id   string
	activity_type string
	task_queue    string
	is_local      bool
	endpoint      string
	service       string
	operation     string
}

pub fn (converter DataConverter) with_context(context SerializationContext) DataConverter {
	return DataConverter{
		...converter
		serialization_context: context
	}
}

fn workflow_serialization_context(namespace string, workflow_id string) SerializationContext {
	return SerializationContext{
		kind:        .workflow
		namespace:   namespace
		workflow_id: workflow_id
	}
}

fn (client &Client) workflow_converter(workflow_id string) DataConverter {
	return client.options.data_converter.with_context(workflow_serialization_context(client.options.namespace,
		workflow_id))
}

fn (context &WorkflowContext) activity_converter(name string, id string, queue string, local bool) DataConverter {
	return context.converter.with_context(SerializationContext{
		kind:          .activity
		namespace:     context.namespace
		workflow_id:   context.workflow_id
		workflow_type: context.workflow_type
		activity_id:   id
		activity_type: name
		task_queue:    queue
		is_local:      local
	})
}

fn nexus_serialization_context(endpoint string, service string, operation string) SerializationContext {
	return SerializationContext{
		kind:      .nexus
		endpoint:  endpoint
		service:   service
		operation: operation
	}
}

fn (context SerializationContext) same_scope(other SerializationContext) bool {
	return context.kind == other.kind && context.namespace == other.namespace
		&& context.workflow_id == other.workflow_id && context.activity_id == other.activity_id
		&& context.is_local == other.is_local && context.endpoint == other.endpoint
		&& context.service == other.service && context.operation == other.operation
}

pub fn (context &ActivityContext) serialization_context() SerializationContext {
	return context.converter.serialization_context
}

pub fn (context &WorkflowContext) serialization_context() SerializationContext {
	return context.converter.serialization_context
}

fn (converter DataConverter) with_result_context(propagated ?temporal.TemporalApiNexusV1_PropagatedSerializationContext) DataConverter {
	if context := propagated {
		return converter.with_context(nexus_serialization_context(context.endpoint,
			context.service, context.operation))
	}
	return converter
}

fn (context &WorkflowContext) result_converter() DataConverter {
	return context.converter.with_result_context(context.initialization.propagated_nexus_serialization_context)
}

pub fn (context &WorkflowContext) result_serialization_context() SerializationContext {
	return context.result_converter().serialization_context
}
