module sdk

import alexvyber.temporal_v.temporal

// Codec callbacks may mutate nested fields of their local payload structs.
// Deep snapshots preserve caller inputs and isolate retained callback buffers,
// including external payload details and forward-compatible unknown fields.
fn owned_payload_messages(values []temporal.TemporalApiCommonV1_Payload) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := []temporal.TemporalApiCommonV1_Payload{cap: values.len}
	for value in values {
		result << temporal.TemporalApiCommonV1_Payload.decode(value.encode())!
	}
	return result
}

fn (converter DataConverter) encode_codec_payloads(values []temporal.TemporalApiCommonV1_Payload) ![]temporal.TemporalApiCommonV1_Payload {
	mut result := owned_payload_messages(values)!
	for codec in converter.codecs {
		result = owned_payload_messages(codec.encode(result, converter.serialization_context)!)!
	}
	return result
}

// Explicit types avoid V 0.5.2 sharing locals across optional generic instances.
fn owned_proto_coresdkactivitytask_activitycancellationdetails(value ?temporal.CoresdkActivityTask_ActivityCancellationDetails) ?temporal.CoresdkActivityTask_ActivityCancellationDetails {
	if message := value {
		return temporal.CoresdkActivityTask_ActivityCancellationDetails.decode(message.encode()) or {
			panic('validated protobuf metadata snapshot failed: ' + err.msg())
		}
	}
	return none
}

// Explicit types avoid V 0.5.2 sharing locals across optional generic instances.
fn owned_proto_coresdkcommon_namespacedworkflowexecution(value ?temporal.CoresdkCommon_NamespacedWorkflowExecution) ?temporal.CoresdkCommon_NamespacedWorkflowExecution {
	if message := value {
		return temporal.CoresdkCommon_NamespacedWorkflowExecution.decode(message.encode()) or {
			panic('validated protobuf metadata snapshot failed: ' + err.msg())
		}
	}
	return none
}

// Explicit types avoid V 0.5.2 sharing locals across optional generic instances.
fn owned_proto_coresdkcommon_workerdeploymentversion(value ?temporal.CoresdkCommon_WorkerDeploymentVersion) ?temporal.CoresdkCommon_WorkerDeploymentVersion {
	if message := value {
		return temporal.CoresdkCommon_WorkerDeploymentVersion.decode(message.encode()) or {
			panic('validated protobuf metadata snapshot failed: ' + err.msg())
		}
	}
	return none
}

// Explicit types avoid V 0.5.2 sharing locals across optional generic instances.
fn owned_proto_googleprotobuf_duration(value ?temporal.GoogleProtobuf_Duration) ?temporal.GoogleProtobuf_Duration {
	if message := value {
		return temporal.GoogleProtobuf_Duration.decode(message.encode()) or {
			panic('validated protobuf metadata snapshot failed: ' + err.msg())
		}
	}
	return none
}

// Explicit types avoid V 0.5.2 sharing locals across optional generic instances.
fn owned_proto_googleprotobuf_timestamp(value ?temporal.GoogleProtobuf_Timestamp) ?temporal.GoogleProtobuf_Timestamp {
	if message := value {
		return temporal.GoogleProtobuf_Timestamp.decode(message.encode()) or {
			panic('validated protobuf metadata snapshot failed: ' + err.msg())
		}
	}
	return none
}

// Explicit types avoid V 0.5.2 sharing locals across optional generic instances.
fn owned_proto_temporalapicommonv1_priority(value ?temporal.TemporalApiCommonV1_Priority) ?temporal.TemporalApiCommonV1_Priority {
	if message := value {
		return temporal.TemporalApiCommonV1_Priority.decode(message.encode()) or {
			panic('validated protobuf metadata snapshot failed: ' + err.msg())
		}
	}
	return none
}

// Explicit types avoid V 0.5.2 sharing locals across optional generic instances.
fn owned_proto_temporalapicommonv1_retrypolicy(value ?temporal.TemporalApiCommonV1_RetryPolicy) ?temporal.TemporalApiCommonV1_RetryPolicy {
	if message := value {
		return temporal.TemporalApiCommonV1_RetryPolicy.decode(message.encode()) or {
			panic('validated protobuf metadata snapshot failed: ' + err.msg())
		}
	}
	return none
}

// Explicit types avoid V 0.5.2 sharing locals across optional generic instances.
fn owned_proto_temporalapicommonv1_workflowexecution(value ?temporal.TemporalApiCommonV1_WorkflowExecution) ?temporal.TemporalApiCommonV1_WorkflowExecution {
	if message := value {
		return temporal.TemporalApiCommonV1_WorkflowExecution.decode(message.encode()) or {
			panic('validated protobuf metadata snapshot failed: ' + err.msg())
		}
	}
	return none
}
