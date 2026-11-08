use super::*;
use temporalio_common::protos::coresdk::{
    workflow_activation::{NotifyWorkflowMarker, WorkflowActivationJob, workflow_activation_job},
    workflow_commands::{RecordWorkflowMarker, workflow_command},
};

use temporalio_common::protos::temporal::api::{
    common::v1::{Header, Memo, SearchAttributes},
    workflowservice::v1::StartWorkflowExecutionRequest,
};

fn payload(text: &str) -> Payload {
    Payload {
        data: text.as_bytes().to_vec(),
        ..Default::default()
    }
}

fn request() -> StartWorkflowExecutionRequest {
    StartWorkflowExecutionRequest {
        input: Some(Payloads {
            payloads: vec![payload("input")],
        }),
        header: Some(Header {
            fields: (0..32)
                .map(|i| (format!("header-{i}"), payload(&format!("value-{i}"))))
                .collect(),
        }),
        memo: Some(Memo {
            fields: HashMap::from([("memo".into(), payload("memo-value"))]),
        }),
        search_attributes: Some(SearchAttributes {
            indexed_fields: HashMap::from([("indexed".into(), payload("search-value"))]),
        }),
        ..Default::default()
    }
}

#[test]
fn slots_preserve_map_associations_and_exclude_search_attributes() {
    let original = request();
    let extracted = dispatch(
        100,
        1,
        b"StartWorkflowExecution",
        &original.encode_to_vec(),
        false,
    )
    .unwrap();
    let mut envelope = Payloads::decode(extracted.as_slice()).unwrap();
    assert_eq!(envelope.payloads.len(), 35);
    for item in &mut envelope.payloads[1..] {
        item.data.extend_from_slice(b"-stored");
    }
    let bytes = envelope.encode_to_vec();
    // Each decode constructs maps with fresh random iteration order.
    for _ in 0..128 {
        let restored = dispatch(100, 1, b"StartWorkflowExecution", &bytes, true).unwrap();
        let result = StartWorkflowExecutionRequest::decode(restored.as_slice()).unwrap();
        assert_eq!(result.search_attributes, original.search_attributes);
        assert_eq!(result.input.unwrap().payloads[0].data, b"input-stored");
        for (name, item) in result.header.unwrap().fields {
            assert_eq!(
                item.data,
                format!("value-{}-stored", name.strip_prefix("header-").unwrap()).as_bytes()
            );
        }
        assert_eq!(
            result.memo.unwrap().fields["memo"].data,
            b"memo-value-stored"
        );
    }
}

#[test]
fn nested_failure_attributes_and_details_are_visited() {
    use temporalio_common::protos::temporal::api::failure::v1::{
        ApplicationFailureInfo, failure::FailureInfo,
    };
    let value = Failure {
        encoded_attributes: Some(payload("outer")),
        cause: Some(Box::new(Failure {
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    details: Some(Payloads {
                        payloads: vec![payload("details")],
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        })),
        ..Default::default()
    };
    let extracted = dispatch(9, 0, b"", &value.encode_to_vec(), false).unwrap();
    let envelope = Payloads::decode(extracted.as_slice()).unwrap();
    assert_eq!(envelope.payloads.len(), 3);
    let restored = dispatch(9, 0, b"", &extracted, true).unwrap();
    assert_eq!(Failure::decode(restored.as_slice()).unwrap(), value);
}

#[test]
fn corrupt_slot_layouts_and_message_shapes_are_rejected() {
    let extracted = dispatch(
        100,
        1,
        b"StartWorkflowExecution",
        &request().encode_to_vec(),
        false,
    )
    .unwrap();
    let original = Payloads::decode(extracted.as_slice()).unwrap();
    for marker in [vec![], 999u64.to_le_bytes().to_vec()] {
        let mut envelope = original.clone();
        let mut template =
            StartWorkflowExecutionRequest::decode(envelope.payloads[0].data.as_slice()).unwrap();
        template.input.as_mut().unwrap().payloads[0]
            .metadata
            .insert(SLOT.into(), marker);
        envelope.payloads[0].data = template.encode_to_vec();
        assert!(
            dispatch(
                100,
                1,
                b"StartWorkflowExecution",
                &envelope.encode_to_vec(),
                true
            )
            .is_err()
        );
    }
    let mut repeated = original.clone();
    let mut template =
        StartWorkflowExecutionRequest::decode(repeated.payloads[0].data.as_slice()).unwrap();
    let duplicated = template.input.as_ref().unwrap().payloads[0].clone();
    template
        .memo
        .as_mut()
        .unwrap()
        .fields
        .insert("memo".into(), duplicated);
    repeated.payloads[0].data = template.encode_to_vec();
    assert!(
        dispatch(
            100,
            1,
            b"StartWorkflowExecution",
            &repeated.encode_to_vec(),
            true
        )
        .is_err()
    );
    let mut extra = original.clone();
    extra.payloads.push(payload("unused"));
    assert!(
        dispatch(
            100,
            1,
            b"StartWorkflowExecution",
            &extra.encode_to_vec(),
            true
        )
        .is_err()
    );
    let mut wrong_shape = original;
    wrong_shape.payloads[0]
        .metadata
        .insert(TEMPLATE.into(), b"wrong".to_vec());
    assert!(
        dispatch(
            100,
            1,
            b"StartWorkflowExecution",
            &wrong_shape.encode_to_vec(),
            true
        )
        .is_err()
    );
    assert!(dispatch(100, 1, b"UnknownMethod", &[], false).is_err());
}

#[test]
fn ffi_rejects_null_nonempty_input_and_frees_both_result_kinds() {
    unsafe {
        let result = temporal_v_payload_visit(8, 0, ptr::null(), 0, ptr::null(), 1, false);
        assert!(!result.failure.is_null());
        assert!(result.body.is_null());
        temporal_v_payload_bytes_free(result.failure);
        let result = temporal_v_payload_visit(8, 0, ptr::null(), 0, ptr::null(), 0, false);
        assert!(!result.body.is_null());
        assert!(result.failure.is_null());
        temporal_v_payload_bytes_free(result.body);
        temporal_v_payload_bytes_free(ptr::null_mut());
    }
}

#[test]
fn workflow_marker_payload_groups_preserve_identity_and_value_associations() {
    let details = HashMap::from([
        ("side-effect-id".to_string(), Payloads { payloads: vec![payload("1")] }),
        ("data".to_string(), Payloads { payloads: vec![payload("first"), payload("second")] }),
    ]);
    let command = WorkflowCommand {
        variant: Some(workflow_command::Variant::RecordWorkflowMarker(RecordWorkflowMarker {
            marker_name: "SideEffect".to_string(), details: details.clone(),
        })),
        ..Default::default()
    };
    let activation = WorkflowActivation {
        run_id: "marker-run".to_string(),
        is_replaying: true,
        jobs: vec![WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::NotifyWorkflowMarker(NotifyWorkflowMarker {
                marker_name: "SideEffect".to_string(), details, event_id: 42,
            })),
        }],
        ..Default::default()
    };
    for (kind, input) in [(1, command.encode_to_vec()), (2, activation.encode_to_vec())] {
        for _ in 0..64 {
            let extracted = dispatch(kind, 0, b"", &input, false).unwrap();
            let mut envelope = Payloads::decode(extracted.as_slice()).unwrap();
            assert_eq!(envelope.payloads.len(), 4);
            for value in &mut envelope.payloads[1..] { value.data.extend_from_slice(b"-stored"); }
            let restored = dispatch(kind, 0, b"", &envelope.encode_to_vec(), true).unwrap();
            let fields = if kind == 1 {
                let value = WorkflowCommand::decode(restored.as_slice()).unwrap();
                let Some(workflow_command::Variant::RecordWorkflowMarker(marker)) = value.variant else { panic!("marker command lost its variant") };
                assert_eq!(marker.marker_name, "SideEffect");
                marker.details
            } else {
                let value = WorkflowActivation::decode(restored.as_slice()).unwrap();
                assert_eq!(value.run_id, "marker-run");
                assert!(value.is_replaying);
                let Some(workflow_activation_job::Variant::NotifyWorkflowMarker(marker)) = value.jobs.into_iter().next().unwrap().variant else { panic!("marker notification lost its variant") };
                assert_eq!(marker.event_id, 42);
                marker.details
            };
            assert_eq!(fields["side-effect-id"].payloads[0].data, b"1-stored");
            assert_eq!(fields["data"].payloads[0].data, b"first-stored");
            assert_eq!(fields["data"].payloads[1].data, b"second-stored");
        }
    }
}
