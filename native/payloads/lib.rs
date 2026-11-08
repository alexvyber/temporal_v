use futures_util::{FutureExt, future::BoxFuture};
use prost::Message;
use std::{
    any::type_name,
    collections::HashMap,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr, slice, str,
};
use temporalio_common::{
    data_converters::PayloadConversionError,
    payload_visitor::{AsyncPayloadVisitor, PayloadField, PayloadFieldData, PayloadVisitable},
    protos::{
        coresdk::{
            ActivityHeartbeat, ActivityTaskCompletion,
            activity_task::ActivityTask,
            nexus::{NexusTask, NexusTaskCompletion},
            workflow_activation::WorkflowActivation,
            workflow_commands::WorkflowCommand,
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::{Payload, Payloads},
            failure::v1::Failure,
        },
    },
};

const TEMPLATE: &str = "temporal-v/payload-template";
const SLOT: &str = "temporal-v/payload-slot";

#[repr(C)]
pub struct PayloadBytes {
    pub data: *mut u8,
    pub size: usize,
    capacity: usize,
}

impl PayloadBytes {
    fn own(value: Vec<u8>) -> *mut Self {
        let mut value = std::mem::ManuallyDrop::new(value);
        Box::into_raw(Box::new(Self {
            data: value.as_mut_ptr(),
            size: value.len(),
            capacity: value.capacity(),
        }))
    }
}

#[repr(C)]
pub struct PayloadVisitResult {
    pub body: *mut PayloadBytes,
    pub failure: *mut PayloadBytes,
}

fn conversion_error(message: &str) -> PayloadConversionError {
    PayloadConversionError::EncodingError(Box::new(io::Error::new(
        io::ErrorKind::InvalidData,
        message,
    )))
}

struct Extract {
    payloads: Vec<Payload>,
    paths: Vec<&'static str>,
}

impl Extract {
    fn payload(&mut self, payload: &mut Payload, path: &'static str) {
        let index = self.payloads.len() as u64;
        self.payloads.push(std::mem::take(payload));
        self.paths.push(path);
        *payload = Payload {
            metadata: HashMap::from([(SLOT.into(), index.to_le_bytes().to_vec())]),
            ..Default::default()
        };
    }
}

impl AsyncPayloadVisitor for Extract {
    fn visit<'a>(
        &'a mut self,
        field: PayloadField<'a>,
    ) -> BoxFuture<'a, Result<(), PayloadConversionError>> {
        Box::pin(async move {
            // Search attributes must remain readable by Temporal's indexer.
            if field.path.contains("SearchAttributes.indexed_fields") {
                return Ok(());
            }
            match field.data {
                PayloadFieldData::Single(payload) => self.payload(payload, field.path),
                PayloadFieldData::Repeated(payloads) => {
                    for payload in payloads {
                        self.payload(payload, field.path);
                    }
                }
                PayloadFieldData::Payloads(payloads) => {
                    for payload in &mut payloads.payloads {
                        self.payload(payload, field.path);
                    }
                }
            }
            Ok(())
        })
    }
}

struct Replace {
    payloads: Vec<Payload>,
    seen: Vec<bool>,
}

impl Replace {
    fn payload(&mut self, payload: &mut Payload) -> Result<(), PayloadConversionError> {
        let marker = payload
            .metadata
            .get(SLOT)
            .ok_or_else(|| conversion_error("payload template is missing a slot marker"))?;
        let encoded: [u8; 8] = marker
            .as_slice()
            .try_into()
            .map_err(|_| conversion_error("invalid payload slot marker"))?;
        let index = usize::try_from(u64::from_le_bytes(encoded))
            .map_err(|_| conversion_error("payload slot index overflow"))?;
        if index >= self.payloads.len() || self.seen[index] {
            return Err(conversion_error("payload slot is out of range or repeated"));
        }
        self.seen[index] = true;
        *payload = std::mem::take(&mut self.payloads[index]);
        Ok(())
    }
}

impl AsyncPayloadVisitor for Replace {
    fn visit<'a>(
        &'a mut self,
        field: PayloadField<'a>,
    ) -> BoxFuture<'a, Result<(), PayloadConversionError>> {
        Box::pin(async move {
            if field.path.contains("SearchAttributes.indexed_fields") {
                return Ok(());
            }
            match field.data {
                PayloadFieldData::Single(payload) => self.payload(payload)?,
                PayloadFieldData::Repeated(payloads) => {
                    for payload in payloads {
                        self.payload(payload)?;
                    }
                }
                PayloadFieldData::Payloads(payloads) => {
                    for payload in &mut payloads.payloads {
                        self.payload(payload)?;
                    }
                }
            }
            Ok(())
        })
    }
}

fn visit_message<M: Message + Default + PayloadVisitable>(
    input: &[u8],
    replacements: Option<Vec<Payload>>,
) -> Result<Vec<u8>, String> {
    match replacements {
        None => {
            let mut message = M::decode(input).map_err(|e| e.to_string())?;
            let mut visitor = Extract {
                payloads: Vec::new(),
                paths: Vec::new(),
            };
            message
                .visit_payloads_mut(&mut visitor)
                .now_or_never()
                .ok_or("synchronous payload visitor unexpectedly yielded")?
                .map_err(|e| e.to_string())?;
            let mut metadata =
                HashMap::from([(TEMPLATE.into(), type_name::<M>().as_bytes().to_vec())]);
            for (index, path) in visitor.paths.into_iter().enumerate() {
                metadata.insert(format!("field/{index}"), path.as_bytes().to_vec());
            }
            let mut payloads = vec![Payload {
                metadata,
                data: message.encode_to_vec(),
                ..Default::default()
            }];
            payloads.extend(visitor.payloads);
            Ok(Payloads { payloads }.encode_to_vec())
        }
        Some(payloads) => {
            let mut message = M::decode(input).map_err(|e| e.to_string())?;
            let mut visitor = Replace {
                seen: vec![false; payloads.len()],
                payloads,
            };
            message
                .visit_payloads_mut(&mut visitor)
                .now_or_never()
                .ok_or("synchronous payload visitor unexpectedly yielded")?
                .map_err(|e| e.to_string())?;
            if visitor.seen.iter().any(|seen| !seen) {
                return Err("unused replacement payloads".into());
            }
            Ok(message.encode_to_vec())
        }
    }
}

fn visit_opaque(input: &[u8], replacements: Option<Vec<Payload>>) -> Result<Vec<u8>, String> {
    match replacements {
        None => Ok(Payloads {
            payloads: vec![Payload {
                metadata: HashMap::from([(TEMPLATE.into(), b"opaque".to_vec())]),
                data: input.to_vec(),
                ..Default::default()
            }],
        }
        .encode_to_vec()),
        Some(payloads) if payloads.is_empty() => Ok(input.to_vec()),
        Some(_) => Err("a payload-free RPC cannot accept replacement payloads".into()),
    }
}

fn process(
    kind: u32,
    service: u32,
    method: &str,
    input: &[u8],
    replacements: Option<Vec<Payload>>,
) -> Result<Vec<u8>, String> {
    match kind {
        1 => visit_message::<WorkflowCommand>(input, replacements),
        2 => visit_message::<WorkflowActivation>(input, replacements),
        3 => visit_message::<ActivityTask>(input, replacements),
        4 => visit_message::<ActivityTaskCompletion>(input, replacements),
        5 => visit_message::<ActivityHeartbeat>(input, replacements),
        6 => visit_message::<NexusTask>(input, replacements),
        7 => visit_message::<NexusTaskCompletion>(input, replacements),
        8 => visit_message::<Payloads>(input, replacements),
        9 => visit_message::<Failure>(input, replacements),
        10 => visit_message::<WorkflowActivationCompletion>(input, replacements),
        100 | 101 => rpc_message(service, method, kind == 101, input, replacements),
        _ => Err("unknown payload message kind".into()),
    }
}

include!("messages.rs");

unsafe fn bytes<'a>(data: *const u8, size: usize) -> Result<&'a [u8], String> {
    if size == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err("null nonempty payload buffer".into());
    }
    Ok(unsafe { slice::from_raw_parts(data, size) })
}

fn result(value: Result<Vec<u8>, String>) -> PayloadVisitResult {
    match value {
        Ok(value) => PayloadVisitResult {
            body: PayloadBytes::own(value),
            failure: ptr::null_mut(),
        },
        Err(error) => PayloadVisitResult {
            body: ptr::null_mut(),
            failure: PayloadBytes::own(error.into_bytes()),
        },
    }
}

fn dispatch(
    kind: u32,
    service: u32,
    method: &[u8],
    input: &[u8],
    restore: bool,
) -> Result<Vec<u8>, String> {
    let method = str::from_utf8(method).map_err(|_| "invalid RPC method UTF-8")?;
    if !restore {
        return process(kind, service, method, input, None);
    }
    let mut envelope = Payloads::decode(input).map_err(|e| e.to_string())?;
    if envelope.payloads.is_empty() {
        return Err("empty payload envelope".into());
    }
    let template = envelope.payloads.remove(0);
    let expected = process(kind, service, method, &template.data, None)?;
    let expected = Payloads::decode(expected.as_slice()).map_err(|e| e.to_string())?;
    if template.metadata.get(TEMPLATE) != expected.payloads[0].metadata.get(TEMPLATE) {
        return Err("payload envelope has the wrong message shape".into());
    }
    process(
        kind,
        service,
        method,
        &template.data,
        Some(envelope.payloads),
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn temporal_v_payload_visit(
    kind: u32,
    service: u32,
    method: *const u8,
    method_size: usize,
    input: *const u8,
    input_size: usize,
    restore: bool,
) -> PayloadVisitResult {
    let value = catch_unwind(AssertUnwindSafe(|| {
        dispatch(
            kind,
            service,
            unsafe { bytes(method, method_size)? },
            unsafe { bytes(input, input_size)? },
            restore,
        )
    }))
    .unwrap_or_else(|_| Err("payload visitor panicked".into()));
    result(value)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn temporal_v_payload_bytes_free(value: *mut PayloadBytes) {
    if value.is_null() {
        return;
    }
    let value = unsafe { Box::from_raw(value) };
    drop(unsafe { Vec::from_raw_parts(value.data, value.size, value.capacity) });
}

#[cfg(test)]
#[path = "../../_tests/native/payloads.rs"]
mod tests;
