use prost::Message;
use std::{env, fs};
use temporalio_common::protos::temporal::api::{common::v1::{WorkflowExecution, Payload, Link}, taskqueue::v1::TaskQueueReachability, failure::v1::Failure};
fn main() {
    let input = fs::read_to_string(env::args().nth(1).unwrap()).unwrap();
    for line in input.lines() {
        let (kind, hex) = line.split_once('|').unwrap();
        let data: Vec<u8> = hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(),16).unwrap()).collect();
        let accepted = match kind {
            "workflow" => WorkflowExecution::decode(data.as_slice()).is_ok(),
            "payload" => Payload::decode(data.as_slice()).is_ok(),
            "reach" => TaskQueueReachability::decode(data.as_slice()).is_ok(),
            "link" => Link::decode(data.as_slice()).is_ok(),
            "failure" => Failure::decode(data.as_slice()).is_ok(),
            "varint" => { let mut bytes=data.as_slice(); prost::encoding::decode_varint(&mut bytes).is_ok() && bytes.is_empty() },
            _ => panic!("unknown oracle type"),
        };
        println!("{}", u8::from(accepted));
    }
}
