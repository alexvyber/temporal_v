module main

import temporal_v.temporal
import temporal_v.protobuf
import os
import encoding.hex

fn wire_oracle(path string) ! {
	for line in os.read_lines(path)! {
		parts := line.split('|')
		data := hex.decode(parts[1])!
		accepted := match parts[0] {
			'workflow' {
				if _ := temporal.TemporalApiCommonV1_WorkflowExecution.decode(data) {
					true
				} else {
					false
				}
			}
			'payload' {
				if _ := temporal.TemporalApiCommonV1_Payload.decode(data) { true } else { false }
			}
			'reach' {
				if _ := temporal.TemporalApiTaskqueueV1_TaskQueueReachability.decode(data) {
					true
				} else {
					false
				}
			}
			'link' {
				if _ := temporal.TemporalApiCommonV1_Link.decode(data) { true } else { false }
			}
			'failure' {
				if _ := temporal.TemporalApiFailureV1_Failure.decode(data) { true } else { false }
			}
			'varint' {
				mut decoder := protobuf.Decoder{
					buf: data
				}
				if _ := decoder.read_varint() {
					!decoder.more()
				} else {
					false
				}
			}
			else {
				return error('unknown oracle kind')
			}
		}

		println(if accepted { '1' } else { '0' })
	}
}
