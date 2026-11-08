module corebridge

#flag @DIR/../native/slot_bridge.c
#include "@DIR/../native/slot_bridge.h"

@[typedef]
struct C.SpikeSlotBridge {}

@[typedef]
struct C.SpikeSlotEvent {
	kind            int
	slot_kind       int
	permit          uint
	has_info        bool
	is_sticky       bool
	task_queue      C.TemporalCoreByteArrayRef
	worker_identity C.TemporalCoreByteArrayRef
	worker_build_id C.TemporalCoreByteArrayRef
	workflow_type   C.TemporalCoreByteArrayRef
	activity_type   C.TemporalCoreByteArrayRef
	service         C.TemporalCoreByteArrayRef
	operation       C.TemporalCoreByteArrayRef
}

fn C.spike_slot_bridge_new() &C.SpikeSlotBridge
fn C.spike_slot_bridge_poll(&C.SpikeSlotBridge, u32) &C.SpikeSlotEvent
fn C.spike_slot_bridge_is_canceled(&C.SpikeSlotBridge, &C.SpikeSlotEvent) bool
fn C.spike_slot_bridge_respond(&C.SpikeSlotBridge, &C.SpikeSlotEvent, uint, bool)
fn C.spike_slot_bridge_stop(&C.SpikeSlotBridge)
fn C.spike_slot_bridge_free(&C.SpikeSlotBridge)

@[heap]
pub struct SlotBridge {
	pointer &C.SpikeSlotBridge
}

pub struct SlotEvent {
	native &C.SpikeSlotEvent
pub:
	kind            int
	slot_kind       int
	permit          u64
	has_info        bool
	is_sticky       bool
	task_queue      string
	worker_identity string
	worker_build_id string
	workflow_type   string
	activity_type   string
	service         string
	operation       string
}

fn slot_text(value C.TemporalCoreByteArrayRef) string {
	if value.size == 0 { return '' }
	return unsafe { value.data.vstring_with_len(int(value.size)).clone() }
}

pub fn new_slot_bridge() &SlotBridge {
	return &SlotBridge{
		pointer: C.spike_slot_bridge_new()
	}
}

pub fn (bridge &SlotBridge) poll(timeout_millis u32) ?SlotEvent {
	event := C.spike_slot_bridge_poll(bridge.pointer, timeout_millis)
	if isnil(event) { return none }
	return SlotEvent{
		native:          event
		kind:            event.kind
		slot_kind:       event.slot_kind
		permit:          u64(event.permit)
		has_info:        event.has_info
		is_sticky:       event.is_sticky
		task_queue:      slot_text(event.task_queue)
		worker_identity: slot_text(event.worker_identity)
		worker_build_id: slot_text(event.worker_build_id)
		workflow_type:   slot_text(event.workflow_type)
		activity_type:   slot_text(event.activity_type)
		service:         slot_text(event.service)
		operation:       slot_text(event.operation)
	}
}

pub fn (bridge &SlotBridge) respond(event SlotEvent, value u64, known bool) {
	C.spike_slot_bridge_respond(bridge.pointer, event.native, uint(value), known)
}

pub fn (bridge &SlotBridge) stop() {
	C.spike_slot_bridge_stop(bridge.pointer)
}

// Core must be closed and the dispatcher joined before this is called.
pub fn (bridge &SlotBridge) free() {
	C.spike_slot_bridge_free(bridge.pointer)
}

pub fn (bridge &SlotBridge) is_canceled(event SlotEvent) bool {
	return C.spike_slot_bridge_is_canceled(bridge.pointer, event.native)
}
