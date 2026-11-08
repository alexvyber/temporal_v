module sdk

import corebridge
import sync

pub enum SlotKind {
	workflow
	activity
	local_activity
	nexus
}

pub struct SlotReservationContext {
pub:
	kind            SlotKind
	task_queue      string
	worker_identity string
	worker_build_id string
	is_sticky       bool
}

pub struct SlotInfo {
pub:
	kind          SlotKind
	workflow_type string
	activity_type string
	service       string
	operation     string
	is_sticky     bool
}

pub struct SlotPermit {
pub:
	id u64
}

// Each worker/supplier has a V dispatcher. Methods must return promptly, and
// suppliers shared across dispatchers must protect their mutable state. Returning
// none retries asynchronous reservation until Core cancels it. IDs are nonzero
// and unique among this supplier's live permits; release balances every grant.
pub interface SlotSupplier {
	reserve(context SlotReservationContext, cancellation &SlotReservationCancellation) ?SlotPermit
	try_reserve(context SlotReservationContext) ?SlotPermit
	mark_used(permit SlotPermit, info SlotInfo)
	release(permit SlotPermit, info ?SlotInfo)
	available_slots() ?u64
}

pub struct CustomTuner {
pub:
	workflow       ?SlotSupplier
	activity       ?SlotSupplier
	local_activity ?SlotSupplier
	nexus          ?SlotSupplier
}

struct SlotDispatcher {
	bridge &corebridge.SlotBridge
	done   chan bool
}

@[heap]
struct SlotReservationState {
	bridge &corebridge.SlotBridge
	event  corebridge.SlotEvent
mut:
	mutex  &sync.Mutex = sync.new_mutex()
	closed bool
}

@[heap]
pub struct SlotReservationCancellation {
	state &SlotReservationState
}

pub fn (cancellation &SlotReservationCancellation) is_canceled() bool {
	mut state := cancellation.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.closed { return true }
	return state.bridge.is_canceled(state.event)
}

fn reserve_slot(supplier SlotSupplier, context SlotReservationContext, cancellation &SlotReservationCancellation, completed chan bool) {
	mut value := u64(0)
	if permit := supplier.reserve(context, cancellation) {
		value = permit.id
	}
	mut state := cancellation.state
	state.mutex.lock()
	state.closed = true
	state.bridge.respond(state.event, value, true)
	state.mutex.unlock()
	completed <- true
}

fn dispatch_slots(bridge &corebridge.SlotBridge, supplier SlotSupplier, kind SlotKind) {
	completed := chan bool{cap: 128}
	mut reservations := 0
	for {
		select {
			_ := <-completed {
				reservations--
			}
			else {}
		}
		event := bridge.poll(100) or { continue }
		if event.kind == 5 {
			for reservations > 0 {
				_ := <-completed
				reservations--
			}
			return
		}
		mut value := u64(0)
		mut known := true
		match event.kind {
			0, 1 {
				context := SlotReservationContext{
					kind:            kind
					task_queue:      event.task_queue
					worker_identity: event.worker_identity
					worker_build_id: event.worker_build_id
					is_sticky:       event.is_sticky
				}
				if event.kind == 0 {
					state := &SlotReservationState{
						bridge: bridge
						event:  event
					}
					reservations++
					spawn reserve_slot(supplier, context, &SlotReservationCancellation{
						state: state
					}, completed)
					continue
				}
				if permit := supplier.try_reserve(context) { value = permit.id }
			}
			2 {
				supplier.mark_used(SlotPermit{ id: event.permit }, SlotInfo{
					kind:          kind
					workflow_type: event.workflow_type
					activity_type: event.activity_type
					service:       event.service
					operation:     event.operation
					is_sticky:     event.is_sticky
				})
			}
			3 {
				mut information := ?SlotInfo(none)
				if event.has_info {
					information = SlotInfo{
						kind:          kind
						workflow_type: event.workflow_type
						activity_type: event.activity_type
						service:       event.service
						operation:     event.operation
						is_sticky:     event.is_sticky
					}
				}
				supplier.release(SlotPermit{ id: event.permit }, information)
			}
			4 {
				if available := supplier.available_slots() {
					value = available
				} else {
					known = false
				}
			}
			else {
				panic('invalid native slot event')
			}
		}

		bridge.respond(event, value, known)
	}
}

fn run_slot_dispatcher(bridge &corebridge.SlotBridge, supplier SlotSupplier, kind SlotKind, done chan bool) {
	dispatch_slots(bridge, supplier, kind)
	done <- true
}

fn start_slot_dispatcher(supplier SlotSupplier, kind SlotKind) SlotDispatcher {
	bridge := corebridge.new_slot_bridge()
	done := chan bool{cap: 1}
	spawn run_slot_dispatcher(bridge, supplier, kind, done)
	return SlotDispatcher{
		bridge: bridge
		done:   done
	}
}

struct WorkerConnection {
	connection  &corebridge.Connection
	dispatchers []SlotDispatcher
}

fn (owner WorkerConnection) close() {
	owner.connection.close()
	for dispatcher in owner.dispatchers {
		dispatcher.bridge.stop()
	}
	for dispatcher in owner.dispatchers {
		_ := <-dispatcher.done
		dispatcher.bridge.free()
	}
}

fn open_worker(options WorkerOptions, kind corebridge.WorkerKind) !WorkerConnection {
	if tracing := options.tracing { tracing.validate()! }
	base := options.core_config()!
	mut config := base
	mut dispatchers := []SlotDispatcher{}
	if tuner := options.custom_tuner {
		mut workflow := ?&corebridge.SlotBridge(none)
		mut activity := ?&corebridge.SlotBridge(none)
		mut local := ?&corebridge.SlotBridge(none)
		mut nexus := ?&corebridge.SlotBridge(none)
		if supplier := tuner.workflow {
			dispatcher := start_slot_dispatcher(supplier, .workflow)
			dispatchers << dispatcher
			workflow = dispatcher.bridge
		}
		if supplier := tuner.activity {
			dispatcher := start_slot_dispatcher(supplier, .activity)
			dispatchers << dispatcher
			activity = dispatcher.bridge
		}
		if supplier := tuner.local_activity {
			dispatcher := start_slot_dispatcher(supplier, .local_activity)
			dispatchers << dispatcher
			local = dispatcher.bridge
		}
		if supplier := tuner.nexus {
			dispatcher := start_slot_dispatcher(supplier, .nexus)
			dispatchers << dispatcher
			nexus = dispatcher.bridge
		}
		config = corebridge.Config{
			...base
			workflow_slot_bridge:       workflow
			activity_slot_bridge:       activity
			local_activity_slot_bridge: local
			nexus_slot_bridge:          nexus
		}
	}
	connection := corebridge.open(config, kind) or {
		for dispatcher in dispatchers {
			dispatcher.bridge.stop()
		}
		for dispatcher in dispatchers {
			_ := <-dispatcher.done
			dispatcher.bridge.free()
		}
		return err
	}
	return WorkerConnection{
		connection:  connection
		dispatchers: dispatchers
	}
}
