module sdk

import corebridge
import sync

struct WorkerControlState {
mut:
	mutex                  &sync.Mutex = sync.new_mutex()
	stopping               bool
	next_id                u64
	connections            map[u64]&corebridge.Connection
	interrupted_local_runs map[string]&corebridge.Connection
}

// Share a control between workers to stop them as a group without OS signals.
@[heap]
pub struct WorkerControl {
	state &WorkerControlState = &WorkerControlState{}
}

pub fn (control &WorkerControl) shutdown() {
	mut state := control.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	state.stopping = true
	for _, connection in state.connections {
		connection.shutdown()
	}
}

fn (control &WorkerControl) attach(connection &corebridge.Connection) u64 {
	mut state := control.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.stopping {
		connection.shutdown()
		return 0
	}
	state.next_id++
	state.connections[state.next_id] = connection
	return state.next_id
}

fn (control &WorkerControl) detach(id u64) {
	mut state := control.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if connection := state.connections[id] {
		for key, owner in state.interrupted_local_runs {
			if owner == connection { state.interrupted_local_runs.delete(key) }
		}
	}
	state.connections.delete(id)
}

fn (control &WorkerControl) mark_local_shutdown(connection &corebridge.Connection, namespace string, run_id string) {
	mut state := control.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	state.interrupted_local_runs[namespace + '/' + run_id] = connection
}

fn (control &WorkerControl) consume_local_shutdown(namespace string, run_id string) bool {
	mut state := control.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	key := namespace + '/' + run_id
	if key !in state.interrupted_local_runs { return false }
	state.interrupted_local_runs.delete(key)
	return true
}
