module corebridge

import sync

#flag @DIR/../native/core_shim.c
#include "@DIR/../native/core_shim.h"

@[typedef]
struct C.SpikeCore {}

@[typedef]
struct C.TemporalCoreByteArray {
	data &u8
	size u32
}

fn C.spike_protocol_capabilities() u64

fn C.spike_open(&C.SpikeConnectOptions, &C.SpikeWorkerOptions, bool, bool, bool) &C.SpikeCore
fn C.spike_open_shared(&C.SpikeCore, &C.SpikeWorkerOptions, bool, bool, bool) &C.SpikeCore
fn C.spike_client_open(&C.SpikeConnectOptions) &C.SpikeCore
fn C.spike_rpc(&C.SpikeCore, u32, &char, &u8, u32, u32, C.TemporalCoreByteArrayRefArray, C.TemporalCoreByteArrayRefArray, bool, &C.TemporalCoreCancellationToken, &&C.TemporalCoreByteArray) &C.TemporalCoreByteArray
fn C.spike_rpc_status() u32
fn C.spike_error(&C.SpikeCore) &char
fn C.spike_poll(&C.SpikeCore) &C.TemporalCoreByteArray
fn C.spike_poll_workflow(&C.SpikeCore) &C.TemporalCoreByteArray
fn C.spike_complete(&C.SpikeCore, &u8, u32) int
fn C.spike_complete_workflow(&C.SpikeCore, &u8, u32) int
fn C.spike_heartbeat(&C.SpikeCore, &u8, u32) int
fn C.spike_bytes_free(&C.SpikeCore, &C.TemporalCoreByteArray)
fn C.spike_poll_nexus(&C.SpikeCore) &C.TemporalCoreByteArray
fn C.spike_complete_nexus(&C.SpikeCore, &u8, u32) int
fn C.spike_shutdown(&C.SpikeCore)
fn C.spike_shutdown_grace_expired(&C.SpikeCore) bool
fn C.spike_replay_open(&char, &char) &C.SpikeCore
fn C.spike_replay_push(&C.SpikeCore, &char, &u8, u32) int
fn C.spike_close(&C.SpikeCore)

pub struct Config {
pub:
	plugins                                   []PluginInfo
	disable_payload_error_limit               bool
	storage_drivers                           []string
	connection                                ?&Connection
	workflow_slot_bridge                      ?&SlotBridge
	activity_slot_bridge                      ?&SlotBridge
	local_activity_slot_bridge                ?&SlotBridge
	nexus_slot_bridge                         ?&SlotBridge
	address                                   string = 'http://127.0.0.1:7233'
	namespace                                 string = 'default'
	task_queue                                string = 'v-sdk'
	activity_concurrency                      int    = 2
	transport                                 Transport
	telemetry                                 Telemetry
	workflow_concurrency                      int = 10
	local_activity_concurrency                int = 2
	nexus_concurrency                         int = 2
	max_cached_workflows                      u32 = 10
	max_eager_activity_reservations           u32 = 3
	workflow_pollers                          int = 2
	activity_pollers                          int = 2
	nexus_pollers                             int = 2
	workflow_poller_autoscaling               PollerAutoscaling
	activity_poller_autoscaling               PollerAutoscaling
	nexus_poller_autoscaling                  PollerAutoscaling
	build_id                                  string = 'temporal-v-dev'
	deployment_name                           string
	deployment_versioning                     bool
	nondeterminism_as_workflow_fail           bool
	nondeterminism_as_workflow_fail_for_types []string
	legacy_versioning                         bool
	versioning_behavior                       int
	sticky_timeout_millis                     u64 = 1000
	shutdown_grace_millis                     u64 = 1000
	max_heartbeat_millis                      u64 = 100
	default_heartbeat_millis                  u64 = 100
	max_activities_per_second                 f64
	max_queue_activities_per_second           f64
	nonsticky_poll_ratio                      f32 = 0.2
	resource_based                            bool
	target_memory_usage                       f64
	target_cpu_usage                          f64
	ramp_throttle_millis                      u64
	minimum_workflow_slots                    int
	minimum_activity_slots                    int
}

@[heap]
pub struct Connection {
	metric_export_bridge ?&MetricExportBridge
	grpc_bridge          ?&GrpcBridge
	core                 &C.SpikeCore
	state                &ConnectionState = &ConnectionState{}
}

struct ConnectionState {
mut:
	mutex  &sync.RwMutex = sync.new_rwmutex()
	closed bool
}

pub fn connect(address string, transport Transport, telemetry Telemetry) !&Connection {
	require_core_protocol()!
	validate_text(address, 'address')!
	telemetry.validate()!
	transport.validate()!
	entries := metadata_entries(string_metadata(transport.metadata), false)!
	binary_entries := metadata_entries(transport.binary_metadata, true)!
	values := refs(entries)
	binary_values := refs(binary_entries)
	metric_startup := start_metric_export(telemetry)!
	metric_bridge := metric_startup.bridge
	mut initialized := false
	defer {
		if !initialized { abandon_metric_export(metric_bridge) }
	}
	startup := start_grpc(transport)!
	bridge := startup.bridge
	defer {
		if !initialized { abandon_grpc(bridge) }
	}
	options := native_transport(address, transport, telemetry, values, binary_values, bridge,
		metric_bridge)!
	core := C.spike_client_open(&options)
	if isnil(core) { return error('cannot allocate Core client') }
	connection := &Connection{
		core:                 core
		grpc_bridge:          bridge
		metric_export_bridge: metric_bridge
	}
	connection.check() or {
		C.spike_close(core)
		return err
	}
	initialized = true
	return connection
}

// details contains serialized google.rpc.Status when supplied by the server.
pub struct RpcError {
pub:
	message string
	status  int
	details []u8
}

pub fn (err RpcError) msg() string {
	return err.message
}

pub fn (err RpcError) code() int {
	return err.status
}

pub enum RpcService {
	workflow = 1
	operator
	cloud
	test
	health
}

// Values for -bin names are raw bytes; ASCII names require visible ASCII.
pub struct RpcMetadataEntry {
pub:
	name  string
	value []u8
}

@[params]
pub struct RpcOptions {
pub:
	metadata     []RpcMetadataEntry
	retry        bool
	cancellation ?&RpcCancellation
}

pub fn (connection &Connection) rpc(service RpcService, method string, request []u8, timeout_millis u32, options RpcOptions) ![]u8 {
	if cancellation := options.cancellation {
		mut state := cancellation.state
		state.mutex.rlock()
		defer { state.mutex.runlock() }
		if state.closed { return error('RPC cancellation token is closed') }
		return connection.rpc_native(service, method, request, timeout_millis, options, state.token)!
	}
	return connection.rpc_native(service, method, request, timeout_millis, options, unsafe { nil })!
}

fn (connection &Connection) rpc_native(service RpcService, method string, request []u8, timeout_millis u32, options RpcOptions, cancellation &C.TemporalCoreCancellationToken) ![]u8 {
	validate_text(method, 'RPC method')!
	entries := rpc_metadata_entries(options.metadata, false)!
	binary_entries := rpc_metadata_entries(options.metadata, true)!
	values := refs(entries)
	binary_values := refs(binary_entries)
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('connection is closed') }

	mut details := unsafe { &C.TemporalCoreByteArray(nil) }
	bytes := C.spike_rpc(connection.core, u32(service), method.str, request.data, u32(request.len),
		timeout_millis, C.TemporalCoreByteArrayRefArray{
		data: values.data
		size: u32(values.len)
	}, C.TemporalCoreByteArrayRefArray{
		data: binary_values.data
		size: u32(binary_values.len)
	}, options.retry, cancellation, &details)
	mut error_details := []u8{}
	if !isnil(details) {
		error_details = unsafe { details.data.vbytes(int(details.size)).clone() }
		C.spike_bytes_free(connection.core, details)
	}
	if isnil(bytes) {
		message := unsafe { C.spike_error(connection.core).vstring().clone() }
		status := int(C.spike_rpc_status())
		// Tonic reports its local request timeout as CANCELLED, not DEADLINE_EXCEEDED.
		if status == 1 && message == 'Timeout expired' {
			return RpcError{
				message: message
				status:  4
				details: error_details
			}
		}
		return RpcError{
			message: message
			status:  status
			details: error_details
		}
	}
	result := unsafe { bytes.data.vbytes(int(bytes.size)).clone() }
	C.spike_bytes_free(connection.core, bytes)
	return result
}

pub enum WorkerKind {
	workflow
	activity
	nexus
	combined
	all
}

pub fn open(config Config, kind WorkerKind) !&Connection {
	require_core_protocol()!
	for name, value in {
		'address':         config.address
		'namespace':       config.namespace
		'task queue':      config.task_queue
		'build ID':        config.build_id
		'deployment name': config.deployment_name
	} {
		validate_text(value, name)!
	}
	for name in config.nondeterminism_as_workflow_fail_for_types {
		validate_text(name, 'nondeterminism workflow type')!
		if name == '' { return error('nondeterminism workflow type must not be empty') }
	}
	for plugin in config.plugins {
		validate_text(plugin.name, 'plugin name')!
		validate_text(plugin.version, 'plugin version')!
		if plugin.name == '' { return error('plugin name must not be empty') }
	}
	for driver in config.storage_drivers {
		validate_text(driver, 'storage driver type')!
		if driver == '' { return error('storage driver type must not be empty') }
	}
	config.telemetry.validate()!
	if config.activity_concurrency < 1 || config.activity_concurrency > 128 {
		return error('activity_concurrency must be between 1 and 128')
	}
	config.transport.validate()!
	entries := metadata_entries(string_metadata(config.transport.metadata), false)!
	binary_entries := metadata_entries(config.transport.binary_metadata, true)!
	values := refs(entries)
	binary_values := refs(binary_entries)
	if config.connection != none && config.transport.grpc_starter != none {
		return error('shared workers inherit the client custom gRPC transport')
	}
	if config.connection != none && config.telemetry.metric_starter != none {
		return error('shared workers inherit the client metric exporter')
	}
	mut metric_bridge := ?&MetricExportBridge(none)
	if connection := config.connection {
		metric_bridge = connection.metric_export_bridge
	} else {
		startup := start_metric_export(config.telemetry)!
		metric_bridge = startup.bridge
	}
	mut initialized := false
	defer {
		if !initialized && config.connection == none { abandon_metric_export(metric_bridge) }
	}
	mut bridge := ?&GrpcBridge(none)
	if connection := config.connection {
		bridge = connection.grpc_bridge
	} else {
		startup := start_grpc(config.transport)!
		bridge = startup.bridge
	}
	defer {
		if !initialized && config.connection == none { abandon_grpc(bridge) }
	}
	options := native_transport(config.address, config.transport, config.telemetry, values,
		binary_values, bridge, metric_bridge)!
	worker_options := config.native_worker_options()
	core := if connection := config.connection {
		connection.open_worker_core(worker_options, kind)!
	} else {
		C.spike_open(&options, &worker_options, kind in [.workflow, .combined, .all], kind in [
			.nexus,
			.all,
		], kind in [
			.activity,
			.combined,
			.all,
		])
	}
	if isnil(core) { return error('cannot allocate Core') }
	connection := &Connection{
		core:                 core
		grpc_bridge:          bridge
		metric_export_bridge: metric_bridge
	}
	connection.check() or {
		C.spike_close(core)
		return err
	}
	initialized = true
	return connection
}

fn (connection &Connection) check() ! {
	message := unsafe { C.spike_error(connection.core).vstring().clone() }
	if message != '' { return error(message) }
}

pub fn (connection &Connection) poll(workflow bool) ![]u8 {
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('connection is closed') }

	bytes := if workflow {
		C.spike_poll_workflow(connection.core)
	} else {
		C.spike_poll(connection.core)
	}
	if isnil(bytes) {
		connection.check()!
		return []u8{}
	}
	result := unsafe { bytes.data.vbytes(int(bytes.size)).clone() }
	C.spike_bytes_free(connection.core, bytes)
	return result
}

pub fn (connection &Connection) complete(bytes []u8, workflow bool) ! {
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('connection is closed') }

	status := if workflow {
		C.spike_complete_workflow(connection.core, bytes.data, u32(bytes.len))
	} else {
		C.spike_complete(connection.core, bytes.data, u32(bytes.len))
	}
	if status != 0 { connection.check()! }
}

pub fn (connection &Connection) heartbeat(bytes []u8) ! {
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('connection is closed') }

	if C.spike_heartbeat(connection.core, bytes.data, u32(bytes.len)) != 0 { connection.check()! }
}

pub fn (connection &Connection) shutdown() {
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if !state.closed { C.spike_shutdown(connection.core) }
}

// Used by cooperative local Activity cancellation after the worker's grace period.
pub fn (connection &Connection) shutdown_grace_expired() bool {
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return true }
	return C.spike_shutdown_grace_expired(connection.core)
}

pub fn replay_open(namespace_name string, queue string) !&Connection {
	require_core_protocol()!
	validate_text(namespace_name, 'namespace')!
	validate_text(queue, 'task queue')!
	core := C.spike_replay_open(namespace_name.str, queue.str)
	if isnil(core) { return error('cannot allocate Core replayer') }
	connection := &Connection{
		core: core
	}
	connection.check() or {
		C.spike_close(core)
		return err
	}
	return connection
}

pub fn (connection &Connection) replay_push(workflow_id string, history []u8) ! {
	validate_text(workflow_id, 'workflow ID')!
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('connection is closed') }

	if C.spike_replay_push(connection.core, workflow_id.str, history.data, u32(history.len)) != 0 {
		connection.check()!
	}
}

pub fn (connection &Connection) close() {
	mut state := connection.state
	state.mutex.lock()
	if state.closed {
		state.mutex.unlock()
		return
	}
	state.closed = true
	C.spike_close(connection.core)
	state.mutex.unlock()
	if bridge := connection.grpc_bridge { bridge.wait_if_core_done() }
	if metrics := connection.metric_export_bridge { metrics.wait_if_core_done() }
}

pub fn (connection &Connection) is_closed() bool {
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	return state.closed
}

pub fn (connection &Connection) poll_nexus() ![]u8 {
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('connection is closed') }
	bytes := C.spike_poll_nexus(connection.core)
	if isnil(bytes) {
		connection.check()!
		return []u8{}
	}
	result := unsafe { bytes.data.vbytes(int(bytes.size)).clone() }
	C.spike_bytes_free(connection.core, bytes)
	return result
}

pub fn (connection &Connection) complete_nexus(bytes []u8) ! {
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('connection is closed') }
	if C.spike_complete_nexus(connection.core, bytes.data, u32(bytes.len)) != 0 {
		connection.check()!
	}
}

fn (connection &Connection) open_worker_core(options C.SpikeWorkerOptions, kind WorkerKind) !&C.SpikeCore {
	// The patched Core bridge borrows shared connection/runtime state. A pending
	// query must not block construction of the worker required to answer it.
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('worker client is closed') }
	return C.spike_open_shared(connection.core, &options, kind in [.workflow, .combined, .all], kind in [
		.nexus,
		.all,
	], kind in [
		.activity,
		.combined,
		.all,
	])
}

fn require_core_protocol() ! {
	if (C.spike_protocol_capabilities() & u64(7)) != u64(7) {
		return error('loaded Core library does not support the required workflow marker replay and tracing protocols')
	}
}

fn C.spike_retain_client(&C.SpikeCore) &C.SpikeCore

pub fn (connection &Connection) retain_client() !&Connection {
	connection.state.mutex.rlock()
	defer { connection.state.mutex.runlock() }
	if connection.state.closed { return error('connection is closed') }
	native := C.spike_retain_client(connection.core)
	if isnil(native) { return error('client connection could not be retained') }
	return &Connection{
		core:                 native
		grpc_bridge:          connection.grpc_bridge
		metric_export_bridge: connection.metric_export_bridge
	}
}
