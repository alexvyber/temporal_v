module sdk

import temporal_v.corebridge
import sync
import temporal_v.temporal
import time

pub enum TestServerMode {
	dev
	time_skipping
}

pub struct TestEnvironmentOptions {
pub:
	plugins           []Plugin
	mode              TestServerMode
	executable        string
	namespace         string = 'default'
	port              u16
	startup_timeout   time.Duration = 10 * time.second
	database_filename string
	extra_args        []string
	data_converter    DataConverter
}

struct TimeSkippingState {
mut:
	mutex &sync.Mutex = sync.new_mutex()
	users int
}

@[heap]
pub struct TestEnvironment {
	server   corebridge.Server
	skipping &TimeSkippingState = &TimeSkippingState{}
pub:
	address       string
	client        &Client
	time_skipping bool
}

pub fn start_test_environment(options TestEnvironmentOptions) !&TestEnvironment {
	if options.startup_timeout < time.millisecond {
		return error('test server startup timeout must be at least one millisecond')
	}
	server := corebridge.start_server(corebridge.ServerOptions{
		executable:             options.executable
		namespace:              options.namespace
		port:                   options.port
		startup_timeout_millis: u64(options.startup_timeout / time.millisecond)
		time_skipping:          options.mode == .time_skipping
		database_filename:      options.database_filename
		extra_args:             options.extra_args
	})!
	client := connect(ClientOptions{
		plugins:        options.plugins
		address:        server.address
		namespace:      options.namespace
		data_converter: options.data_converter
	}) or {
		server.close()
		return err
	}
	return &TestEnvironment{
		address:       server.address
		client:        client
		server:        server
		time_skipping: options.mode == .time_skipping
	}
}

pub fn (environment &TestEnvironment) close() {
	environment.client.close()
	environment.server.close()
}

fn (environment &TestEnvironment) enter_time_skipping() ! {
	if !environment.time_skipping { return }
	mut state := environment.skipping
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.users == 0 {
		environment.client.rpc_test_unlock_time_skipping(temporal.TemporalApiTestserviceV1_UnlockTimeSkippingRequest{})!
	}
	state.users++
}

fn (environment &TestEnvironment) leave_time_skipping() ! {
	if !environment.time_skipping { return }
	mut state := environment.skipping
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.users <= 0 { return error('unbalanced time skipping') }
	state.users--
	if state.users == 0 {
		environment.client.rpc_test_lock_time_skipping(temporal.TemporalApiTestserviceV1_LockTimeSkippingRequest{})!
	}
}

pub fn (environment &TestEnvironment) sleep(duration time.Duration) ! {
	if duration < 0 { return error('negative test sleep') }
	if !environment.time_skipping {
		time.sleep(duration)
		return
	}
	environment.enter_time_skipping()!
	environment.client.rpc_test_sleep(temporal.TemporalApiTestserviceV1_SleepRequest{
		duration: duration_proto(duration)!
	}) or {
		environment.leave_time_skipping()!
		return err
	}
	environment.leave_time_skipping()!
}

pub fn (environment &TestEnvironment) get_result(handle WorkflowHandle, timeout_millis u32) !WorkflowResult {
	environment.enter_time_skipping()!
	result := environment.client.get_chain_result(handle, timeout_millis) or {
		environment.leave_time_skipping()!
		return err
	}
	environment.leave_time_skipping()!
	return result
}

pub fn (environment &TestEnvironment) current_time() !time.Time {
	if !environment.time_skipping { return time.now() }
	response := environment.client.rpc_test_get_current_time(temporal.GoogleProtobuf_Empty{})!
	timestamp := response.time or { return error('test server returned no time') }
	return time.unix(timestamp.seconds).add(timestamp.nanos * time.nanosecond)
}
