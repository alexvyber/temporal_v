module corebridge

import encoding.utf8
import math

fn validate_text(value string, name string) ! {
	if value.contains('\x00') || !utf8.validate_str(value) {
		return error('${name} must be valid UTF-8 without NUL bytes')
	}
}

@[typedef]
struct C.TemporalCoreByteArrayRef {
	data &u8
	size u32
}

@[typedef]
struct C.TemporalCoreByteArrayRefArray {
	data &C.TemporalCoreByteArrayRef
	size u32
}

@[typedef]
struct C.SpikeConnectOptions {
	grpc_bridge                    &C.SpikeGrpcBridge = unsafe { nil }
	target                         &char
	client_name                    &char
	client_version                 &char
	retry_enabled                  bool
	retry_initial_interval_millis  u64
	retry_randomization_factor     f64
	retry_multiplier               f64
	retry_max_interval_millis      u64
	retry_max_elapsed_time_millis  u64
	retry_max_retries              u32
	dns_resolution_interval_millis u64
	grpc_gzip                      bool
	payloads_warn_size             u64
	memo_warn_size                 u64
	identity                       &char
	api_key                        &char
	tls_enabled                    bool
	tls_domain                     &char
	server_root_ca                 C.TemporalCoreByteArrayRef
	client_cert                    C.TemporalCoreByteArrayRef
	client_private_key             C.TemporalCoreByteArrayRef
	metadata                       C.TemporalCoreByteArrayRefArray
	binary_metadata                C.TemporalCoreByteArrayRefArray
	keep_alive_interval_millis     u64
	keep_alive_timeout_millis      u64
	proxy_target                   &char
	proxy_username                 &char
	proxy_password                 &char
	telemetry                      C.SpikeTelemetryOptions
}

pub struct TransportRetryOptions {
pub:
	initial_interval_millis u64 = 100
	randomization_factor    f64 = 0.2
	multiplier              f64 = 1.7
	max_interval_millis     u64 = 5000
	max_elapsed_time_millis u64 = 10000
	max_retries             u32 = 10
}

pub enum GrpcCompression {
	none
	gzip
}

pub struct Transport {
pub:
	grpc_starter                   ?GrpcStarter
	identity                       string = 'temporal-v'
	client_name                    string = 'temporal-v'
	client_version                 string = '1.0.0-rc.1'
	retry                          ?TransportRetryOptions
	dns_resolution_interval_millis u64
	grpc_compression               GrpcCompression
	payloads_warn_size             u64
	memo_warn_size                 u64
	api_key                        string
	tls_enabled                    bool
	tls_domain                     string
	server_root_ca                 []u8
	client_cert                    []u8
	client_private_key             []u8
	metadata                       map[string]string
	binary_metadata                map[string][]u8
	keep_alive_interval_millis     u64
	keep_alive_timeout_millis      u64
	proxy_target                   string
	proxy_username                 string
	proxy_password                 string
}

fn metadata_packet(key string, value []u8, binary bool) ![]u8 {
	if key == '' || key.ends_with('-bin') != binary { return error('invalid gRPC metadata key') }
	for character in key.bytes() {
		if !(character >= `a` && character <= `z`) && !(character >= `0` && character <= `9`)
			&& character !in [`-`, `_`, `.`] {
			return error('invalid gRPC metadata key')
		}
	}
	if !binary {
		for character in value {
			if character < 32 || character > 126 {
				return error('invalid ASCII gRPC metadata value')
			}
		}
	}
	mut packet := (key + '\n').bytes()
	packet << value
	return packet
}

fn metadata_entries(metadata map[string][]u8, binary bool) ![][]u8 {
	mut entries := [][]u8{}
	mut keys := metadata.keys()
	keys.sort()
	for key in keys {
		entries << metadata_packet(key, metadata[key], binary)!
	}
	return entries
}

fn rpc_metadata_entries(metadata []RpcMetadataEntry, binary bool) ![][]u8 {
	mut entries := [][]u8{}
	for entry in metadata {
		if entry.name.ends_with('-bin') == binary {
			entries << metadata_packet(entry.name, entry.value, binary)!
		}
	}
	return entries
}

fn string_metadata(metadata map[string]string) map[string][]u8 {
	mut result := map[string][]u8{}
	for key, value in metadata {
		result[key] = value.bytes()
	}
	return result
}

fn refs(values [][]u8) []C.TemporalCoreByteArrayRef {
	mut result := []C.TemporalCoreByteArrayRef{}
	for value in values {
		result << C.TemporalCoreByteArrayRef{
			data: value.data
			size: u32(value.len)
		}
	}
	return result
}

fn (options Transport) validate() ! {
	if options.grpc_starter != none && (options.tls_enabled
		|| options.proxy_target != '' || options.keep_alive_interval_millis > 0
		|| options.dns_resolution_interval_millis > 0
		|| options.grpc_compression != .none) {
		return error('custom gRPC transport owns TLS, proxy, keep-alive, DNS and compression settings')
	}
	for name, value in {
		'identity':       options.identity
		'client name':    options.client_name
		'client version': options.client_version
		'TLS domain':     options.tls_domain
		'proxy target':   options.proxy_target
		'proxy username': options.proxy_username
		'proxy password': options.proxy_password
	} {
		validate_text(value, name)!
	}
	if options.client_name == '' || options.client_version == '' {
		return error('client name and version cannot be empty')
	}
	for name, value in {
		'client name':    options.client_name
		'client version': options.client_version
	} {
		for character in value.bytes() {
			if character < 32 || character > 126 {
				return error('${name} must use printable ASCII')
			}
		}
	}
	if retry := options.retry {
		if math.is_nan(retry.randomization_factor) || math.is_inf(retry.randomization_factor, 0)
			|| retry.randomization_factor < 0 || retry.randomization_factor > 1 {
			return error('retry randomization factor must be finite and between zero and one')
		}
		if math.is_nan(retry.multiplier) || math.is_inf(retry.multiplier, 0) || retry.multiplier < 1 {
			return error('retry multiplier must be finite and at least one')
		}
		if retry.max_interval_millis < retry.initial_interval_millis {
			return error('retry maximum interval must be at least the initial interval')
		}
	}
	if options.dns_resolution_interval_millis > 0 && options.dns_resolution_interval_millis < 1000 {
		return error('DNS resolution interval must be at least one second')
	}
	if options.dns_resolution_interval_millis > 0 && options.proxy_target != '' {
		return error('DNS load balancing cannot be combined with an HTTP CONNECT proxy')
	}
	if options.identity == '' { return error('connection identity cannot be empty') }
	if (options.client_cert.len == 0) != (options.client_private_key.len == 0) {
		return error('TLS requires both client certificate and private key')
	}
	if !options.tls_enabled && (options.server_root_ca.len > 0
		|| options.client_cert.len > 0 || options.tls_domain != '') {
		return error('TLS certificate/domain options require TLS enabled')
	}
	if (options.keep_alive_interval_millis == 0) != (options.keep_alive_timeout_millis == 0) {
		return error('keep-alive interval and timeout must both be positive')
	}
	if options.proxy_target == '' && (options.proxy_username != '' || options.proxy_password != '') {
		return error('proxy credentials require a proxy target')
	}
	metadata_entries(string_metadata(options.metadata), false)!
	metadata_entries(options.binary_metadata, true)!
	for character in options.api_key.bytes() {
		if character < 32 || character > 126 { return error('invalid API key') }
	}
}

fn native_transport(address string, options Transport, telemetry Telemetry, metadata []C.TemporalCoreByteArrayRef, binary []C.TemporalCoreByteArrayRef, bridge ?&GrpcBridge, metrics ?&MetricExportBridge) !C.SpikeConnectOptions {
	retry := options.retry or { TransportRetryOptions{} }
	mut pointer := unsafe { &C.SpikeGrpcBridge(nil) }
	if value := bridge { pointer = value.native_pointer()! }
	return C.SpikeConnectOptions{
		grpc_bridge:                    pointer
		client_name:                    options.client_name.str
		client_version:                 options.client_version.str
		retry_enabled:                  options.retry != none
		retry_initial_interval_millis:  retry.initial_interval_millis
		retry_randomization_factor:     retry.randomization_factor
		retry_multiplier:               retry.multiplier
		retry_max_interval_millis:      retry.max_interval_millis
		retry_max_elapsed_time_millis:  retry.max_elapsed_time_millis
		retry_max_retries:              retry.max_retries
		dns_resolution_interval_millis: options.dns_resolution_interval_millis
		grpc_gzip:                      options.grpc_compression == .gzip
		payloads_warn_size:             options.payloads_warn_size
		memo_warn_size:                 options.memo_warn_size
		telemetry:                      telemetry.native_options(metrics)!
		target:                         address.str
		identity:                       options.identity.str
		api_key:                        options.api_key.str
		tls_enabled:                    options.tls_enabled
		tls_domain:                     options.tls_domain.str
		server_root_ca:                 C.TemporalCoreByteArrayRef{
			data: options.server_root_ca.data
			size: u32(options.server_root_ca.len)
		}
		client_cert:                    C.TemporalCoreByteArrayRef{
			data: options.client_cert.data
			size: u32(options.client_cert.len)
		}
		client_private_key:             C.TemporalCoreByteArrayRef{
			data: options.client_private_key.data
			size: u32(options.client_private_key.len)
		}
		metadata:                       C.TemporalCoreByteArrayRefArray{
			data: metadata.data
			size: u32(metadata.len)
		}
		binary_metadata:                C.TemporalCoreByteArrayRefArray{
			data: binary.data
			size: u32(binary.len)
		}
		keep_alive_interval_millis:     options.keep_alive_interval_millis
		keep_alive_timeout_millis:      options.keep_alive_timeout_millis
		proxy_target:                   options.proxy_target.str
		proxy_username:                 options.proxy_username.str
		proxy_password:                 options.proxy_password.str
	}
}

fn C.spike_update_api_key(&C.SpikeCore, &char)
fn C.spike_update_metadata(&C.SpikeCore, C.TemporalCoreByteArrayRefArray, bool)

pub fn (connection &Connection) update_api_key(api_key string) ! {
	for character in api_key.bytes() {
		if character < 32 || character > 126 { return error('invalid API key') }
	}
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('connection is closed') }
	C.spike_update_api_key(connection.core, api_key.str)
}

pub fn (connection &Connection) update_metadata(metadata map[string][]u8, binary bool) ! {
	entries := metadata_entries(metadata, binary)!
	values := refs(entries)
	mut state := connection.state
	state.mutex.rlock()
	defer { state.mutex.runlock() }
	if state.closed { return error('connection is closed') }
	C.spike_update_metadata(connection.core, C.TemporalCoreByteArrayRefArray{
		data: values.data
		size: u32(values.len)
	}, binary)
}
