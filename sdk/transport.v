module sdk

import corebridge
import os
import time

pub struct TlsOptions {
pub:
	server_root_ca     []u8
	domain             string
	client_certificate []u8
	client_private_key []u8
}

pub struct ProxyOptions {
pub:
	target   string
	username string
	password string
}

// Applied to retry-enabled unary RPCs. Core maintains separate policies for worker polls.
// max_retries counts total attempts (1 disables retries); 0 means unlimited.
pub struct TransportRetryOptions {
pub:
	initial_interval     time.Duration = 100 * time.millisecond
	randomization_factor f64           = 0.2
	multiplier           f64           = 1.7
	max_interval         time.Duration = 5 * time.second
	max_elapsed_time     time.Duration = 10 * time.second
	max_retries          u32           = 10
}

pub enum GrpcCompression {
	none
	gzip
}

pub struct DnsLoadBalancingOptions {
pub:
	resolution_interval time.Duration = 30 * time.second
}

pub struct TransportOptions {
pub:
	custom_grpc         ?CustomGrpcTransport
	identity            string = 'temporal-v'
	client_name         string = 'temporal-v'
	client_version      string = '1.0.0-rc.1'
	retry               ?TransportRetryOptions
	dns_load_balancing  ?DnsLoadBalancingOptions
	grpc_compression    GrpcCompression
	payloads_warn_size  u64
	memo_warn_size      u64
	tls                 ?TlsOptions
	api_key             string
	metadata            map[string]string
	binary_metadata     map[string][]u8
	keep_alive_interval time.Duration
	keep_alive_timeout  time.Duration
	proxy               ProxyOptions
}

fn transport_millis(value time.Duration, name string, allow_zero bool) !u64 {
	if value < 0 || (!allow_zero && value == 0) || (value > 0 && value < time.millisecond) {
		return error('${name} must be at least one millisecond' + if allow_zero {
			' or zero'
		} else {
			''
		})
	}
	return u64(value / time.millisecond)
}

fn (options TransportOptions) core_transport() !corebridge.Transport {
	if options.keep_alive_interval < 0 || options.keep_alive_timeout < 0 {
		return error('negative keep-alive duration')
	}
	if (options.keep_alive_interval > 0 && options.keep_alive_interval < time.millisecond)
		|| (options.keep_alive_timeout > 0 && options.keep_alive_timeout < time.millisecond) {
		return error('keep-alive durations must be at least one millisecond')
	}
	mut transport := corebridge.Transport{
		identity:                   options.identity
		client_name:                options.client_name
		client_version:             options.client_version
		grpc_compression:           if options.grpc_compression == .gzip { .gzip } else { .none }
		payloads_warn_size:         options.payloads_warn_size
		memo_warn_size:             options.memo_warn_size
		api_key:                    options.api_key
		metadata:                   options.metadata
		binary_metadata:            options.binary_metadata
		keep_alive_interval_millis: u64(options.keep_alive_interval / time.millisecond)
		keep_alive_timeout_millis:  u64(options.keep_alive_timeout / time.millisecond)
		proxy_target:               options.proxy.target
		proxy_username:             options.proxy.username
		proxy_password:             options.proxy.password
	}
	if custom := options.custom_grpc {
		custom.validate()!
		transport = corebridge.Transport{
			...transport
			grpc_starter: fn [custom] () !&corebridge.GrpcBridge {
				return start_custom_grpc(custom)!
			}
		}
	}
	if retry := options.retry {
		transport = corebridge.Transport{
			...transport
			retry: corebridge.TransportRetryOptions{
				initial_interval_millis: transport_millis(retry.initial_interval,
					'retry initial interval', true)!
				randomization_factor:    retry.randomization_factor
				multiplier:              retry.multiplier
				max_interval_millis:     transport_millis(retry.max_interval,
					'retry maximum interval', true)!
				max_elapsed_time_millis: transport_millis(retry.max_elapsed_time,
					'retry maximum elapsed time', true)!
				max_retries:             retry.max_retries
			}
		}
	}
	if dns := options.dns_load_balancing {
		if dns.resolution_interval < time.second {
			return error('DNS resolution interval must be at least one second')
		}
		transport = corebridge.Transport{
			...transport
			dns_resolution_interval_millis: transport_millis(dns.resolution_interval,
				'DNS resolution interval', false)!
		}
	}
	if tls := options.tls {
		transport = corebridge.Transport{
			...transport
			tls_enabled:        true
			tls_domain:         tls.domain
			server_root_ca:     tls.server_root_ca
			client_cert:        tls.client_certificate
			client_private_key: tls.client_private_key
		}
	}
	return transport
}

fn optional_file(variable string) ![]u8 {
	path := os.getenv(variable)
	if path == '' { return []u8{} }
	return os.read_bytes(path)!
}

// File paths keep PEM key material out of CLI arguments and diagnostic output.
pub fn transport_from_env() !TransportOptions {
	mut tls := ?TlsOptions(none)
	enabled := os.getenv('TEMPORAL_TLS')
	if enabled !in ['', 'true', 'false'] { return error('TEMPORAL_TLS must be true or false') }
	if enabled == 'true' {
		tls = TlsOptions{
			server_root_ca:     optional_file('TEMPORAL_TLS_SERVER_CA_FILE')!
			domain:             os.getenv('TEMPORAL_TLS_DOMAIN')
			client_certificate: optional_file('TEMPORAL_TLS_CLIENT_CERT_FILE')!
			client_private_key: optional_file('TEMPORAL_TLS_CLIENT_KEY_FILE')!
		}
	} else {
		for name in ['TEMPORAL_TLS_SERVER_CA_FILE', 'TEMPORAL_TLS_DOMAIN',
			'TEMPORAL_TLS_CLIENT_CERT_FILE', 'TEMPORAL_TLS_CLIENT_KEY_FILE'] {
			if os.getenv(name) != '' { return error('${name} requires TEMPORAL_TLS=true') }
		}
	}
	return TransportOptions{
		proxy:    ProxyOptions{
			target:   os.getenv('TEMPORAL_PROXY_TARGET')
			username: os.getenv('TEMPORAL_PROXY_USERNAME')
			password: os.getenv('TEMPORAL_PROXY_PASSWORD')
		}
		tls:      tls
		api_key:  os.getenv('TEMPORAL_API_KEY')
		identity: os.getenv_opt('TEMPORAL_IDENTITY') or { 'temporal-v' }
	}
}

pub fn (client &Client) update_api_key(api_key string) ! {
	client.connection.update_api_key(api_key)!
}

pub fn (client &Client) update_metadata(metadata map[string]string) ! {
	mut values := map[string][]u8{}
	for key, value in metadata {
		values[key] = value.bytes()
	}
	client.connection.update_metadata(values, false)!
}

pub fn (client &Client) update_binary_metadata(metadata map[string][]u8) ! {
	client.connection.update_metadata(metadata, true)!
}
