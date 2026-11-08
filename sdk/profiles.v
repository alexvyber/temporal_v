module sdk

import alexvyber.temporal_v.corebridge
import encoding.utf8
import x.json2
import net.urllib
import os

pub struct ClientProfileLoadOptions {
pub:
	profile      string
	path         string
	data         []u8
	disable_file bool
	disable_env  bool
	strict       bool
	environment  ?map[string]string
}

pub struct ClientProfileSource {
pub:
	path ?string
	data ?[]u8
}

pub struct ClientProfileTls {
pub:
	disable_host_verification bool
	disabled                  ?bool
	server_name               ?string
	server_ca_cert            ?ClientProfileSource
	client_cert               ?ClientProfileSource
	client_key                ?ClientProfileSource
}

pub struct ClientProfileCodec {
pub:
	endpoint ?string
	auth     ?string
}

pub struct ClientProfile {
pub:
	address   ?string
	namespace ?string
	api_key   ?string
	tls       ?ClientProfileTls
	codec     ?ClientProfileCodec
	grpc_meta map[string]string
}

pub struct ClientConfig {
pub:
	profiles map[string]ClientProfile
}

pub struct ConfigurationError {
	diagnostics string
}

pub fn (err ConfigurationError) msg() string {
	return 'Temporal client configuration could not be loaded or applied'
}

pub fn (err ConfigurationError) code() int {
	return 0
}

pub fn (err ConfigurationError) diagnostic() string {
	return err.diagnostics
}

fn core_profile_options(options ClientProfileLoadOptions) corebridge.EnvConfigLoadOptions {
	return corebridge.EnvConfigLoadOptions{
		profile:      options.profile
		path:         options.path
		data:         options.data
		disable_file: options.disable_file
		disable_env:  options.disable_env
		strict:       options.strict
		environment:  options.environment
	}
}

fn configuration_error(err IError) ConfigurationError {
	return ConfigurationError{
		diagnostics: if err is corebridge.EnvConfigError {
			err.diagnostic()
		} else {
			err.msg()
		}
	}
}

pub fn load_client_config(options ClientProfileLoadOptions) !ClientConfig {
	if options.profile != '' || options.disable_env || options.disable_file {
		return error('profile/disable flags apply only to profile loading')
	}
	bytes := corebridge.load_client_config(core_profile_options(options)) or {
		return configuration_error(err)
	}
	return json2.decode[ClientConfig](bytes.bytestr()) or { return configuration_error(err) }
}

pub fn load_client_profile(options ClientProfileLoadOptions) !ClientProfile {
	bytes := corebridge.load_client_profile(core_profile_options(options)) or {
		return configuration_error(err)
	}
	return json2.decode[ClientProfile](bytes.bytestr()) or { return configuration_error(err) }
}

fn profile_source(source ClientProfileSource) ![]u8 {
	if source.path != none && source.data != none {
		return error('certificate source must contain a path or data')
	}
	bytes := if path := source.path {
		if path.contains('\x00') || !utf8.validate_str(path) {
			return error('invalid certificate path')
		}
		os.read_bytes(path)!
	} else if data := source.data {
		data.clone()
	} else {
		return error('certificate source must contain a path or data')
	}
	if bytes.len == 0 { return error('certificate source cannot be empty') }
	return bytes
}

fn (profile ClientProfile) options() !ClientOptions {
	mut tls_enabled := profile.api_key != none
	if tls := profile.tls { tls_enabled = !(tls.disabled or { false }) }
	mut transport := TransportOptions{
		api_key:  profile.api_key or { '' }
		metadata: profile.grpc_meta.clone()
	}
	if tls_enabled {
		settings := profile.tls or { ClientProfileTls{} }
		if settings.disable_host_verification {
			return error('the pinned Core TLS bridge does not support disabling host verification')
		}
		if server_name := settings.server_name {
			if server_name == '' { return error('TLS server name cannot be empty') }
		}
		if (settings.client_cert != none) != (settings.client_key != none) {
			return error('client TLS requires both certificate and private key')
		}
		transport = TransportOptions{
			...transport
			tls: TlsOptions{
				domain:             settings.server_name or { '' }
				server_root_ca:     if source := settings.server_ca_cert {
					profile_source(source)!
				} else {
					[]u8{}
				}
				client_certificate: if source := settings.client_cert {
					profile_source(source)!
				} else {
					[]u8{}
				}
				client_private_key: if source := settings.client_key {
					profile_source(source)!
				} else {
					[]u8{}
				}
			}
		}
	}
	mut address := profile.address or { 'localhost:7233' }
	if !address.contains('://') {
		address = (if tls_enabled { 'https://' } else { 'http://' }) + address
	}
	validated := urllib.parse(address)!
	if validated.host == '' || validated.scheme.to_lower() !in ['http', 'https'] {
		return error('invalid Temporal address')
	}
	return ClientOptions{
		address:   address
		namespace: profile.namespace or { 'default' }
		transport: transport
	}
}

pub fn (profile ClientProfile) client_options() !ClientOptions {
	return profile.options() or { return configuration_error(err) }
}
