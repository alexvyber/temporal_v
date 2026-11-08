module corebridge

import x.json2
import os

@[typedef]
struct C.TemporalCoreClientEnvConfigLoadOptions {
	path               C.TemporalCoreByteArrayRef
	data               C.TemporalCoreByteArrayRef
	config_file_strict bool
	env_vars           C.TemporalCoreByteArrayRef
}

@[typedef]
struct C.TemporalCoreClientEnvConfigProfileLoadOptions {
	profile            C.TemporalCoreByteArrayRef
	path               C.TemporalCoreByteArrayRef
	data               C.TemporalCoreByteArrayRef
	disable_file       bool
	disable_env        bool
	config_file_strict bool
	env_vars           C.TemporalCoreByteArrayRef
}

@[typedef]
struct C.TemporalCoreClientEnvConfigOrFail {
	success &C.TemporalCoreByteArray
	fail    &C.TemporalCoreByteArray
}

@[typedef]
struct C.TemporalCoreClientEnvConfigProfileOrFail {
	success &C.TemporalCoreByteArray
	fail    &C.TemporalCoreByteArray
}

fn C.temporal_core_client_env_config_load(&C.TemporalCoreClientEnvConfigLoadOptions) C.TemporalCoreClientEnvConfigOrFail
fn C.temporal_core_client_env_config_profile_load(&C.TemporalCoreClientEnvConfigProfileLoadOptions) C.TemporalCoreClientEnvConfigProfileOrFail
fn C.temporal_core_byte_array_free(voidptr, &C.TemporalCoreByteArray)

pub struct EnvConfigLoadOptions {
pub:
	profile      string
	path         string
	data         []u8
	disable_file bool
	disable_env  bool
	strict       bool
	environment  ?map[string]string
}

// Parser diagnostics can contain complete TOML lines, including credentials.
pub struct EnvConfigError {
	details string
}

pub fn (err EnvConfigError) msg() string {
	return 'Temporal client configuration could not be loaded'
}

pub fn (err EnvConfigError) code() int {
	return 0
}

pub fn (err EnvConfigError) diagnostic() string {
	return err.details
}

fn envconfig_ref(value []u8) C.TemporalCoreByteArrayRef {
	return C.TemporalCoreByteArrayRef{
		data: value.data
		size: u32(value.len)
	}
}

fn envconfig_environment(options EnvConfigLoadOptions) !string {
	validate_text(options.path, 'configuration path')!
	validate_text(options.profile, 'configuration profile')!
	if options.path != '' && options.data.len > 0 {
		return error('choose a configuration path or inline data')
	}
	if environment := options.environment {
		for name, value in environment {
			validate_text(name, 'environment key')!
			validate_text(value, 'environment value')!
		}
		return json2.encode(environment)
	}
	return ''
}

fn envconfig_result(success &C.TemporalCoreByteArray, fail &C.TemporalCoreByteArray) ![]u8 {
	defer {
		C.temporal_core_byte_array_free(unsafe { nil }, success)
		C.temporal_core_byte_array_free(unsafe { nil }, fail)
	}
	if !isnil(fail) {
		if fail.size > 2147483647 { return error('configuration diagnostic is too large') }
		details := unsafe { fail.data.vbytes(int(fail.size)).clone().bytestr() }
		return EnvConfigError{
			details: details
		}
	}
	if isnil(success) { return error('Core returned no client configuration') }
	if success.size > 2147483647 { return error('configuration is too large') }
	return unsafe { success.data.vbytes(int(success.size)).clone() }
}

pub fn load_client_config(options EnvConfigLoadOptions) ![]u8 {
	environment := envconfig_environment(options)!
	settings := C.TemporalCoreClientEnvConfigLoadOptions{
		path:               envconfig_ref(options.path.bytes())
		data:               envconfig_ref(options.data)
		config_file_strict: options.strict
		env_vars:           envconfig_ref(environment.bytes())
	}
	result := C.temporal_core_client_env_config_load(&settings)
	return envconfig_result(result.success, result.fail)
}

pub fn load_client_profile(options EnvConfigLoadOptions) ![]u8 {
	if !options.disable_env {
		for name in ['TEMPORAL_TLS', 'TEMPORAL_TLS_DISABLE_HOST_VERIFICATION'] {
			value := if environment := options.environment {
				environment[name] or { continue }
			} else {
				os.getenv_opt(name) or { continue }
			}
			if value.to_lower() !in ['true', 'false', '1', '0'] {
				return EnvConfigError{
					details: '${name} must be true, false, 1 or 0'
				}
			}
		}
	}

	environment := envconfig_environment(options)!
	settings := C.TemporalCoreClientEnvConfigProfileLoadOptions{
		profile:            envconfig_ref(options.profile.bytes())
		path:               envconfig_ref(options.path.bytes())
		data:               envconfig_ref(options.data)
		disable_file:       options.disable_file
		disable_env:        options.disable_env
		config_file_strict: options.strict
		env_vars:           envconfig_ref(environment.bytes())
	}
	result := C.temporal_core_client_env_config_profile_load(&settings)
	return envconfig_result(result.success, result.fail)
}
