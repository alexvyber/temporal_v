module main

import json2
import os
import sdk
import temporal

const configuration = '[profile.default]\naddress = "localhost:7233"\nnamespace = "file-default"\napi_key = "disposable-private-profile-key"\n[profile.default.tls]\ndisabled = true\n[profile.default.grpc_meta]\nx_file = "file"\n[profile.alternate]\nnamespace = "file-alternate"\n[profile.alternate.codec]\nendpoint = "http://codec.invalid"\nauth = "disposable-codec-auth"\n'

fn expect_error(options sdk.ClientProfileLoadOptions) ! {
	if _ := sdk.load_client_profile(options) {
		return error('invalid profile was accepted')
	} else {
		if err !is sdk.ConfigurationError { return error('untyped configuration error') }
		if err.msg().contains('disposable-private') {
			return error('configuration diagnostic leaked a secret')
		}
	}
}

fn validation() ! {
	empty := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
		disable_file: true
		environment:  map[string]string{}
	})!
	assert empty.client_options()!.address == 'http://localhost:7233'
	assert sdk.ClientOptions{}.address == 'http://127.0.0.1:7233'
	assert sdk.WorkerOptions{}.address == 'http://127.0.0.1:7233'
	profile := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
		data:        configuration.bytes()
		environment: {
			'TEMPORAL_NAMESPACE':        'environment'
			'TEMPORAL_GRPC_META_X_FILE': 'overridden'
			'TEMPORAL_GRPC_META_X_NEW':  'new'
		}
	})!
	options := profile.client_options()!
	assert options.namespace == 'environment'
	assert options.address == 'http://localhost:7233'
	assert options.transport.tls == none
	assert options.transport.api_key == 'disposable-private-profile-key'
	assert options.transport.metadata == {
		'x-file': 'overridden'
		'x-new':  'new'
	}
	selected := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
		data:        configuration.bytes()
		environment: {
			'TEMPORAL_PROFILE': 'alternate'
		}
	})!
	assert (selected.namespace or { return error('expected optional profile field missing') }) == 'file-alternate'
	codec := selected.codec or { return error('codec metadata missing') }
	assert (codec.endpoint or { return error('expected optional profile field missing') }) == 'http://codec.invalid' && (codec.auth or {
		return error('expected optional profile field missing')
	}) == 'disposable-codec-auth'
	explicit := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
		profile:     'default'
		data:        configuration.bytes()
		disable_env: true
		environment: {
			'TEMPORAL_PROFILE':   'alternate'
			'TEMPORAL_NAMESPACE': 'ignored'
		}
	})!
	assert (explicit.namespace or { return error('expected optional profile field missing') }) == 'file-default'
	all := sdk.load_client_config(sdk.ClientProfileLoadOptions{
		data:        configuration.bytes()
		environment: map[string]string{}
	})!
	assert all.profiles.len == 2 && (all.profiles['default'].namespace or {
		return error('expected optional profile field missing')
	}) == 'file-default'
	removed := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
		data:        configuration.bytes()
		environment: {
			'TEMPORAL_GRPC_META_X_FILE': ''
		}
	})!
	assert removed.grpc_meta.len == 0
	tls := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
		disable_file: true
		environment:  {
			'TEMPORAL_API_KEY':                 'disposable'
			'TEMPORAL_TLS_SERVER_CA_CERT_DATA': 'ca bytes'
		}
	})!.client_options()!
	assert tls.address == 'https://localhost:7233'
	assert (tls.transport.tls or { return error('API key did not enable TLS') }).server_root_ca == 'ca bytes'.bytes()
	for _ in 0 .. 100 {
		expect_error(sdk.ClientProfileLoadOptions{
			data:        '[profile.default]\napi_key = "disposable-private'.bytes()
			environment: map[string]string{}
		})!
	}
	expect_error(sdk.ClientProfileLoadOptions{
		data:        configuration.bytes()
		profile:     'missing'
		environment: map[string]string{}
	})!
	expect_error(sdk.ClientProfileLoadOptions{
		data:        (configuration + '\nunknown = "private"\n').bytes()
		strict:      true
		environment: map[string]string{}
	})!
	expect_error(sdk.ClientProfileLoadOptions{
		disable_file: true
		environment:  {
			'TEMPORAL_TLS': 'disposable-private-invalid-bool'
		}
	})!
	expect_error(sdk.ClientProfileLoadOptions{
		disable_file: true
		environment:  {
			'TEMPORAL_TLS_CLIENT_CERT_PATH': '/disposable/private'
			'TEMPORAL_TLS_CLIENT_CERT_DATA': 'disposable-private-data'
		}
	})!
	if _ := (sdk.ClientProfile{
		tls: sdk.ClientProfileTls{
			server_ca_cert: sdk.ClientProfileSource{
				data: []u8{}
			}
		}
	}).client_options()
	{
		return error('empty certificate accepted')
	}
	if _ := (sdk.ClientProfile{
		tls: sdk.ClientProfileTls{
			client_cert: sdk.ClientProfileSource{
				data: 'cert'.bytes()
			}
		}
	}).client_options()
	{
		return error('partial client key pair accepted')
	}
	if _ := (sdk.ClientProfile{
		tls: sdk.ClientProfileTls{
			server_ca_cert: sdk.ClientProfileSource{
				path: '/unused'
				data: 'bytes'.bytes()
			}
		}
	}).client_options()
	{
		return error('ambiguous source accepted')
	}
	expect_error(sdk.ClientProfileLoadOptions{
		disable_file: true
		disable_env:  true
		environment:  map[string]string{}
	})!
	empty_key := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
		data:        '[profile.default]\napi_key = ""\n'.bytes()
		environment: map[string]string{}
	})!
	assert empty_key.api_key != none && empty_key.client_options()!.transport.tls != none
	insecure := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
		data:        '[profile.default.tls]\ndisable_host_verification = true\n'.bytes()
		environment: map[string]string{}
	})!
	assert (insecure.tls or { return error('missing TLS config') }).disable_host_verification
	if _ := insecure.client_options() {
		return error('unsupported host-verification bypass was silently ignored')
	}
	if _ := (sdk.ClientProfile{
		tls: sdk.ClientProfileTls{
			server_name: ''
		}
	}).client_options()
	{
		return error('empty TLS server name accepted')
	}
	println('VALIDATION_OK')
}

fn client() !&sdk.Client {
	profile := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
		path:        os.getenv('V_SDK_EXAMPLE_PROFILE_PATH')
		profile:     'default'
		environment: {
			'TEMPORAL_ADDRESS':             os.getenv('TEMPORAL_ADDRESS')
			'TEMPORAL_NAMESPACE':           'default'
			'TEMPORAL_GRPC_META_X_PROFILE': 'live'
		}
	})!
	return sdk.connect(profile.client_options()!)
}

fn run() ! {
	role := os.args[1]
	if role == 'validation' { return validation() }
	if role == 'tls' || role == 'tls-reject' {
		profile := sdk.load_client_profile(sdk.ClientProfileLoadOptions{
			path:        os.args[2]
			disable_env: true
			environment: map[string]string{}
		})!
		connection := sdk.connect(profile.client_options()!) or {
			if role == 'tls-reject' {
				println('TLS_REJECTED')
				return
			}
			return err
		}
		defer { connection.close() }
		if role == 'tls-reject' { return error('wrong TLS hostname accepted') }
		connection.rpc_get_system_info(temporal.TemporalApiWorkflowserviceV1_GetSystemInfoRequest{})!
		println('TLS_OK')
		return
	}
	connection := client()!
	defer { connection.close() }
	handle := sdk.WorkflowHandle{
		workflow_id: os.getenv('TEMPORAL_TASK_QUEUE')
	}
	match role {
		'start' {
			started := connection.start(sdk.WorkflowStart{
				workflow_id:   handle.workflow_id
				workflow_type: 'v.counter'
				task_queue:    os.getenv('TEMPORAL_TASK_QUEUE')
				arguments:     '{"initial":7}'
			})!
			connection.signal(started, 'add', '{"amount":9}')!
			println(json2.encode(started))
		}
		'finish' {
			state := connection.query(handle, 'state', 'null')!
			if !state.contains('16') { return error('counter replay state mismatch') }
			connection.signal(handle, 'finish', 'null')!
			if connection.get_result(handle, 5000)!.status != .completed {
				return error('profile client workflow failed')
			}
			println('CLIENT_OK')
		}
		'history' {
			os.write_file(os.args[2], connection.history(handle)!.encode().bytestr())!
		}
		else {
			return error('unknown role')
		}
	}
}

fn main() {
	run() or {
		eprintln(err)
		exit(1)
	}
}
