module sdk

import encoding.utf8

// Hooks run synchronously before construction and must be safe when the same
// plugin value is used concurrently by independent clients/workers/replayers.
pub interface Plugin {
	name() string
}

pub interface VersionedPlugin {
	version() string
}

pub interface ConnectionConfigurator {
	configure_connection(options ClientOptions) !ClientOptions
}

pub interface ClientConfigurator {
	configure_client(options ClientOptions) !ClientOptions
}

pub interface WorkerConfigurator {
	configure_worker(mut setup WorkerSetup) !
}

pub interface ReplayConfigurator {
	configure_replay(mut setup ReplaySetup) !
}

pub struct PluginInfo {
pub:
	name    string
	version string
}

pub enum PluginTarget {
	connection
	client
	worker
	replay
}

pub struct PluginError {
pub:
	plugin_name string
	target      PluginTarget
	cause       IError
}

pub fn (err PluginError) msg() string {
	return "plugin '${err.plugin_name}' failed to configure ${err.target}: ${protocol_text(err.cause.msg())}"
}

pub fn (err PluginError) code() int {
	return err.cause.code()
}

pub enum WorkerRole {
	workflow
	combined
	activity
	nexus
}

pub struct WorkerSetup {
pub:
	role WorkerRole
pub mut:
	options        WorkerOptions
	workflows      Runtime
	activities     Activities
	nexus_services NexusServices
}

pub struct ReplaySetup {
pub mut:
	options   ReplayOptions
	workflows Runtime
}

struct PluginRegistration {
	value       Plugin
	information PluginInfo
}

fn plugin_registrations(plugins []Plugin, target PluginTarget) ![]PluginRegistration {
	mut registrations := []PluginRegistration{}
	for plugin in plugins.clone() {
		name := plugin.name()
		version := if plugin is VersionedPlugin { plugin.version() } else { '' }
		if name == '' || !utf8.validate_str(name) || name.contains('\x00')
			|| !utf8.validate_str(version) || version.contains('\x00') {
			return PluginError{
				plugin_name: protocol_text(name)
				target:      target
				cause:       error('invalid plugin name or version')
			}
		}
		registrations << PluginRegistration{
			value:       plugin
			information: PluginInfo{
				name:    name
				version: version
			}
		}
	}
	return registrations
}

fn configure_client(original ClientOptions) !(ClientOptions, []PluginRegistration) {
	registrations := plugin_registrations(original.plugins, .client)!
	mut options := original
	for registration in registrations {
		if registration.value is ConnectionConfigurator {
			options = registration.value.configure_connection(options) or {
				return PluginError{
					plugin_name: registration.information.name
					target:      .connection
					cause:       err
				}
			}
		}
	}
	for registration in registrations {
		if registration.value is ClientConfigurator {
			options = registration.value.configure_client(options) or {
				return PluginError{
					plugin_name: registration.information.name
					target:      .client
					cause:       err
				}
			}
		}
	}
	// Registration changes made by hooks do not recurse or alter propagation.
	return ClientOptions{
		...options
		plugins: registrations.map(it.value)
	}, registrations
}

fn (runtime &Runtime) definitions_copy() Runtime {
	return Runtime{
		factories:            runtime.factories.clone()
		registrations:        runtime.registrations.clone()
		dynamic_factory:      runtime.dynamic_factory
		dynamic_registration: runtime.dynamic_registration
		local_handlers:       runtime.local_handlers.registry_copy()
	}
}

fn (activities Activities) registry_copy() Activities {
	return Activities{
		handlers:        activities.handlers.clone()
		dynamic_handler: activities.dynamic_handler
	}
}

fn (services NexusServices) registry_copy() NexusServices {
	return NexusServices{
		operations: services.operations.clone()
	}
}

fn prepare_worker_plugins(original WorkerOptions, runtime &Runtime, activities Activities, services NexusServices, role WorkerRole) !WorkerSetup {
	mut inherited := []PluginRegistration{}
	if client := original.client { inherited = client.plugins.clone() }
	mut registrations := inherited.clone()
	registrations << plugin_registrations(original.plugins, .worker)!
	mut setup := WorkerSetup{
		role:           role
		options:        original
		workflows:      runtime.definitions_copy()
		activities:     activities.registry_copy()
		nexus_services: services.registry_copy()
	}
	mut names := map[string]bool{}
	mut information := []PluginInfo{}
	for registration in registrations {
		if registration.information.name in names {
			emit_log(original.logger, .warn,
				'Multiple worker plugins with the same name were registered: ${registration.information.name}')
		}
		names[registration.information.name] = true
		information << registration.information
	}
	for registration in registrations {
		if registration.value is WorkerConfigurator {
			registration.value.configure_worker(mut setup) or {
				return PluginError{
					plugin_name: registration.information.name
					target:      .worker
					cause:       err
				}
			}
		}
	}
	setup.options = WorkerOptions{
		...setup.options
		plugins:            original.plugins.clone()
		plugin_information: information
	}
	return setup
}

fn prepare_replay_plugins(runtime &Runtime, original ReplayOptions) !ReplaySetup {
	registrations := plugin_registrations(original.plugins, .replay)!
	mut setup := ReplaySetup{
		options:   original
		workflows: runtime.definitions_copy()
	}
	for registration in registrations {
		if registration.value is ReplayConfigurator {
			registration.value.configure_replay(mut setup) or {
				return PluginError{
					plugin_name: registration.information.name
					target:      .replay
					cause:       err
				}
			}
		}
	}
	setup.options = ReplayOptions{
		...setup.options
		plugins: original.plugins.clone()
	}
	return setup
}

pub fn (client &Client) plugin_info() []PluginInfo {
	return client.plugins.map(it.information)
}
