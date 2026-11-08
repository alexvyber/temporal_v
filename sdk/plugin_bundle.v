module sdk

pub struct WorkflowRegistration {
pub:
	name    string
	factory WorkflowFactory @[required]
	options WorkflowRegistrationOptions
}

pub struct ActivityRegistration {
pub:
	name    string
	handler ActivityHandler @[required]
}

pub struct NexusRegistration {
pub:
	service   string
	operation string
	handler   NexusOperation @[required]
}

// Collections append; a supplied converter replaces the current converter.
// Custom configurators can implement other policies using the same setup API.
pub struct PluginBundle {
pub:
	information                   PluginInfo
	data_converter                ?DataConverter
	client_interceptors           []ClientInterceptor
	activity_interceptors         []ActivityInterceptor
	workflow_interceptors         []WorkflowInterceptor
	command_interceptors          []CommandInterceptor
	query_interceptors            []QueryInterceptor
	update_validator_interceptors []UpdateValidatorInterceptor
	workflows                     []WorkflowRegistration
	activities                    []ActivityRegistration
	local_activities              []ActivityRegistration
	nexus_services                []NexusRegistration
}

pub fn (plugin PluginBundle) name() string {
	return plugin.information.name
}

pub fn (plugin PluginBundle) version() string {
	return plugin.information.version
}

// V requires a one-letter generic parameter name.
fn plugin_append[E](current []E, additions []E) []E {
	mut result := current.clone()
	result << additions
	return result
}

pub fn (plugin PluginBundle) configure_client(options ClientOptions) !ClientOptions {
	return ClientOptions{
		...options
		data_converter: plugin.data_converter or { options.data_converter }
		interceptors:   plugin_append(options.interceptors, plugin.client_interceptors)
	}
}

pub fn (plugin PluginBundle) configure_worker(mut setup WorkerSetup) ! {
	options := setup.options
	setup.options = WorkerOptions{
		...options
		data_converter:                plugin.data_converter or { options.data_converter }
		activity_interceptors:         plugin_append(options.activity_interceptors,
			plugin.activity_interceptors)
		workflow_interceptors:         plugin_append(options.workflow_interceptors,
			plugin.workflow_interceptors)
		command_interceptors:          plugin_append(options.command_interceptors,
			plugin.command_interceptors)
		query_interceptors:            plugin_append(options.query_interceptors,
			plugin.query_interceptors)
		update_validator_interceptors: plugin_append(options.update_validator_interceptors,
			plugin.update_validator_interceptors)
	}
	for definition in plugin.workflows {
		setup.workflows.register(definition.name, definition.factory, definition.options)!
	}
	for definition in plugin.activities {
		setup.activities.register(definition.name, definition.handler)!
	}
	for definition in plugin.local_activities {
		setup.workflows.register_local_activity(definition.name, definition.handler)!
	}
	for definition in plugin.nexus_services {
		setup.nexus_services.register(definition.service, definition.operation, definition.handler)!
	}
}

pub fn (plugin PluginBundle) configure_replay(mut setup ReplaySetup) ! {
	options := setup.options
	setup.options = ReplayOptions{
		...options
		data_converter:                plugin.data_converter or { options.data_converter }
		command_interceptors:          plugin_append(options.command_interceptors,
			plugin.command_interceptors)
		workflow_interceptors:         plugin_append(options.workflow_interceptors,
			plugin.workflow_interceptors)
		query_interceptors:            plugin_append(options.query_interceptors,
			plugin.query_interceptors)
		update_validator_interceptors: plugin_append(options.update_validator_interceptors,
			plugin.update_validator_interceptors)
	}
	for definition in plugin.workflows {
		setup.workflows.register(definition.name, definition.factory, definition.options)!
	}
}
