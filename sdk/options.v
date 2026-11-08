module sdk

import alexvyber.temporal_v.corebridge
import os
import strconv
import time
import math

pub enum VersioningBehavior {
	unspecified
	pinned
	auto_upgrade
}

pub struct WorkerDeployment {
pub:
	name             string
	use_versioning   bool               = true
	default_behavior VersioningBehavior = .pinned
}

pub struct ResourceTuner {
pub:
	target_memory_usage    f64           = 0.8
	target_cpu_usage       f64           = 0.8
	ramp_throttle          time.Duration = 50 * time.millisecond
	minimum_workflow_slots int           = 2
	minimum_activity_slots int           = 1
}

pub struct PollerAutoscaling {
pub:
	minimum int = 1
	maximum int = 5
	initial int = 2
}

fn poller_autoscaling(options ?PollerAutoscaling) !corebridge.PollerAutoscaling {
	if value := options {
		if value.minimum < 1 || value.maximum > 128 || value.maximum < value.minimum
			|| value.initial < value.minimum || value.initial > value.maximum {
			return error('invalid poller autoscaling bounds')
		}
		return corebridge.PollerAutoscaling{
			enabled: true
			minimum: value.minimum
			maximum: value.maximum
			initial: value.initial
		}
	}
	return corebridge.PollerAutoscaling{}
}

pub enum WorkflowPanicPolicy {
	block_workflow
	fail_workflow
}

pub struct WorkerOptions {
	plugin_information []PluginInfo
pub:
	worker_interceptors                       []WorkerInterceptor      @[json: '-']
	patch_activation_callback                 ?PatchActivationCallback @[json: '-']
	experimental_event_groups                 bool
	workflow_panic_policy                     WorkflowPanicPolicy
	tracing                                   ?TraceOptions
	plugins                                   []Plugin
	disable_payload_error_limit               bool
	client                                    ?&Client
	nondeterminism_as_workflow_fail           bool
	nondeterminism_as_workflow_fail_for_types []string
	custom_tuner                              ?CustomTuner
	address                                   string = 'http://127.0.0.1:7233'
	namespace                                 string = 'default'
	task_queue                                string = 'v-sdk'
	activity_concurrency                      int    = 2
	transport                                 TransportOptions
	data_converter                            DataConverter
	logger                                    Logger = ConsoleLogger{}
	telemetry                                 TelemetryOptions
	activity_interceptors                     []ActivityInterceptor
	command_interceptors                      []CommandInterceptor
	workflow_interceptors                     []WorkflowInterceptor
	query_interceptors                        []QueryInterceptor
	update_validator_interceptors             []UpdateValidatorInterceptor
	control                                   &WorkerControl = &WorkerControl{}
	workflow_concurrency                      int            = 10
	local_activity_concurrency                int            = 2
	nexus_concurrency                         int            = 2
	max_cached_workflows                      u32            = 10
	max_eager_activity_reservations           int            = 3
	workflow_pollers                          int            = 2
	activity_pollers                          int            = 2
	nexus_pollers                             int            = 2
	workflow_poller_autoscaling               ?PollerAutoscaling
	activity_poller_autoscaling               ?PollerAutoscaling
	nexus_poller_autoscaling                  ?PollerAutoscaling
	build_id                                  string = 'temporal-v-dev'
	deployment                                ?WorkerDeployment
	legacy_build_id_versioning                bool
	sticky_schedule_to_start                  time.Duration = time.second
	graceful_shutdown                         time.Duration = time.second
	max_heartbeat_throttle                    time.Duration = 100 * time.millisecond
	default_heartbeat_throttle                time.Duration = 100 * time.millisecond
	max_activities_per_second                 f64
	max_task_queue_activities_per_second      f64
	nonsticky_to_sticky_poll_ratio            f32 = 0.2
	resource_tuner                            ?ResourceTuner
}

fn env_integer(name string, default_value int) !int {
	value := os.getenv_opt(name) or { return default_value }
	return int(strconv.atoi64(value)!)
}

pub fn options_from_env() !WorkerOptions {
	mut deployment := ?WorkerDeployment(none)
	name := os.getenv('TEMPORAL_DEPLOYMENT_NAME')
	if name != '' {
		behavior := match os.getenv_opt('TEMPORAL_VERSIONING_BEHAVIOR') {
			'pinned' { VersioningBehavior.pinned }
			'auto_upgrade' { VersioningBehavior.auto_upgrade }
			else { return error('TEMPORAL_VERSIONING_BEHAVIOR must be pinned or auto_upgrade') }
		} or { 'pinned' }

		deployment = WorkerDeployment{
			name:             name
			default_behavior: behavior
		}
	}
	cache := env_integer('TEMPORAL_MAX_CACHED_WORKFLOWS', 10)!
	if cache < 0 || cache > 4294967295 { return error('invalid workflow cache size') }
	mut tuner := ?ResourceTuner(none)
	resource := os.getenv('TEMPORAL_RESOURCE_TUNER')
	if resource !in ['', 'true', 'false'] {
		return error('TEMPORAL_RESOURCE_TUNER must be true or false')
	}
	if resource == 'true' { tuner = ResourceTuner{} }
	return WorkerOptions{
		transport:                  transport_from_env()!
		telemetry:                  telemetry_from_env()!
		logger:                     logger_from_env()!
		address:                    os.getenv_opt('TEMPORAL_ADDRESS') or { 'http://127.0.0.1:7233' }
		namespace:                  os.getenv_opt('TEMPORAL_NAMESPACE') or { 'default' }
		task_queue:                 os.getenv_opt('TEMPORAL_TASK_QUEUE') or { 'v-sdk-spike' }
		activity_concurrency:       env_integer('TEMPORAL_ACTIVITY_CONCURRENCY', 2)!
		workflow_concurrency:       env_integer('TEMPORAL_WORKFLOW_CONCURRENCY', 10)!
		local_activity_concurrency: env_integer('TEMPORAL_LOCAL_ACTIVITY_CONCURRENCY', 2)!
		nexus_concurrency:          env_integer('TEMPORAL_NEXUS_CONCURRENCY', 2)!
		max_cached_workflows:       u32(cache)
		build_id:                   os.getenv_opt('TEMPORAL_BUILD_ID') or { 'temporal-v-dev' }
		deployment:                 deployment
		resource_tuner:             tuner
	}
}

fn (options WorkerOptions) core_config() !corebridge.Config {
	if client := options.client {
		if client.options.namespace != options.namespace {
			return error('worker namespace must match the shared client namespace')
		}
	}

	if options.max_eager_activity_reservations < 0 || options.max_eager_activity_reservations > 128 {
		return error('eager Activity reservation limit must be 0–128')
	}
	if options.namespace == '' || options.task_queue == '' || options.build_id == '' {
		return error('worker namespace, task queue and build ID are required')
	}
	for count in [options.workflow_concurrency, options.activity_concurrency,
		options.local_activity_concurrency, options.nexus_concurrency, options.workflow_pollers,
		options.activity_pollers, options.nexus_pollers] {
		if count < 1 || count > 128 { return error('worker slots and pollers must be 1–128') }
	}
	if options.max_cached_workflows > 0 && options.workflow_pollers < 2
		&& options.workflow_poller_autoscaling == none {
		return error('sticky workflow polling requires at least two workflow pollers')
	}
	if options.max_cached_workflows > 0 && options.workflow_concurrency < 2 {
		return error('sticky workflow polling requires at least two workflow slots')
	}
	if options.graceful_shutdown < 0 || options.sticky_schedule_to_start < time.millisecond
		|| options.default_heartbeat_throttle < time.millisecond
		|| options.max_heartbeat_throttle < options.default_heartbeat_throttle {
		return error('invalid worker timeouts/throttles')
	}
	for rate in [options.max_activities_per_second, options.max_task_queue_activities_per_second] {
		if rate < 0 || math.is_nan(rate) || math.is_inf(rate, 0) {
			return error('invalid worker Activity rate limit')
		}
	}
	if options.nonsticky_to_sticky_poll_ratio < 0 || options.nonsticky_to_sticky_poll_ratio > 1
		|| math.is_nan(f64(options.nonsticky_to_sticky_poll_ratio)) {
		return error('invalid nonsticky/sticky poll ratio')
	}
	mut storage_drivers := []string{}
	if storage := options.data_converter.external_storage {
		for driver in storage.options.drivers {
			storage_drivers << driver.driver_type()
		}
	}
	mut connection := ?&corebridge.Connection(none)
	if client := options.client { connection = client.connection }
	mut config := corebridge.Config{
		plugins:                                   options.plugin_information.map(corebridge.PluginInfo{
			name:    it.name
			version: it.version
		})
		disable_payload_error_limit:               options.disable_payload_error_limit
		storage_drivers:                           storage_drivers
		connection:                                connection
		workflow_poller_autoscaling:               poller_autoscaling(options.workflow_poller_autoscaling)!
		activity_poller_autoscaling:               poller_autoscaling(options.activity_poller_autoscaling)!
		nexus_poller_autoscaling:                  poller_autoscaling(options.nexus_poller_autoscaling)!
		transport:                                 options.transport.core_transport()!
		telemetry:                                 options.telemetry.core_telemetry()!
		address:                                   options.address
		namespace:                                 options.namespace
		task_queue:                                options.task_queue
		activity_concurrency:                      options.activity_concurrency
		workflow_concurrency:                      options.workflow_concurrency
		local_activity_concurrency:                options.local_activity_concurrency
		nexus_concurrency:                         options.nexus_concurrency
		max_eager_activity_reservations:           u32(options.max_eager_activity_reservations)
		max_cached_workflows:                      options.max_cached_workflows
		workflow_pollers:                          options.workflow_pollers
		activity_pollers:                          options.activity_pollers
		nexus_pollers:                             options.nexus_pollers
		build_id:                                  options.build_id
		nondeterminism_as_workflow_fail:           options.nondeterminism_as_workflow_fail
		nondeterminism_as_workflow_fail_for_types: options.nondeterminism_as_workflow_fail_for_types
		legacy_versioning:                         options.legacy_build_id_versioning
		sticky_timeout_millis:                     u64(options.sticky_schedule_to_start / time.millisecond)
		shutdown_grace_millis:                     u64(options.graceful_shutdown / time.millisecond)
		max_heartbeat_millis:                      u64(options.max_heartbeat_throttle / time.millisecond)
		default_heartbeat_millis:                  u64(options.default_heartbeat_throttle / time.millisecond)
		max_activities_per_second:                 options.max_activities_per_second
		max_queue_activities_per_second:           options.max_task_queue_activities_per_second
		nonsticky_poll_ratio:                      options.nonsticky_to_sticky_poll_ratio
	}
	if deployment := options.deployment {
		if deployment.name == '' || options.legacy_build_id_versioning {
			return error('invalid or conflicting worker deployment settings')
		}
		if deployment.use_versioning && deployment.default_behavior == .unspecified {
			return error('versioned deployment requires a default behavior')
		}
		config = corebridge.Config{
			...config
			deployment_name:       deployment.name
			deployment_versioning: deployment.use_versioning
			versioning_behavior:   int(deployment.default_behavior)
		}
	}
	if tuner := options.resource_tuner {
		for target in [tuner.target_memory_usage, tuner.target_cpu_usage] {
			if target <= 0 || target >= 1 || math.is_nan(target) {
				return error('resource targets must be between zero and one')
			}
		}
		if tuner.ramp_throttle < 0 || tuner.minimum_workflow_slots < 1
			|| tuner.minimum_workflow_slots > options.workflow_concurrency
			|| tuner.minimum_activity_slots < 1
			|| tuner.minimum_activity_slots > options.activity_concurrency
			|| tuner.minimum_activity_slots > options.local_activity_concurrency
			|| tuner.minimum_activity_slots > options.nexus_concurrency {
			return error('invalid resource slot limits')
		}
		config = corebridge.Config{
			...config
			resource_based:         true
			target_memory_usage:    tuner.target_memory_usage
			target_cpu_usage:       tuner.target_cpu_usage
			ramp_throttle_millis:   u64(tuner.ramp_throttle / time.millisecond)
			minimum_workflow_slots: tuner.minimum_workflow_slots
			minimum_activity_slots: tuner.minimum_activity_slots
		}
	}
	return config
}
