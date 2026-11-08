module corebridge

@[typedef]
struct C.SpikePollerAutoscaling {
	enabled bool
	minimum u32
	maximum u32
	initial u32
}

pub struct PollerAutoscaling {
pub:
	enabled bool
	minimum int
	maximum int
	initial int
}

fn (options PollerAutoscaling) native_options() C.SpikePollerAutoscaling {
	return C.SpikePollerAutoscaling{
		enabled: options.enabled
		minimum: u32(options.minimum)
		maximum: u32(options.maximum)
		initial: u32(options.initial)
	}
}

@[typedef]
struct C.SpikeWorkerOptions {
	plugins                                   C.TemporalCorePluginInfoArray
	disable_payload_error_limit               bool
	storage_drivers                           C.TemporalCoreByteArrayRefArray
	workflow_slot_bridge                      &C.SpikeSlotBridge
	activity_slot_bridge                      &C.SpikeSlotBridge
	local_activity_slot_bridge                &C.SpikeSlotBridge
	nexus_slot_bridge                         &C.SpikeSlotBridge
	namespace_name                            &char
	queue                                     &char
	build_id                                  &char
	deployment_name                           &char
	deployment_versioning                     bool
	nondeterminism_as_workflow_fail           bool
	nondeterminism_as_workflow_fail_for_types C.TemporalCoreByteArrayRefArray
	legacy_versioning                         bool
	versioning_behavior                       i32
	workflow_slots                            u32
	activity_slots                            u32
	local_activity_slots                      u32
	nexus_slots                               u32
	max_cached_workflows                      u32
	max_eager_activity_reservations           u32
	workflow_pollers                          u32
	activity_pollers                          u32
	nexus_pollers                             u32
	workflow_poller_autoscaling               C.SpikePollerAutoscaling
	activity_poller_autoscaling               C.SpikePollerAutoscaling
	nexus_poller_autoscaling                  C.SpikePollerAutoscaling
	sticky_timeout_millis                     u64
	shutdown_grace_millis                     u64
	max_heartbeat_millis                      u64
	default_heartbeat_millis                  u64
	max_activities_per_second                 f64
	max_queue_activities_per_second           f64
	nonsticky_poll_ratio                      f32
	resource_based                            bool
	target_memory_usage                       f64
	target_cpu_usage                          f64
	ramp_throttle_millis                      u64
	minimum_workflow_slots                    u32
	minimum_activity_slots                    u32
}

pub struct PluginInfo {
pub:
	name    string
	version string
}

@[typedef]
struct C.TemporalCorePluginInfo {
	name    C.TemporalCoreByteArrayRef
	version C.TemporalCoreByteArrayRef
}

@[typedef]
struct C.TemporalCorePluginInfoArray {
	data &C.TemporalCorePluginInfo
	size u32
}

fn (config Config) native_worker_options() C.SpikeWorkerOptions {
	mut plugins := []C.TemporalCorePluginInfo{}
	for plugin in config.plugins {
		plugins << C.TemporalCorePluginInfo{
			name:    C.TemporalCoreByteArrayRef{
				data: plugin.name.str
				size: u32(plugin.name.len)
			}
			version: C.TemporalCoreByteArrayRef{
				data: plugin.version.str
				size: u32(plugin.version.len)
			}
		}
	}
	values := refs(config.nondeterminism_as_workflow_fail_for_types.map(it.bytes()))
	drivers := refs(config.storage_drivers.map(it.bytes()))
	return C.SpikeWorkerOptions{
		plugins:                                   C.TemporalCorePluginInfoArray{
			data: plugins.data
			size: u32(plugins.len)
		}
		disable_payload_error_limit:               config.disable_payload_error_limit
		storage_drivers:                           C.TemporalCoreByteArrayRefArray{
			data: drivers.data
			size: u32(drivers.len)
		}
		workflow_slot_bridge:                      if bridge := config.workflow_slot_bridge {
			bridge.pointer
		} else {
			unsafe { &C.SpikeSlotBridge(nil) }
		}
		activity_slot_bridge:                      if bridge := config.activity_slot_bridge {
			bridge.pointer
		} else {
			unsafe { &C.SpikeSlotBridge(nil) }
		}
		local_activity_slot_bridge:                if bridge := config.local_activity_slot_bridge {
			bridge.pointer
		} else {
			unsafe { &C.SpikeSlotBridge(nil) }
		}
		nexus_slot_bridge:                         if bridge := config.nexus_slot_bridge {
			bridge.pointer
		} else {
			unsafe { &C.SpikeSlotBridge(nil) }
		}
		workflow_poller_autoscaling:               config.workflow_poller_autoscaling.native_options()
		activity_poller_autoscaling:               config.activity_poller_autoscaling.native_options()
		nexus_poller_autoscaling:                  config.nexus_poller_autoscaling.native_options()
		namespace_name:                            config.namespace.str
		queue:                                     config.task_queue.str
		build_id:                                  config.build_id.str
		deployment_name:                           config.deployment_name.str
		deployment_versioning:                     config.deployment_versioning
		nondeterminism_as_workflow_fail:           config.nondeterminism_as_workflow_fail
		nondeterminism_as_workflow_fail_for_types: C.TemporalCoreByteArrayRefArray{
			data: values.data
			size: u32(values.len)
		}
		legacy_versioning:                         config.legacy_versioning
		versioning_behavior:                       i32(config.versioning_behavior)
		workflow_slots:                            u32(config.workflow_concurrency)
		activity_slots:                            u32(config.activity_concurrency)
		local_activity_slots:                      u32(config.local_activity_concurrency)
		nexus_slots:                               u32(config.nexus_concurrency)
		max_cached_workflows:                      config.max_cached_workflows
		max_eager_activity_reservations:           config.max_eager_activity_reservations
		workflow_pollers:                          u32(config.workflow_pollers)
		activity_pollers:                          u32(config.activity_pollers)
		nexus_pollers:                             u32(config.nexus_pollers)
		sticky_timeout_millis:                     config.sticky_timeout_millis
		shutdown_grace_millis:                     config.shutdown_grace_millis
		max_heartbeat_millis:                      config.max_heartbeat_millis
		default_heartbeat_millis:                  config.default_heartbeat_millis
		max_activities_per_second:                 config.max_activities_per_second
		max_queue_activities_per_second:           config.max_queue_activities_per_second
		nonsticky_poll_ratio:                      config.nonsticky_poll_ratio
		resource_based:                            config.resource_based
		target_memory_usage:                       config.target_memory_usage
		target_cpu_usage:                          config.target_cpu_usage
		ramp_throttle_millis:                      config.ramp_throttle_millis
		minimum_workflow_slots:                    u32(config.minimum_workflow_slots)
		minimum_activity_slots:                    u32(config.minimum_activity_slots)
	}
}
