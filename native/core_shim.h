#pragma once
#include "grpc_bridge.h"
#include "metric_export.h"
#include "include/temporal-sdk-core-c-bridge.h"
#include "slot_bridge.h"

typedef struct SpikeCore SpikeCore;
typedef struct {
    bool metric_attach_service_name;
    bool prom_counters_total_suffix;
    bool prom_unit_suffix;
    bool prom_durations_as_seconds;
    const char * prom_histogram_buckets;
    bool otel_delta;
    bool otel_durations_as_seconds;
    const char * otel_histogram_buckets;
    bool otel_exponential_histograms;
    uint32_t otel_exponential_max_size;
    int8_t otel_exponential_max_scale;
    const char * otel_exponential_name_suffix;
    uint64_t worker_heartbeat_millis;
    TemporalCoreRuntimeInfoArray runtime_info;
    bool disable_environment_info;
    const char *log_filter;
    bool forward_logs;
    const char *prometheus_address;
    const char *otel_url;
    const char *otel_headers;
    bool otel_http;
    uint32_t metric_period_millis;
    const char *metric_tags;
    const char *metric_prefix;
    SpikeMetricExport *metric_exporter;
} SpikeTelemetryOptions;
typedef struct {
    const char *target;
    const char *client_name;
    const char *client_version;
    bool retry_enabled;
    uint64_t retry_initial_interval_millis;
    double retry_randomization_factor;
    double retry_multiplier;
    uint64_t retry_max_interval_millis;
    uint64_t retry_max_elapsed_time_millis;
    uint32_t retry_max_retries;
    uint64_t dns_resolution_interval_millis;
    bool grpc_gzip;
    uint64_t payloads_warn_size;
    uint64_t memo_warn_size;
    const char *identity;
    const char *api_key;
    bool tls_enabled;
    const char *tls_domain;
    TemporalCoreByteArrayRef server_root_ca;
    TemporalCoreByteArrayRef client_cert;
    TemporalCoreByteArrayRef client_private_key;
    TemporalCoreMetadataRef metadata;
    TemporalCoreMetadataRef binary_metadata;
    uint64_t keep_alive_interval_millis;
    uint64_t keep_alive_timeout_millis;
    const char *proxy_target;
    const char *proxy_username;
    const char *proxy_password;
    SpikeTelemetryOptions telemetry;
    SpikeGrpcBridge *grpc_bridge;
} SpikeConnectOptions;
SpikeCore *spike_client_open(const SpikeConnectOptions *config);
void spike_update_api_key(SpikeCore *core, const char *api_key);
void spike_update_metadata(SpikeCore *core, TemporalCoreMetadataRef metadata, bool binary);
const TemporalCoreByteArray *spike_rpc(SpikeCore *core, uint32_t service, const char *method, const unsigned char *bytes, size_t size, uint32_t timeout_millis, TemporalCoreMetadataRef metadata, TemporalCoreMetadataRef binary_metadata, bool retry, const TemporalCoreCancellationToken *cancellation_token, const TemporalCoreByteArray **failure_details);
uint32_t spike_rpc_status(void);
typedef struct {
    bool enabled;
    size_t minimum;
    size_t maximum;
    size_t initial;
} SpikePollerAutoscaling;
typedef struct {
    TemporalCorePluginInfoArray plugins;
    bool disable_payload_error_limit;
    TemporalCoreByteArrayRefArray storage_drivers;
    SpikeSlotBridge *workflow_slot_bridge;
    SpikeSlotBridge *activity_slot_bridge;
    SpikeSlotBridge *local_activity_slot_bridge;
    SpikeSlotBridge *nexus_slot_bridge;
    const char *namespace_name;
    const char *queue;
    const char *build_id;
    const char *deployment_name;
    bool deployment_versioning;
    bool nondeterminism_as_workflow_fail;
    TemporalCoreByteArrayRefArray nondeterminism_as_workflow_fail_for_types;
    bool legacy_versioning;
    int32_t versioning_behavior;
    size_t workflow_slots;
    size_t activity_slots;
    size_t local_activity_slots;
    size_t nexus_slots;
    uint32_t max_cached_workflows;
    uint32_t max_eager_activity_reservations;
    size_t workflow_pollers;
    size_t activity_pollers;
    size_t nexus_pollers;
    SpikePollerAutoscaling workflow_poller_autoscaling;
    SpikePollerAutoscaling activity_poller_autoscaling;
    SpikePollerAutoscaling nexus_poller_autoscaling;
    uint64_t sticky_timeout_millis;
    uint64_t shutdown_grace_millis;
    uint64_t max_heartbeat_millis;
    uint64_t default_heartbeat_millis;
    double max_activities_per_second;
    double max_queue_activities_per_second;
    float nonsticky_poll_ratio;
    bool resource_based;
    double target_memory_usage;
    double target_cpu_usage;
    uint64_t ramp_throttle_millis;
    size_t minimum_workflow_slots;
    size_t minimum_activity_slots;
} SpikeWorkerOptions;
SpikeCore *spike_open(const SpikeConnectOptions *config, const SpikeWorkerOptions *worker_config, bool workflows, bool nexus, bool remote_activities);
SpikeCore *spike_open_shared(SpikeCore *client, const SpikeWorkerOptions *worker_config, bool workflows, bool nexus, bool remote_activities);
const char *spike_error(SpikeCore *core);
const TemporalCoreByteArray *spike_poll(SpikeCore *core);
int spike_complete(SpikeCore *core, const unsigned char *bytes, size_t size);
const TemporalCoreByteArray *spike_poll_workflow(SpikeCore *core);
int spike_complete_workflow(SpikeCore *core, const unsigned char *bytes, size_t size);
int spike_heartbeat(SpikeCore *core, const unsigned char *bytes, size_t size);
void spike_bytes_free(SpikeCore *core, const TemporalCoreByteArray *bytes);
SpikeCore *spike_replay_open(const char *namespace_name, const char *queue);
int spike_replay_push(SpikeCore *core, const char *workflow_id, const unsigned char *history, size_t size);
const TemporalCoreByteArray *spike_poll_nexus(SpikeCore *core);
int spike_complete_nexus(SpikeCore *core, const unsigned char *bytes, size_t size);
void spike_shutdown(SpikeCore *core);
bool spike_shutdown_grace_expired(SpikeCore *core);
void spike_close(SpikeCore *core);

typedef struct {
    const char *executable;
    const char *namespace_name;
    const char *ip;
    const char *database_filename;
    const char *extra_args;
    uint16_t port;
    uint64_t startup_timeout_millis;
    bool time_skipping;
} SpikeServerOptions;
SpikeCore *spike_server_open(const SpikeServerOptions *settings);
const char *spike_server_target(SpikeCore *core);

void spike_log_line(const unsigned char *bytes, size_t size);

uint64_t spike_protocol_capabilities(void);

SpikeCore *spike_retain_client(SpikeCore *core);
