#include "core_shim.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <stdatomic.h>
#include <time.h>

static _Thread_local char last_error[4096];

static void copy_error(const TemporalCoreByteArray *bytes) {
    size_t size = bytes->size;
    if (size >= sizeof(last_error)) {
        size = sizeof(last_error) - 1;
        /* Do not split a UTF-8 code point at the truncation boundary. */
        while (size && (bytes->data[size] & 0xc0) == 0x80) --size;
    }
    memcpy(last_error, bytes->data, size);
    last_error[size] = 0;
}

static _Thread_local uint32_t last_rpc_status;
static volatile sig_atomic_t shutdown_requested;
static pthread_mutex_t signal_mutex = PTHREAD_MUTEX_INITIALIZER;
static size_t signal_users;
static struct sigaction previous_term;
static struct sigaction previous_int;

static void request_shutdown(int signal_number) {
    (void)signal_number;
    shutdown_requested = 1;
}

static int acquire_signals(void) {
    pthread_mutex_lock(&signal_mutex);
    if (!signal_users) {
        struct sigaction action = {0};
        action.sa_handler = request_shutdown;
        sigemptyset(&action.sa_mask);
        if (sigaction(SIGTERM, &action, &previous_term)) {
            pthread_mutex_unlock(&signal_mutex);
            return -1;
        }
        if (sigaction(SIGINT, &action, &previous_int)) {
            sigaction(SIGTERM, &previous_term, NULL);
            pthread_mutex_unlock(&signal_mutex);
            return -1;
        }
        shutdown_requested = 0;
    }
    signal_users++;
    pthread_mutex_unlock(&signal_mutex);
    return 0;
}

static void release_signals(void) {
    pthread_mutex_lock(&signal_mutex);
    if (--signal_users == 0) {
        sigaction(SIGTERM, &previous_term, NULL);
        sigaction(SIGINT, &previous_int, NULL);
    }
    pthread_mutex_unlock(&signal_mutex);
}

/* Rust callbacks stay in C so V's allocator never runs on Tokio-owned threads. */
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t ready;
    bool done;
    void *connection;
    const TemporalCoreByteArray *bytes;
    const TemporalCoreByteArray *fail;
    const TemporalCoreByteArray *details;
    uint32_t status;
} SpikeWait;

struct SpikeCore {
    atomic_size_t references;
    SpikeGrpcBridge *grpc_bridge;
    struct SpikeCore *shared_owner;
    TemporalCoreRuntime *runtime;
    TemporalCoreConnection *connection;
    TemporalCoreWorker *worker;
    TemporalCoreEphemeralServer *server;
    char server_target[512];
    pthread_t signal_monitor;
    bool monitor_started;
    bool finalize_required;
    TemporalCoreWorkerReplayPusher *replay_pusher;
    bool signals_owned;
    atomic_bool stop_monitor;
    atomic_uint_fast64_t shutdown_started_millis;
    uint64_t shutdown_grace_millis;
};

static void *monitor_signals(void *data) {
    SpikeCore *core = data;
    while (!atomic_load(&core->stop_monitor)) {
        if (shutdown_requested) {
            spike_shutdown(core);
            break;
        }
        struct timespec interval = {.tv_nsec = 10000000};
        nanosleep(&interval, NULL);
    }
    return NULL;
}

static TemporalCoreByteArrayRef text_ref(const char *text) {
    return (TemporalCoreByteArrayRef){(const uint8_t *)text, strlen(text)};
}

/* Rust slices require a non-null pointer even when their length is zero. */
static TemporalCoreByteArrayRef bytes_ref(const unsigned char *bytes, size_t size) {
    static const uint8_t empty = 0;
    return (TemporalCoreByteArrayRef){size ? bytes : &empty, size};
}

#define SPIKE_WAIT_INITIALIZER { .mutex = PTHREAD_MUTEX_INITIALIZER, .ready = PTHREAD_COND_INITIALIZER }

static void wait_done(SpikeWait *wait) {
    wait->done = true;
    pthread_cond_signal(&wait->ready);
    pthread_mutex_unlock(&wait->mutex);
}

static void connected(void *data, TemporalCoreConnection *connection, const TemporalCoreByteArray *fail) {
    SpikeWait *wait = data;
    pthread_mutex_lock(&wait->mutex);
    wait->connection = connection;
    wait->fail = fail;
    wait_done(wait);
}

static void completed(void *data, const TemporalCoreByteArray *fail) {
    SpikeWait *wait = data;
    pthread_mutex_lock(&wait->mutex);
    wait->fail = fail;
    wait_done(wait);
}

static void polled(void *data, const TemporalCoreByteArray *bytes, const TemporalCoreByteArray *fail) {
    SpikeWait *wait = data;
    pthread_mutex_lock(&wait->mutex);
    wait->bytes = bytes;
    wait->fail = fail;
    wait_done(wait);
}

static void server_started(void *data, TemporalCoreEphemeralServer *server, const TemporalCoreByteArray *target, const TemporalCoreByteArray *fail) {
    SpikeWait *wait = data;
    pthread_mutex_lock(&wait->mutex);
    wait->connection = server;
    wait->bytes = target;
    wait->fail = fail;
    wait_done(wait);
}

static void rpc_completed(void *data, const TemporalCoreByteArray *bytes, uint32_t status,
                          const TemporalCoreByteArray *fail, const TemporalCoreByteArray *details) {
    SpikeWait *wait = data;
    pthread_mutex_lock(&wait->mutex);
    wait->bytes = bytes;
    wait->fail = fail;
    wait->details = details;
    wait->status = status;
    wait_done(wait);
}

static int await_result(SpikeCore *core, SpikeWait *wait) {
    pthread_mutex_lock(&wait->mutex);
    while (!wait->done) pthread_cond_wait(&wait->ready, &wait->mutex);
    pthread_mutex_unlock(&wait->mutex);
    pthread_cond_destroy(&wait->ready);
    pthread_mutex_destroy(&wait->mutex);
    if (!wait->fail) return 0;
    copy_error(wait->fail);
    temporal_core_byte_array_free(core->runtime, wait->fail);
    return -1;
}

static void log_json_string(TemporalCoreByteArrayRef value) {
    fputc('"', stderr);
    for (size_t index = 0; index < value.size; index++) {
        unsigned char character = value.data[index];
        if (character == '"' || character == '\\') { fputc('\\', stderr); fputc(character, stderr); }
        else if (character < 32) fprintf(stderr, "\\u%04x", character);
        else fputc(character, stderr);
    }
    fputc('"', stderr);
}

/* Forward Core logs entirely in C: no V allocation/callback on Tokio threads. */
static void forwarded_log(TemporalCoreForwardedLogLevel level, const TemporalCoreForwardedLog *log) {
    const char *levels[] = {"trace", "debug", "info", "warn", "error"};
    TemporalCoreByteArrayRef fields = temporal_core_forwarded_log_fields_json(log);
    flockfile(stderr);
    fprintf(stderr, "{\"source\":\"temporal-core\",\"level\":\"%s\",\"timestamp_millis\":%llu,\"target\":",
            level <= TemporalCoreForwardedLogLevel_Error ? levels[level] : "unknown",
            (unsigned long long)temporal_core_forwarded_log_timestamp_millis(log));
    log_json_string(temporal_core_forwarded_log_target(log));
    fputs(",\"message\":", stderr);
    log_json_string(temporal_core_forwarded_log_message(log));
    fputs(",\"fields\":", stderr);
    if (fields.size) fwrite(fields.data, 1, fields.size, stderr);
    else fputs("{}", stderr);
    fputs("}\n", stderr);
    funlockfile(stderr);
}

static SpikeCore *runtime_open(const SpikeTelemetryOptions *settings) {
    last_error[0] = 0;
    SpikeCore *core = calloc(1, sizeof(*core));
    if (!core) return NULL;
    atomic_init(&core->references, 1);
    atomic_init(&core->stop_monitor, false);
    atomic_init(&core->shutdown_started_millis, 0);
    TemporalCoreLoggingOptions logging = {0};
    TemporalCorePrometheusOptions prometheus = {0};
    TemporalCoreOpenTelemetryOptions otel = {0};
    TemporalCoreMetricsOptions metrics = {0};
    TemporalCoreTelemetryOptions telemetry = {0};
    TemporalCoreRuntimeOptions runtime_options = {0};
    if (settings) {
        runtime_options.worker_heartbeat_interval_millis = settings->worker_heartbeat_millis;
        runtime_options.runtime_info = settings->runtime_info;
        runtime_options.disable_environment_info = settings->disable_environment_info;
        if (settings->log_filter[0]) {
            logging.filter = text_ref(settings->log_filter);
            logging.forward_to = settings->forward_logs ? forwarded_log : NULL;
            telemetry.logging = &logging;
        }
        if (settings->prometheus_address[0]) {
            prometheus.bind_address = text_ref(settings->prometheus_address);
            prometheus.counters_total_suffix = settings->prom_counters_total_suffix;
            prometheus.unit_suffix = settings->prom_unit_suffix;
            prometheus.durations_as_seconds = settings->prom_durations_as_seconds;
            prometheus.histogram_bucket_overrides = text_ref(settings->prom_histogram_buckets);
            metrics.prometheus = &prometheus;
        }
        if (settings->otel_url[0]) {
            otel.url = text_ref(settings->otel_url);
            otel.headers = text_ref(settings->otel_headers);
            otel.protocol = settings->otel_http ? TemporalCoreOpenTelemetryProtocol_Http : TemporalCoreOpenTelemetryProtocol_Grpc;
            otel.metric_periodicity_millis = settings->metric_period_millis;
            otel.metric_temporality = settings->otel_delta ? TemporalCoreOpenTelemetryMetricTemporality_Delta : TemporalCoreOpenTelemetryMetricTemporality_Cumulative;
            otel.durations_as_seconds = settings->otel_durations_as_seconds;
            otel.histogram_bucket_overrides = text_ref(settings->otel_histogram_buckets);
            otel.histogram_aggregation = settings->otel_exponential_histograms ? TemporalCoreOpenTelemetryHistogramAggregation_Base2Exponential : TemporalCoreOpenTelemetryHistogramAggregation_ExplicitBuckets;
            otel.exponential_histogram_max_size = settings->otel_exponential_max_size;
            otel.exponential_histogram_max_scale = settings->otel_exponential_max_scale;
            otel.exponential_histogram_max_scale_set = true;
            otel.exponential_histogram_name_suffix = text_ref(settings->otel_exponential_name_suffix);
            metrics.opentelemetry = &otel;
        }
        if (settings->metric_exporter) {
            metrics.custom_meter = spike_metric_export_claim(settings->metric_exporter);
            if (!metrics.custom_meter) {
                snprintf(last_error, sizeof(last_error), "custom metric exporter already claimed");
                return core;
            }
        }
        if (metrics.prometheus || metrics.opentelemetry || metrics.custom_meter) {
            metrics.attach_service_name = settings->metric_attach_service_name;
            metrics.global_tags = text_ref(settings->metric_tags);
            metrics.metric_prefix = text_ref(settings->metric_prefix);
            telemetry.metrics = &metrics;
        }
        if (telemetry.logging || telemetry.metrics) runtime_options.telemetry = &telemetry;
    }
    TemporalCoreRuntimeOrFail runtime = temporal_core_runtime_new(&runtime_options);
    core->runtime = runtime.runtime;
    if (runtime.fail) {
        SpikeWait wait = SPIKE_WAIT_INITIALIZER;
        wait.fail = runtime.fail;
        wait.done = true;
        await_result(core, &wait);
        return core;
    }
    return core;
}

SpikeCore *spike_server_open(const SpikeServerOptions *settings) {
    SpikeCore *core = runtime_open(NULL);
    if (!core || last_error[0]) return core;
    TemporalCoreTestServerOptions test = {
        .existing_path = text_ref(settings->executable),
        .sdk_name = text_ref("temporal-v"),
        .sdk_version = text_ref("1.0.0-rc.1"),
        .download_version = text_ref(""),
        .download_dest_dir = text_ref(""),
        .port = settings->port,
        .extra_args = text_ref(settings->extra_args),
        .startup_timeout_millis = settings->startup_timeout_millis,
    };
    SpikeWait wait = SPIKE_WAIT_INITIALIZER;
    if (settings->time_skipping) {
        temporal_core_ephemeral_server_start_test_server(core->runtime, &test, &wait, server_started);
    } else {
        TemporalCoreDevServerOptions dev = {
            .test_server = &test,
            .namespace_ = text_ref(settings->namespace_name),
            .ip = text_ref(settings->ip),
            .database_filename = text_ref(settings->database_filename),
            .log_format = text_ref("json"),
            .log_level = text_ref("warn"),
        };
        temporal_core_ephemeral_server_start_dev_server(core->runtime, &dev, &wait, server_started);
    }
    if (await_result(core, &wait)) return core;
    core->server = wait.connection;
    if (wait.bytes) {
        snprintf(core->server_target, sizeof(core->server_target), "%.*s", (int)wait.bytes->size, wait.bytes->data);
        temporal_core_byte_array_free(core->runtime, wait.bytes);
    }
    return core;
}

const char *spike_server_target(SpikeCore *core) { return core->server_target; }

SpikeCore *spike_client_open(const SpikeConnectOptions *config) {
    SpikeCore *core = runtime_open(&config->telemetry);
    if (core && config->grpc_bridge) {
        if (!spike_grpc_bridge_attach_cpp(config->grpc_bridge)) {
            snprintf(last_error, sizeof(last_error), "custom gRPC bridge already attached");
            return core;
        }
        core->grpc_bridge = config->grpc_bridge;
    }
    if (!core || last_error[0]) return core;
    TemporalCoreClientTlsOptions tls = {
        .domain = text_ref(config->tls_domain),
        .server_root_ca_cert = config->server_root_ca,
        .client_cert = config->client_cert,
        .client_private_key = config->client_private_key,
    };
    TemporalCoreClientKeepAliveOptions keep_alive = {
        .interval_millis = config->keep_alive_interval_millis,
        .timeout_millis = config->keep_alive_timeout_millis,
    };
    TemporalCoreClientHttpConnectProxyOptions proxy = {
        .target_host = text_ref(config->proxy_target),
        .username = text_ref(config->proxy_username),
        .password = text_ref(config->proxy_password),
    };
    TemporalCoreClientRetryOptions retry = {
        .initial_interval_millis = config->retry_initial_interval_millis,
        .randomization_factor = config->retry_randomization_factor,
        .multiplier = config->retry_multiplier,
        .max_interval_millis = config->retry_max_interval_millis,
        .max_elapsed_time_millis = config->retry_max_elapsed_time_millis,
        .max_retries = config->retry_max_retries,
    };
    TemporalCoreClientDnsLoadBalancingOptions dns = {
        .resolution_interval_millis = config->dns_resolution_interval_millis,
    };
    TemporalCoreConnectionOptions options = {
        .target_url = text_ref(config->target),
        .client_name = text_ref(config->client_name),
        .client_version = text_ref(config->client_version),
        .identity = text_ref(config->identity),
        .api_key = text_ref(config->api_key),
        .metadata = config->metadata,
        .binary_metadata = config->binary_metadata,
        .tls_options = config->tls_enabled ? &tls : NULL,
        .keep_alive_options = config->keep_alive_interval_millis ? &keep_alive : NULL,
        .http_connect_proxy_options = config->proxy_target[0] ? &proxy : NULL,
        .retry_options = config->retry_enabled ? &retry : NULL,
        .dns_load_balancing_options = config->dns_resolution_interval_millis ? &dns : NULL,
        .grpc_compression = config->grpc_gzip ? TemporalCoreClientGrpcCompression_Gzip : TemporalCoreClientGrpcCompression_None,
        .payloads_warn_size = config->payloads_warn_size,
        .memo_warn_size = config->memo_warn_size,
    };
    if (core->grpc_bridge) {
        if (!spike_grpc_bridge_claim_rust(core->grpc_bridge)) {
            snprintf(last_error, sizeof(last_error), "custom gRPC bridge already claimed");
            return core;
        }
        options.grpc_override_callback = spike_grpc_override_callback;
        options.grpc_override_callback_user_data = core->grpc_bridge;
        options.grpc_override_callback_user_data_free = spike_grpc_override_user_data_free;
    }
    SpikeWait connection_wait = SPIKE_WAIT_INITIALIZER;
    temporal_core_client_connect(core->runtime, &options, &connection_wait, connected);
    if (await_result(core, &connection_wait)) return core;
    core->connection = connection_wait.connection;
    return core;
}

void spike_update_api_key(SpikeCore *core, const char *api_key) {
    temporal_core_client_update_api_key(core->connection, text_ref(api_key));
}

void spike_update_metadata(SpikeCore *core, TemporalCoreMetadataRef metadata, bool binary) {
    if (binary) temporal_core_client_update_binary_metadata(core->connection, metadata);
    else temporal_core_client_update_metadata(core->connection, metadata);
}

const TemporalCoreByteArray *spike_rpc(SpikeCore *core, uint32_t service, const char *method,
                                     const unsigned char *bytes, size_t size, uint32_t timeout_millis,
                                     TemporalCoreMetadataRef metadata, TemporalCoreMetadataRef binary_metadata, bool retry, const TemporalCoreCancellationToken *cancellation_token, const TemporalCoreByteArray **failure_details) {
    last_error[0] = 0;
    last_rpc_status = 0;
    TemporalCoreRpcCallOptions options = {
        .service = (TemporalCoreRpcService)service,
        .rpc = text_ref(method),
        .req = bytes_ref(bytes, size),
        .retry = retry,
        .cancellation_token = cancellation_token,
        .metadata = metadata,
        .binary_metadata = binary_metadata,
        .timeout_millis = timeout_millis,
    };
    SpikeWait wait = SPIKE_WAIT_INITIALIZER;
    temporal_core_client_rpc_call(core->connection, &options, &wait, rpc_completed);
    int failed = await_result(core, &wait);
    last_rpc_status = wait.status;
    *failure_details = wait.details;
    if (failed) {
        if (wait.bytes) temporal_core_byte_array_free(core->runtime, wait.bytes);
        if (!last_rpc_status) last_rpc_status = 13;
        return NULL;
    }
    return wait.bytes;
}

uint32_t spike_rpc_status(void) { return last_rpc_status; }

SpikeCore *spike_replay_open(const char *namespace_name, const char *queue) {
    SpikeCore *core = runtime_open(NULL);
    if (!core || last_error[0]) return core;
    TemporalCoreSlotSupplier slots = {.tag = TemporalCoreSlotSupplier_FixedSize,
                                     .fixed_size = {.num_slots = 2}};
    TemporalCoreWorkerOptions options = {
        .namespace_ = text_ref(namespace_name),
        .task_queue = text_ref(queue),
        .versioning_strategy = {.tag = TemporalCoreWorkerVersioningStrategy_None,
                               .none = {.build_id = text_ref("temporal-v-replay")}},
        .max_cached_workflows = 2,
        .tuner = {slots, slots, slots, slots},
        .task_types = {.enable_workflows = true},
    };
    TemporalCoreWorkerReplayerOrFail replay = temporal_core_worker_replayer_new(core->runtime, &options);
    core->worker = replay.worker;
    core->replay_pusher = replay.worker_replay_pusher;
    if (replay.fail) {
        SpikeWait wait = SPIKE_WAIT_INITIALIZER;
        wait.fail = replay.fail;
        wait.done = true;
        await_result(core, &wait);
    } else core->finalize_required = true;
    return core;
}

int spike_replay_push(SpikeCore *core, const char *workflow_id, const unsigned char *history, size_t size) {
    last_error[0] = 0;
    TemporalCoreWorkerReplayPushResult result = temporal_core_worker_replay_push(
        core->worker, core->replay_pusher, text_ref(workflow_id), bytes_ref(history, size));
    if (!result.fail) return 0;
    SpikeWait wait = SPIKE_WAIT_INITIALIZER;
    wait.fail = result.fail;
    wait.done = true;
    return await_result(core, &wait);
}

static SpikeCore *configure_worker(SpikeCore *core, const SpikeWorkerOptions *settings, bool workflows, bool nexus, bool remote_activities);

SpikeCore *spike_open(const SpikeConnectOptions *config, const SpikeWorkerOptions *settings, bool workflows, bool nexus, bool remote_activities) {
    /* Keep the connection owner independent of worker finalization. Activity
       clients can retain it without keeping the worker or its slot leases alive. */
    SpikeCore *client = spike_client_open(config);
    if (!client || last_error[0]) return client;
    SpikeCore *worker = spike_open_shared(client, settings, workflows, nexus, remote_activities);
    spike_close(client);
    return worker;
}

SpikeCore *spike_retain_client(SpikeCore *core) {
    last_error[0] = 0;
    if (core && core->worker && core->shared_owner) core = core->shared_owner;
    if (!core || !core->connection || core->worker || core->server || core->replay_pusher) {
        snprintf(last_error, sizeof(last_error), "A client connection is required");
        return NULL;
    }
    atomic_fetch_add_explicit(&core->references, 1, memory_order_relaxed);
    return core;
}

SpikeCore *spike_open_shared(SpikeCore *client, const SpikeWorkerOptions *settings, bool workflows, bool nexus, bool remote_activities) {
    last_error[0] = 0;
    SpikeCore *core = calloc(1, sizeof(*core));
    if (!core) return NULL;
    atomic_init(&core->references, 1);
    atomic_init(&core->stop_monitor, false);
    atomic_init(&core->shutdown_started_millis, 0);
    atomic_fetch_add_explicit(&client->references, 1, memory_order_relaxed);
    core->shared_owner = client;
    core->runtime = client->runtime;
    core->connection = client->connection;
    return configure_worker(core, settings, workflows, nexus, remote_activities);
}

static SpikeCore *configure_worker(SpikeCore *core, const SpikeWorkerOptions *settings, bool workflows, bool nexus, bool remote_activities) {
    core->shutdown_grace_millis = settings->shutdown_grace_millis;
    if (acquire_signals()) {
        snprintf(last_error, sizeof(last_error), "Cannot install worker signal handlers");
        return core;
    }
    core->signals_owned = true;

    TemporalCoreSlotSupplier workflow_slots = {.tag = TemporalCoreSlotSupplier_FixedSize, .fixed_size = {.num_slots = settings->workflow_slots}};
    TemporalCoreSlotSupplier activity_slots = {.tag = TemporalCoreSlotSupplier_FixedSize, .fixed_size = {.num_slots = settings->activity_slots}};
    TemporalCoreSlotSupplier local_slots = {.tag = TemporalCoreSlotSupplier_FixedSize, .fixed_size = {.num_slots = settings->local_activity_slots}};
    TemporalCoreSlotSupplier nexus_slots = {.tag = TemporalCoreSlotSupplier_FixedSize, .fixed_size = {.num_slots = settings->nexus_slots}};
    if (settings->resource_based) {
        TemporalCoreResourceBasedSlotSupplier resource = {
            .minimum_slots = settings->minimum_activity_slots,
            .ramp_throttle_ms = settings->ramp_throttle_millis,
            .tuner_options = {settings->target_memory_usage, settings->target_cpu_usage},
        };
        resource.maximum_slots = settings->activity_slots;
        activity_slots = (TemporalCoreSlotSupplier){.tag = TemporalCoreSlotSupplier_ResourceBased, .resource_based = resource};
        resource.maximum_slots = settings->local_activity_slots;
        local_slots = (TemporalCoreSlotSupplier){.tag = TemporalCoreSlotSupplier_ResourceBased, .resource_based = resource};
        resource.maximum_slots = settings->nexus_slots;
        nexus_slots = (TemporalCoreSlotSupplier){.tag = TemporalCoreSlotSupplier_ResourceBased, .resource_based = resource};
        resource.minimum_slots = settings->minimum_workflow_slots;
        resource.maximum_slots = settings->workflow_slots;
        workflow_slots = (TemporalCoreSlotSupplier){.tag = TemporalCoreSlotSupplier_ResourceBased, .resource_based = resource};
    }
    if (settings->workflow_slot_bridge) workflow_slots = spike_slot_bridge_supplier(settings->workflow_slot_bridge);
    if (settings->activity_slot_bridge) activity_slots = spike_slot_bridge_supplier(settings->activity_slot_bridge);
    if (settings->local_activity_slot_bridge) local_slots = spike_slot_bridge_supplier(settings->local_activity_slot_bridge);
    if (settings->nexus_slot_bridge) nexus_slots = spike_slot_bridge_supplier(settings->nexus_slot_bridge);
    TemporalCoreWorkerVersioningStrategy versioning = {
        .tag = TemporalCoreWorkerVersioningStrategy_None, .none = {.build_id = text_ref(settings->build_id)},
    };
    if (settings->deployment_name[0]) {
        versioning = (TemporalCoreWorkerVersioningStrategy){
            .tag = TemporalCoreWorkerVersioningStrategy_DeploymentBased,
            .deployment_based = {
                .version = {text_ref(settings->deployment_name), text_ref(settings->build_id)},
                .use_worker_versioning = settings->deployment_versioning,
                .default_versioning_behavior = settings->versioning_behavior,
            },
        };
    } else if (settings->legacy_versioning) {
        versioning = (TemporalCoreWorkerVersioningStrategy){
            .tag = TemporalCoreWorkerVersioningStrategy_LegacyBuildIdBased,
            .legacy_build_id_based = {.build_id = text_ref(settings->build_id)},
        };
    }
    TemporalCorePollerBehaviorSimpleMaximum workflow_pollers = {.simple_maximum = settings->workflow_pollers};
    TemporalCorePollerBehaviorSimpleMaximum activity_pollers = {.simple_maximum = settings->activity_pollers};
    TemporalCorePollerBehaviorSimpleMaximum nexus_pollers = {.simple_maximum = settings->nexus_pollers};
    TemporalCorePollerBehaviorAutoscaling workflow_autoscaling = {
        .minimum = settings->workflow_poller_autoscaling.minimum,
        .maximum = settings->workflow_poller_autoscaling.maximum,
        .initial = settings->workflow_poller_autoscaling.initial,
    };
    TemporalCorePollerBehaviorAutoscaling activity_autoscaling = {
        .minimum = settings->activity_poller_autoscaling.minimum,
        .maximum = settings->activity_poller_autoscaling.maximum,
        .initial = settings->activity_poller_autoscaling.initial,
    };
    TemporalCorePollerBehaviorAutoscaling nexus_autoscaling = {
        .minimum = settings->nexus_poller_autoscaling.minimum,
        .maximum = settings->nexus_poller_autoscaling.maximum,
        .initial = settings->nexus_poller_autoscaling.initial,
    };
    TemporalCoreWorkerOptions worker_options = {
        .namespace_ = text_ref(settings->namespace_name),
        .task_queue = text_ref(settings->queue),
        .versioning_strategy = versioning,
        .tuner = {workflow_slots, activity_slots, local_slots, nexus_slots},
        .max_eager_activity_reservations_per_workflow_task = remote_activities ? settings->max_eager_activity_reservations : 0,
        .plugins = settings->plugins,
        .disable_payload_error_limit = settings->disable_payload_error_limit,
        .storage_drivers = settings->storage_drivers,
        .max_cached_workflows = workflows ? settings->max_cached_workflows : 0,
        .nondeterminism_as_workflow_fail = settings->nondeterminism_as_workflow_fail,
        .nondeterminism_as_workflow_fail_for_types = settings->nondeterminism_as_workflow_fail_for_types,
        .task_types = {.enable_workflows = workflows, .enable_local_activities = workflows, .enable_remote_activities = remote_activities, .enable_nexus = nexus},
        .workflow_task_poller_behavior = {.simple_maximum = settings->workflow_poller_autoscaling.enabled ? NULL : &workflow_pollers, .autoscaling = settings->workflow_poller_autoscaling.enabled ? &workflow_autoscaling : NULL},
        .activity_task_poller_behavior = {.simple_maximum = settings->activity_poller_autoscaling.enabled ? NULL : &activity_pollers, .autoscaling = settings->activity_poller_autoscaling.enabled ? &activity_autoscaling : NULL},
        .nexus_task_poller_behavior = {.simple_maximum = settings->nexus_poller_autoscaling.enabled ? NULL : &nexus_pollers, .autoscaling = settings->nexus_poller_autoscaling.enabled ? &nexus_autoscaling : NULL},
        .sticky_queue_schedule_to_start_timeout_millis = settings->sticky_timeout_millis,
        .max_heartbeat_throttle_interval_millis = settings->max_heartbeat_millis,
        .default_heartbeat_throttle_interval_millis = settings->default_heartbeat_millis,
        .graceful_shutdown_period_millis = settings->shutdown_grace_millis,
        .nonsticky_to_sticky_poll_ratio = settings->nonsticky_poll_ratio,
        .max_activities_per_second = settings->max_activities_per_second,
        .max_task_queue_activities_per_second = settings->max_queue_activities_per_second,
    };
    TemporalCoreWorkerOrFail worker = temporal_core_worker_new(core->connection, &worker_options);
    core->worker = worker.worker;
    SpikeWait worker_wait = SPIKE_WAIT_INITIALIZER;
    if (worker.fail) {
        worker_wait.fail = worker.fail;
        worker_wait.done = true;
    } else {
        temporal_core_worker_validate(core->worker, &worker_wait, completed);
    }
    if (!await_result(core, &worker_wait)) {
        core->finalize_required = true;
        int status = pthread_create(&core->signal_monitor, NULL, monitor_signals, core);
        if (status) snprintf(last_error, sizeof(last_error), "Cannot create signal monitor: %d", status);
        else core->monitor_started = true;
    }
    return core;
}

const char *spike_error(SpikeCore *core) { (void)core; return last_error; }

const TemporalCoreByteArray *spike_poll(SpikeCore *core) {
    last_error[0] = 0;
    SpikeWait wait = SPIKE_WAIT_INITIALIZER;
    temporal_core_worker_poll_activity_task(core->worker, &wait, polled);
    if (await_result(core, &wait)) return NULL;
    return wait.bytes;
}

const TemporalCoreByteArray *spike_poll_workflow(SpikeCore *core) {
    last_error[0] = 0;
    SpikeWait wait = SPIKE_WAIT_INITIALIZER;
    temporal_core_worker_poll_workflow_activation(core->worker, &wait, polled);
    if (await_result(core, &wait)) return NULL;
    return wait.bytes;
}

const TemporalCoreByteArray *spike_poll_nexus(SpikeCore *core) {
    last_error[0] = 0;
    SpikeWait wait = SPIKE_WAIT_INITIALIZER;
    temporal_core_worker_poll_nexus_task(core->worker, &wait, polled);
    if (await_result(core, &wait)) return NULL;
    return wait.bytes;
}

int spike_complete_nexus(SpikeCore *core, const unsigned char *bytes, size_t size) {
    last_error[0] = 0;
    SpikeWait wait = SPIKE_WAIT_INITIALIZER;
    temporal_core_worker_complete_nexus_task(core->worker, bytes_ref(bytes, size), &wait, completed);
    return await_result(core, &wait);
}

int spike_complete_workflow(SpikeCore *core, const unsigned char *bytes, size_t size) {
    last_error[0] = 0;
    SpikeWait wait = SPIKE_WAIT_INITIALIZER;
    temporal_core_worker_complete_workflow_activation(core->worker,
        bytes_ref(bytes, size), &wait, completed);
    return await_result(core, &wait);
}

int spike_complete(SpikeCore *core, const unsigned char *bytes, size_t size) {
    last_error[0] = 0;
    SpikeWait wait = SPIKE_WAIT_INITIALIZER;
    temporal_core_worker_complete_activity_task(core->worker,
        bytes_ref(bytes, size), &wait, completed);
    return await_result(core, &wait);
}

void spike_bytes_free(SpikeCore *core, const TemporalCoreByteArray *bytes) {
    temporal_core_byte_array_free(core->runtime, bytes);
}

int spike_heartbeat(SpikeCore *core, const unsigned char *bytes, size_t size) {
    last_error[0] = 0;
    const TemporalCoreByteArray *fail = temporal_core_worker_record_activity_heartbeat(
        core->worker, bytes_ref(bytes, size));
    if (!fail) return 0;
    copy_error(fail);
    temporal_core_byte_array_free(core->runtime, fail);
    return -1;
}

static uint64_t monotonic_millis(void) {
    struct timespec now = {0};
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000 + (uint64_t)now.tv_nsec / 1000000;
}

void spike_shutdown(SpikeCore *core) {
    if (core && core->worker) {
        uint_fast64_t unset = 0;
        uint_fast64_t started = monotonic_millis();
        atomic_compare_exchange_strong(&core->shutdown_started_millis, &unset, started ? started : 1);
        temporal_core_worker_initiate_shutdown(core->worker);
    }
}

bool spike_shutdown_grace_expired(SpikeCore *core) {
    if (!core || !core->worker) return false;
    uint_fast64_t started = atomic_load(&core->shutdown_started_millis);
    uint_fast64_t now = monotonic_millis();
    return started && now >= started && now - started >= core->shutdown_grace_millis;
}

void spike_close(SpikeCore *core) {
    if (!core) return;
    if (atomic_fetch_sub_explicit(&core->references, 1, memory_order_acq_rel) != 1) return;
    if (core->monitor_started) {
        atomic_store(&core->stop_monitor, true);
        pthread_join(core->signal_monitor, NULL);
    }
    if (core->worker) {
        /* Polling must be drained by V before finalization. */
        if (core->finalize_required) {
            SpikeWait wait = SPIKE_WAIT_INITIALIZER;
            temporal_core_worker_finalize_shutdown(core->worker, &wait, completed);
            await_result(core, &wait);
        }
        temporal_core_worker_free(core->worker);
    }
    if (core->replay_pusher) temporal_core_worker_replay_pusher_free(core->replay_pusher);
    if (core->server) {
        SpikeWait wait = SPIKE_WAIT_INITIALIZER;
        temporal_core_ephemeral_server_shutdown(core->server, &wait, completed);
        await_result(core, &wait);
        temporal_core_ephemeral_server_free(core->server);
    }
    if (!core->shared_owner) {
        if (core->connection) temporal_core_client_free(core->connection);
        if (core->runtime) temporal_core_runtime_free(core->runtime);
    }
    if (core->grpc_bridge) spike_grpc_bridge_detach_cpp(core->grpc_bridge);
    if (core->signals_owned) release_signals();
    SpikeCore *shared_owner = core->shared_owner;
    free(core);
    if (shared_owner) spike_close(shared_owner);
}

/* Share stderr's lock with Core's JSON log callback so concurrent SDK/Core
 * records cannot interleave when the application's streams are collected. */
void spike_log_line(const unsigned char *bytes, size_t size) {
    flockfile(stderr);
    fwrite(bytes, 1, size, stderr);
    fputc('\n', stderr);
    fflush(stderr);
    funlockfile(stderr);
}

#include "metrics_impl.h"

uint64_t spike_protocol_capabilities(void) { return temporal_core_protocol_capabilities(); }
