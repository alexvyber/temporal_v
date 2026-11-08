#define _POSIX_C_SOURCE 200809L
#include "grpc_bridge.h"
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct SpikeGrpcRequest {
    pthread_mutex_t mutex;
    TemporalCoreClientGrpcOverrideRequest *request;
    struct SpikeGrpcBridge *bridge;
    struct SpikeGrpcRequest *next;
    uint64_t bytes;
    uint32_t class;
    bool cancelled_counted;
};

struct SpikeGrpcBridge {
    atomic_size_t references;
    atomic_bool stopping;
    pthread_mutex_t mutex;
    pthread_cond_t ready;
    SpikeGrpcRequest *heads[3];
    SpikeGrpcRequest *tails[3];
    uint64_t queued[3];
    size_t max_pending;
    uint64_t max_pending_bytes;
    SpikeGrpcStats stats;
    bool cpp_attached;
    bool rust_claimed;
    bool rust_dropped;
};

static const uint8_t grpc_empty = 0;
static const TemporalCoreByteArrayRef grpc_empty_ref = {&grpc_empty, 0};
static const TemporalCoreMetadataRef grpc_empty_headers = {&grpc_empty_ref, 0};

static void bridge_retain(SpikeGrpcBridge *bridge) {
    atomic_fetch_add_explicit(&bridge->references, 1, memory_order_relaxed);
}

static void bridge_release(SpikeGrpcBridge *bridge) {
    if (atomic_fetch_sub_explicit(&bridge->references, 1, memory_order_acq_rel) != 1) return;
    pthread_cond_destroy(&bridge->ready);
    pthread_mutex_destroy(&bridge->mutex);
    free(bridge);
}

static void respond_failure(TemporalCoreClientGrpcOverrideRequest *request, int32_t status, const char *message) {
    TemporalCoreClientGrpcOverrideResponse response = {
        .status_code = status,
        .headers = grpc_empty_headers,
        .success_proto = grpc_empty_ref,
        .fail_message = {(const uint8_t *)message, strlen(message)},
        .fail_details = grpc_empty_ref,
    };
    temporal_core_client_grpc_override_request_respond(request, response);
}

static bool rpc_equals(TemporalCoreByteArrayRef rpc, const char *name) {
    size_t size = strlen(name);
    return rpc.size == size && memcmp(rpc.data, name, size) == 0;
}

static uint32_t request_class(TemporalCoreClientGrpcOverrideRequest *request) {
    TemporalCoreByteArrayRef rpc = temporal_core_client_grpc_override_request_rpc(request);
    if (rpc_equals(rpc, "PollWorkflowTaskQueue") || rpc_equals(rpc, "PollActivityTaskQueue") || rpc_equals(rpc, "PollNexusTaskQueue")) return 1;
    if (rpc_equals(rpc, "GetWorkflowExecutionHistory") || rpc_equals(rpc, "PollWorkflowExecutionUpdate") ||
        rpc_equals(rpc, "UpdateWorkflowExecution") || rpc_equals(rpc, "ExecuteMultiOperation") ||
        rpc_equals(rpc, "PollNexusOperationExecution") || rpc_equals(rpc, "PollActivityExecution")) return 2;
    return 0;
}

SpikeGrpcBridge *spike_grpc_bridge_new(size_t max_pending, uint64_t max_pending_bytes) {
    if (!max_pending || !max_pending_bytes) return NULL;
    SpikeGrpcBridge *bridge = calloc(1, sizeof(*bridge));
    if (!bridge) return NULL;
    atomic_init(&bridge->references, 1);
    atomic_init(&bridge->stopping, false);
    bridge->max_pending = max_pending;
    bridge->max_pending_bytes = max_pending_bytes;
    if (pthread_mutex_init(&bridge->mutex, NULL)) { free(bridge); return NULL; }
    if (pthread_cond_init(&bridge->ready, NULL)) {
        pthread_mutex_destroy(&bridge->mutex); free(bridge); return NULL;
    }
    return bridge;
}

void spike_grpc_bridge_release_owner(SpikeGrpcBridge *bridge) { bridge_release(bridge); }

bool spike_grpc_bridge_attach_cpp(SpikeGrpcBridge *bridge) {
    pthread_mutex_lock(&bridge->mutex);
    bool valid = !bridge->cpp_attached && !atomic_load(&bridge->stopping);
    if (valid) { bridge->cpp_attached = true; bridge_retain(bridge); }
    pthread_mutex_unlock(&bridge->mutex);
    return valid;
}

void spike_grpc_bridge_detach_cpp(SpikeGrpcBridge *bridge) {
    pthread_mutex_lock(&bridge->mutex);
    bool attached = bridge->cpp_attached;
    bridge->cpp_attached = false;
    if (!bridge->rust_claimed) {
        bridge->stats.core_done = true;
        atomic_store(&bridge->stopping, true);
    }
    pthread_cond_broadcast(&bridge->ready);
    pthread_mutex_unlock(&bridge->mutex);
    if (attached) bridge_release(bridge);
}

bool spike_grpc_bridge_claim_rust(SpikeGrpcBridge *bridge) {
    pthread_mutex_lock(&bridge->mutex);
    bool valid = !bridge->rust_claimed && !atomic_load(&bridge->stopping);
    if (valid) { bridge->rust_claimed = true; bridge_retain(bridge); }
    pthread_mutex_unlock(&bridge->mutex);
    return valid;
}

void spike_grpc_bridge_abort_unclaimed(SpikeGrpcBridge *bridge) {
    pthread_mutex_lock(&bridge->mutex);
    if (!bridge->rust_claimed) {
        atomic_store(&bridge->stopping, true);
        bridge->stats.core_done = true;
        pthread_cond_broadcast(&bridge->ready);
    }
    pthread_mutex_unlock(&bridge->mutex);
}

void spike_grpc_override_user_data_free(void *user_data) {
    SpikeGrpcBridge *bridge = user_data;
    pthread_mutex_lock(&bridge->mutex);
    bool release = bridge->rust_claimed && !bridge->rust_dropped;
    if (release) {
        bridge->rust_dropped = true;
        bridge->stats.core_done = true;
        atomic_store(&bridge->stopping, true);
        pthread_cond_broadcast(&bridge->ready);
    }
    pthread_mutex_unlock(&bridge->mutex);
    if (release) bridge_release(bridge);
}

void spike_grpc_override_callback(TemporalCoreClientGrpcOverrideRequest *request, void *user_data) {
    SpikeGrpcBridge *bridge = user_data;
    SpikeGrpcRequest *event = calloc(1, sizeof(*event));
    if (!event) { respond_failure(request, 8, "custom gRPC transport allocation failed"); return; }
    if (pthread_mutex_init(&event->mutex, NULL)) {
        free(event); respond_failure(request, 8, "custom gRPC transport allocation failed"); return;
    }
    event->request = request;
    event->bridge = bridge;
    event->class = request_class(request);
    event->bytes = temporal_core_client_grpc_override_request_proto(request).size;
    pthread_mutex_lock(&bridge->mutex);
    bool stopping = atomic_load(&bridge->stopping);
    bool full = bridge->stats.pending >= bridge->max_pending || event->bytes > bridge->max_pending_bytes - bridge->stats.pending_bytes;
    if (stopping || full) {
        bridge->stats.rejected++;
        pthread_mutex_unlock(&bridge->mutex);
        pthread_mutex_destroy(&event->mutex); free(event);
        respond_failure(request, stopping ? 1 : 8, stopping ? "custom gRPC transport stopped" : "custom gRPC transport capacity exceeded");
        return;
    }
    bridge_retain(bridge);
    bridge->stats.accepted++;
    bridge->stats.pending++;
    bridge->stats.pending_bytes += event->bytes;
    if (bridge->tails[event->class]) bridge->tails[event->class]->next = event;
    else bridge->heads[event->class] = event;
    bridge->tails[event->class] = event;
    bridge->queued[event->class]++;
    pthread_cond_broadcast(&bridge->ready);
    pthread_mutex_unlock(&bridge->mutex);
}

SpikeGrpcRequest *spike_grpc_bridge_poll(SpikeGrpcBridge *bridge, uint32_t allowed_classes, uint32_t timeout_millis) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += timeout_millis / 1000;
    deadline.tv_nsec += (long)(timeout_millis % 1000) * 1000000;
    if (deadline.tv_nsec >= 1000000000) { deadline.tv_sec++; deadline.tv_nsec -= 1000000000; }
    pthread_mutex_lock(&bridge->mutex);
    for (;;) {
        /* Completions must not queue behind workers holding long polls. */
        const uint32_t order[] = {0, 2, 1};
        for (size_t index = 0; index < 3; index++) {
            uint32_t kind = order[index];
            if (!(allowed_classes & (1u << kind)) || !bridge->heads[kind]) continue;
            SpikeGrpcRequest *event = bridge->heads[kind];
            bridge->heads[kind] = event->next;
            if (!bridge->heads[kind]) bridge->tails[kind] = NULL;
            event->next = NULL;
            bridge->queued[kind]--;
            pthread_mutex_unlock(&bridge->mutex);
            return event;
        }
        if (!timeout_millis || (bridge->stats.core_done && !bridge->stats.pending)) break;
        int status = pthread_cond_timedwait(&bridge->ready, &bridge->mutex, &deadline);
        if (status != 0) break;
    }
    pthread_mutex_unlock(&bridge->mutex);
    return NULL;
}

bool spike_grpc_bridge_done(SpikeGrpcBridge *bridge) {
    pthread_mutex_lock(&bridge->mutex);
    bool done = bridge->stats.core_done && !bridge->stats.pending;
    pthread_mutex_unlock(&bridge->mutex);
    return done;
}

SpikeGrpcStats spike_grpc_bridge_stats(SpikeGrpcBridge *bridge) {
    pthread_mutex_lock(&bridge->mutex);
    SpikeGrpcStats stats = bridge->stats;
    stats.queued_control = bridge->queued[0]; stats.queued_polls = bridge->queued[1]; stats.queued_waits = bridge->queued[2];
    stats.stopping = atomic_load(&bridge->stopping);
    pthread_mutex_unlock(&bridge->mutex);
    return stats;
}

uint32_t spike_grpc_request_class(const SpikeGrpcRequest *request) { return request->class; }

#define REQUEST_REF_GETTER(name, core_getter) \
TemporalCoreByteArrayRef name(SpikeGrpcRequest *request) { \
    pthread_mutex_lock(&request->mutex); \
    TemporalCoreByteArrayRef value = request->request ? core_getter(request->request) : grpc_empty_ref; \
    pthread_mutex_unlock(&request->mutex); \
    return value; \
}
REQUEST_REF_GETTER(spike_grpc_request_service, temporal_core_client_grpc_override_request_service)
REQUEST_REF_GETTER(spike_grpc_request_rpc, temporal_core_client_grpc_override_request_rpc)
REQUEST_REF_GETTER(spike_grpc_request_proto, temporal_core_client_grpc_override_request_proto)

TemporalCoreMetadataRef spike_grpc_request_headers(SpikeGrpcRequest *request) {
    pthread_mutex_lock(&request->mutex);
    TemporalCoreMetadataRef headers = request->request ? temporal_core_client_grpc_override_request_headers(request->request) : grpc_empty_headers;
    pthread_mutex_unlock(&request->mutex);
    return headers;
}

bool spike_grpc_request_cancelled(SpikeGrpcRequest *request) {
    pthread_mutex_lock(&request->mutex);
    bool cancelled = !request->request || atomic_load(&request->bridge->stopping) || temporal_core_client_grpc_override_request_is_cancelled(request->request);
    if (request->request && cancelled && !request->cancelled_counted) {
        request->cancelled_counted = true;
        pthread_mutex_lock(&request->bridge->mutex); request->bridge->stats.cancelled++; pthread_mutex_unlock(&request->bridge->mutex);
    }
    pthread_mutex_unlock(&request->mutex);
    return cancelled;
}

bool spike_grpc_request_respond(SpikeGrpcRequest *request, const TemporalCoreClientGrpcOverrideResponse *response) {
    pthread_mutex_lock(&request->mutex);
    TemporalCoreClientGrpcOverrideRequest *native = request->request;
    request->request = NULL;
    pthread_mutex_unlock(&request->mutex);
    if (!native) return false;
    TemporalCoreClientGrpcOverrideResponse normalized = *response;
    if (!normalized.headers.size) normalized.headers = grpc_empty_headers;
    if (!normalized.success_proto.size) normalized.success_proto = grpc_empty_ref;
    if (!normalized.fail_message.size) normalized.fail_message = grpc_empty_ref;
    if (!normalized.fail_details.size) normalized.fail_details = grpc_empty_ref;
    temporal_core_client_grpc_override_request_respond(native, normalized);
    pthread_mutex_lock(&request->bridge->mutex); request->bridge->stats.responded++; pthread_mutex_unlock(&request->bridge->mutex);
    return true;
}

void spike_grpc_request_free(SpikeGrpcRequest *request) {
    if (!request) return;
    TemporalCoreClientGrpcOverrideResponse cancelled = {
        .status_code = 1, .headers = grpc_empty_headers, .success_proto = grpc_empty_ref,
        .fail_message = {(const uint8_t *)"custom gRPC request abandoned", 29}, .fail_details = grpc_empty_ref,
    };
    spike_grpc_request_respond(request, &cancelled);
    SpikeGrpcBridge *bridge = request->bridge;
    pthread_mutex_lock(&bridge->mutex);
    bridge->stats.pending--; bridge->stats.pending_bytes -= request->bytes;
    pthread_cond_broadcast(&bridge->ready);
    pthread_mutex_unlock(&bridge->mutex);
    pthread_mutex_destroy(&request->mutex); free(request);
    bridge_release(bridge);
}
