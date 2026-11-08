#define _POSIX_C_SOURCE 200809L
#include "native/grpc_bridge.h"
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct TemporalCoreClientGrpcOverrideRequest {
    const char *rpc;
    uint8_t body[16];
    size_t size;
    atomic_bool cancelled;
};
static atomic_uint allocated;
static atomic_uint freed;
static atomic_uint status_count[17];
static const uint8_t empty = 0;
static const TemporalCoreByteArrayRef empty_ref = {&empty, 0};
static const TemporalCoreMetadataRef empty_headers = {&empty_ref, 0};

static TemporalCoreClientGrpcOverrideRequest *new_request(const char *rpc, size_t size, bool cancelled) {
    TemporalCoreClientGrpcOverrideRequest *request = calloc(1, sizeof(*request));
    assert(request);
    request->rpc = rpc;
    request->size = size;
    atomic_init(&request->cancelled, cancelled);
    atomic_fetch_add(&allocated, 1);
    return request;
}
TemporalCoreByteArrayRef temporal_core_client_grpc_override_request_service(const TemporalCoreClientGrpcOverrideRequest *request) {
    (void)request;
    const char *service = "temporal.api.workflowservice.v1.WorkflowService";
    return (TemporalCoreByteArrayRef){(const uint8_t *)service, strlen(service)};
}
TemporalCoreByteArrayRef temporal_core_client_grpc_override_request_rpc(const TemporalCoreClientGrpcOverrideRequest *request) {
    return (TemporalCoreByteArrayRef){(const uint8_t *)request->rpc, strlen(request->rpc)};
}
TemporalCoreByteArrayRef temporal_core_client_grpc_override_request_proto(const TemporalCoreClientGrpcOverrideRequest *request) {
    return (TemporalCoreByteArrayRef){request->body, request->size};
}
TemporalCoreMetadataRef temporal_core_client_grpc_override_request_headers(const TemporalCoreClientGrpcOverrideRequest *request) {
    (void)request; return empty_headers;
}
bool temporal_core_client_grpc_override_request_is_cancelled(const TemporalCoreClientGrpcOverrideRequest *request) {
    return atomic_load(&request->cancelled);
}
void temporal_core_client_grpc_override_request_respond(TemporalCoreClientGrpcOverrideRequest *request, TemporalCoreClientGrpcOverrideResponse response) {
    assert(response.status_code >= 0 && response.status_code <= 16);
    assert(response.headers.data && response.success_proto.data && response.fail_message.data && response.fail_details.data);
    atomic_fetch_add(&status_count[response.status_code], 1);
    atomic_fetch_add(&freed, 1);
    free(request);
}
static void respond(SpikeGrpcRequest *request, int32_t status) {
    TemporalCoreClientGrpcOverrideResponse response = {
        .status_code = status, .headers = empty_headers,
        .success_proto = empty_ref, .fail_message = empty_ref, .fail_details = empty_ref,
    };
    assert(spike_grpc_request_respond(request, &response));
    assert(!spike_grpc_request_respond(request, &response));
}
static void lane_and_capacity_test(void) {
    SpikeGrpcBridge *bridge = spike_grpc_bridge_new(3, 16);
    assert(bridge && spike_grpc_bridge_attach_cpp(bridge) && spike_grpc_bridge_claim_rust(bridge));
    spike_grpc_override_callback(new_request("PollWorkflowTaskQueue", 4, false), bridge);
    spike_grpc_override_callback(new_request("GetWorkflowExecutionHistory", 4, true), bridge);
    spike_grpc_override_callback(new_request("GetSystemInfo", 4, false), bridge);
    spike_grpc_override_callback(new_request("GetSystemInfo", 4, false), bridge);
    SpikeGrpcRequest *control = spike_grpc_bridge_poll(bridge, 1, 0);
    assert(control && spike_grpc_request_class(control) == 0);
    assert(spike_grpc_request_service(control).size && spike_grpc_request_proto(control).size == 4);
    assert(spike_grpc_request_headers(control).data);
    respond(control, 0); spike_grpc_request_free(control);
    spike_grpc_override_callback(new_request("GetSystemInfo", 16, false), bridge);
    SpikeGrpcRequest *wait = spike_grpc_bridge_poll(bridge, 4, 0);
    assert(wait && spike_grpc_request_class(wait) == 2);
    assert(spike_grpc_request_cancelled(wait) && spike_grpc_request_cancelled(wait));
    respond(wait, 1); spike_grpc_request_free(wait);
    SpikeGrpcRequest *poll = spike_grpc_bridge_poll(bridge, 2, 0);
    assert(poll && spike_grpc_request_class(poll) == 1);
    spike_grpc_request_free(poll);
    spike_grpc_override_user_data_free(bridge);
    spike_grpc_override_callback(new_request("GetSystemInfo", 1, false), bridge);
    SpikeGrpcStats stats = spike_grpc_bridge_stats(bridge);
    assert(stats.accepted == 3 && stats.responded == 3 && stats.rejected == 3 && stats.cancelled == 1);
    assert(!stats.pending && !stats.pending_bytes && stats.core_done && spike_grpc_bridge_done(bridge));
    spike_grpc_bridge_detach_cpp(bridge);
    spike_grpc_bridge_release_owner(bridge);
}
static void release_order_test(void) {
    SpikeGrpcBridge *bridge = spike_grpc_bridge_new(1, 16);
    assert(bridge && spike_grpc_bridge_attach_cpp(bridge));
    spike_grpc_bridge_abort_unclaimed(bridge);
    assert(spike_grpc_bridge_done(bridge));
    assert(!spike_grpc_bridge_claim_rust(bridge));
    spike_grpc_bridge_detach_cpp(bridge);
    spike_grpc_bridge_release_owner(bridge);
    bridge = spike_grpc_bridge_new(1, 16);
    assert(spike_grpc_bridge_attach_cpp(bridge) && spike_grpc_bridge_claim_rust(bridge));
    TemporalCoreClientGrpcOverrideRequest *native = new_request("GetSystemInfo", 16, false);
    spike_grpc_override_callback(native, bridge);
    SpikeGrpcRequest *event = spike_grpc_bridge_poll(bridge, 7, 0);
    assert(event && !spike_grpc_request_cancelled(event));
    atomic_store(&native->cancelled, true);
    assert(spike_grpc_request_cancelled(event));
    spike_grpc_override_user_data_free(bridge);
    spike_grpc_bridge_detach_cpp(bridge);
    spike_grpc_bridge_release_owner(bridge);
    /* The queued event retains the bridge after all three other owners leave. */
    TemporalCoreClientGrpcOverrideResponse zero_response = {0};
    assert(spike_grpc_request_respond(event, &zero_response));
    spike_grpc_request_free(event);
}
static void *producer(void *argument) {
    SpikeGrpcBridge *bridge = argument;
    for (unsigned index = 0; index < 250; index++) {
        const char *rpc = index % 3 == 0 ? "PollActivityTaskQueue" : index % 3 == 1 ? "PollNexusOperationExecution" : "RespondWorkflowTaskCompleted";
        spike_grpc_override_callback(new_request(rpc, 16, index % 7 == 0), bridge);
    }
    return NULL;
}
static void *consumer(void *argument) {
    SpikeGrpcBridge *bridge = argument;
    while (!spike_grpc_bridge_done(bridge)) {
        SpikeGrpcRequest *event = spike_grpc_bridge_poll(bridge, 7, 1);
        if (!event) continue;
        assert(spike_grpc_request_proto(event).size == 16);
        respond(event, spike_grpc_request_cancelled(event) ? 1 : 0);
        spike_grpc_request_free(event);
    }
    return NULL;
}
static void concurrent_test(void) {
    SpikeGrpcBridge *bridge = spike_grpc_bridge_new(64, 1024);
    assert(bridge && spike_grpc_bridge_attach_cpp(bridge) && spike_grpc_bridge_claim_rust(bridge));
    pthread_t writers[8], reader;
    assert(!pthread_create(&reader, NULL, consumer, bridge));
    for (size_t index = 0; index < 8; index++) assert(!pthread_create(&writers[index], NULL, producer, bridge));
    for (size_t index = 0; index < 8; index++) assert(!pthread_join(writers[index], NULL));
    spike_grpc_bridge_detach_cpp(bridge);
    spike_grpc_override_user_data_free(bridge);
    assert(!pthread_join(reader, NULL));
    SpikeGrpcStats stats = spike_grpc_bridge_stats(bridge);
    assert(stats.accepted + stats.rejected == 2000 && stats.responded == stats.accepted);
    assert(!stats.pending && !stats.pending_bytes && stats.core_done);
    spike_grpc_bridge_release_owner(bridge);
}
int main(void) {
    lane_and_capacity_test(); release_order_test(); concurrent_test();
    assert(atomic_load(&allocated) == atomic_load(&freed));
    printf("%u owned requests balanced lanes, byte/count limits, cancellation, concurrent dispatch and release order\n", atomic_load(&freed));
    return 0;
}
