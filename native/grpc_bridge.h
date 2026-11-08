#pragma once
#include "include/temporal-sdk-core-c-bridge.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct SpikeGrpcBridge SpikeGrpcBridge;
typedef struct SpikeGrpcRequest SpikeGrpcRequest;
typedef struct {
    uint64_t accepted;
    uint64_t responded;
    uint64_t rejected;
    uint64_t cancelled;
    uint64_t pending;
    uint64_t pending_bytes;
    uint64_t queued_control;
    uint64_t queued_polls;
    uint64_t queued_waits;
    bool stopping;
    bool core_done;
} SpikeGrpcStats;

SpikeGrpcBridge *spike_grpc_bridge_new(size_t max_pending, uint64_t max_pending_bytes);
void spike_grpc_bridge_release_owner(SpikeGrpcBridge *bridge);
bool spike_grpc_bridge_attach_cpp(SpikeGrpcBridge *bridge);
void spike_grpc_bridge_detach_cpp(SpikeGrpcBridge *bridge);
bool spike_grpc_bridge_claim_rust(SpikeGrpcBridge *bridge);
void spike_grpc_bridge_abort_unclaimed(SpikeGrpcBridge *bridge);
void spike_grpc_override_callback(TemporalCoreClientGrpcOverrideRequest *request, void *user_data);
void spike_grpc_override_user_data_free(void *user_data);
SpikeGrpcRequest *spike_grpc_bridge_poll(SpikeGrpcBridge *bridge, uint32_t allowed_classes, uint32_t timeout_millis);
bool spike_grpc_bridge_done(SpikeGrpcBridge *bridge);
SpikeGrpcStats spike_grpc_bridge_stats(SpikeGrpcBridge *bridge);
uint32_t spike_grpc_request_class(const SpikeGrpcRequest *request);
TemporalCoreByteArrayRef spike_grpc_request_service(SpikeGrpcRequest *request);
TemporalCoreByteArrayRef spike_grpc_request_rpc(SpikeGrpcRequest *request);
TemporalCoreByteArrayRef spike_grpc_request_proto(SpikeGrpcRequest *request);
TemporalCoreMetadataRef spike_grpc_request_headers(SpikeGrpcRequest *request);
bool spike_grpc_request_cancelled(SpikeGrpcRequest *request);
bool spike_grpc_request_respond(SpikeGrpcRequest *request, const TemporalCoreClientGrpcOverrideResponse *response);
void spike_grpc_request_free(SpikeGrpcRequest *request);
