#pragma once
#include "include/temporal-sdk-core-c-bridge.h"

typedef struct SpikeSlotBridge SpikeSlotBridge;
typedef enum { SPIKE_SLOT_RESERVE, SPIKE_SLOT_TRY_RESERVE, SPIKE_SLOT_MARK_USED,
               SPIKE_SLOT_RELEASE, SPIKE_SLOT_AVAILABLE, SPIKE_SLOT_STOP } SpikeSlotEventKind;
typedef struct SpikeSlotEvent {
    int kind;
    int slot_kind;
    uintptr_t permit;
    bool has_info;
    bool is_sticky;
    TemporalCoreByteArrayRef task_queue;
    TemporalCoreByteArrayRef worker_identity;
    TemporalCoreByteArrayRef worker_build_id;
    TemporalCoreByteArrayRef workflow_type;
    TemporalCoreByteArrayRef activity_type;
    TemporalCoreByteArrayRef service;
    TemporalCoreByteArrayRef operation;
} SpikeSlotEvent;

SpikeSlotBridge *spike_slot_bridge_new(void);
TemporalCoreSlotSupplier spike_slot_bridge_supplier(SpikeSlotBridge *bridge);
/* Poll/respond run only on a V-created dispatcher thread. Core callbacks never
 * allocate V objects or call V functions on foreign runtime threads. */
SpikeSlotEvent *spike_slot_bridge_poll(SpikeSlotBridge *bridge, uint32_t timeout_millis);
bool spike_slot_bridge_is_canceled(SpikeSlotBridge *bridge, SpikeSlotEvent *event);
void spike_slot_bridge_respond(SpikeSlotBridge *bridge, SpikeSlotEvent *event, uintptr_t value, bool known);
/* Stop only after the Core worker/runtime have been freed; drain then join the
 * dispatcher before freeing the bridge. The owner retains the callback storage. */
void spike_slot_bridge_stop(SpikeSlotBridge *bridge);
void spike_slot_bridge_free(SpikeSlotBridge *bridge);
