#include "slot_bridge.h"
#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct SlotEvent {
    SpikeSlotEvent value;
    struct SlotEvent *next;
    const struct TemporalCoreSlotReserveCompletionCtx *completion;
    pthread_cond_t completed;
    bool synchronous;
    bool done;
    bool known;
    bool in_flight;
    bool canceled;
    bool waiting_cancel;
    uintptr_t reply;
    uint64_t ready_at;
} SlotEvent;
struct SpikeSlotBridge {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    SlotEvent *head, *tail, *pending;
    bool stopping;
    SpikeSlotEvent stop;
    TemporalCoreCustomSlotSupplierCallbacks callbacks;
};

static uint64_t monotonic_millis(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000 + (uint64_t)now.tv_nsec / 1000000;
}
static void *allocate(size_t size) {
    void *result = calloc(1, size);
    /* Slot reservation cannot error. A native allocation failure is fatal,
     * rather than returning a fabricated permit to Core. */
    if (!result) abort();
    return result;
}
static TemporalCoreByteArrayRef copy_ref(TemporalCoreByteArrayRef value) {
    if (!value.size) return (TemporalCoreByteArrayRef){0};
    unsigned char *bytes = allocate(value.size);
    memcpy(bytes, value.data, value.size);
    return (TemporalCoreByteArrayRef){bytes, value.size};
}
static SlotEvent *new_event(int kind) {
    SlotEvent *event = allocate(sizeof(*event));
    event->value.kind = kind;
    if (pthread_cond_init(&event->completed, NULL)) abort();
    return event;
}
static void free_event(SlotEvent *event) {
    free((void *)event->value.task_queue.data);
    free((void *)event->value.worker_identity.data);
    free((void *)event->value.worker_build_id.data);
    free((void *)event->value.workflow_type.data);
    free((void *)event->value.activity_type.data);
    free((void *)event->value.service.data);
    free((void *)event->value.operation.data);
    pthread_cond_destroy(&event->completed);
    free(event);
}
static void enqueue(SpikeSlotBridge *bridge, SlotEvent *event) {
    event->next = NULL;
    if (bridge->tail) bridge->tail->next = event;
    else bridge->head = event;
    bridge->tail = event;
    pthread_cond_signal(&bridge->changed);
}
static void reserve_context(SlotEvent *event, const TemporalCoreSlotReserveCtx *context) {
    event->value.slot_kind = context->slot_type;
    event->value.task_queue = copy_ref(context->task_queue);
    event->value.worker_identity = copy_ref(context->worker_identity);
    event->value.worker_build_id = copy_ref(context->worker_build_id);
    event->value.is_sticky = context->is_sticky;
}
static void slot_information(SlotEvent *event, const TemporalCoreSlotInfo *information) {
    if (!information) return;
    event->value.has_info = true;
    event->value.slot_kind = information->tag;
    switch (information->tag) {
        case TemporalCoreSlotInfo_WorkflowSlotInfo:
            event->value.workflow_type = copy_ref(information->workflow_slot_info.workflow_type);
            event->value.is_sticky = information->workflow_slot_info.is_sticky;
            break;
        case TemporalCoreSlotInfo_ActivitySlotInfo:
            event->value.activity_type = copy_ref(information->activity_slot_info.activity_type);
            break;
        case TemporalCoreSlotInfo_LocalActivitySlotInfo:
            event->value.activity_type = copy_ref(information->local_activity_slot_info.activity_type);
            break;
        case TemporalCoreSlotInfo_NexusSlotInfo:
            event->value.service = copy_ref(information->nexus_slot_info.service);
            event->value.operation = copy_ref(information->nexus_slot_info.operation);
            break;
    }
}
static void remove_pending(SpikeSlotBridge *bridge, SlotEvent *event) {
    SlotEvent **cursor = &bridge->pending;
    while (*cursor && *cursor != event) cursor = &(*cursor)->next;
    assert(*cursor == event);
    *cursor = event->next;
}
static void release_unused(SpikeSlotBridge *bridge, uintptr_t permit) {
    if (!permit) return;
    SlotEvent *event = new_event(SPIKE_SLOT_RELEASE);
    event->value.permit = permit;
    enqueue(bridge, event);
}
static void finish_cancel(SpikeSlotBridge *bridge, SlotEvent *event) {
    assert(event->canceled && !event->in_flight);
    if (!temporal_core_complete_async_cancel_reserve(event->completion)) abort();
    release_unused(bridge, event->reply);
    remove_pending(bridge, event);
    free_event(event);
}
static void on_reserve(const TemporalCoreSlotReserveCtx *context,
                       const struct TemporalCoreSlotReserveCompletionCtx *completion, void *data) {
    SpikeSlotBridge *bridge = data;
    SlotEvent *event = new_event(SPIKE_SLOT_RESERVE);
    reserve_context(event, context);
    event->completion = completion;
    pthread_mutex_lock(&bridge->mutex);
    assert(!bridge->stopping);
    /* Append: a policy with one slot must not starve older reservations. */
    SlotEvent **cursor = &bridge->pending;
    while (*cursor) cursor = &(*cursor)->next;
    *cursor = event;
    pthread_cond_signal(&bridge->changed);
    pthread_mutex_unlock(&bridge->mutex);
}
static void on_cancel(const struct TemporalCoreSlotReserveCompletionCtx *completion, void *data) {
    SpikeSlotBridge *bridge = data;
    pthread_mutex_lock(&bridge->mutex);
    SlotEvent *event = bridge->pending;
    while (event && event->completion != completion) event = event->next;
    assert(event); /* Core only cancels reservations that remain pending. */
    event->canceled = true;
    if (!event->in_flight) finish_cancel(bridge, event);
    pthread_cond_signal(&bridge->changed);
    pthread_mutex_unlock(&bridge->mutex);
}
static uintptr_t synchronous_call(SpikeSlotBridge *bridge, SlotEvent *event, bool *known) {
    event->synchronous = true;
    pthread_mutex_lock(&bridge->mutex);
    assert(!bridge->stopping);
    enqueue(bridge, event);
    while (!event->done) pthread_cond_wait(&event->completed, &bridge->mutex);
    uintptr_t result = event->reply;
    if (known) *known = event->known;
    pthread_mutex_unlock(&bridge->mutex);
    free_event(event);
    return result;
}
static uintptr_t on_try_reserve(const TemporalCoreSlotReserveCtx *context, void *data) {
    SlotEvent *event = new_event(SPIKE_SLOT_TRY_RESERVE);
    reserve_context(event, context);
    return synchronous_call(data, event, NULL);
}
static void on_mark_used(const TemporalCoreSlotMarkUsedCtx *context, void *data) {
    SpikeSlotBridge *bridge = data;
    SlotEvent *event = new_event(SPIKE_SLOT_MARK_USED);
    event->value.permit = context->slot_permit;
    slot_information(event, &context->slot_info);
    pthread_mutex_lock(&bridge->mutex);
    enqueue(bridge, event);
    pthread_mutex_unlock(&bridge->mutex);
}
static void on_release(const TemporalCoreSlotReleaseCtx *context, void *data) {
    SpikeSlotBridge *bridge = data;
    SlotEvent *event = new_event(SPIKE_SLOT_RELEASE);
    event->value.permit = context->slot_permit;
    slot_information(event, context->slot_info);
    pthread_mutex_lock(&bridge->mutex);
    enqueue(bridge, event);
    pthread_mutex_unlock(&bridge->mutex);
}
static bool on_available(uintptr_t *available, void *data) {
    SlotEvent *event = new_event(SPIKE_SLOT_AVAILABLE);
    bool known = false;
    *available = synchronous_call(data, event, &known);
    return known;
}
static void on_free(const TemporalCoreCustomSlotSupplierCallbacks *callbacks) {
    /* Storage is owned by the V connection. Its owner frees it only after Core
     * close and dispatcher drain, including Core versions that omit this hook. */
    (void)callbacks;
}
SpikeSlotBridge *spike_slot_bridge_new(void) {
    SpikeSlotBridge *bridge = allocate(sizeof(*bridge));
    if (pthread_mutex_init(&bridge->mutex, NULL) || pthread_cond_init(&bridge->changed, NULL)) abort();
    bridge->stop.kind = SPIKE_SLOT_STOP;
    bridge->callbacks = (TemporalCoreCustomSlotSupplierCallbacks){
        .reserve = on_reserve, .cancel_reserve = on_cancel,
        .try_reserve = on_try_reserve, .mark_used = on_mark_used,
        .release = on_release, .available_slots = on_available,
        .free = on_free, .user_data = bridge,
    };
    return bridge;
}
TemporalCoreSlotSupplier spike_slot_bridge_supplier(SpikeSlotBridge *bridge) {
    return (TemporalCoreSlotSupplier){.tag = TemporalCoreSlotSupplier_Custom,
                                     .custom = {&bridge->callbacks}};
}
SpikeSlotEvent *spike_slot_bridge_poll(SpikeSlotBridge *bridge, uint32_t timeout_millis) {
    uint64_t deadline = monotonic_millis() + timeout_millis;
    pthread_mutex_lock(&bridge->mutex);
    for (;;) {
        if (bridge->head) {
            SlotEvent *event = bridge->head;
            bridge->head = event->next;
            if (!bridge->head) bridge->tail = NULL;
            pthread_mutex_unlock(&bridge->mutex);
            return &event->value;
        }
        uint64_t now = monotonic_millis();
        for (SlotEvent *event = bridge->pending; event; event = event->next) {
            if (!event->in_flight && !event->canceled && !event->waiting_cancel && now >= event->ready_at) {
                event->in_flight = true;
                pthread_mutex_unlock(&bridge->mutex);
                return &event->value;
            }
        }
        if (bridge->stopping && !bridge->pending) {
            pthread_mutex_unlock(&bridge->mutex);
            return &bridge->stop;
        }
        if (now >= deadline) {
            pthread_mutex_unlock(&bridge->mutex);
            return NULL;
        }
        struct timespec until;
        clock_gettime(CLOCK_REALTIME, &until);
        uint64_t wait = deadline - now;
        if (wait > 10) wait = 10;
        until.tv_nsec += (long)wait * 1000000;
        until.tv_sec += until.tv_nsec / 1000000000;
        until.tv_nsec %= 1000000000;
        pthread_cond_timedwait(&bridge->changed, &bridge->mutex, &until);
    }
}
bool spike_slot_bridge_is_canceled(SpikeSlotBridge *bridge, SpikeSlotEvent *public_event) {
    SlotEvent *event = (SlotEvent *)public_event;
    pthread_mutex_lock(&bridge->mutex);
    assert(event->value.kind == SPIKE_SLOT_RESERVE && event->in_flight);
    bool result = event->canceled;
    pthread_mutex_unlock(&bridge->mutex);
    return result;
}
void spike_slot_bridge_respond(SpikeSlotBridge *bridge, SpikeSlotEvent *public_event, uintptr_t value, bool known) {
    SlotEvent *event = (SlotEvent *)public_event;
    pthread_mutex_lock(&bridge->mutex);
    if (event->value.kind == SPIKE_SLOT_RESERVE) {
        assert(event->in_flight);
        event->in_flight = false;
        event->reply = value;
        if (event->canceled) {
            finish_cancel(bridge, event);
        } else if (!value) {
            event->ready_at = monotonic_millis() + 10;
        } else if (temporal_core_complete_async_reserve(event->completion, value)) {
            remove_pending(bridge, event);
            free_event(event);
        } else {
            /* Core won the race. Its cancel callback must acknowledge ownership
             * before this record/context can be freed; return this unused permit. */
            event->waiting_cancel = true;
        }
    } else if (event->synchronous) {
        event->reply = value;
        event->known = known;
        event->done = true;
        pthread_cond_signal(&event->completed);
    } else {
        free_event(event);
    }
    pthread_cond_signal(&bridge->changed);
    pthread_mutex_unlock(&bridge->mutex);
}
void spike_slot_bridge_stop(SpikeSlotBridge *bridge) {
    pthread_mutex_lock(&bridge->mutex);
    bridge->stopping = true;
    pthread_cond_signal(&bridge->changed);
    pthread_mutex_unlock(&bridge->mutex);
}
void spike_slot_bridge_free(SpikeSlotBridge *bridge) {
    assert(bridge->stopping && !bridge->head && !bridge->pending);
    pthread_cond_destroy(&bridge->changed);
    pthread_mutex_destroy(&bridge->mutex);
    free(bridge);
}
