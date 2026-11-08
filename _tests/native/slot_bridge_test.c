#include "native/slot_bridge.h"
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct TemporalCoreSlotReserveCompletionCtx {
    atomic_int state; /* 0 pending, 1 canceled, 2 completed, 3 acknowledged cancel */
    atomic_int acknowledgments;
    atomic_uintptr_t permit;
};
bool temporal_core_complete_async_reserve(const struct TemporalCoreSlotReserveCompletionCtx *raw, uintptr_t permit) {
    struct TemporalCoreSlotReserveCompletionCtx *context = (void *)raw;
    atomic_store(&context->permit, permit);
    int expected = 0;
    if (!atomic_compare_exchange_strong(&context->state, &expected, 2)) {
        assert(expected == 1);
        return false;
    }
    assert(atomic_fetch_add(&context->acknowledgments, 1) == 0);
    return true;
}
bool temporal_core_complete_async_cancel_reserve(const struct TemporalCoreSlotReserveCompletionCtx *raw) {
    struct TemporalCoreSlotReserveCompletionCtx *context = (void *)raw;
    int expected = 1;
    if (!atomic_compare_exchange_strong(&context->state, &expected, 3)) return false;
    assert(atomic_fetch_add(&context->acknowledgments, 1) == 0);
    return true;
}
static void pause_millis(unsigned milliseconds) {
    struct timespec duration = {milliseconds / 1000, (milliseconds % 1000) * 1000000};
    nanosleep(&duration, NULL);
}
static void wait_at_least(atomic_int *value, int expected) {
    for (unsigned count = 0; count < 10000; ++count) {
        if (atomic_load(value) >= expected) return;
        pause_millis(1);
    }
    assert(!"condition deadline exceeded");
}
typedef struct {
    SpikeSlotBridge *bridge;
    atomic_int capacity, live, entered, responses, hold, marks, releases;
    uintptr_t next;
    bool active[10000];
} Policy;
static void assert_text(TemporalCoreByteArrayRef value, const char *text) {
    assert(value.size == strlen(text) && !memcmp(value.data, text, value.size));
}
static void *dispatch(void *data) {
    Policy *policy = data;
    for (;;) {
        SpikeSlotEvent *event = spike_slot_bridge_poll(policy->bridge, 100);
        if (!event) continue;
        if (event->kind == SPIKE_SLOT_STOP) break;
        uintptr_t permit = 0;
        switch (event->kind) {
            case SPIKE_SLOT_RESERVE:
            case SPIKE_SLOT_TRY_RESERVE:
                assert_text(event->task_queue, "queue-世界");
                assert_text(event->worker_identity, "worker-id");
                assert_text(event->worker_build_id, "build-id");
                if (atomic_load(&policy->live) < atomic_load(&policy->capacity)) {
                    permit = ++policy->next;
                    assert(permit < 10000 && !policy->active[permit]);
                    policy->active[permit] = true;
                    atomic_fetch_add(&policy->live, 1);
                }
                atomic_fetch_add(&policy->entered, 1);
                while (atomic_load(&policy->hold)) pause_millis(1);
                break;
            case SPIKE_SLOT_MARK_USED:
                assert(event->has_info && policy->active[event->permit]);
                assert_text(event->workflow_type, "MyWorkflow");
                atomic_fetch_add(&policy->marks, 1);
                break;
            case SPIKE_SLOT_RELEASE:
                assert(event->permit > 0 && policy->active[event->permit]);
                policy->active[event->permit] = false;
                atomic_fetch_sub(&policy->live, 1);
                atomic_fetch_add(&policy->releases, 1);
                break;
            case SPIKE_SLOT_AVAILABLE:
                permit = atomic_load(&policy->capacity) - atomic_load(&policy->live);
                break;
            default: abort();
        }
        spike_slot_bridge_respond(policy->bridge, event, permit, true);
        atomic_fetch_add(&policy->responses, 1);
    }
    return NULL;
}
static TemporalCoreByteArrayRef text(const char *value) {
    return (TemporalCoreByteArrayRef){(const unsigned char *)value, strlen(value)};
}
static TemporalCoreSlotReserveCtx reservation(void) {
    return (TemporalCoreSlotReserveCtx){
        .slot_type = TemporalCoreSlotKindType_WorkflowSlotKindType,
        .task_queue = text("queue-世界"), .worker_identity = text("worker-id"),
        .worker_build_id = text("build-id"), .is_sticky = true,
    };
}
static void release(const TemporalCoreCustomSlotSupplierCallbacks *callbacks, uintptr_t permit, bool used) {
    TemporalCoreSlotInfo info = {.tag = TemporalCoreSlotInfo_WorkflowSlotInfo,
                                .workflow_slot_info = {text("MyWorkflow"), true}};
    if (used) {
        TemporalCoreSlotMarkUsedCtx mark = {info, permit};
        callbacks->mark_used(&mark, callbacks->user_data);
    }
    TemporalCoreSlotReleaseCtx released = {used ? &info : NULL, permit};
    callbacks->release(&released, callbacks->user_data);
}
static void wait_drained(Policy *policy) {
    for (unsigned count = 0; count < 10000; ++count) {
        if (!atomic_load(&policy->live)) return;
        pause_millis(1);
    }
    assert(!"permits not drained");
}
int main(void) {
    SpikeSlotBridge *bridge = spike_slot_bridge_new();
    const TemporalCoreCustomSlotSupplierCallbacks *callbacks = spike_slot_bridge_supplier(bridge).custom._0;
    Policy policy = {.bridge = bridge, .capacity = 1};
    pthread_t dispatcher;
    assert(!pthread_create(&dispatcher, NULL, dispatch, &policy));
    TemporalCoreSlotReserveCtx context = reservation();
    /* Repeated synchronous reservations and lifecycle/metric callbacks. */
    for (unsigned iteration = 0; iteration < 100; ++iteration) {
        uintptr_t permit = callbacks->try_reserve(&context, callbacks->user_data);
        assert(permit);
        uintptr_t available = 99;
        assert(callbacks->available_slots(&available, callbacks->user_data) && available == 0);
        release(callbacks, permit, true);
        wait_drained(&policy);
    }
    /* Normal asynchronous completion. */
    for (unsigned iteration = 0; iteration < 100; ++iteration) {
        struct TemporalCoreSlotReserveCompletionCtx completion = {0};
        callbacks->reserve(&context, &completion, callbacks->user_data);
        wait_at_least(&completion.acknowledgments, 1);
        assert(atomic_load(&completion.state) == 2);
        release(callbacks, atomic_load(&completion.permit), true);
        wait_drained(&policy);
    }
    /* Cancellation while V owns a newly allocated but unreported permit. */
    for (unsigned iteration = 0; iteration < 100; ++iteration) {
        atomic_store(&policy.hold, 1);
        int before = atomic_load(&policy.entered);
        struct TemporalCoreSlotReserveCompletionCtx completion = {0};
        callbacks->reserve(&context, &completion, callbacks->user_data);
        wait_at_least(&policy.entered, before + 1);
        int expected = 0;
        assert(atomic_compare_exchange_strong(&completion.state, &expected, 1));
        callbacks->cancel_reserve(&completion, callbacks->user_data);
        atomic_store(&policy.hold, 0);
        wait_at_least(&completion.acknowledgments, 1);
        assert(atomic_load(&completion.state) == 3);
        wait_drained(&policy);
    }
    /* Completion sees cancellation before Core can deliver its callback. */
    for (unsigned iteration = 0; iteration < 100; ++iteration) {
        atomic_store(&policy.hold, 1);
        int before = atomic_load(&policy.entered);
        struct TemporalCoreSlotReserveCompletionCtx completion = {0};
        callbacks->reserve(&context, &completion, callbacks->user_data);
        wait_at_least(&policy.entered, before + 1);
        int expected = 0;
        assert(atomic_compare_exchange_strong(&completion.state, &expected, 1));
        int responses = atomic_load(&policy.responses);
        atomic_store(&policy.hold, 0);
        wait_at_least(&policy.responses, responses + 1);
        callbacks->cancel_reserve(&completion, callbacks->user_data);
        wait_at_least(&completion.acknowledgments, 1);
        wait_drained(&policy);
    }
    /* Pending reservations that never receive a permit must all cancel once. */
    atomic_store(&policy.capacity, 0);
    struct TemporalCoreSlotReserveCompletionCtx pending[100] = {0};
    for (unsigned index = 0; index < 100; ++index) callbacks->reserve(&context, &pending[index], callbacks->user_data);
    for (unsigned index = 0; index < 100; ++index) {
        atomic_store(&pending[index].state, 1);
        callbacks->cancel_reserve(&pending[index], callbacks->user_data);
    }
    for (unsigned index = 0; index < 100; ++index) wait_at_least(&pending[index].acknowledgments, 1);
    spike_slot_bridge_stop(bridge);
    assert(!pthread_join(dispatcher, NULL));
    assert(atomic_load(&policy.live) == 0);
    assert(atomic_load(&policy.marks) == 200 && atomic_load(&policy.releases) == 400);
    spike_slot_bridge_free(bridge);
    puts("500 synchronous/asynchronous/canceled reservations balanced permits and completion ownership");
    return 0;
}
