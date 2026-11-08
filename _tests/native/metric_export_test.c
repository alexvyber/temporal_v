#define _POSIX_C_SOURCE 200809L
#include "native/metric_export.h"
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

static TemporalCoreByteArrayRef text(const char *value) { return (TemporalCoreByteArrayRef){(const uint8_t *)value, strlen(value)}; }
static void equal(TemporalCoreByteArrayRef value, const char *expected) { assert(value.size == strlen(expected) && !memcmp(value.data, expected, value.size)); }
static void balanced(SpikeMetricExport *owner) {
    SpikeMetricExportStats stats = spike_metric_export_stats(owner);
    assert(!stats.pending && !stats.pending_bytes && !stats.live_definitions && !stats.live_labels);
    assert(stats.core_done && spike_metric_export_done(owner));
}
static void values_test(void) {
    SpikeMetricExport *owner = spike_metric_export_new(2, 65536);
    const TemporalCoreCustomMetricMeter *meter = spike_metric_export_claim(owner);
    assert(meter && !spike_metric_export_claim(owner));
    TemporalCoreCustomMetricAttribute attributes[4] = {
        {.key = text("string"), .value_type = TemporalCoreMetricAttributeValueType_String, .value.string_value = {(const uint8_t *)"a\0b", 3}},
        {.key = text("int"), .value_type = TemporalCoreMetricAttributeValueType_Int, .value.int_value = -9},
        {.key = text("float"), .value_type = TemporalCoreMetricAttributeValueType_Float, .value.float_value = 1.5},
        {.key = text("bool"), .value_type = TemporalCoreMetricAttributeValueType_Bool, .value.bool_value = true},
    };
    const void *initial = meter->attributes_new(meter, NULL, attributes, 4);
    attributes[1].value.int_value = -100;
    const void *labels = meter->attributes_new(meter, initial, &attributes[1], 1);
    meter->attributes_free(initial);
    for (int kind = 1; kind <= 7; kind++) {
        const void *metric = meter->metric_new(meter, text("name"), text("description"), text("unit"), (enum TemporalCoreMetricKind)kind);
        if (kind == 3 || kind == 6) meter->metric_record_float(metric, 2.75, labels);
        else if (kind == 4) meter->metric_record_duration(metric, 12345, labels);
        else meter->metric_record_integer(metric, kind == 7 ? (uint64_t)(int64_t)-7 : UINT64_MAX, labels);
        meter->metric_free(metric);
        SpikeMetricPoint *point = spike_metric_export_poll(owner, 0);
        assert(point && spike_metric_point_kind(point) == (uint32_t)kind);
        equal(spike_metric_point_name(point), "name"); equal(spike_metric_point_description(point), "description"); equal(spike_metric_point_unit(point), "unit");
        assert(spike_metric_point_attribute_count(point) == 4);
        assert(spike_metric_point_attribute_type(point, 0) == TemporalCoreMetricAttributeValueType_String);
        TemporalCoreByteArrayRef string = spike_metric_point_attribute_string(point, 0);
        assert(string.size == 3 && !memcmp(string.data, "a\0b", 3));
        assert(spike_metric_point_attribute_integer(point, 1) == -100);
        assert(spike_metric_point_attribute_float(point, 2) == 1.5 && spike_metric_point_attribute_bool(point, 3));
        if (kind == 3 || kind == 6) assert(spike_metric_point_float(point) == 2.75);
        else if (kind == 7) assert(spike_metric_point_signed(point) == -7);
        else assert(spike_metric_point_integer(point) == (kind == 4 ? 12345 : UINT64_MAX));
        spike_metric_point_free(point);
    }
    const void *metric = meter->metric_new(meter, text("limited"), text(""), text(""), TemporalCoreMetricKind_CounterInteger);
    for (int index = 0; index < 3; index++) meter->metric_record_integer(metric, 1, labels);
    assert(spike_metric_export_stats(owner).dropped == 1);
    meter->metric_free(metric); meter->attributes_free(labels); meter->meter_free(meter);
    assert(!spike_metric_export_done(owner));
    SpikeMetricPoint *point;
    while ((point = spike_metric_export_poll(owner, 0))) spike_metric_point_free(point);
    balanced(owner); spike_metric_export_release_owner(owner);
    owner = spike_metric_export_new(2, 1); meter = spike_metric_export_claim(owner);
    metric = meter->metric_new(meter, text("bytes"), text(""), text(""), TemporalCoreMetricKind_CounterInteger);
    meter->metric_record_integer(metric, 1, NULL);
    assert(spike_metric_export_stats(owner).dropped == 1);
    meter->metric_free(metric); meter->meter_free(meter); balanced(owner); spike_metric_export_release_owner(owner);
}
static void independent_runtime_test(void) {
    SpikeMetricExport *first = spike_metric_export_new(2, 65536), *second = spike_metric_export_new(2, 65536);
    const TemporalCoreCustomMetricMeter *left = spike_metric_export_claim(first), *right = spike_metric_export_claim(second);
    char key[] = "dynamic", value[] = "source";
    TemporalCoreCustomMetricAttribute source = {.key = text(key), .value_type = TemporalCoreMetricAttributeValueType_String,
        .value.string_value = {(const uint8_t *)value, 6}};
    const void *labels = left->attributes_new(left, NULL, &source, 1);
    memset(key, 'x', sizeof(key) - 1); memset(value, 'x', sizeof(value) - 1);
    const void *metric = left->metric_new(left, text("first"), text(""), text(""), TemporalCoreMetricKind_CounterInteger);
    const void *other_metric = right->metric_new(right, text("second"), text(""), text(""), TemporalCoreMetricKind_CounterInteger);
    left->metric_record_integer(metric, 1, labels);
    right->metric_record_integer(other_metric, 2, labels);
    assert(spike_metric_export_stats(second).dropped == 1);
    const void *invalid = right->attributes_new(right, labels, NULL, 0);
    right->metric_record_integer(other_metric, 2, invalid);
    right->attributes_free(invalid);
    assert(spike_metric_export_stats(second).dropped == 2);
    SpikeMetricPoint *point = spike_metric_export_poll(first, 0);
    equal(spike_metric_point_name(point), "first"); equal(spike_metric_point_attribute_key(point, 0), "dynamic");
    equal(spike_metric_point_attribute_string(point, 0), "source");
    spike_metric_point_free(point);
    left->metric_free(metric); left->attributes_free(labels); left->meter_free(left);
    right->metric_free(other_metric); right->meter_free(right);
    balanced(first); balanced(second); spike_metric_export_release_owner(first); spike_metric_export_release_owner(second);
}
static void owner_order_test(void) {
    SpikeMetricExport *owner = spike_metric_export_new(1, 65536);
    spike_metric_export_abort_unclaimed(owner); balanced(owner);
    assert(!spike_metric_export_claim(owner)); spike_metric_export_release_owner(owner);
    owner = spike_metric_export_new(1, 65536);
    const TemporalCoreCustomMetricMeter *meter = spike_metric_export_claim(owner);
    const void *metric = meter->metric_new(meter, text("retained"), text(""), text(""), TemporalCoreMetricKind_CounterInteger);
    meter->metric_record_integer(metric, 19, NULL);
    SpikeMetricPoint *point = spike_metric_export_poll(owner, 0);
    spike_metric_export_release_owner(owner);
    meter->metric_free(metric); meter->meter_free(meter);
    equal(spike_metric_point_name(point), "retained");
    assert(spike_metric_point_integer(point) == 19);
    spike_metric_point_free(point);
}
typedef struct { SpikeMetricExport *owner; const TemporalCoreCustomMetricMeter *meter; const void *metric; } Context;
static void *produce(void *argument) {
    Context *context = argument;
    assert(!spike_metric_export_on_dispatcher(context->owner));
    for (size_t index = 0; index < 2000; index++) context->meter->metric_record_integer(context->metric, index, NULL);
    return NULL;
}
static void *consume(void *argument) {
    Context *context = argument;
    while (!spike_metric_export_done(context->owner)) {
        SpikeMetricPoint *point = spike_metric_export_poll(context->owner, 1);
        if (point) { assert(spike_metric_point_integer(point) < 2000); spike_metric_point_free(point); }
    }
    return NULL;
}
static void concurrent_test(void) {
    SpikeMetricExport *owner = spike_metric_export_new(64, 65536);
    const TemporalCoreCustomMetricMeter *meter = spike_metric_export_claim(owner);
    Context context = {owner, meter, meter->metric_new(meter, text("concurrent"), text(""), text(""), TemporalCoreMetricKind_CounterInteger)};
    spike_metric_export_mark_dispatcher(owner);
    assert(spike_metric_export_on_dispatcher(owner));
    pthread_t reader, writers[8];
    assert(!pthread_create(&reader, NULL, consume, &context));
    for (size_t index = 0; index < 8; index++) assert(!pthread_create(&writers[index], NULL, produce, &context));
    for (size_t index = 0; index < 8; index++) assert(!pthread_join(writers[index], NULL));
    meter->metric_free(context.metric); meter->meter_free(meter);
    assert(!pthread_join(reader, NULL));
    balanced(owner);
    SpikeMetricExportStats stats = spike_metric_export_stats(owner);
    assert(stats.accepted + stats.dropped == 16000);
    spike_metric_export_release_owner(owner);
}
int main(void) {
    values_test(); independent_runtime_test(); owner_order_test(); concurrent_test();
    puts("16015 record attempts covered typed values, u64/signed ranges, duration, immutable label override, limits, concurrent dispatch, retained events and balanced definition/label ownership");
    return 0;
}
