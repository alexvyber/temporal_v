#define _POSIX_C_SOURCE 200809L
#include "metric_export.h"
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    atomic_size_t references;
    SpikeMetricExport *owner;
    TemporalCoreByteArrayRef name, description, unit;
    uint64_t bytes;
    enum TemporalCoreMetricKind kind;
    bool failed, counted;
} MetricDefinition;

typedef struct {
    atomic_size_t references;
    SpikeMetricExport *owner;
    TemporalCoreCustomMetricAttribute *values;
    size_t count;
    uint64_t bytes;
    bool failed, counted;
} MetricLabels;

struct SpikeMetricPoint {
    SpikeMetricPoint *next;
    SpikeMetricExport *owner;
    MetricDefinition *definition;
    MetricLabels *labels;
    uint64_t bytes;
    union { uint64_t integer; double floating; } value;
};

struct SpikeMetricExport {
    TemporalCoreCustomMetricMeter meter;
    atomic_size_t references;
    pthread_mutex_t mutex;
    pthread_cond_t ready;
    SpikeMetricPoint *head, *tail;
    size_t max_pending;
    uint64_t max_bytes;
    SpikeMetricExportStats stats;
    bool claimed, dispatcher_set;
    pthread_t dispatcher;
    MetricDefinition failed_definition;
    MetricLabels failed_labels;
};
_Static_assert(offsetof(SpikeMetricExport, meter) == 0, "meter callback context must be first");

static void retain_export(SpikeMetricExport *owner) { atomic_fetch_add(&owner->references, 1); }
static void release_export(SpikeMetricExport *owner) {
    if (atomic_fetch_sub(&owner->references, 1) != 1) return;
    pthread_cond_destroy(&owner->ready);
    pthread_mutex_destroy(&owner->mutex);
    free(owner);
}
static void allocation_failed(SpikeMetricExport *owner) {
    pthread_mutex_lock(&owner->mutex); owner->stats.allocation_failures++; pthread_mutex_unlock(&owner->mutex);
}
static bool copy_bytes(TemporalCoreByteArrayRef source, TemporalCoreByteArrayRef *destination) {
    if (source.size == SIZE_MAX || (!source.data && source.size)) return false;
    uint8_t *data = malloc(source.size + 1);
    if (!data) return false;
    if (source.size) memcpy(data, source.data, source.size);
    data[source.size] = 0;
    *destination = (TemporalCoreByteArrayRef){data, source.size};
    return true;
}
static void retain_definition(MetricDefinition *definition) {
    if (!definition->failed) atomic_fetch_add(&definition->references, 1);
}
static void free_definition(const void *pointer) {
    MetricDefinition *definition = (MetricDefinition *)pointer;
    if (!definition || definition->failed || atomic_fetch_sub(&definition->references, 1) != 1) return;
    if (definition->counted) {
        pthread_mutex_lock(&definition->owner->mutex); definition->owner->stats.live_definitions--; pthread_mutex_unlock(&definition->owner->mutex);
    }
    free((void *)definition->name.data); free((void *)definition->description.data); free((void *)definition->unit.data);
    free(definition);
}
static const void *new_definition(const TemporalCoreCustomMetricMeter *meter, TemporalCoreByteArrayRef name,
                                  TemporalCoreByteArrayRef description, TemporalCoreByteArrayRef unit,
                                  enum TemporalCoreMetricKind kind) {
    SpikeMetricExport *owner = (SpikeMetricExport *)meter;
    MetricDefinition *definition = calloc(1, sizeof(*definition));
    if (!definition) { allocation_failed(owner); return &owner->failed_definition; }
    atomic_init(&definition->references, 1); definition->owner = owner; definition->kind = kind;
    if (!copy_bytes(name, &definition->name) || !copy_bytes(description, &definition->description) || !copy_bytes(unit, &definition->unit)) {
        free_definition(definition); allocation_failed(owner); return &owner->failed_definition;
    }
    definition->bytes = sizeof(*definition) + name.size + description.size + unit.size;
    definition->counted = true;
    pthread_mutex_lock(&owner->mutex); owner->stats.live_definitions++; pthread_mutex_unlock(&owner->mutex);
    return definition;
}
static void clear_label(TemporalCoreCustomMetricAttribute *label) {
    free((void *)label->key.data);
    if (label->value_type == TemporalCoreMetricAttributeValueType_String) free((void *)label->value.string_value.data);
    memset(label, 0, sizeof(*label));
}
static bool copy_label(const TemporalCoreCustomMetricAttribute *source, TemporalCoreCustomMetricAttribute *destination) {
    destination->value_type = source->value_type;
    if (!copy_bytes(source->key, &destination->key)) return false;
    if (source->value_type == TemporalCoreMetricAttributeValueType_String) {
        TemporalCoreByteArrayRef value = {source->value.string_value.data, source->value.string_value.size}, copied = {0};
        if (!copy_bytes(value, &copied)) { clear_label(destination); return false; }
        destination->value.string_value = (TemporalCoreCustomMetricAttributeValueString){copied.data, copied.size};
    } else { destination->value = source->value; }
    return true;
}
static void retain_labels(MetricLabels *labels) { if (labels && !labels->failed) atomic_fetch_add(&labels->references, 1); }
static void free_labels(const void *pointer) {
    MetricLabels *labels = (MetricLabels *)pointer;
    if (!labels || labels->failed || atomic_fetch_sub(&labels->references, 1) != 1) return;
    if (labels->counted) {
        pthread_mutex_lock(&labels->owner->mutex); labels->owner->stats.live_labels--; pthread_mutex_unlock(&labels->owner->mutex);
    }
    for (size_t index = 0; index < labels->count; index++) clear_label(&labels->values[index]);
    free(labels->values); free(labels);
}
static bool append_label(MetricLabels *labels, const TemporalCoreCustomMetricAttribute *source) {
    size_t position = 0;
    for (; position < labels->count; position++) {
        TemporalCoreByteArrayRef key = labels->values[position].key;
        if (key.size == source->key.size && (!key.size || !memcmp(key.data, source->key.data, key.size))) break;
    }
    TemporalCoreCustomMetricAttribute copy = {0};
    if (!copy_label(source, &copy)) return false;
    if (position < labels->count) clear_label(&labels->values[position]); else labels->count++;
    labels->values[position] = copy;
    return true;
}
static const void *new_labels(const TemporalCoreCustomMetricMeter *meter, const void *append_from,
                              const TemporalCoreCustomMetricAttribute *attributes, size_t size) {
    SpikeMetricExport *owner = (SpikeMetricExport *)meter;
    const MetricLabels *previous = append_from;
    if (previous && (previous->failed || previous->owner != owner)) return &owner->failed_labels;
    size_t prior_count = previous ? previous->count : 0;
    if (size > SIZE_MAX / sizeof(*attributes) - prior_count) { allocation_failed(owner); return &owner->failed_labels; }
    MetricLabels *labels = calloc(1, sizeof(*labels));
    if (!labels) { allocation_failed(owner); return &owner->failed_labels; }
    atomic_init(&labels->references, 1); labels->owner = owner;
    if (prior_count + size) {
        labels->values = calloc(prior_count + size, sizeof(*attributes));
        if (!labels->values) goto failed;
    }
    for (size_t index = 0; index < prior_count; index++) if (!append_label(labels, &previous->values[index])) goto failed;
    for (size_t index = 0; index < size; index++) if (!append_label(labels, &attributes[index])) goto failed;
    labels->bytes = sizeof(*labels) + labels->count * sizeof(*attributes);
    for (size_t index = 0; index < labels->count; index++) {
        labels->bytes += labels->values[index].key.size;
        if (labels->values[index].value_type == TemporalCoreMetricAttributeValueType_String) labels->bytes += labels->values[index].value.string_value.size;
    }
    labels->counted = true;
    pthread_mutex_lock(&owner->mutex); owner->stats.live_labels++; pthread_mutex_unlock(&owner->mutex);
    return labels;
failed:
    free_labels(labels); allocation_failed(owner); return &owner->failed_labels;
}
static void record_point(const void *metric, const void *attributes, uint64_t integer, double floating) {
    MetricDefinition *definition = (MetricDefinition *)metric;
    if (!definition) return;
    MetricLabels *labels = (MetricLabels *)attributes;
    SpikeMetricExport *owner = definition->owner;
    uint64_t cost = sizeof(SpikeMetricPoint) + definition->bytes + (labels ? labels->bytes : 0);
    pthread_mutex_lock(&owner->mutex);
    if (definition->failed || (labels && (labels->failed || labels->owner != owner)) || owner->stats.core_done ||
        owner->stats.pending >= owner->max_pending || cost > owner->max_bytes - owner->stats.pending_bytes) {
        owner->stats.dropped++; pthread_mutex_unlock(&owner->mutex); return;
    }
    SpikeMetricPoint *point = calloc(1, sizeof(*point));
    if (!point) { owner->stats.dropped++; owner->stats.allocation_failures++; pthread_mutex_unlock(&owner->mutex); return; }
    point->owner = owner; point->definition = definition; point->labels = labels; point->bytes = cost;
    if (definition->kind == TemporalCoreMetricKind_HistogramFloat || definition->kind == TemporalCoreMetricKind_GaugeFloat) point->value.floating = floating;
    else point->value.integer = integer;
    retain_definition(definition); retain_labels(labels); retain_export(owner);
    if (owner->tail) owner->tail->next = point; else owner->head = point;
    owner->tail = point; owner->stats.accepted++; owner->stats.pending++; owner->stats.pending_bytes += cost;
    pthread_cond_signal(&owner->ready); pthread_mutex_unlock(&owner->mutex);
}
static void record_integer(const void *metric, uint64_t value, const void *attributes) { record_point(metric, attributes, value, 0); }
static void record_float(const void *metric, double value, const void *attributes) { record_point(metric, attributes, 0, value); }
static void free_meter(const TemporalCoreCustomMetricMeter *meter) {
    SpikeMetricExport *owner = (SpikeMetricExport *)meter;
    pthread_mutex_lock(&owner->mutex);
    bool release = owner->claimed && !owner->stats.core_done;
    owner->stats.core_done = true;
    pthread_cond_broadcast(&owner->ready); pthread_mutex_unlock(&owner->mutex);
    if (release) release_export(owner);
}
SpikeMetricExport *spike_metric_export_new(size_t max_pending, uint64_t max_bytes) {
    if (!max_pending || !max_bytes) return NULL;
    SpikeMetricExport *owner = calloc(1, sizeof(*owner));
    if (!owner) return NULL;
    if (pthread_mutex_init(&owner->mutex, NULL)) { free(owner); return NULL; }
    if (pthread_cond_init(&owner->ready, NULL)) { pthread_mutex_destroy(&owner->mutex); free(owner); return NULL; }
    atomic_init(&owner->references, 1); owner->max_pending = max_pending; owner->max_bytes = max_bytes;
    owner->failed_definition.failed = true; owner->failed_definition.owner = owner;
    owner->failed_labels.failed = true; owner->failed_labels.owner = owner;
    owner->meter = (TemporalCoreCustomMetricMeter){
        .metric_new = new_definition, .metric_free = free_definition,
        .metric_record_integer = record_integer, .metric_record_float = record_float, .metric_record_duration = record_integer,
        .attributes_new = new_labels, .attributes_free = free_labels, .meter_free = free_meter,
    };
    return owner;
}
const TemporalCoreCustomMetricMeter *spike_metric_export_claim(SpikeMetricExport *owner) {
    pthread_mutex_lock(&owner->mutex);
    bool valid = !owner->claimed && !owner->stats.core_done;
    if (valid) { owner->claimed = true; retain_export(owner); }
    pthread_mutex_unlock(&owner->mutex);
    return valid ? &owner->meter : NULL;
}
void spike_metric_export_abort_unclaimed(SpikeMetricExport *owner) {
    pthread_mutex_lock(&owner->mutex);
    if (!owner->claimed) { owner->stats.core_done = true; pthread_cond_broadcast(&owner->ready); }
    pthread_mutex_unlock(&owner->mutex);
}
void spike_metric_export_mark_dispatcher(SpikeMetricExport *owner) {
    pthread_mutex_lock(&owner->mutex); owner->dispatcher = pthread_self(); owner->dispatcher_set = true; pthread_mutex_unlock(&owner->mutex);
}
bool spike_metric_export_on_dispatcher(SpikeMetricExport *owner) {
    pthread_mutex_lock(&owner->mutex); bool result = owner->dispatcher_set && pthread_equal(owner->dispatcher, pthread_self()); pthread_mutex_unlock(&owner->mutex); return result;
}
void spike_metric_export_release_owner(SpikeMetricExport *owner) { release_export(owner); }
SpikeMetricExportStats spike_metric_export_stats(SpikeMetricExport *owner) {
    pthread_mutex_lock(&owner->mutex); SpikeMetricExportStats stats = owner->stats; pthread_mutex_unlock(&owner->mutex); return stats;
}
bool spike_metric_export_done(SpikeMetricExport *owner) {
    SpikeMetricExportStats stats = spike_metric_export_stats(owner); return stats.core_done && !stats.pending;
}
SpikeMetricPoint *spike_metric_export_poll(SpikeMetricExport *owner, uint32_t timeout_millis) {
    struct timespec deadline; clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += timeout_millis / 1000; deadline.tv_nsec += (long)(timeout_millis % 1000) * 1000000;
    if (deadline.tv_nsec >= 1000000000) { deadline.tv_sec++; deadline.tv_nsec -= 1000000000; }
    pthread_mutex_lock(&owner->mutex);
    while (!owner->head && !owner->stats.core_done && timeout_millis) {
        if (pthread_cond_timedwait(&owner->ready, &owner->mutex, &deadline)) break;
    }
    SpikeMetricPoint *point = owner->head;
    if (point) { owner->head = point->next; if (!owner->head) owner->tail = NULL; point->next = NULL; }
    pthread_mutex_unlock(&owner->mutex); return point;
}
void spike_metric_point_free(SpikeMetricPoint *point) {
    if (!point) return;
    SpikeMetricExport *owner = point->owner;
    free_definition(point->definition); free_labels(point->labels);
    pthread_mutex_lock(&owner->mutex); owner->stats.pending--; owner->stats.pending_bytes -= point->bytes; pthread_mutex_unlock(&owner->mutex);
    free(point); release_export(owner);
}
uint32_t spike_metric_point_kind(const SpikeMetricPoint *point) { return (uint32_t)point->definition->kind; }
TemporalCoreByteArrayRef spike_metric_point_name(const SpikeMetricPoint *point) { return point->definition->name; }
TemporalCoreByteArrayRef spike_metric_point_description(const SpikeMetricPoint *point) { return point->definition->description; }
TemporalCoreByteArrayRef spike_metric_point_unit(const SpikeMetricPoint *point) { return point->definition->unit; }
uint64_t spike_metric_point_integer(const SpikeMetricPoint *point) { return point->value.integer; }
int64_t spike_metric_point_signed(const SpikeMetricPoint *point) { int64_t result; memcpy(&result, &point->value.integer, sizeof(result)); return result; }
double spike_metric_point_float(const SpikeMetricPoint *point) { return point->value.floating; }
size_t spike_metric_point_attribute_count(const SpikeMetricPoint *point) { return point->labels ? point->labels->count : 0; }
TemporalCoreByteArrayRef spike_metric_point_attribute_key(const SpikeMetricPoint *point, size_t index) { return point->labels->values[index].key; }
uint32_t spike_metric_point_attribute_type(const SpikeMetricPoint *point, size_t index) { return (uint32_t)point->labels->values[index].value_type; }
TemporalCoreByteArrayRef spike_metric_point_attribute_string(const SpikeMetricPoint *point, size_t index) {
    TemporalCoreCustomMetricAttributeValueString value = point->labels->values[index].value.string_value; return (TemporalCoreByteArrayRef){value.data, value.size};
}
int64_t spike_metric_point_attribute_integer(const SpikeMetricPoint *point, size_t index) { return point->labels->values[index].value.int_value; }
double spike_metric_point_attribute_float(const SpikeMetricPoint *point, size_t index) { return point->labels->values[index].value.float_value; }
bool spike_metric_point_attribute_bool(const SpikeMetricPoint *point, size_t index) { return point->labels->values[index].value.bool_value; }
