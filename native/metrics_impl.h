/* Included after SpikeCore and spike_close are declared; no V callbacks enter Tokio. */
#include "metrics.h"
struct SpikeMetricMeter {
    SpikeCore *owner;
    TemporalCoreMetricMeter *meter;
};

SpikeMetricMeter *spike_metric_meter_open(SpikeCore *core) {
    SpikeMetricMeter *meter = calloc(1, sizeof(*meter));
    if (!meter) return NULL;
    meter->owner = core->shared_owner ? core->shared_owner : core;
    atomic_fetch_add_explicit(&meter->owner->references, 1, memory_order_relaxed);
    meter->meter = temporal_core_metric_meter_new(core->runtime);
    return meter;
}

void spike_metric_meter_close(SpikeMetricMeter *meter) {
    if (meter->meter) temporal_core_metric_meter_free(meter->meter);
    SpikeCore *owner = meter->owner;
    free(meter);
    spike_close(owner);
}

TemporalCoreMetric *spike_metric_new(SpikeMetricMeter *meter, const char *name, const char *description, const char *unit, int32_t kind) {
    if (!meter->meter) return NULL;
    TemporalCoreMetricOptions options = {
        .name = text_ref(name), .description = text_ref(description), .unit = text_ref(unit),
        .kind = (TemporalCoreMetricKind)kind,
    };
    return temporal_core_metric_new(meter->meter, &options);
}

void spike_metric_free(TemporalCoreMetric *metric) {
    if (metric) temporal_core_metric_free(metric);
}

int spike_metric_record(SpikeMetricMeter *meter, TemporalCoreMetric *metric, int32_t metric_kind, int32_t value_kind, uint64_t integer_value, int64_t signed_value, double float_value, const SpikeMetricAttribute *attributes, size_t count) {
    bool compatible = (value_kind == 1 && (metric_kind == 1 || metric_kind == 2 || metric_kind == 5)) ||
                      (value_kind == 2 && metric_kind == 7) ||
                      (value_kind == 3 && (metric_kind == 3 || metric_kind == 6)) ||
                      (value_kind == 4 && metric_kind == 4);
    if (!compatible) return -2;
    if (!metric) return 0;
    if (count > SIZE_MAX / sizeof(TemporalCoreMetricAttribute)) return -1;
    TemporalCoreMetricAttribute empty = {0};
    TemporalCoreMetricAttribute *values = count ? calloc(count, sizeof(*values)) : &empty;
    if (!values) return -1;
    for (size_t index = 0; index < count; index++) {
        values[index].key = text_ref(attributes[index].key);
        values[index].value_type = (TemporalCoreMetricAttributeValueType)attributes[index].kind;
        switch (attributes[index].kind) {
        case 1: values[index].value.string_value = text_ref(attributes[index].string_value); break;
        case 2: values[index].value.int_value = attributes[index].integer_value; break;
        case 3: values[index].value.float_value = attributes[index].float_value; break;
        case 4: values[index].value.bool_value = attributes[index].boolean_value; break;
        default: if (count) free(values); return -2;
        }
    }
    TemporalCoreMetricAttributes *tags = temporal_core_metric_attributes_new(meter->meter, values, count);
    if (count) free(values);
    switch (value_kind) {
    case 1: temporal_core_metric_record_integer(metric, integer_value, tags); break;
    case 2: temporal_core_metric_record_integer_signed(metric, signed_value, tags); break;
    case 3: temporal_core_metric_record_float(metric, float_value, tags); break;
    case 4: temporal_core_metric_record_duration(metric, integer_value, tags); break;
    }
    temporal_core_metric_attributes_free(tags);
    return 0;
}
