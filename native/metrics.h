#pragma once
#include "core_shim.h"
typedef struct SpikeMetricMeter SpikeMetricMeter;
typedef struct {
    const char *key;
    int32_t kind;
    const char *string_value;
    int64_t integer_value;
    double float_value;
    bool boolean_value;
} SpikeMetricAttribute;
SpikeMetricMeter *spike_metric_meter_open(SpikeCore *core);
void spike_metric_meter_close(SpikeMetricMeter *meter);
TemporalCoreMetric *spike_metric_new(SpikeMetricMeter *meter, const char *name, const char *description, const char *unit, int32_t kind);
void spike_metric_free(TemporalCoreMetric *metric);
int spike_metric_record(SpikeMetricMeter *meter, TemporalCoreMetric *metric, int32_t metric_kind, int32_t value_kind, uint64_t integer_value, int64_t signed_value, double float_value, const SpikeMetricAttribute *attributes, size_t count);
