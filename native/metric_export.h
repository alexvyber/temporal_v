#pragma once
#include "include/temporal-sdk-core-c-bridge.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct SpikeMetricExport SpikeMetricExport;
typedef struct SpikeMetricPoint SpikeMetricPoint;
typedef struct {
    uint64_t accepted, dropped, allocation_failures, pending, pending_bytes, live_definitions, live_labels;
    bool core_done;
} SpikeMetricExportStats;

SpikeMetricExport *spike_metric_export_new(size_t max_pending, uint64_t max_bytes);
const TemporalCoreCustomMetricMeter *spike_metric_export_claim(SpikeMetricExport *exporter);
void spike_metric_export_abort_unclaimed(SpikeMetricExport *exporter);
void spike_metric_export_mark_dispatcher(SpikeMetricExport *exporter);
bool spike_metric_export_on_dispatcher(SpikeMetricExport *exporter);
void spike_metric_export_release_owner(SpikeMetricExport *exporter);
SpikeMetricExportStats spike_metric_export_stats(SpikeMetricExport *exporter);
bool spike_metric_export_done(SpikeMetricExport *exporter);
SpikeMetricPoint *spike_metric_export_poll(SpikeMetricExport *exporter, uint32_t timeout_millis);
void spike_metric_point_free(SpikeMetricPoint *point);
uint32_t spike_metric_point_kind(const SpikeMetricPoint *point);
TemporalCoreByteArrayRef spike_metric_point_name(const SpikeMetricPoint *point);
TemporalCoreByteArrayRef spike_metric_point_description(const SpikeMetricPoint *point);
TemporalCoreByteArrayRef spike_metric_point_unit(const SpikeMetricPoint *point);
uint64_t spike_metric_point_integer(const SpikeMetricPoint *point);
int64_t spike_metric_point_signed(const SpikeMetricPoint *point);
double spike_metric_point_float(const SpikeMetricPoint *point);
size_t spike_metric_point_attribute_count(const SpikeMetricPoint *point);
TemporalCoreByteArrayRef spike_metric_point_attribute_key(const SpikeMetricPoint *point, size_t index);
uint32_t spike_metric_point_attribute_type(const SpikeMetricPoint *point, size_t index);
TemporalCoreByteArrayRef spike_metric_point_attribute_string(const SpikeMetricPoint *point, size_t index);
int64_t spike_metric_point_attribute_integer(const SpikeMetricPoint *point, size_t index);
double spike_metric_point_attribute_float(const SpikeMetricPoint *point, size_t index);
bool spike_metric_point_attribute_bool(const SpikeMetricPoint *point, size_t index);
