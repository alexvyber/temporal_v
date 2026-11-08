#pragma once
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
typedef struct {
    unsigned char *data;
    size_t size;
    size_t capacity;
} TemporalVPayloadBytes;
typedef struct {
    TemporalVPayloadBytes *body;
    TemporalVPayloadBytes *failure;
} TemporalVPayloadVisitResult;
TemporalVPayloadVisitResult temporal_v_payload_visit(uint32_t kind, uint32_t service,
    const unsigned char *method, size_t method_size, const unsigned char *input, size_t input_size, bool restore);
void temporal_v_payload_bytes_free(TemporalVPayloadBytes *bytes);
