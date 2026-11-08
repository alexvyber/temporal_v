# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
FROM rust:1.99.1-bookworm@sha256:c9155318d07341301a78b45ecdd81af2e43b8b297d64e8fd4c5e80d7735607c3 AS toolchain
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang libclang-rt-dev make curl ca-certificates python3 pkg-config patchelf openssl libssl-dev zlib1g-dev liblzma-dev \
    && rm -rf /var/lib/apt/lists/*
ENV SPIKE_ARTIFACTS=/build/artifacts \
    CARGO_BIN=/usr/local/cargo/bin/cargo \
    CARGO_BUILD_JOBS=1 \
    SPIKE_BUILD_PROFILE=release
WORKDIR /build/v-sdk
COPY scripts/bootstrap.sh scripts/env.sh scripts/install_test_server.py scripts/prepare_v_runtime.py scripts/prepare_formatter.py scripts/
COPY locks/test-server.json locks/v-formatter.json locks/
COPY patches/oneof-case-names.patch patches/v-panic-stack.patch patches/
COPY locks/v-runtime.json locks/v-runtime.json
RUN bash scripts/bootstrap.sh
ENV PATH=/build/artifacts/tools/v:$PATH

FROM toolchain AS build
ARG TARGETARCH
COPY . .
# Keep dependency compilation across SDK/test edits, with separate native targets.
# Copy the finished library out while the cache is mounted; validation restores
# that exact copy rather than borrowing a later build's mutable cache.
RUN --mount=type=cache,id=temporal-v-${TARGETARCH}-rust-1-99-1,target=/build/artifacts/rust-target,sharing=locked \
    --mount=type=cache,id=temporal-v-${TARGETARCH}-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=temporal-v-${TARGETARCH}-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    bash scripts/build.sh && \
    mkdir -p /opt/v-sdk/bin /opt/v-sdk/lib && \
    cp /build/artifacts/tools/v-worker /build/artifacts/tools/v-workflow-worker /build/artifacts/tools/v-nexus-worker /build/artifacts/tools/v-client /build/artifacts/tools/v-replay /opt/v-sdk/bin/ && \
    cp /build/artifacts/rust-target/release/libtemporalio_sdk_core_c_bridge.so /build/artifacts/rust-target/release/libtemporal_v_payloads.so /opt/v-sdk/lib/

RUN for binary in /opt/v-sdk/bin/*; do patchelf --set-rpath '$ORIGIN/../lib' "$binary"; done
RUN mkdir -p /opt/replay-baseline-v1-bin && \
    cp /build/artifacts/tools/v-replay-baseline-v1-worker /build/artifacts/tools/v-replay-baseline-v1-activities /build/artifacts/tools/v-replay-baseline-v1-replay /build/artifacts/tools/v-replay-baseline-v1-side-effects /opt/replay-baseline-v1-bin/ && \
    for binary in /opt/replay-baseline-v1-bin/*; do patchelf --set-rpath '$ORIGIN/../lib' "$binary"; done

FROM build AS validation
RUN mkdir -p /build/artifacts/rust-target/release && \
    cp /opt/v-sdk/lib/*.so /build/artifacts/rust-target/release/ && \
    bash scripts/check.sh

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc AS runtime-base
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 libstdc++6 libssl3 zlib1g liblzma5 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/v-sdk /opt/v-sdk
ENV PATH=/opt/v-sdk/bin:$PATH
USER 65532:65532
ENTRYPOINT ["v-workflow-worker"]

# Frozen replay-baseline-v1 binaries are test-only.
FROM runtime-base AS compatibility-runtime
COPY --from=build /opt/replay-baseline-v1-bin/v-replay-baseline-v1-worker /opt/v-sdk/bin/v-workflow-worker
COPY --from=build /opt/replay-baseline-v1-bin/v-replay-baseline-v1-activities /opt/v-sdk/bin/v-worker
COPY --from=build /opt/replay-baseline-v1-bin/v-replay-baseline-v1-replay /opt/v-sdk/bin/v-replay

COPY --from=build /opt/replay-baseline-v1-bin/v-replay-baseline-v1-side-effects /opt/v-sdk/bin/v-side-effects

FROM runtime-base AS runtime
