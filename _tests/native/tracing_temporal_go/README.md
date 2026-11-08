# Actual Go Temporal tracing reference

Test tooling pinned to Temporal SDK 1.35.0, the official OpenTelemetry interceptor
0.6.0 and OpenTelemetry 1.43.0. It runs a query/signal workflow on either side of
a live Go/V worker handoff. It exports span identities to an append-only fixture
journal and independently replays the completed JSON history.

Build with `go build -o <artifact-path> .`, then run
`scripts/generate_tracing_go_corpus.py --reference <artifact-path>` from the SDK
root with its pinned V tracing binary built. The generator starts disposable
Temporal servers and records four histories, Go/V spans and a hash manifest.
The required `tracing_go_probe.py` consumes the frozen corpus without Go or a
running server. V workers do not link the Go SDK.
