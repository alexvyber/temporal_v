# SDK parity implementation

The release scope is full Temporal SDK feature parity with Linux amd64/arm64
containers. This is ongoing engineering work; passing the current tests does not
make the full-parity release ready. `release/readiness.json` is the requirement
inventory, and `python3 scripts/release_gate.py` must remain blocked while required
work is missing or unverified.

## Implemented and exercised

- Duration timers, explicit cancellation, deterministic RNG/UUIDs, external signals
  and external cancellation, including worker-loss replay.
- Remote/local Activities with Temporal-compatible unlimited default retries and explicit
  retry limits, timeout/cancellation options and attempt
  information. Local retry backoff persists through durable timers and survives
  restart; replay uses markers instead of executing finished local work again.
- Deferred Activity completion with external heartbeat, complete, fail and
  cancellation acknowledgement by token or execution ID. Completion survives loss
  of both original workers. Applications must persist task tokens privately.
- Signals, readonly queries, Updates, validators, deferred Update completion,
  Update-with-start, completion-stage waiting and timeout errors that preserve
  Update identity. An immediate validation rejection remains in its returned handle.
- Child retries, duplicate IDs, cancellation before/after start, parent-close
  policies, timeouts, headers and memo. Normal parent completion honors the child
  parent-close policy rather than unconditionally cancelling every child.
- Continue-as-new and retry-chain result following. Continuations inherit run/task
  timeouts unless explicitly overridden. Patch deprecation is checked against
  histories created by earlier workflow code versions.
- Data converters and ordered codec pipelines across client/workflow/Activity/
  Update boundaries and replay. Binary conversion is supported. The example
  framing codecs are test fixtures, not encryption. Multi-argument inputs retain
  independent payload boundaries, including arbitrary binary values and zero args.
- TLS/mTLS, configurable identity/API keys, ASCII/binary metadata and refresh,
  keep-alive and HTTP CONNECT proxy options. Certificate verification and client
  certificate enforcement have real loopback integration coverage.
- Generated typed unary RPCs use actual pinned schemas and Core's service
  dispatchers. High-level client APIs cover common workflow, Update, history and
  asynchronous Activity operations; low-level methods expose the broader service
  surface with explicit request/response types.
- Synchronous Nexus operations and workflow-backed asynchronous handlers. Durable
  server callbacks propagate results/failures/cancellation after handler loss.
  Worker shutdown cancels/drains in-flight handler requests.
- Fixed/resource-based slot limits, separate workflow/remote/local/Nexus
  concurrency, pollers, cache, shutdown and throttles. Live deployment tests check
  both pinned routing and auto-upgrade when changing the current build.
- Typed search attributes and memo updates/deletion. Search attribute payloads are
  indexed JSON and intentionally do not pass through application codecs.
- Nested/shielded cancellation scopes and workflow cancellation events permit
  durable cleanup before acknowledging cancellation. Restart/offline replay tested.
- Managed dev/time-skipping environments, concurrent automatic time skipping and
  programmatic worker shutdown. Separate application package import/build tested.
- Structured SDK/Core logs, replay-aware workflow logs, Prometheus metrics and
  OTLP HTTP export exercised against live receivers.
- Ordered client/workflow/Activity/query/Update validator interception tested
  through SIGKILL and offline replay; structured gRPC status details retained.
- Client-wide/per-call RPC cancellation with safe shared-token close; caller wait
  cancellation is independent of workflow cancellation.
- Explicit WorkflowTaskError and workflow decode task failures support repair by
  redeployment; reserved metadata queries expose replay-reconstructed details.
- Outbound command interception, combined/eager worker execution, poller
  autoscaling, Nexus links and CONNECT/outage probes are included in the build
  and integration checks; consult their evidence reports for the latest results.
- Offline history replay with no server or Activity execution. Deliberately
  incompatible code must produce a nondeterminism error. Worker polling continues
  while completions are in flight, and shutdown drains all native users before
  freeing the connection. Concurrent client close has integration coverage.

## Verification

The plugin/combined-Nexus/runtime-metadata candidate passed all 53 host reports,
source `b2004d95bdc6e7f19aee8468c26eca5fcc699cbc2076b6ddb5a94061935bf625`.
Its Linux arm64 check stopped during the outage test on filesystem I/O errors;
BuildKit also failed to write its metadata database. It did not complete Linux
validation. Earlier green Linux evidence remains tied to its own source revisions.

Further connection work adds configurable retry/backoff, gzip, DNS resolution,
client SDK headers and warning thresholds. The wire probe exposed per-attempt
timeouts allowing a 120 ms RPC to retry for over four seconds; the Rust bridge
now enforces that deadline across the complete retry future. Focused tests include
compressed request/response frames, finite and unlimited retry configurations,
terminal statuses, cancellation, stopped retries and real Temporal input/memo
round trips. All 54 host reports passed for source
`2d963869a6a967f3c45a9ed8004c6acfea81486ca8491c62e6e05beb0a7780bc`.
Linux remains blocked by the Docker filesystem/metadata errors above.

The next candidate exposes structured Prometheus formatting/bucket options and
OTLP temporality, units and explicit/exponential histogram settings. Focused live
checks passed, including independently decoded 11/10 delta observations versus
21 cumulative observations and pre-construction validation of unsafe options.
All 55 host reports passed for source
`bbb3d244e74d2d9d06d9e6372f9dc47288d60c395a60935c0d1a32fb2d9749b1`.
Both Linux gates remain required for this source.

The following candidate adds scoped, cached user instruments for all seven Core
metric kinds, four attribute types and workflow/Activity/Nexus context meters.
Focused normal and V/C sanitizer tests passed exact exported values, eight
concurrent writers, meter/runtime retention after client close, isolated scope
cleanup and concurrent close. A fresh worker suppressed four historical workflow
emissions during replay, then recorded one new signal through the same cached
instrument; offline replay passed too. This newer source needs a full host gate
and both Linux targets. Rust Core/payload visitor instrumentation and custom
metric exporter callbacks remain outstanding.

The first 56-report aggregate caught a shutdown regression: a metric runtime lease
delayed Core worker finalization until after custom slot dispatchers were freed.
Workflow metrics now close after handler draining and before worker/dispatcher
teardown. Shared-worker metric leases retain the owning client runtime directly.
The targeted tuner/replay regression passed after that correction. Probe reports
now declare unique explicit identities; nested sanitizer reports no longer affect
which outer report the verification gate requires. A fresh aggregate is required.

The corrected aggregate then exposed a separate shared-client constructor race:
an already-running query held a read lock while waiting for the worker whose
constructor needed that lock exclusively. The owned Rust bridge now uses shared
connection/runtime borrows for these read-only operations, including independent
buffer allocation/return. Constructor/RPC serialization was removed. A proxy
barrier test observed the query before construction: the serialized V variant
timed out, and the corrected variant answered it and drained. The plugin suite,
including both metadata masks, restart/replay and this regression, passed focused
verification. Six maintained patches reproduce the modified pinned Core source.

Profile loading now reuses Core's actual TOML/environment implementation, retaining
optional values, normalized metadata, TLS sources and codec metadata. Applying a
profile infers URL schemes and TLS defaults, rejects empty/partial credentials,
validates TLS booleans and redacts parser diagnostics by default. The unsupported
hostname-verification bypass is preserved and explicitly rejected, rather than
silently ignored. All former local port defaults moved from 17233 to 7233.
Focused client, TLS/mTLS, worker restart and offline replay checks passed. The new
aggregate requires 57 reports, including profile V/C sanitizer coverage.

A later Docker health check found its API socket absent. Linux validation remains
unavailable; the SDK task has not restarted Docker or changed unrelated services.

Run `bash scripts/build.sh && bash scripts/check.sh`. Each integration probe starts
an isolated Temporal dev server and shuts down its own processes. Reports, binary
histories and logs go to `../artifacts/v-sdk-spike` (override `SPIKE_ARTIFACTS`). The
individual `scripts/*_probe.py` files document and assert the precise scenarios.
Build/check verification records exact source/binary hashes and archives fresh reports
under the artifact directory. C Address/UB sanitizer coverage excludes Rust Core and
individual Boehm object bounds. Bundled GC C is instrumented. Reports are local evidence, not assertions that untested SDK behavior is complete.

The native bridge and schemas are pinned to the same Rust SDK revision. The build
applies `patches/binary-metadata.patch` to the copied Rust source: binary metadata
must not be converted to an unchecked UTF-8 string. Original reference checkouts
are not changed.

## Still required

Continue implementing remaining workflow execution/advanced options and expand
coverage for codec failures,
legacy versioning, ramps and cross-namespace behavior. Add replay evidence across
SDK releases, Rust sanitizer/failure injection, sustained memory/load tests and
server/network outage recovery. Finish packaging, licensing/SBOM/checksum/signing
and both Linux release validations before claiming production readiness.

Docker recovered without restarting it or deleting unrelated data. Linux arm64
passed 34 reports against source
`c23b6a8ac680085e9f674d54a32d10845bea1cdfa69502e89faa528cf61b792d`, also
verified on the host. Its nonroot read-only runtime passed execution, restart replay
and graceful shutdown. Amd64 passed 33 reports against the earlier source recorded
below. Both require fresh validation after serialization-context changes.

Structured failures now support nested application/generic causes, stack traces,
multiple details and benign categories. Captured failures can be returned as
errors while preserving protocol metadata; payload bytes are rebound when actor scope changes. Remote/local propagation, Update
failure isolation and codec-aware offline replay passed `failure_chain_probe.py`.
Typed Activity cancellation retains pause/reset details; real management tests
verified interrupted attempts resume. Codec recovery tests cover unavailable
decoders, invalid payload counts and malformed heartbeat data without wedging
worker shutdown. OTLP probes decode actual metric names over HTTP and gRPC.

Activity options now expose custom IDs, remote priority/versioning intent and
command summaries. Handler information includes workflow type/queue, timestamps,
timeouts, retry policy and priority. Timer summaries and child summary/details
are independently observed in server history. Nexus summaries and structured
Update-validator failures are implemented with dedicated integration assertions.

The new local retry restart test uncovered reused Core sequence numbers across
durable retries. Retries now allocate fresh Core sequences and map resolutions
and cancellation back to the original V handle. Completed retried Activities
replay after restart and offline; cancellation of a later attempt also passes.

Custom failure converters are configurable through DataConverter. The default
converter optionally encodes messages/stacks into standard JSON common
attributes processed by the configured codecs. Captured protocol bytes remain
unchanged when rethrown in the same serialization scope. Default, custom and encoded-attribute policies passed
remote/local Activity and Update/validator propagation and offline replay.
Decoder diagnostics retain raw bytes when a common-attribute codec is unavailable.

Activity heartbeats now accept multiple detail values. Cooperative sleep uses a
monotonic clock and retains the last encoded checkpoint, including details from
a prior attempt. A real worker-loss test observed both codec-framed payloads on
the server, killed the worker and verified retry/replay preserved them.

Workflow metadata now includes type and sorted handler definitions/descriptions.
Workflow memo/search-attribute reads track initialization and local mutations.
Additional WorkflowInfo fields and prior completion/failure accessors expose Core
initialization data. The bounded load gate passed 192 workflows with cancellation,
restart and offline replay; sampled peak RSS was about 50/36 MiB for workflow and
Activity workers. Sustained production-duration validation remains required.

The first amd64 validation compiled all production binaries and passed functional
probes, then failed ASAN in GC initialization. A Temporal-free V allocation
reproduced it. Supplying Boehm's supported HEAP_START hint within ASAN's application
address range made the minimal program pass; the sanitizer gate now uses that
placement hint on Linux amd64, retaining instrumentation and failure checks.
Full target validation must still pass. Bundled GC C inherits sanitizer flags;
individual GC heap-object boundaries are not tracked by ASAN, and Rust Core is
not instrumented.

The amd64 heap-placement fix passed the entire 33-report validation suite against
the same `7c989c5190eb99278a3b1e9694720900b7782d89f947531fda534da260407eda`
source as the host. Its nonroot read-only runtime image passed execution,
SIGKILL/restart replay and graceful shutdown. Evidence is under
`../artifacts/v-sdk-spike/linux-amd64-evidence/` and `container-amd64-results.json`.
Newer custom supplier changes require fresh platform validation.

Custom workflow/Activity/local/Nexus slot suppliers now support asynchronous
cancellation-aware reservation, immediate reservation, use/release context and
available-slot metrics. A C-owned queue keeps V execution on V-created threads.
Five hundred native sanitizer reservation/cancellation cases balanced ownership;
real V suppliers enforced zero/one-slot capacity, resumed work dynamically,
handled local retries/Nexus, restarted through replay and released all permits.

Serialization contexts now reach both converter and codec layers for workflows,
remote/local Activities, child workflows, signals/queries/Updates and Nexus.
Deferred completion records retain actor context through worker loss. Captured
failures rebind known payloads across actor scopes while preserving execution
identity and unknown protocol fields. A fourth copied-Core patch preserves
structured query failures on normal and legacy response paths; the high-level
query API decodes these into QueryError while retaining RPC diagnostics.
Context tests include cross-namespace child failures/signals and both continuation
runs. Cross-namespace commands are enabled only in that isolated test server.

The serialization-context revision passed all 35 reports on Linux arm64 at source
`ee21cb0a415643256c6d5442348ea4667d3fa461b0a8058f74c58aadcc01e0fd`.
All 35 host probe reports passed individually; the aggregate run correctly rejected
an external formatting-only change to generated schemas. Build now canonicalizes
generated schemas before hashing. Standalone Activity changes need fresh evidence.

Standalone Activity client APIs now cover start/result/describe, cancellation,
termination, pause/unpause/reset and deferred operations addressed by execution ID.
Start options retain original request fields; descriptions retain raw responses
alongside decoded payloads. Standalone run identity takes precedence over an empty
workflow-execution submessage in Core task metadata.

All 36 host reports passed against stable source
`50a9cd1be301fe9e3d656921dc5c8d3b62d76e68816d4ea687ea9c1b2534d7e7`.
Linux amd64 passed the same 36 reports and source hash. The suite now includes
contextual and binary standalone Activity lifecycle tests and sanitizer coverage.

Workflow registration now carries per-type versioning behavior through live and
replay completions, independently of the worker default. Mixed pinned/auto types
under both defaults passed deployment switching, SIGKILL/restart and offline
replay. Core activation deployment/SDK/change metadata is exposed in WorkflowInfo.
Invalid disabled-deployment configurations are rejected before native setup.

Per-type versioning passed all 37 reports on both the host and Linux arm64 for
source `198b5e3134d49e5829db2c64459910c7d8182096ce6ba863dadb9e115a7fe632`.
The amd64 standalone revision's nonroot read-only runtime also passed execution,
restart replay and graceful shutdown.

Dynamic workflow factories and remote/local Activity handlers now dispatch unknown
types while preserving their actual type names. Named registrations take priority;
duplicate dynamic registrations fail. Replay retains factories/options and versioning
validation applies to dynamic workflows. A separate prototype passed real Temporal
execution, failure propagation, restart without redispatch and offline replay;
the integrated probe adds transient retry recovery and registration validation.

Dynamic handlers passed all 38 host reports at source
`c6685c528bcf15d08eb5abd9acfec977650602818f07c90ba60a3cf9ff29bfbb`.
Standalone Nexus client start/start-wait/result/describe/cancel/terminate now use
target-aware conversion, preserve stored execution identity for use-existing and
bound result polling plus target lookup with one deadline. The live probe passed
sync/async operations, protected failures, conflict/use-existing, exact cancellation/
termination/timeout statuses and callback completion after both workers were killed.
The new client paths are included in the native sanitizer driver.

Workflow cancellation is now fallible and carries multiple detail payloads; every
known caller was updated. Client termination supports details, links and the first
execution run identity guard. Result objects decode cancellation/termination
details and retain failure/timeout retry state. Cancel reasons and signal sender/
history-event identity reach handlers and replay. The first lifecycle probe passed
binary/scoped details, the guard, server metadata, restart and offline replay.

The amd64 dynamic revision passed functional probes but failed the load oracle:
a selected workflow could finish before its cancel request. Load cancellation
candidates now remain open while waiting for cancellation. The service probe also
recognizes Tonic's narrowly identified CANCELLED/h2 timeout-reset representation
while retaining the native status and requiring resumable Update identity.
Targeted load/service runs passed after these test corrections. The separate
6,400-workflow soak uses frozen binaries and its own artifact directory.

The 6,400-workflow frozen-binary run passed in 680 seconds: 5,600 completed and
800 canceled, with restart and offline replay checks. Peak workflow/Activity RSS
was 116,816/53,696 KiB; final drained RSS was 75,568/44,752 KiB. Exact frozen build
and native library hashes are in `../artifacts/v-sdk-spike/soak-load/verification.json`.
This is an eleven-minute bounded run, not production-duration validation.

All 40 host probe reports passed, but aggregate verification rejected the source
change that fixed a cancellation caller missed in environment_demo. Linux release
compilation caught that caller; it is now fallible and repository-wide searches
found no remaining unhandled WorkflowContext.cancel calls. Fresh complete checks
are required.

Propagated Nexus workflow result contexts now bind terminal output separately
from workflow inputs/interactions. The live stable-server test demonstrated that
1.32.2 discards this new field; upstream source confirmed the omission. A separately
checksum-pinned official prerelease embeds server 1.33.0/69c692e8773e with support.
Its probe passed completion, nested failure rebinding, cancellation details,
paginated context responses, continuation inheritance and offline replay.

The corrected shared fixture now creates the binary-history directory before any
probe uses it. All 41 reports pass on the host and Linux arm64 against source
`a9236eb754f8d7a4ab518f1bec908c55fe11b8a6ae92e0ae701ade2a16b71def`.

Propagated Nexus Activity result contexts are implemented separately from input
scope, including heartbeat checkpoints, deferred completion tokens and by-ID
completion. The targeted real-server probe passed worker loss/retry, nested
failures, external completion and timeout checkpoint details with independently
checked wire metadata. A fresh full run is required for this revision.

The signal-with-start high-level API and typed query rejection policies passed
their targeted context-codec probe: twenty concurrent calls started one execution
and delivered every unique signal, an explicit request ID deduplicated a signal,
and completed/failed query policies survived worker loss and offline replay.
The full 43-report host and Linux checks are being rerun for this revision.

All 43 host reports passed for source
`138d2caae285a0e71194bd41598acdbaeabe8e395fc2d05b1185a06221044aa8`.
Linux amd64 release compilation/validation is running against that frozen source.
The 41-report arm64 runtime image also passed nonroot read-only container execution,
worker SIGKILL/restart replay, and graceful shutdown.

Nondeterminism policies passed default, per-type and global live-server tests.
Default-blocked executions resumed after a code repair and replayed offline.
Generic Nexus links now cover every upstream Nexus-representable common link
variant, including request references and callbacks with repeated component paths.
Targeted conversion checks passed; full regression is required for these changes.

All 44 host reports passed for source
`496ab14a45da715ad1e7a8a977545e8ffa84dc9e6e67a0e57b2705e84e2be8b0`.
Activity-backed Nexus start/cancel helpers now pass their targeted live-server
probe: checkpoint retry and external completion after both workers were killed,
scoped nested failures, backing-Activity cancellation and workflow caller replay.
The test enables activity.enableCallbacks on its disposable supporting server.
One Nexus invocation cannot start multiple async Activities/workflows.

Linux amd64 passed all 43 reports for source 138d2caae285a0e71194bd41598acdbaeabe8e395fc2d05b1185a06221044aa8; its read-only runtime passed restart/drain.
Linux arm64 passed all 44 reports for source 496ab14a45da715ad1e7a8a977545e8ffa84dc9e6e67a0e57b2705e84e2be8b0.
The 45-report host run stopped at packaging with ENOSPC, not an SDK failure.
Removed only obsolete intermediate bridge .rcgu.o files from this task's Cargo
build; native libraries, dependency archives and verification evidence retained.

Per-Update output scopes and request/callback/link metadata passed a prototype
probe: separate concurrent scopes, validation and execution failures, detached
polling after worker loss, original scope on duplicate Update IDs, execution-chain
guards, independent wire metadata and offline replay. Fresh full checks required.

All 46 host reports passed for source
`3b9361decfc58063c288cdb7445119928063755a47dfd6d7154fc6bd3db7a9c2`.
Nexus Update helpers and query/signal interactions passed targeted real-server
probes. Pending Update callbacks survived worker loss; completed duplicates
returned original results, scoped validation/handler failures reached callers and
backing workflow history replayed offline. Nexus signal retries delivered one
signal; one-signal guards cover mixed signal/signal-with-start calls. Synchronous
queries preserved results, protected failure causes and closed-query policy.

The 44-report arm64 runtime also passed nonroot read-only crash/restart and drain.
Pruned only explicitly enumerated old immutable v-sdk Docker cache records
(1.537 GB); retained images, Cargo caches and verification evidence. The current
48-report revision requires fresh complete host/Linux checks.

All 48 host reports passed for source
`e62ef7dd975ea6ce63d4c261a26e0e801b17d8a8042a68a09f6e33dd1f1743ff`.
Linux amd64 is checking that frozen revision. Arm64 hit a fixture race: captured
history identifies WorkflowTaskFailed FORCE_CLOSE_COMMAND from history-service
when termination overlapped the initial task. The termination fixture now waits
for an actual WorkflowTaskCompleted event before sending the close command; it
keeps the strict zero-task-failure oracle.

Shared native client ownership and eager workflow starts passed the live prototype.
Actual eager tasks appeared in responses and reached matching queues; missing
workers used normal polling. Two workers remained operational after public client
close and drained independently. Replacement workers and offline replay rebuilt
all four completed histories. No Rust Core patch was needed.

The first 49-report host run was deliberately stopped for a native ownership
review fix: shared worker construction must hold the connection's exclusive lock
because the pinned Rust C constructor takes &mut Connection. RPCs and concurrent
constructors are now serialized at that boundary. A concurrent health-RPC loop
and close scenario is included in the eager driver and native sanitizer gate.

The exclusive-lock eager/RPC-close probe and corrected lifecycle probe passed.
The following host run failed an initial buffered-signal query. An instrumented
reproduction captured an actual automatic request-ID collision across independent
V processes: a later finish signal reused an earlier add signal's UUID, and the
server correctly deduplicated it. Evidence is in
`../artifacts/v-sdk-spike/buffered-signal-diagnostic/request-id-collision.json`.
Pinned V's macOS time seed uses process-relative monotonic time, and its global
pseudorandom generator is also unsuitable for concurrent client identity creation.
All SDK client-generated identities now use OS-random UUIDv4; explicit identities
and recorded workflow randomness retain their existing semantics. Separate-process
and concurrent-signal probes cover this data-loss regression. Native transport
locks are shared across borrowed worker connections to serialize every raw client
access against the Rust mutable worker constructor.

Linux amd64 passed all 48 reports for source e62ef7dd975ea6ce63d4c261a26e0e801b17d8a8042a68a09f6e33dd1f1743ff.

Host, Linux amd64 and Linux arm64 passed all 50 reports for source
`6a3c50751f545e5405f80d52a7f7fbb38660a7a2fec9ea9d030400c3653bd899`.
Both corresponding nonroot, read-only runtime container probes passed actual
SIGKILL/restart/replay and SIGTERM drain. Frozen evidence is under
`../artifacts/v-sdk-spike/{host,linux-amd64,linux-arm64}-evidence-50-6a3c507`;
these results predate external storage.

External storage is the next implementation. A separate synchronous Rust payload
visitor library reuses the pinned graph (247 payload-bearing types, 436 RPC
request/response directions, 79 with payloads). Slot markers preserve map
associations across decode order changes; search attributes are excluded. Native
traversal/corruption/FFI ownership tests passed. The first isolated prototype
round-tripped 3 MiB workflow and remote Activity payloads through references,
survived worker loss, kept independently inspected history under 100 KiB and
made no storage writes during offline replay. Repository integration adds manager
limits, a driver API/selector, deadline context, raw history export, typed retryable
storage failures and I/O outside polling coordinators. Local Activity markers,
protected query/Update failures and concurrent/outage behavior are being checked.
Production readiness remains false; Linux evidence for this new implementation
has not yet been established.

The first full storage suite (source 0395e033d4d8aa5885463dbf5d8734c819bccd90027dc06d158d9c538ccc7b57)
stopped at codec-failure diagnostics: moving Activity decoding into handler threads
lost the existing heartbeat/header failure context. Isolation still worked; the
prefixes are restored. Deliberate storage outages independently passed repair,
retryable Activity completion and replay checks. Native unit tests are now built
with Cargo caches mounted and copied as a frozen executable for validation, so
Linux validation does not need to download/rebuild the Rust dependency tree.

Recorded workflow time now retains nanoseconds instead of rounding the only
internal clock to milliseconds. `WorkflowContext.now()` exposes that time; existing
millisecond metadata/log fields derive from it. An isolated synthetic activation
probe passed timestamp updates and timestamp-free activations. Live history,
restart and offline replay checks are the next required gate.

Live clock precision, restored heartbeat diagnostics, V storage reference/limiter
unit tests, and storage ownership tests passed independently. Child input storage
belongs to the child, first child results to the parent, external signals to their
target, and continue-as-new inputs to the next run (unknown run ID). Cached-run
metadata retains the workflow type. The command union is enumerated explicitly so
a schema addition requires an ownership decision. Core receives configured driver
types for worker heartbeat metadata. Proto-JSON references accept both standard
field-name forms and reject malformed UTF-8 and conflicting field spellings.
A new aggregate requires 52 reports; Linux proof for this source is pending.

Source e7e7dbb658a73b2f584c071f8d1aee4fbbc3fd5ad67ec756ae0b07fe21094143
passed all 52 reports on host and Linux arm64. Its read-only, nonroot arm64 runtime
passed SIGKILL/restart/replay and SIGTERM drain. The amd64 run stopped in the eager
fixture with a refused server connection; the same isolated test passed on the
frozen amd64 build image. Fixture startup now requires a successful gRPC health
response instead of merely an open TCP port. The prior 50-check source and both
runtime images were preserved in `../artifacts/v-sdk-spike/release-corpus/sdk50`;
only its superseded validation build images were removed to recover space.

Plugin integration adds connection/client/worker/replay hooks, shared-client
propagation, bundles, phase-specific errors and stable Core metadata. Each worker
uses an isolated definition snapshot; public worker creation now borrows Runtime
without `mut`. Combined workers can serve Nexus, workflows and local/remote
Activities on one Core worker. Runtime heartbeat/environment options and the
payload-error-limit switch are wired through the native boundary. Prototypes
passed live execution, worker recreation, SIGKILL and replay without repeated
committed Activities; real worker APIs reported plugin versions, driver types and
runtime/environment masking. The integrated implementation adds report 53 and
still requires aggregate and Linux validation. Production readiness remains false.


The profile/user-metrics/shared-runtime candidate passed all 57 host reports at
source `33412e375078a0bbef6f02f88cdab84584687089c349c12554f1704bc9c3b4f2`.
This includes instrumented V/C profile and metric probes. Current Linux validation
remains blocked because the Docker daemon is unavailable; no service restart was
attempted. The latest complete Linux evidence still covers earlier sources.

Custom gRPC transport is now implemented for the next candidate: bounded native
queues, separate V control/poll/wait pools, cancellation-aware forwarding,
structured status/details and explicit Rust callback-data ownership. Native
ASAN/UBSAN unit tests passed for 2,007 requests. Focused live tests also passed
for 80 concurrent RPCs, count/byte overload, configuration errors, repeated failed
construction, metadata/status/details, cancellation and deadlines, shared workers
after client close, SIGKILL/replacement and offline replay. Full aggregate and
instrumented integration verification of this candidate are still required. A seventh maintained Core
patch exposes cancellation state and releases callback data after the final
service owner, including failed construction. All C ABI users must rebuild with
that header; the build checks header equality after applying patches.


The custom gRPC transport candidate passed all 58 host reports at source
`7765e16ab82949a653578bcbcfefeb43721e0f13f5c8de10defd37990a477ab0`, including
native queue unit tests and instrumented live transport restart/replay. Rust's
C-bridge test targets type-checked successfully. Exact source/library/evidence
were archived before starting the next candidate.

The eighth patch implements standalone eager Activity reservation and actual
inline task dispatch through Core's tracked Activity channel. The V option,
response-observing probe and sanitizer integration are implemented. Focused live
tests passed for actual inline dispatch, no-worker/full/shutdown/throttled fallback,
ten rejected starts releasing permits, and heartbeat recovery after SIGKILL.
The supporting 1.33 server needs `activity.enableEagerStart=true`; 1.32 ignores
the request and normal polling still completes tasks. A layered RPC-hook bug was
fixed so inner proxies preserve the outer client reservation. Full aggregate and
instrumented integration verification of this candidate remain. Development
builds omit debug information for the client/Core crates to keep rebuilds within
available local disk; Linux release builds retain their existing profile.


The eager standalone Activity candidate passed all 59 host reports at source
`7a6b1755028ac7b2000902cf16ff12d32f6a447f77210ab221b73e001b85f9cf`, including
instrumented accepted/declined inline tasks, reservation cleanup and SIGKILL
heartbeat recovery. Exact source, verification and Core8 library were archived.

Custom metric exporting is implemented for the next candidate: per-runtime C
queues, immutable metric/attribute ownership, typed V batches, bounded point/
estimated-byte buffering, cooperative export contexts, private error accounting,
shared-worker/metric-meter leases and final drain. Native ASAN/UBSAN tests passed
for 16,015 record attempts, including all value kinds, overrides, independent
runtimes, concurrent producers and owner release. The ninth Core patch passes the
meter pointer into its allocation callbacks; no global callback router or V code
runs on Tokio threads. V API and consumer type checks passed. Focused actual Core/live tests passed for all seven kinds, integer extremes,
803 concurrent bound plus two overridden counter events, ten failed constructors,
independent runtimes, late meter leases, cooperative timeouts/drop accounting,
private exporter errors, self-close, SIGKILL/replacement replay suppression and
offline replay. Full aggregate/instrumented integration and current Linux
verification remain.


The custom metric exporter candidate passed all 60 host reports at source
`10465589acf624d7c75bb2b0e33a66d24098a1591086f7731760b14fe0ca6874`, including
instrumented queue, startup, multi-runtime, self-close, worker-loss and replay
coverage. Exact source, verification and Core9 library were archived. A fresh
read-only check still found the local Docker socket absent; no restart was attempted.

Typed deployment routing/override APIs are implemented for the next candidate.
Workflow/child start options now take SDK override variants, matching the pinned
Rust SDK's canonical protobuf oneofs. Current/ramping controls return conflict
tokens; execution override updates use a targeted field mask. Focused tests passed
against both embedded server versions for local validation, stale token/unknown
version rejection, 0/100 ramps, pinned/auto/one-time/clear routing, preserved
priority, pinned restart, cross-version children, continue-as-new inheritance and
offline replay. Full aggregate/instrumented integration and Linux validation of
this newer source remain pending. No additional Core patch or C ABI change was needed.


The deployment candidate passed all 61 host reports at source
`4f2ed0f3376d172d5874d04ece5384ca7e01f49b9de46c33b94d9ac850d36a8e`,
including instrumented 1.32/1.33 routing, child/continue-as-new and replay coverage.
Exact source, verification and Core9 library are archived.

Legacy build-ID compatibility APIs now expose typed new/compatible/promote/merge
operations, set snapshots and reachability. A live baseline proved an ordinary
start completed on default C before a subsequent eager start incorrectly ran on
incompatible local A. The tenth maintained Core patch excludes legacy workers
from eager workflow reservations so server matching enforces compatibility.
Normal/instrumented two-server coverage is being integrated; Linux remains blocked.

The focused legacy suite passed on both 1.32 and 1.33, including typed input
validation before interception, real reachability, all five compatibility
operations, worker SIGKILL/replacement, matching-safe eager fallback and offline
replay after each server stopped. Full aggregate/instrumented verification is next.
The Core rebuild initially exhausted disk; only this task's regenerable compiler
incremental caches were removed, with manifests retained in artifact logs.
All 13 modified Core files exactly reconstruct from the ten maintained patches;
the original Rust checkout remains unchanged apart from its preexisting mise.toml.

The first 62-report aggregate stopped in the legacy eager fixture: a query's
5-second RPC deadline expired while the default worker was receiving its first
task. The assertion now waits for completion and checks independently recorded
worker identity, retaining the ordinary-C readiness barrier and inline rejection.
No successful aggregate evidence is claimed for that interrupted run.

The corrected legacy candidate passed all 62 host reports at source
`3bffa26694c33918cc526ab2c7c976972031b669f670f54cb261463b62197cd5`,
including instrumented legacy routing, eager fallback and offline replay. Exact
source, both verification records and Core10 library are archived. Docker remained
unavailable on a fresh read-only check; Linux results are unchanged.

Reusable schedule and batch clients are now integrated for the next candidate.
Schedules support workflow-action conversion, described-action-preserving updates,
optional memo/attribute replacement, list pagination, pause/unpause, triggers with
scheduled timestamps, backfills, matching times and raw typed patches. The isolated
four-case matrix passed normally and with V/C sanitizers against legacy 1.32 and
CHASM 1.33 with/without codecs, including concurrent backfills, restart and offline
replay. The new scheduler rejected stale tokens and cleared memo; the older one
accepted stale tokens. Both observed schedulers appended workflow ID timestamps
despite retaining keep_original_workflow_id, so strict exact-ID codecs are not
claimed to work for generated scheduled IDs. The SDK preserves actual scope rules.

Batch lifecycle/selection/rate validation, bounded wait, list/stop and payload helpers
are implemented. Initial live prototype tests passed for six explicit targets,
signal headers/state, cancellation/termination/details, duplicate job IDs, stop/list,
SIGKILL/replacement and offline replay with/without codecs. Expanded query, input,
wait-deadline, pagination and two-server matrix validation is next. No additional
Core patch or ABI change is needed for either client feature.

The expanded batch matrix passed against both 1.32.2 and the supporting 1.33 server,
with and without codecs. It covers local validation, explicit/query targets,
independent signal/termination history, duplicate job IDs, all list pages, missing
jobs, wait deadlines that leave jobs running, explicit stop, worker SIGKILL and
36 offline histories. Server 1.32 omits signal headers/termination details and
ignores the request rate; its primary implementation confirms those limitations.
The fixture caps server batcher throughput to one operation/second for deterministic
stop/wait assertions. Expected header support is recorded in workflow input.
The integrated schedule matrix also passed; the full 64-report host gate is next.

An isolated archived-candidate replay prototype rebuilt exact SDK57 source with
its archived Core6 and matching payload library (relocated copies only). SDK57 to
current and current to SDK57 workflow replacement passed with/without codecs; 16
counter/Update, durable timer/remote Activity, cancellation and local retry histories
replayed offline under both SDKs. This is host candidate-to-candidate evidence,
not a published-release or Linux compatibility matrix. Activity-worker replacement
coverage and a portable maintained corpus gate remain to be integrated.

The schedule/batch candidate passed all 64 host reports at source
`5499386ff5f1755d997435103edc69c8e9b651bcb0a97b79e99ec6eec0ba74d8`,
including instrumented full matrices. Exact source, verification and Core10 library
are archived. No current Linux result is claimed. An additional isolated host
upgrade/rollback matrix replaced both workflow and Activity workers between
archived SDK57/Core6 and this candidate, with codecs enabled/disabled. All 16
histories replayed under both SDKs; source/library/tool/history hashes are retained
in `release-corpus/sdk57-to-candidate64/verification.json`. This remains internal
candidate evidence rather than a portable published-release matrix.

The next candidate adds `ProtobufPayloadConverter[T]` for generated binary/protobuf
messages, with optional messageType metadata, concrete wire validation and unknown
byte preservation. Initial live tests passed 16 workflows across both embedded
servers, named/unnamed Rust-style metadata and codecs, including remote Activity
failures/details, queries/results, metadata queries, SIGKILL and offline replay.
Mixed typed arguments and protobuf JSON remain separate API gaps.

Adversarial probes exposed invalid UTF-8, known-field wire mismatch and varint
overflow acceptance in the pinned V protobuf dependency. The maintained patch now
adds those checks, bounded nested message/group parsing, unknown group preservation,
and acceptance of valid redundant tag varints. Generated JSON/WKT oneof names and
test fixtures now follow the established `_Case` naming; child test compilers use
the required bundled GC. All eight dependency test files passed. A deterministic
2,672-vector corpus matched the pinned Rust decoder with zero differences across
strings, maps, oneofs, packed enums, recursion/overflow and group boundaries.
Patch application verifies complete before/after file hashes and refuses unknown
dependency states. The Rust oracle is now a reproducibly built validation tool;
integrated aggregate/instrumented validation is next. Task-owned duplicate Rust
object intermediates were removed only when their members were retained in complete
non-thin rlib archives, with the cleanup manifest retained.

The integrated protobuf probe passed: all eight dependency test files, validation
against two generated message types, 2,672 Rust/V accept/reject comparisons with
zero differences (corpus SHA-256 `53d925d6aa27cc3e6ab19d3dee44c4edc32129323ff57508b5fc551737d78763`),
and 16 real workflow histories across 1.32/1.33, optional type names and codecs.
Independent wire parsing verified input/result and Activity failure details; both
actors survived SIGKILL/replacement and every history replayed offline. The probe
sets explicit compiler/module environment for its generated consumers. A fresh
read-only Docker check still found the daemon unavailable. Full aggregate and
instrumented integration of this candidate are next.


The protobuf candidate passed all 65 host reports at source
`629d396ee4aea4005a5faa2c8bd3fae48f6882a4f8c79e82a692cd245f6a04e9`.
Exact source and both native libraries are archived in
`../artifacts/v-sdk-spike/host-evidence-65-629d396/`. Docker is still unavailable;
no current Linux success is claimed.

The next candidate implements mixed per-value JSON, binary, protobuf and null
arguments, typed workflow/deferred Activity results and owned raw payload views.
The focused matrix passed on both servers with codecs on/off: 16 mixed-input
histories and 32 direct/deferred-output histories survived worker loss and offline
replay. Null and binary bytes `null` remain distinguishable. Two query/validator
interceptors retain raw inputs, custom converters are honored, and mutation of
returned payload copies cannot alter retained views. Required normal/sanitizer
integration is added; its aggregate validation is pending.


The callback candidate hard-cuts Activity handlers and Activity/query interceptors
and workflow query methods to `!sdk.PayloadValue`, updating all owned callbacks.
Update completion also accepts this sum. A focused matrix passed 32 histories on
both servers with/without codecs across remote/local results, queries/Updates,
dedupe, worker replacement and offline replay. Two Activity interceptors were
added for required validation. Linux arm64 compilation of the previous candidate
exposed deprecated `usize` in custom metric C declarations; this candidate uses
`uint`, matching the pinned compiler and existing bridges. Aggregate validation
is pending.


The 66-report host gate failed its newly added instrumented deferred-completion
case; the other required reports passed. A disposable diagnostic independently
confirmed a server heartbeat timeout and retry after two seconds under the
inherited one-second heartbeat default. Remote Activity heartbeat timeout now
defaults to disabled, matching normal Temporal semantics. The deferred fixture
explicitly disables heartbeat timeout and allows one minute for completion;
heartbeat-specific behavior remains covered by dedicated probes. Failed evidence
and source were retained, and completion failures now capture history. The next
aggregate must pass before claiming a new host success.


The next candidate adds concrete and per-value protobuf JSON, explicit JSON values
under custom application converters, and JSON methods for the full generated
Temporal schema. A cumulative ten-file dependency patch reconstructs exactly.
All eight dependency test files, 592 Go-derived semantic/acceptance vectors and
sixteen two-server codec/named restart/offline histories passed normally and with
V/C AddressSanitizer/UndefinedBehaviorSanitizer. Exact numeric tokens fix overflow
clamping/fraction loss; duplicate keys/aliases, unknown fields, UTF-8, Timestamp,
Duration and FieldMask validation were corrected. The reference corpus uses the
actual pinned Temporal descriptor set with Go protobuf v1.36.6, and records its
source/module hashes. Broad conformance and full aggregate/platform gates remain.

Linux arm64 compiled the typed callback candidate and passed through schedules,
then exposed a one-millisecond batch wait returning an HTTP/2 cancellation error.
The new wait code reports the overall deadline only after its budget is spent,
and preserves earlier errors. Four focused Linux matrices passed forty repeated
one-millisecond waits, missing-job errors and thirty-six offline histories. The
full Linux run remains incomplete. The host schedule fixture also found an exact
minute start-boundary mismatch; moving endpoints between ticks fixed all four
schedule cases. Archived SDK57/Core6 to candidate67/Core10 and rollback passed
both workflow/Activity worker replacement and thirty-two replays of sixteen
histories. This remains internal host candidate compatibility.

The typed callback candidate passed all 67 host reports at source
`601740a9b15efee09edb039b62ee4d9a5f011ace0adfefba66194a8ee4b8a608`, including normal and instrumented mixed-value/callback matrices.
Source, reports and both native libraries are retained in
`../artifacts/v-sdk-spike/host-evidence-67-601740a/`. Its Linux run exposed
the batch deadline issue above; no complete current Linux success is claimed.


The protobuf JSON candidate passed all 68 host reports at source
`480c6372298db1e43b7b054df9c3f0163456da3d804cb840b8859930d3ecd7e4`.
Source, reports and both native libraries are archived in
`../artifacts/v-sdk-spike/host-evidence-68-480c637/`. Its Linux amd64/arm64
validation is running against that frozen source; no new platform success is
claimed yet.

The next candidate hard-cuts payload converters to typed input/output, allowing
custom opaque formats to retain semantic types when failures cross scopes.
Application failure details and external Activity checkpoint/failure/cancellation
APIs accept typed arguments. Failure/cancellation/termination views, Activity
checkpoint views, query/validator interceptors and workflow/client results retain
typed values. Nexus inputs and sync outputs also retain their types. Five-policy
nested/context failure validation passed. Focused two-server/seven-profile normal
matrices passed 112 failure/checkpoint/deferred/cancellation histories with
independent wire/type/context checks, plus 98 Nexus histories and 49 standalone
Nexus operations. Every history replayed offline. The final normal and V/C Address/UB sanitizer
matrices passed all 210 histories and 49 standalone operations. Full aggregate
validation of this candidate is pending. The bounded two-attempt checkpoint case
finishes its intentional retry before worker replacement; independent worker-loss
probes cover in-flight recovery. Replay filenames include unique fixture queues
to prevent concurrent focused runs overwriting one another.

Nexus testing independently exposed the prefix fixture's nonempty binary/null
representation becoming unknown/nexus-content at the HTTP boundary. The pinned
1.32.2 server serializer confirms the behavior. The Nexus fixture now composes a
whole-payload envelope that declares its transformed encoding and retains the
complete inner payload, including scope metadata. SDK decoding was not relaxed.

The next isolated candidate supports cardinality-changing payload codecs and
checked singular conversion. A seven-to-one-to-two-to-one pack/split/pack pipeline
passed normally and with V/C Address/UB sanitizers on both servers: 112 typed
failure/checkpoint histories, 98 Nexus histories and 49 standalone operations.
Empty/single argument roundtrips and malformed singular/split/packed output
checks passed; every workflow history replayed offline.

Its maintained ten-file protobuf patch also corrects strict padded/unpadded
base64, nonfinite or unset Value output, malformed/unresolved Any output,
non-reversible FieldMask output and float-string grammar. Normal and instrumented
focused tests passed 13,682 unique cases checked against Go protobuf v1.36.6
using the pinned Temporal descriptor set, plus 16 live/restart/offline histories.
All eight dependency test files passed, and the new cumulative patch reconstructs
the exact hash-locked tree from its prerequisite state. Full aggregate and
platform validation remain pending; release_ready remains false.

The typed-converter candidate passed all 70 host reports, including instrumented
integration, at source `da51617156a6418e1f0e8a92f4c87e8aea92dc47fce24446434e7d6a1756f183`.
Exact source, reports and both native libraries are archived in
`../artifacts/v-sdk-spike/host-evidence-70-da51617/`. The next aggregate candidate
combines the isolated count-changing codecs and protobuf fixes above.

Linux arm64 source 480c637 passed normal integration but failed entering native
sanitizer validation with Docker ResourceExhausted/cannot-allocate-memory. The
Docker VM has 6 GB of memory; no successful new platform gate is claimed. Its
amd64 build finished and validation is running; retry arm64 serially afterward.

All 71 host reports passed at source
`6de0817dd248ac4fd2b2352598a57d0cb8d41896d76bace6f9063e6ef15681da`,
including normal/instrumented count-changing codec and 13,682-case protobuf JSON
matrices. Exact source/reports/native libraries are archived in
`../artifacts/v-sdk-spike/host-evidence-71-6de0817/`. The first 71-report attempt
(1092f40) exposed a fixture that still prohibited collection count changes; the
corrected fixture tests singular encode/decode boundaries and the full gate passed.

The next combined candidate adds typed header/memo maps, raw construction and
owned Activity information snapshots. Normal and V/C Address/UB sanitizer tests
passed 28 two-server/seven-profile packed/unpacked histories, final raw server memo
comparisons, worker replacement and offline replay. It also adds typed cancellation
error details and distinguishes explicit Nexus operation cancellation from task
cancellation acknowledgments. Five-policy constructor/context validation and both
normal/instrumented matrices passed 90 histories plus 14 standalone cancellations.
Local cancellation uses short local work, matching documented signal latency.
Aggregate and platform validation of these API changes remain pending.

Frozen source 480c637 passed all 68 reports on Linux amd64 and arm64. The arm64
retry ran serially after a concurrent run exhausted the 6 GB Docker VM. Evidence
is in linux-{architecture}-evidence-68-480c637; amd64's matching nonroot read-only
runtime also passed restart/drain. The matching arm64 runtime has now passed the
same nonroot/read-only restart/drain checks. Newer feature snapshots still need platform
validation. Archive extraction was checked against the source hash; only task-made
AppleDouble metadata files were removed before the cached serial builds.


SDK73 aggregate source 0f0f1ef failed the interceptor probe: serializing typed
Activity headers produced `_type` fields that the generic JSON payload validator
incorrectly interpreted as V sum-type tags. Failed source/report/log evidence is
retained in host-evidence-73-failed-0f0f1ef. The Linux build of that same known-bad
candidate was cancelled; it is not platform evidence. SDK74 replaces the JSON
validation boundary with the existing hardened parser; focused regression units
passed. It also normalizes captured non-application Nexus operation failures,
preserving typed causes and rejecting incompatible custom failure converters.
The normal focused Nexus matrix passed all 84 two-server histories; instrumented
and integrated validation of SDK74 remain pending.

The first SDK50-to-SDK68 arm64 container upgrade case passed both worker types
and eight offline replays, but Docker timed out during cleanup and subsequent
image inspection. The portable four-case matrix is incomplete. No Linux or full
parity claim is based on that partial result.


SDK74 source ab642493d0876cec3e02a54a47936851a144f7b66537eaeb06375dd1eb75faa5
built successfully; corrected 74-report host gate and Linux arm64 validation are
running. The focused Nexus matrix passed both normal and V/C Address/UB sanitizer
runs (90 histories each). The portable SDK50/Core snapshot to SDK68 matrix now
passed on both Linux architectures: 32 histories and 64 offline replays in total,
replacing both workflow and Activity workers for upgrade/rollback, codecs on/off.
Evidence: release-corpus/sdk50-to-sdk68/verification.json. These are internal
frozen SDK snapshots, not a published release support matrix.

SDK75 fixes external-storage claim parsing: _type is a normal claim key and
ambiguous duplicate keys / malformed nested arrays are rejected. The added claim
regression failed on SDK74, then passed with the hardened parser in normal and
instrumented units. Combined SDK75 validation is pending.


SDK74 aggregate stopped in the preexisting standalone Nexus short-wait case with
an HTTP/2 transport error. Its failed source/log/report evidence is retained in
host-evidence-74-failed-ab64249. The fixed interceptor path passed separately.
SDK77 adds consistent overall deadline handling for workflow, Update, Activity
and Nexus result waits, including Nexus's final description phase. Normal live
validation passed 480 one-millisecond waits plus early missing-execution errors,
execution survival and six offline histories. Injected early/late errors test
budget precedence; caller cancellation is tested independently. The focused
standalone Nexus regression also passed. Full combined validation remains.

Raw-history support passed normal two-server default/prefix/context matrices,
with and without external storage: 24 pending/completed histories replayed after
server shutdown without new uploads. Raw close-event responses contain whole
batches, so result selection skips nonterminal events. Wire exports retain
external references; structured pages normalize before retrieval. Binary unknown
fields, corruption, encoding validation and ambiguous-response units passed.
Sanitizer matrices and final aggregate/platform validation remain pending.

The build script no longer compiles mixed/protobuf-JSON/typed fixture binaries
twice. Both new fixture binaries are included in exact build verification.


The full 76-report host gate passed at source
f4504f079522d92af8ca4f5e139ad3b525d45aa1734f3edc66641fd6ac0a0135,
including all native sanitizer matrices. Exact source, reports, original native
libraries and eight fixture executables are archived in host-evidence-76-f4504f0.
Linux arm64 is still checking the earlier SDK74 snapshot.

SDK78 adds ordered per-call RPC metadata and removes the two per-call map fields
from every owned caller. Repeated ASCII/binary values, empty bytes and coalesced
binary forwarding passed 163 concurrent callback requests. Running the test with
Core10 reproduced lost values; Core11 passed. The existing real-server custom
gRPC suite passed normal/instrumented execution, shared-client lifetime, worker
replacement and offline replay. All 199 Rust client and 16 bridge units passed
on the formatted metadata-only source. Core11 reconstructs exactly from Core10
plus rpc-metadata.patch. Source proof is in core-eleven-patch-proof.

The pinned upstream full Rust lint alias fails existing compiler/derive lints.
An isolated Core12 cleanup experiment is retained under artifacts and is not
promoted. No lint suppressions or constructor cleanup are included in Core11.
Only the task-local incremental compiler cache was removed after the final
native build/unit runs; cleanup evidence is retained. Combined SDK78 validation
and the newer platform matrices remain pending; release_ready stays false.

### Visibility and scoped activation retrieval candidate

Added current/archived/scan pagination and counting, lazy page iteration, original
wire execution information and per-execution typed memo conversion. Injected
response tests cover all routes, empty pages, token cycles, retry after errors,
EOF, unknown fields, overlapping advances and client close. Actual archived
storage backends remain untested. Normal and V/C ASan/UBSan focused tests passed
72 histories against both server versions, across three codec policies with and
without storage. The workflow reads its memo during initialization and receives a
stored signal on a later activation; live query, restart and offline replay all
check workflow-specific retrieval scope. Raw listings perform no retrieval.
Full aggregate integration and current Linux validation remain outstanding.

### Parallel workflow callback candidate

The coordinator now owns scheduling and independent per-run runtimes, transfers
exclusive ownership to bounded activation tasks, and serializes each run through
Core completion and storage. Idle/evicted state and metric leases are released
only after the owning task returns. The prior shared runtime map is no longer
accessed from callback threads. A dedicated focused matrix passed 90 histories:
held callbacks did not block unrelated fast workflows, same-run signals stayed
ordered, six held workflows remained within three slots, and failures,
cancellation, worker replacement, graceful shutdown and offline replay passed
both servers with three codecs. The existing 72-history sticky visibility and
storage matrix also passed with the concurrent worker. The same 90 histories passed V/C ASan/UBSan, including shutdown while a callback
remained held. Aggregate integration remains pending. Fatal panic isolation and callback deadlock
handling still require work.

The prior repeated-metadata candidate passed all 77 host reports including
native sanitizers at source
`956b90d0b51f47686fc0044d54be4a18fbe58dd7e47c27d7a8af3e58dc7fdec2`.
Its immutable source, reports and original native/executable artifacts are under
`artifacts/v-sdk-spike/host-evidence-77-956b90d`. The visibility/concurrency
candidate requires a new aggregate check; release readiness remains false.

### Workflow management and pause candidates

High-level typed/raw describe, reset and delete passed a focused 90-history matrix
against both servers, three codecs and storage on/off. It covers memo scope,
reset validation/identity propagation, signal exclusion, typed subsequent signals,
replacement, asynchronous run-specific deletion and offline replay without
uploads. The same 90 histories passed V/C ASan/UBSan. Temporal 1.32 did not deduplicate reset IDs; 1.33 did. Both reject the
schema's post-reset signal operation. The high-level reset does not automatically
retry, even with a retry-enabled client. One default-configuration deletion case
passed after the server's 30-second close-task acknowledgement interval; the
matrix shortens that interval while preserving the safety check.

Pause/unpause passed 48 focused histories: namespace gating, pause metadata,
duplicate pause, held signals/cancellation, unrelated progress, replacement,
resumption and offline replay on both servers with three codecs. The same 48
histories passed V/C ASan/UBSan. Aggregate/current Linux validation remains pending.

The request-identity stress fixture now accepts only history-service
FORCE_CLOSE_COMMAND rollovers during the 256-signal burst. The server uses those
when its event buffer fills. Exact unique signal identities, totals, completion
and offline replay remain mandatory, and every other failure is rejected. The
failure was reproduced in the frozen Linux amd64 build; corrected source
`ee16b63b31894aec819f48ccaceb2f6aaa92af1b882e30145932d5fa0f1e7f86`
reuses byte-identical compiled inputs with only that test-driver change. Its new
full gate is running separately. Probe diagnostics now include actual workflow-task
failure attributes rather than only event counts.

### Batch replay candidate

Added bounded parallel corpus replay with per-history configuration, immutable
registration snapshots, independent caches, input-ordered success/error/skipped
outcomes and fail-fast draining. A two-server/three-codec/storage on-off matrix
passed 72 live histories across worker replacement, then 228 successful offline
replays, 108 expected corrupt-history/command-incompatibility rejections and 132
skipped cases. Callback instrumentation checked actual concurrency and draining;
all external payload files remained unchanged. The same matrix passed V/C
ASan/UBSan. Aggregate/current Linux validation remains outstanding.


## Recorded SideEffect candidate

The maintained workflow-markers patch adds two language/Core protocol messages,
a restricted marker state machine and history lookahead. The bridge advertises
its required marker capability; linking this SDK against Core11 fails on the
missing capability symbol. All 434 Rust/schema/header files reconstruct exactly
from the pinned reference and twelve patches. Core unit tests passed 558 with one
ignored test; client/bridge tests passed 200/16. The first parallel Core test run
hit an unchanged ephemeral-port bind race; the serial run passed. cargo lint still
fails on the pinned source's redundant formatting borrow under Rust 1.99; no lint
suppressions or incomplete Core12 changes are included.

Focused SideEffect tests passed 36 histories/132 markers normally and under V/C
ASan/UBSan. Packing/splitting chains and external storage passed another 36
histories/132 markers. Eight changed-code histories verified nondeterminism
policies, repair and cancellation; 24 extra/removed/renamed offline replays were
rejected. Go SDK 1.35.0 produced two histories independently checked and replayed
by Go and V after Go-to-V replacement with an explicit sticky-queue reset. The
13,682 Go protobuf JSON expectations were regenerated against the extended
actual descriptor: zero semantic changes. Payload traversal now verifies the
active Cargo build graph instead of stale cached build-script outputs; two marker
payload types and 64 map-order round trips were added. The example file storage
driver now accepts concurrent successful directory initialization.

Mixed marker, aggregate and current Linux validation remain pending. Full-parity
release readiness remains false.


The preceding Core11 candidate passed its complete 82-report host gate, including
native V/C sanitizers, at source
236f0a76672b82cf4b67ee08fcb0c8c03f3d3ea3406e0e8643eb164e9008a7f0.
Exact source, all reports, binaries and native libraries are frozen in
host-evidence-82-236f0a7. The amd64 SDK76 fixture correction passed all 76 container
reports at ee16b63b31894aec819f48ccaceb2f6aaa92af1b882e30145932d5fa0f1e7f86;
its nonroot/read-only runtime passed SIGKILL replacement, offline replay and
SIGTERM drain. The latest arm64 complete evidence remains SDK74 at ab642493;
the frozen 82-report candidate is being rebuilt there serially. These earlier
Linux successes do not verify the newer SideEffect extension.

The expanded SideEffect V/C ASan/UBSan matrix passed 144 histories/672 markers,
including patch/local Activity interleaving, six codec profiles and storage.
Eight changed-code histories and all 24 rejected incompatible replays also passed
instrumentation, as did both frozen Go histories. Mixed-marker normal validation
passed 72 histories/408 markers. This qualifies the extension for the integrated
build/check gate; it does not provide current Linux or full-release verification.


## Tracing candidate and archived Core13 host gate

Core13 source 572498031cf0eb12ba9ff9a4d83e94330c6b53ffb413ca9c8804b4b7d598baa4
passed all 85 required host reports, including the expanded SideEffect matrices,
frozen Go replay and native V/C ASan/UBSan. Exact source, reports and every bound
artifact are retained in host-evidence-85-5724980. The matching arm64 release
build reached validation before Docker stopped responding; no completed Linux
result is claimed. SDK feature implementation continues independently.

Added typed spans, events/links/status/limits, W3C context propagation, explicit
parent scopes, bounded concurrent custom export with flush/deadline/shutdown,
and Rust-runtime OTLP gRPC/HTTP protobuf/HTTP JSON exporters. Custom CA/mTLS
and strict hostname/trust/identity rejection passed on all three transports.
The 14,896-vector pinned Go OpenTelemetry 1.43 propagation corpus matched V.

Client tracing covers starts, signals, queries, Updates, Update-with-Start and
standalone Activity starts. Retained connection clones survived original/parent
close and sixteen concurrent real RPCs on both servers. Automatic workflow,
event, command, local/remote Activity, query and validator spans preserve
application headers/codecs and reserve _tracer-data only in header maps; memo
retains its entire key space. Workflow span identities do not consume the
workflow RNG. A two-server, three-codec, storage on/off matrix passed 108
histories normally and under V/C ASan/UBSan, including concurrent Activities,
children, Updates, failure/cancellation, Continue-as-New and worker SIGKILL.
All nine histories per case replayed without exporting spans.

Core14 reconstructs exactly from the pin and thirteen maintained patches; its
new tracing bridge passed twenty unit tests. Current aggregate/build validation
is pending. Nexus tracing, W3C baggage and sampler/exporter breadth remain, along
with the other production requirements in release/readiness.json. Release
readiness remains false. V/C instrumentation does not instrument Rust Core.

The broader Core14 Rust Core unit build failed before tests while compiling
Wasmtime dev-dependencies because the host ran out of disk. Its failure log and
cleanup manifest are retained; no broader Core14 unit pass is claimed. Only
newly generated private Cargo compiler files were reclaimed. Core13's archived
558 Core/200 client unit passes remain prior-version evidence.


## Nexus tracing, baggage and Go worker handoffs candidate

Added Nexus start/cancel spans and independently retained trace scopes for
workflow/Activity/Update/query/signal-backed helpers. The focused workflow-backed
Nexus matrix passed 84 histories across both server versions, three codecs and
storage on/off, including async completion/cancellation and worker replacement.

W3C baggage now travels separately from SpanContext and is exposed through
propagation_context() on workflow/Activity/Nexus contexts and readonly
query/validator interceptor chains. Immutable members/properties, explicit client
propagation scopes and per-RPC baggage overrides are implemented. Parsing keeps
valid members around invalid ones, resolves duplicate keys, repairs invalid UTF-8
and bounds members/encoded bytes; an additional 1 MiB input/1024-property resource
limit prevents pathological allocations. All 11,935 pinned Go OpenTelemetry 1.43
vectors matched normally and under V/C ASan/UBSan.

A 194-history normal and instrumented matrix passed baggage through children,
Continue-as-New, local/remote Activities, scoped signal/Update changes, query and
validator interceptors, Nexus helpers/callbacks, worker replacement and replay.
Unlike a storage-configured fixture with only small values, this fixture forces
actual claims with a one-byte threshold. Raw history inspection verifies trace
and baggage carriers remain inline while application memo named _tracer-data
still offloads. Internal temporal-v/propagation metadata protects only carrier
payloads in header fields; the application memo key space is unchanged.

An independently built Go Temporal SDK 1.35.0 worker with the official
OpenTelemetry interceptor 0.6.0/OpenTelemetry 1.43.0 passed both Go-to-V and
V-to-Go live handoffs on both servers. State, signal/query completion, trace
ancestry and baggage survived replacement; both SDKs replayed every history.
Frozen histories and original Go/V span exports carry source/file hashes and
independent wire/ancestry checks. The V runtime has no Go SDK dependency.

These remain focused candidate results. The preceding integrated Core14 source
864c3f46ce2f340cfde5713ed7c64aa3c3c90926f1b1ee184525036d24043b1d
built all forty bound artifacts but failed its 92-report host gate at HTTP metrics export. The exact failed source, all forty artifacts and diagnostics are frozen in host-failed-92-864c3f4.
Current-source Linux and full-release verification remain outstanding.


## Shared OTLP feature regression correction

The integrated tracing gate exposed two Cargo feature-unification regressions:
async reqwest became the default for metrics' non-Tokio PeriodicReader, and
HTTP/JSON trace support changed the default metrics wire protocol. The tracing
bridge now enables the async HTTP trait directly and supplies its own async
client without changing OTLP's default client selection. Core's HTTP metrics
builder explicitly selects protobuf. Registry versions/checksums are unchanged.
A native loopback counter regression checks actual delivery and content type;
all 21 bridge units passed. Real Prometheus/HTTP/gRPC metrics, explicit/delta/
exponential histogram options, all three trace transports and trace TLS/mTLS
passed against the corrected native build. The five-file tracing patch and
457 Rust/schema/header/manifest files reconstruct exactly from the pinned
reference plus thirteen maintained patches. The two failed metrics attempts and
the ENOSPC link failure are retained; no failed run is counted as a pass.

The four live Go/V handoffs and their offline V replays also passed V/C
ASan/UBSan. Go and Rust themselves remain uninstrumented. The combined
97-report SDK candidate still requires a fresh full build/check gate and both
current-source Linux targets. Full release readiness remains false.

The build now verifies all 457 Core source/manifest files and thirteen maintained
patch hashes before and after native compilation. Negative tests reject changed
metrics code, header-compatible stale Cargo features and missing source while
leaving the original tree unchanged. The focused V release binary replayed all
four frozen Go/V histories successfully.

Sampling candidate: reusable always-on/off, exact ratio and configurable
parent-based/custom samplers are implemented. Sampling attributes/state and
record-only decisions are separate from export; workflow command propagation
uses the final sampled context. The 19,020-case pinned Go oracle passed normal
and V/C sanitizer validation. Two-server restart/replay matrices and the full
aggregate gate are being validated; Linux remains blocked and release_ready is
false. Native V recover was discovered in the pinned compiler and is under
separate investigation; callback panic recovery is not part of this candidate.

Native V callback panic isolation is implemented without an external unwinding
shim. The pinned compiler passed 800 nested/deferred panics across eight threads
in ordinary and optimized builds. Normal and V/C ASan/UBSan focused matrices
passed 96 histories for workflows, factory/command panic repair, query/validator
isolation, Update handler task retry, concurrent local/remote Activity retries,
deferred cleanup and fail-workflow policy replay. Nexus start/cancel panic
retries passed 12 more instrumented histories, including SIGKILL/replacement.
Metric scopes are allocated after factories and released if initialization fails.
Original panic stack capture, other extension callback boundaries, the integrated
102-report gate and Linux validation remain pending; release_ready remains false.

Activity preparation/result ownership is now centralized: each attempt returns
one result to the worker loop, and preparation failures close their metric lease.
The new recovery boundary includes input/output/failure conversion and storage
callbacks; its raw retryable PanicError does not call a broken application
converter again. Focused normal tests passed 44 histories, including one-slot
store/retrieve limits, nine concurrent failing workflows, fresh successful work,
SIGKILL/replacement and offline replay. The original 96-history panic regression
matrix passed after the refactor. Expanded sanitizer/optimized and aggregate
103-report validation remain pending; release_ready remains false.

The final extension matrix passed all 44 histories in normal, optimized and V/C
ASan/UBSan builds, including maximum-attempt retry state and one-slot storage
permit recovery. The ordinary and instrumented 96-history panic regression
matrices passed after the Activity ownership refactor. Source, compiler, native
libraries, executables and reports are bound in the focused validation receipt.
Aggregate and Linux validation remain pending.

Unfinished Update lifecycle now matches the pinned Go SDK policy: per-handler
warn-and-abandon/abandon, completion/CAN/cancellation with pending handlers, silent
abandonment on failure, handler-status inspection and client/server-owned abort
outcomes. Explicit completion/rejection removes the preserved result scope.
Normal/optimized/V-C-ASan-UBSan matrices passed 28 histories; original instrumented
panic/extension regressions passed 96/44 histories. Actual Go/V handoffs passed
sixteen live runs and four continuations, with forty dual-language replays.
Twenty frozen reference histories also passed optimized/instrumented V replay.
The next integrated gate requires 105 reports and 42 bound artifacts, including
the exact time-skipping server. The private 102-report gate stopped on a missing
fixture executable, preserved its failure and resumed from that step after the
existing pinned cache was linked. No SDK source was changed for that repair.
Aggregate/current Linux verification and release readiness remain outstanding.

The preceding tracing/baggage/Core14 source
9419cbc55de28c99849db6ec04f3b2f159f4d7335cbcdb7e3c95845e1733993d
passed all 97 host reports including the complete V/C sanitizer matrix. Its exact
source, reports and 40 bound artifacts are frozen in host-evidence-97-9419cbc.
The newer sampling/panic/handler candidate requires its own 105-report gate;
Linux readiness is still unverified.

Numeric workflow version choices now build on the existing Core patch protocol,
with default=-1, zero, supported ranges, per-run choice caching, a reserved hashed
namespace and explicit final-version deprecation. The recorded-value error latch
now covers both these choices and SideEffects. Normal and optimized matrices
passed 60 histories/72 incompatible rejections each; previous SideEffect
regressions passed 144 histories/672 markers, eight changed-code histories/24
rejections and two actual Go histories. The initial instrumented attempt stopped
on a transport reset while querying a deliberately task-failed run during repair.
The bounded polling fixture now records/retries that specific reset and deadline
status while asserting worker liveness; the new instrumented matrix is running.
Integrated 106-report and Linux validation remain pending; release_ready=false.

The complete numeric matrix passed 60 histories and 72 incompatible rejections
in all three normal/optimized/V-C-instrumented builds. Bounded query polling
recorded transient transport resets/deadlines during deliberate task failure and
repair. Independent raw-history audit verified actual external storage references
versus inline payloads. Source/compiler/native/executable bindings were preserved.
The integrated 106-report gate and Linux validation remain outstanding.

Dynamic Updates now support dynamic-only workflows and fallback alongside named
handlers. Raw owned arguments retain opaque encodings through codecs and external
storage, while named handlers keep converter precedence. Normal, optimized and
V/C ASan/UBSan matrices each passed 24 histories across two servers, three codec
profiles and storage on/off. They cover metadata, opaque bytes, named conversion
rejection, validator rejection/panic, handler failure, deferred completion,
SIGKILL/replacement, Update-ID deduplication and offline replay. A replay poison
validator proves accepted Updates do not validate twice. All 28 handler lifecycle
regressions also passed under instrumentation. Source/native/binary hashes are
bound in focused/sdk107/dynamic-validation-proof.json. The candidate now requires
107 aggregate reports; both Linux architectures remain blocked and unverified.

Original recovered panic stacks are implemented in an isolated candidate through
a maintained change to the pinned V builtin runtime. C-heap ownership respects
thread-local state that the collector does not scan; records free stack buffers
after recovery, and returned traces own their copies. Captures are bounded to
64 frames/16 KiB and happen before longjmp. Normal, optimized and sanitizer stress
tests passed 2,400 panics across eight threads, including nested recovery and
trace lifetime. Upstream recovery tests passed; the hook fixture needed the
repository-required bundled-libgc flag in its child compilation. Compiler/runtime
guards reject unknown sources, changed receipts and failed rebuilds.
Initial live matrices passed 96 workflow/Activity and 44 extension histories
with original stack checks. The expanded query/validator and optimized matrices,
package integration, aggregate and Linux validation are still in progress.

Explicit forced workflow-task failure is implemented in an isolated candidate.
The initial two-server/three-codec/storage matrix passed 84 histories: task
failure before and after queued completion, subsequent application errors and
panics under fail-workflow policy, signals/Updates, first-request precedence,
no committed timers/completion, unrelated progress, replacement and offline
replay. Final optimized/instrumented validation and integration remain.

The corrected original-stack candidate passed normal, optimized and V/C ASan/UBSan
matrices: 96 workflow/Activity histories, 44 converter/storage histories and 12
Nexus histories in each build. Optimized native symbol lookup now adjusts return
addresses to call sites, avoiding attribution to the next function at noreturn
callback boundaries. Exact source/runtime/compiler/native/executable hashes and
all nine reports are bound in focused/sdk108/panic-stack-callsite-validation-proof.json.
The forced-task candidate also passed 84 histories per normal/optimized/sanitizer
build, bound in focused/sdk109/forced-task-validation-proof.json. Full integration
and current Linux verification remain outstanding.

Named deterministic random streams now preserve shared-handle progress and isolate
different names from the default RNG. Core seed updates reset every retained
handle after server reset. Default RNG behavior remains fixed; float/bool methods
and named UUID generation are included. All 384 independent frozen Python vectors
and basic isolation/UUID/float tests passed. The initial two-server/three-codec/
storage matrix passed 24 histories, including added independent draws after
SIGKILL, real reset reseeding, a second replacement and offline replay. Optimized/
instrumented final checks, aggregate and Linux verification remain pending.

Final named-random-stream validation passed all 24 histories in each normal,
optimized and V/C sanitizer build. The 384 frozen Python vectors and shared
handle/default isolation/UUID/float checks passed in all three builds, together
with original panic stack ownership/protected failure/export checks. Exact source,
runtime/compiler/native/executable and report hashes are bound in
focused/sdk110/random-stream-validation-proof.json. Aggregate and Linux evidence
remain outstanding.

Raw dynamic signal/query dispatch is implemented in an isolated candidate. Named
query/signal declarations select normal conversion; fallback receives owned raw
values after codec/storage decoding. Interceptors, complete signal metadata and
query isolation remain in the same dispatch chain. Metadata advertises both
named and dynamic handlers. A five-history prototype passed opaque non-UTF-8
bytes, ownership checks, named priority/rejection, query/signal panic behavior,
unrelated progress, three invalid declaration classes, repaired replacement and
offline replay. One query deadline during sticky-queue replacement was recorded
and retried with a bounded budget and worker-liveness checks. Full normal,
optimized, instrumented, aggregate and Linux validation remain pending.

The preceding integrated candidate passed all 105 host reports, including the
full V/C sanitizer suite, at source
60c17d39b962d4f68e5e9daceb2cbd9f18f297e1068321373a8af69ee2ebd857.
Its source, reports and all 42 bound executable/native/tool artifacts are frozen
in host-evidence-105-60c17d3. The combined candidate adds numeric versions, raw
dynamic Updates/signals/queries, original panic stacks, explicit forced task
failures and named random streams. It requires its own 111-report host gate and
43 bound artifacts, including the patched V compiler. Both current Linux
architectures remain unverified; Docker health still times out without a daemon
restart. The original Rust checkout remains untouched apart from its preexisting
mise.toml change.

Final raw signal/query matrices passed all 60 histories in normal, optimized and
V/C sanitizer builds. Each recorded 12 query deadlines during deliberate task
failure/replacement; bounded retry also recognizes the exact HTTP/2 reset seen
in the earlier failed instrumented attempt, whose log/report/driver are retained.
All SDK/runtime/compiler/native/executable hashes remained unchanged. Dynamic
Update regressions passed 24 histories in normal and instrumented builds; numeric
version regressions passed 60 histories/72 incompatible-history rejections.
Evidence is bound in focused/sdk111/raw-interactions-validation-proof.json.
This completes focused validation of the combined candidate; the 111-report
aggregate host gate and both Linux targets still require fresh verification.

Custom gRPC transport and metric/trace exporter panic isolation passed normal,
optimized and V/C sanitizer validation. Each build covered two Temporal servers,
80 panicking/80 succeeding concurrent RPCs per server plus five failed connection
handshakes, persistent/one-shot metric export panics and exporter shutdown panics.
Trace failure/recovery, user defer execution, retained context closure, redacted
RPC errors, native pending-point/definition/label release and dispatcher drain
passed. The existing gRPC worker restart/replay, metric export lifecycle/replay
and bounded trace provider suites also passed in all three builds. Twelve
reports and exact code/runtime/compiler/native/binary hashes are bound in
focused/sdk112/extension-validation-proof.json. Aggregate and Linux validation
remain pending; the preceding SDK111 aggregate is still running.

Raw dynamic Activities now receive owned RawValue lists after codec/storage
decoding, with named-handler precedence and explicit scoped conversion available
through ActivityContext.decode_raw_value. The old converted dynamic signature
was removed from remote/local registration and all known callers. Normal,
optimized and V/C sanitizer matrices each passed 24 workflow histories plus
12 standalone Activities across two servers, three codecs and storage on/off,
including panic retries, failures, cancellation, SIGKILL/replacement and offline
replay without redispatch. The existing dynamic suite passed; normal and
instrumented named Activity panic and converter/storage regressions passed
96 and 44 histories respectively. Exact bindings and eight reports are in
focused/sdk113/raw-activities-validation-proof.json. Aggregate/current Linux
validation remain pending; release_ready=false.

Query-only activation observability now follows the pinned Rust distinction
between is_replaying() and is_replaying_history_events(). Durable command,
patch and SideEffect decisions retain the original Core activation flag; logs,
metrics and automatic workflow spans suppress only actual history replay.
Both flags are available to query interceptors. Normal, optimized and V/C
sanitizer query matrices each passed six histories; every cached live query
observed activation=true/history=false and exported its metric/span through
SIGKILL/replacement. Historical start metrics/logs and offline spans stayed
suppressed. Normal/instrumented tracing and sampling regressions passed
108 and 198 histories respectively. Seven reports and exact SDK/compiler/native/
executable bindings are in focused/sdk114/query-observation-validation-proof.json.
Aggregate and current Linux verification remain pending.

Activity heartbeat details now stay as owned codec/storage-decoded raw values
until the handler requests typed/string conversion. The eager ActivityInfo
fields were removed; known callers use context.heartbeat_values()! or
heartbeat_details()!, and raw_heartbeat_details() remains available after a
conversion error. Normal, optimized and V/C sanitizer matrices each passed
12 two-server/codec/storage histories, with named and dynamic handlers, two
SIGKILL replacements, unknown binary encoding/protobuf metadata, ownership,
explicit conversion rejection, completion and offline replay. The existing
112-history typed detail suite, checkpoint restart, malformed heartbeat and
eager standalone retry regressions passed. The deferred example now explicitly
reads its typed checkpoint instead of relying on preparation-time conversion.
Seven final reports and exact bindings are in
focused/sdk115/raw-heartbeat-validation-proof.json. Earlier failed fixture and
pre-example-decoding evidence are preserved. Aggregate and current Linux
verification remain pending; release_ready=false.

Dynamic workflow start arguments now bypass payload conversion after codec/storage
decoding. Named registrations retain converter priority; fallback factories see
owned raw values and can explicitly convert them in the workflow scope. All known
converted-input dynamic callers were updated. Normal, optimized and V/C sanitizer
matrices each passed 48 histories across two servers, three codecs and storage
on/off: empty/opaque mixed inputs, named priority/rejection, task repair after
registration removal, SIGKILL/replacement and offline replay. The original dynamic
workflow/remote/local Activity dispatch suite passed with updated executables.
Exact bindings and four reports are in
focused/sdk116/raw-workflow-validation-proof.json. Aggregate/current Linux
verification remain pending; release_ready=false.

ActivityContext.client() now retains the worker connection with independent
client ownership and current Activity trace/baggage. Shared client options,
interceptors and plugin registrations propagate without rerunning hooks;
otherwise worker settings provide the client configuration. The C shim now
constructs direct workers with an independent connection owner, allowing retained
clients to outlive worker finalization without retaining worker slot leases.
Normal, optimized and V/C sanitizer matrices each passed 144 histories across
two servers, three codecs, storage on/off and direct/shared clients: concurrent
local/remote handlers started workflows, read results and signaled callers;
original/sibling close, client use after worker drain, trace ancestry, SIGKILL/
replacement and six offline histories per case passed. Custom transport/metric/
trace callback recovery and original transport/metric lifecycle/replay suites
passed normal and instrumented regressions. Nine reports and exact V/C/native/
compiler/executable bindings are in
focused/sdk117/activity-client-validation-proof.json. Aggregate/current Linux
verification remain pending; release_ready=false.

NexusContext.client() now reuses the Nexus worker connection with independent
ownership, current trace/baggage and direct/shared configuration/plugin snapshots.
The Nexus example start/cancel/workflow/Activity/Update/query/signal handlers no
longer open connections per request. Normal, optimized and V/C sanitizer matrices
each passed 144 histories across two servers, three codecs, storage on/off and
direct/shared clients: closed-handle isolation, one-time configuration hooks,
large protected results, asynchronous callbacks, target cancellation, both worker
losses, new progress, drain and six offline histories per case passed. Existing
Nexus workflow, Activity, interaction and Update suites passed normal and
instrumented regressions. Eleven reports and exact source/compiler/native/
executable bindings are in focused/sdk118/nexus-context-validation-proof.json.
Aggregate/current Linux verification remain pending; release_ready=false.

ActivityInfo now exposes owned JSON-redacted task_token bytes and the minimum
original schedule-to-close/current-attempt start-to-close deadline. Zero schedule
timeout is unlimited; missing/invalid fields leave no deadline. Seconds and nanos
are added separately. Normal, optimized and V/C sanitizer matrices each passed
12 two-server/codec/storage histories, 48 remote deadlines checked against
independent server history at exact nanosecond precision and 48 local bounds,
including both minimum cases, unlimited scheduling, retries, authenticated
external token heartbeats/checkpoint conversion, owned copies, redaction,
SIGKILL/replacement and offline replay. The original scheduling-metadata and
Activity retry/backoff/cancellation suites passed with current executables.
The new probe was renamed activity_deadline_probe.py after detecting a collision
with the existing metadata test; the old probe is restored and all 119 aggregate
report identities are distinct. Earlier focused evidence/source are retained in
sdk119-before-deadline-fixture-rename. Five final reports and exact bindings are
in focused/sdk119/activity-deadline-validation-proof.json. Aggregate/current
Linux verification remain pending; release_ready=false.

The integrated SDK111/Core14 source
4501007a028c1ac23abd7f9b9f4fd2ebb14f9a870164217600da3cdda5aa0424
passed all 111 host reports, including the complete V/C sanitizer suite. Its
exact source, reports and 43 bound executable/native/tool artifacts are frozen
in host-evidence-111-4501007. Two disk-full linker interruptions and their
partial results/logs remain preserved; unchanged-source continuations reran the
failed compilations and every remaining test, retaining the original validation
interval and artifact hashes. Completed task-owned caches/fixtures were
transparently compressed without deleting source or evidence. The next integrated
candidate includes extension callback recovery, raw dynamic Activities/workflows,
query history-replay observability, lazy raw heartbeat checkpoints, Activity and
Nexus context clients, and owned task tokens/exact Activity deadlines. These
features have focused normal/optimized/instrumented evidence; their combined
119-report gate remains pending. Both Linux targets remain blocked by Docker
health timeouts and release_ready remains false.


replay-baseline-v1 implements experimental Event Groups from the pinned Rust workflow SDK.
Explicit synchronous scopes compose and restore, direct values override by key,
signal/Update handlers use implicit originating IDs, and UI label encoding bypasses
the application converter while retaining workflow codec scope. Command emission
is centralized; automatic cancellation inherits Core's scheduled annotations.
Local backoff retains captured groups. Feature-disabled calls return a workflow
task error, permitting configuration repair. Both server wire spikes and a
12-history expanded quick profile passed. Normal/optimized/instrumented matrices
and aggregate integration are in progress; release_ready remains false.


SDK121 adds runtime workflow context values, typed lookup, scoped callbacks and
explicit captured-branch re-entry. Outbound interceptors receive a per-command
snapshot, including local Activity rescheduling/backoff. Context values are
excluded from WorkflowInfo JSON and do not propagate as payloads or headers.
The first quick run passed SDK assertions but exposed a fixture assumption:
Core suppresses timers started/cancelled in the completion task. The corrected
nine-history quick run passed eight concurrent executions and a panic repair,
replacement and offline replay. Full matrices are pending.


SDK122 adds deterministic event-driven condition waits, lazy durable timeouts,
sorted/fixed-point predicate polling, scoped/root/shielded cancellation, explicit
idempotent cancellation, and captured context/Event Group continuation. Root and
scope cancellation causes retain first-writer semantics and descendant inheritance.
Eight quick histories passed, including concurrent state changes, timeout,
ready-over-cancel, scope release protection, abandonment, handler failure,
predicate panic task repair, disabled Event Groups, replacement and offline replay.
Expanded matrices and aggregate validation remain pending.


SDK123 removes eager Activity header payload conversion. Owned raw maps preserve
opaque encodings and arbitrary binary data/metadata after codecs/storage; scoped
conversion is explicit. Repository interceptor, typed-details and tracing callers
use the new methods. Four quick histories/16 named/dynamic remote/local Activities
passed with converter-error isolation, snapshot mutation, restart and replay.
The full matrix and typed metadata/tracing regressions are pending.


SDK124 adds Activity workflow handles and explicit worker namespace metadata.
Named/local handler tests now signal their parent through the accessor; standalone
handlers require no parent and use their retained client to start a child in the
same namespace. The feature fixture includes direct/shared clients, plugin and
trace ancestry checks, default/custom namespaces, replacement and offline replay.
The first custom-namespace setup started in the CLI default namespace; its logs
and failed report are retained. Explicit start namespace fixed the fixture; both
quick profiles passed 14 histories and two standalone Activities. Full matrices
and regressions remain pending.

SDK121 focused validation is green: 108 histories per normal/optimized/V-C
sanitizer build and eight normal/instrumented regression reports. The eleven
reports and unchanged SDK/native/compiler/executable bindings are retained in
focused/sdk121/context-values-validation-proof.json.

SDK123 focused validation is green: 48 histories and 192 Activities per
normal/optimized/V-C sanitizer build, plus normal/instrumented typed metadata
(112), tracing (108), and converter/storage panic (44) histories. Nine bound
reports are in focused/sdk123/raw-headers-validation-proof.json. Aggregate and
both Linux validations remain pending for the integrated candidate.


SDK125 exposes codec/storage-decoded raw headers on workflow starts and events,
query interceptors and Update validators, plus scoped individual raw conversion
for read-only interceptor continuations. The four-history quick profile passed
12 opaque-header queries and eight Update/deduplication calls, explicit converter
failures, owned binary metadata, restart with poisoned validators and offline
replay without storage writes. The initial fixture omitted its named Update
declaration; that failed report is retained. Full matrices and regressions remain.


SDK126 fixes an actual codec ownership defect. A legal nested mutation through a
local payload struct changed retained wire input: two reads produced 2 then 3,
and the wire changed from 1 to 3. The failing binary/source/log are preserved.
The first attempted array alias was rejected by V; the nested struct path then
reproduced the defect. Deep snapshots now isolate each codec boundary and cached
payload-converter/codec outputs, including unknown fields and external details.
UI labels use that same codec-only pipeline. The fixed basic reproduction gives
2/2 with wire 1. Shared-source 1,800-round/eight-thread tests passed. A rebuilt
matching fixture client was required for the deliberately mutating live codec;
initial mismatched-client failures are retained. The corrected 20-history quick
probe passed Activities, workflow/query/Update headers, UI groups, restart and
replay. Expanded matrices and full integration remain pending.


SDK127 adds the pinned Rust SDK's experimental fresh-patch activation policy.
True/false results memoize; replay/notified markers and deprecation bypass the
callback, and numeric version internals remain independent. The callback gets
owned protobuf metadata and lazy raw header/memo/search views; captured-context
commands are rejected. Public deprecation now preserves cached decisions and
ignores absent historical patches, matching the reference. The first prototype
passed six histories with cohorts, legacy unmarked history, inverse policy,
poisoned replay, current metadata, read-only guard, panic repair and restart.
Expanded matrices and regressions remain pending.


SDK128 binds every persistent sanitizer executable to its compiler, explicit
compile/link sanitizer flags and produced image hash. Final verification rejects
changed images even when the sanitizer report passes; completed-check archives
retain both normal and instrumented images. Tests cover changed compiler/images,
malformed commands, path escapes, final-gate rejection and immutable archive
copies. Real Address/UB-instrumented execution passed the 1,800-round/eight-thread
codec ownership fixture and deliberate image-tamper rejection. The full aggregate
and current Linux verification remain pending; release_ready=false.


SDK129 adds worker execution/replay middleware for combined, workflow, Activity
and Nexus workers after plugin configuration. Continuations are one-use and
expire when middleware returns; simultaneous calls admit one terminal path.
Middleware can replace setup/options but cannot change worker role or recursively
replace the registered chain. Shutdown hooks run after all polling/handler work
drains and before Core ownership closes. Hook and diagnostic logger panics cannot
prevent later notifications/cleanup. Twelve offline cases and nine quick live
histories passed with split roles, actual Activity drain, failures, cancellation,
SIGKILL replacement and offline replay. The first quick fixture used an unsupported
CLI reason flag; that failed report/log are retained and the fixture was corrected.
Expanded matrices and aggregate/Linux remain; activation/completion worker
observers are still a separate gap.


SDK130 adds worker completion observers after outbound codecs/storage and before
Core submission, including failed tasks, draining and offline replay. Every
observer gets a fresh protobuf snapshot backed by its own bytes; nested mutation
cannot affect another observer or the wire completion. Observer/diagnostic logger
panics are isolated so later hooks and Core submission continue. The initial
quick fixture did not append observers when lifecycle hooks were enabled; that
failed report/log are retained and the setup was corrected. Live nested mutation,
codec/storage, panic, restart and replay validation is in progress. Activation
observers and aggregate/current Linux remain outstanding.


SDK131 adds pre-execution, codec/storage-decoded activation observers with owned
snapshots and fatal typed/panic causes. Schema-derived traversal covers 66
reachable protobuf types, repeated codec cardinality, failure causes, memo/header
fields and recorded Activity/child/Nexus scopes. It leaves indexed search
attributes and propagation-owned headers untouched. A nineteen-history quick
profile passed nine lifecycle histories, a two-to-one batched codec parent/child
workflow with queries/signals/Updates and SIGKILL replay, and typed/native-stack
fatal guards with four concurrent executions each followed by worker repair and
replay. The first repair query used a five-second fixture RPC budget; retained
logs show the deadline, and the twenty-second recovery budget passed. Initial V
generator errors and the nested panic(err) C-codegen error are retained; corrected
code builds. Expanded matrices, aggregate integration and Linux remain pending.


SDK132 adds nonthrowing Activity cancellation polling and heartbeat-preserving
waits, stable first observed cause and owned details. The initial shutdown test
exposed a local Activity drain gap: shutdown could wait for its full timeout and
retry. The C shim now records a monotonic shutdown deadline; cooperative local
Activity cancellation after grace marks its run and fails the interrupted WFT
before user code commits a cancellation. Replacement replay retries it normally.
Control markers are removed on consumption/worker detach. A ten-history quick
profile passed cancellation, SIGTERM/control shutdown with grace, remote/local
timeout or Core not-found causes, repeated APIs, advancing/preserved heartbeats,
four concurrent mixed Activities, replacement and replay. Fixture enum and
worker-shutdown retry assumptions, the pre-fix 30-second local drain and disk-full
failures remain recorded. Expanded matrices and aggregate/Linux remain pending.


SDK133 candidate: query and Update-validator handlers now receive owned
`WorkflowContextView` snapshots, with immutable interceptor context branches and
command guards for captured contexts. All named/dynamic handler declarations and
known delegates were cut over together. Initial live/restart/offline checks passed;
expanded failure, codec, storage and sanitizer validation is pending. This candidate
has not passed the aggregate host or Linux gates and is not a production release.


Integration candidate 1.0.0-rc.1 freezes major-1 public declarations and adds
replay-baseline-v1 live upgrade/rollback plus dual replay to the full platform suite.
It includes SDK131 activation observers, SDK132 cooperative local shutdown,
SDK133 read-only interaction contexts and SDK134 contextual factories.
The integration and both Linux architecture gates are pending; release_ready=false.


The Linux API1 runs exposed two concrete defects. Large Nexus fixture inputs
exceeded Linux argv limits; the V test client now reads those inputs from files,
and a 12-history Linux preflight passed with the full 300 KB payload. A later run
captured a real Core replay defect: a local-Activity marker and its following
SideEffect were committed in different workflow tasks. The captured history fails
deterministically on the old runtime. The marker lookahead fix and pagination
regressions passed 783 Rust client/Core/bridge unit tests, with one upstream ignore.
The maintained patch/source lock and native capability guard include the fix.
replay-baseline-v1 side-effect replay joins the build, bringing bound artifacts to 48.
The fixed V/native build, full 137-report Linux amd64/arm64 gates and runtime
container replay remain pending. The API is still frozen at 1.0.0-rc.1 and
release_ready=false.
