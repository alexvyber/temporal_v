API contract: **1.0.0-rc.1 / major 1**, frozen in
[release/api-contract.json](release/api-contract.json). See
[release/API-POLICY.md](release/API-POLICY.md) for the exact handler/factory
signatures, upgrade replay gate and deployment compatibility policy. This
candidate still requires its source-bound Linux and runtime checks.

# V workflow SDK on Temporal Rust Core

The SDK provides V client APIs, event-driven workflows, remote and local Activities,
Updates, child workflows, run chains, patching, external operations, Nexus handlers,
converters/codecs, TLS/mTLS, and deployment versioning on Temporal Rust Core.
Rust Core owns transport, polling, protocol state machines and replay validation;
V owns workflow state and decisions. Workers contain no TypeScript, Bun or Go SDK.

**Full production parity is still in progress.** See [PROGRESS.md](PROGRESS.md) for
implemented scenarios, evidence, and remaining release requirements.

## Install in your V project

Requires V 0.5.2 or later and a working C compiler. The repository includes
native libraries for macOS arm64 and Linux amd64/arm64 (glibc 2.36 or newer).
Consumers do not need Rust, Python, a custom V compiler, or `VMODULES` settings.

```sh
v install https://github.com/alexvyber/temporal_v
```

Import the SDK in your application:

```v
import alexvyber.temporal_v.sdk
```

Add `https://github.com/alexvyber/temporal_v` to your project's `v.mod`
`dependencies` list. Build your application with `v .` as usual. No SDK-specific
compiler or linker flags are needed. Register your own workflows and Activities
and call `sdk.run_worker(...)` or `sdk.run_workflows(...)`.

Typed protobuf helpers work with the bundled generated messages. For custom
message types on V 0.5.2, supply the instance methods `decode_owned([]u8) !YourType`
and `validate_protojson(string) !` alongside `encode()` / `json()`; its compiler
cannot resolve generic static decoder calls across installed modules.

On released V 0.5.2, a panic exits the worker process; returned `!` errors retain
normal SDK error handling. Run workers under a supervisor. Current upstream V
with native panic recovery isolates covered callbacks; recovered stack traces
identify the recovery boundary, not the discarded original panic-site frames.

## SDK development and verification

```sh
bash scripts/bootstrap.sh
bash scripts/build.sh
bash scripts/check.sh
```

`check.sh` runs protocol tests, standalone Activity checks, the multi-workflow
milestone suite, a two-workflow crash/replay test, client/signal/query tests, and
Update/child/continue-as-new/patch replay tests, local retry/backoff, external
operations, converter, TLS, service, Nexus, async Activity, metadata and deployment
routing tests. The suite also checks offline replay and intentional nondeterminism.
All integration tests start
isolated in-memory Temporal servers on free loopback ports and stop them afterward.
They do not connect to deployed installations. The CLI is a dev-server launcher
and independent test client/history observer; neither the V client API nor the
workers invoke it.

The tested host is macOS arm64. Requirements: C compiler, Make, Python 3, curl,
Git, and Rust toolchain `1.99.1`. `SPIKE_RUST_TOOLCHAIN` and `CARGO_BIN` select an
installed alternative without changing global toolchains. Bootstrap reads the
pinned Rust commit from `../../repos/sdk-rust`; `SDK_RUST_SOURCE` overrides that.
When the local checkout is absent, bootstrap downloads that exact upstream commit.
All downloaded sources, binaries and logs go to `../artifacts/v-sdk-spike`,
overridable through `SPIKE_ARTIFACTS`. Rust dependencies use `locks/core.Cargo.lock`.

## Client API

The client calls Rust Core's WorkflowService RPC bridge with generated protobuf
requests. It does not create a worker or install the worker's signal handlers.

```v
mut client := sdk.connect(sdk.ClientOptions{})!
defer { client.close() }

handle := client.start(sdk.WorkflowStart{
    workflow_id: 'counter-example'
    workflow_type: 'v.counter'
    task_queue: 'v-sdk-spike'
    arguments: '{"initial":10,"label":"example"}'
})!
client.signal(handle, 'add', '{"amount":3}')!
state_json := client.query(handle, 'state', 'null')!
println(state_json)
client.signal(handle, 'finish', 'null')!
result := client.get_result(handle, 30000)!
assert result.status == .completed
println(result.data)
```

`ClientOptions` accepts `address`, `namespace`, and `rpc_timeout_millis` (default
5000). `WorkflowHandle` pins both workflow and run IDs; a manually constructed
handle may omit its run ID to address the current execution. `close()` is
idempotent; close only after outstanding calls finish.

| Operation | Behavior |
| --- | --- |
| `start(WorkflowStart)!` | Start a workflow and return its handle |
| `signal(handle, name, input_json)!` | Persist a signal; success acknowledges acceptance, not handler completion |
| `query(handle, name, input_json)!` | Return JSON from a workflow query, including completed runs |
| `cancel(handle, reason)!` | Request workflow cancellation |
| `get_result(handle, wait_timeout_millis)!` | Long-poll for a run's terminal result with an overall deadline |

Start rejects duplicate workflow IDs by default. `WorkflowStart.request_id` allows
the caller to retain an idempotency key; otherwise a UUID is generated. RPCs are
not automatically retried. `execution_timeout_seconds` is optional (zero means
unset); the workflow task timeout defaults to ten seconds.

`WorkflowResult.status` distinguishes `completed`, `failed`, `canceled`,
`terminated`, `timed_out`, and `continued_as_new`. `data` contains completed JSON;
`message` describes failure/termination. Results are run-scoped: the client returns
`next_run_id` when available rather than automatically following retries or
continue-as-new. Missing completion results decode as JSON `null`.

Transport/query errors preserve the gRPC status in `err.code()`. A result deadline
is code 4; duplicate starts are code 6 and missing executions code 5. Ordinary
workflow failure/cancellation returns a typed `WorkflowResult`, not an RPC error.

`build.sh` also builds `tools/v-client`, a small executable exercising this public
API. From the project directory, with the example workflow worker running:

```sh
../artifacts/v-sdk-spike/tools/v-client --workflow-id example --input '{"initial":10}' start
../artifacts/v-sdk-spike/tools/v-client --workflow-id example --name add --input '{"amount":3}' signal
../artifacts/v-sdk-spike/tools/v-client --workflow-id example --name state query
../artifacts/v-sdk-spike/tools/v-client --workflow-id example --name finish signal
../artifacts/v-sdk-spike/tools/v-client --workflow-id example result
```

Use `--run-id` to pin a run, `--address`/`--namespace` for the target, and
`--rpc-ms`/`--wait-ms` for deadlines. The executable's default workflow type is
`v.counter`. It prints JSON replies and JSON errors.

## Workflow API

A factory creates one state object per workflow run. Handlers receive SDK events
and use a context for durable operations; application code does not manipulate
Core protobuf commands or sequence numbers.

```v
module main

import json2
import sdk

struct Greeting {
mut:
    activity u32
}

fn new_greeting(view sdk.WorkflowContextView, inputs []sdk.PayloadValue) !sdk.Workflow { return Greeting{} }

fn (mut greeting Greeting) on_event(event sdk.Event, mut context sdk.WorkflowContext) ! {
    match event.kind {
        .started {
            name := json2.decode[string](event.data)!
            greeting.activity = context.activity('v.echo', json2.encode(name), sdk.ActivityOptions{})!
        }
        .activity_completed { context.complete(event.data)! }
        .activity_failed { return error(event.message) }
        .activity_cancelled { return error('Activity cancelled') }
        else {}
    }
}

fn main() {
    mut runtime := sdk.Runtime{}
    runtime.register('example.greeting', new_greeting) or { panic(err) }
    options := sdk.WorkerOptions{task_queue: 'greetings'}
    sdk.run_workflows(options, runtime) or { eprintln(err); exit(1) }
}
```

`Workflow.on_event` is a deterministic state machine API, not arbitrary V functions
with automatic durable suspension. Use only event data and stored workflow state
for decisions. Keep filesystem/network calls, wall-clock time, nondeterministic random values,
threads and other side effects in Activities. Use `context.info().time_millis`,
`context.random_u64()!` and `context.random_uuid()!` for deterministic decisions. A handler must return promptly.

The supported events are `started`, `activity_completed`, `activity_failed`,
`activity_cancelled`, `timer_fired`, and `signal_received`. `data` contains a single
decoded value, `arguments` contains the original input list, `sequence` identifies
an operation, and `message` describes an Activity failure.

Signals arrive as `Event{kind: .signal_received, name: signal_name, data: input_json}`.
They are delivered in Core's history order and replayed to reconstruct workflow
state. `event.arguments` preserves zero, one or multiple decoded payloads. `event.data`
contains the single argument, `null` for no arguments, or an empty string for multiple
arguments. Normal signal-handler errors follow the
same workflow-failure policy as other handler errors.

Query support is optional. Implement `sdk.QueryableWorkflow` on the same concrete
workflow type:

```v
fn (counter &Counter) query(name string, arguments []string, view sdk.WorkflowContextView) !sdk.PayloadValue {
    if name == 'state' { return counter.snapshot() }
    return error('unknown query: ${name}')
}
```

The query method has a read-only receiver and no workflow context, so there is no
SDK command API available from a query. Query code must not mutate global/external
state. Results are validated as JSON. Unknown/throwing queries, invalid arguments,
and workflows without query support return query failures without closing the
workflow or recording query events in history. Queries can trigger replay of an
evicted or completed run before their handler executes.

Context operations:

| Operation | Behavior |
| --- | --- |
| `activity(name, input_json, options)!` | Schedule an Activity and return its deterministic sequence number |
| `timer(time.Duration)!` | Start a durable timer, including sub-second durations |
| `cancel_timer(sequence)!` | Cancel a pending timer; return false once resolved/cancelled |
| `local_activity(name, input, options)!` | Execute local work with marker replay and durable retry backoff |
| `cancel_activity(sequence)!` | Request cancellation using its configured policy |
| `complete(result_json)!` | Complete and cancel pending timers/Activities; child parent-close policies apply |
| `fail(message, category)` | Fail the workflow and request cancellation of pending operations |

The context owns per-run sequence counters and pending-operation tracking.
Cancellation commands are emitted in sorted sequence order. Ordinary handler
errors become non-retryable `VWorkflowError` workflow failures, not worker crashes.
Unknown workflow names fail the affected execution. Initial/signal payload decode
errors fail the workflow task so repaired decoder code can resume execution.
`WorkflowTaskError` explicitly requests the same retryable task behavior; ordinary
handler errors still fail the workflow. Core cache
evictions remove V state; fresh initialization and replay reconstruct it.

Workflow cancellation cancels operations in the root cancellation scope and
emits `cancel_requested`. A handler acknowledges it with `context.cancel()!`.
Handlers may first schedule durable cleanup in a shielded scope. Nested scopes
propagate cancellation to descendants; shielded scopes stop that propagation.
Running Activities observe cancellation through heartbeats. Cleanup and scope
state reconstruct through replay after a worker crash.

Activity options default to a ten-second start-to-close timeout, one-second
heartbeat timeout, and unlimited retry attempts unless bounded by a schedule-to-close
timeout or an explicit retry policy. Timeouts are driven by Temporal and resolve
as Activity failure events. JSON payload encoding is `json/plain`.

The examples register four independent workflow types:

- `v.restart`: echo → durable timer → echo → completion.
- `v.parallel`: schedule two Activities together, collect and sort their results;
  options exercise handler failure, Activity failure and timeout.
- `v.wait`: wait on a durable timer, used to verify cancellation.
- `v.counter` (`workflows/counter.v`): accumulate `add` signals, expose `state` and
  `offset` queries, and complete on `finish`.

## Activity API and worker lifecycle

Register handlers on `sdk.Activities`, then call `sdk.run_activities(options, activities)`.
Each handler has signature:

```v
fn handler(mut context sdk.ActivityContext, arguments []string) !sdk.PayloadValue
```

Arguments are decoded independently. `sdk.single_argument(arguments)!` validates
single-argument handlers. Return one value using the configured converter (JSON
text for the default converter). An ordinary error becomes a retryable `VActivityError`, subject to the configured
retry policy (unlimited attempts by default). `sdk.ApplicationError` adds a type name
(`category`), multiple `details` values, `stack_trace`, an optional `cause`,
`error_category: .benign`, non-retryable behavior and an optional next retry delay.
Returning a captured `sdk.Failure` preserves its original protocol metadata,
including nested causes, retry state and Activity identifiers. Failure details use
the configured converter and codecs as a batch. `failure_chain_probe.py` checks
remote/local propagation, Update isolation and offline replay.

`sdk.DataConverter.failure_converter` accepts an `sdk.FailureConverter`.
Implement `to_failure` and `from_failure`, or delegate to
`sdk.DefaultFailureConverter`. Configure
`sdk.DefaultFailureConverter{encode_common_attributes: true}` to move messages
and stack traces into Temporal's standard `encoded_attributes` payloads. These
use standard JSON plus the configured codecs; codec framing alone is not encryption.
Decoding restores common attributes regardless of whether encoding is enabled.
`Failure.attributes_error` records an unavailable/invalid attribute decoder while
retaining the original bytes; `details_error` does the same for detail payloads.
Custom converters should preserve `Failure.protobuf` when adapting decoded errors.
Internal activation/replay diagnostics bypass custom conversion if needed to
report a broken converter. `context.fail(message, category)!` is fallible.
`context.heartbeat(details)!` accepts one value or an argument list and checks cancellation;
`context.check_cancelled()!` checks a delivered request. Cancellation is error code
499, reserved by the SDK. `context.sleep(duration)!` is a cooperative Activity wait
that heartbeats and checks cancellation every 50 milliseconds. It must not be used
inside a workflow.
Sleep uses a monotonic clock and resends the last heartbeat payloads without
overwriting application progress. Recorded detail lists are restored on retry.

Remote and local Activity options accept a custom `activity_id` and a `summary`.
Remote options also expose Core `versioning_intent` and `priority`. Timer options
accept `summary`; child options accept `static_summary` and `static_details`;
Nexus options accept `summary`. These descriptions use the configured converter
and codecs. Local retry summaries survive durable backoff. Each timer-backed
local retry gets a fresh Core sequence while results and cancellation retain the
original application handle.

`context.info()` exposes the Activity's workflow type, task queue, scheduling and
start timestamps, timeout durations, retry policy, priority, attempt, heartbeat
details and headers. Cancellation throws `sdk.ActivityCancelledError` with the
original Core reason and optional detail flags. `context.cancellation_info()`
retains the observed information. Pause/reset interruptions are reported as
retryable failures so Temporal can resume the Activity.

Activity handlers run on bounded V threads while another V thread polls Core.
Completions are serialized by the Activity worker's event loop. A cancellation
channel is routed by task token to each handler. Native bridge errors are
thread-local, so concurrent polls, completions and heartbeats cannot overwrite
another thread's error. Activity callback panics are recovered as retryable
`PanicError` failures. Deferred cleanup runs during recovery; native faults remain
process failures.

Both workers stay alive after workflow/Activity completion. There is no finite-task
mode. SIGTERM or SIGINT triggers a C signal flag; a monitor thread requests Core
shutdown outside the signal handler. Workers drain polls, acknowledge workflow
cache evictions, finish or cooperatively cancel in-flight Activities, then finalize
Core. The Activity shutdown grace period is one second. Non-cooperative handlers
can still delay shutdown and should be isolated if they cannot honor cancellation.

Example binaries, built under `../artifacts/v-sdk-spike/tools/`:

- `v-workflow-worker`: registers the example workflows.
- `v-worker`: registers example remote Activities.
- `v-nexus-worker`: registers synchronous and workflow-backed asynchronous Nexus operations.
- `v-replay`: replays binary histories without connecting to Temporal.

Both use `sdk.options_from_env()`:

| Variable | Default |
| --- | --- |
| `TEMPORAL_ADDRESS` | `http://127.0.0.1:7233` |
| `TEMPORAL_NAMESPACE` | `default` |
| `TEMPORAL_TASK_QUEUE` | `v-sdk-spike` |
| `TEMPORAL_ACTIVITY_CONCURRENCY` | `2`, accepted range 1–128 |

Workflow activations execute serially, interleaving independent workflow runs.
Their durable waits do not block other runs. Activity handlers execute in parallel.
The bridge uses ten workflow slots and a ten-run sticky cache; a one-second sticky
queue timeout allows tasks to move away from a dead worker.

## Verification and evidence

Run individual integration suites after building:

```sh
python3 scripts/smoke.py
python3 scripts/milestone_probe.py
python3 scripts/restart_probe.py
python3 scripts/client_probe.py
```

The milestone suite verifies:

- Six overlapping executions of two workflow types complete on the same workers.
- Server event timestamps prove concurrent Activity execution, bounded at two;
  worker logs independently confirm the bound.
- Workflow-handler errors, Activity errors/timeouts, unknown types and invalid
  inputs fail their executions without SDK task errors or worker exits. If another
  Activity completes while a workflow is closing, the server can reject that task
  with `UNHANDLED_COMMAND`. Core evicts/replays it with the newly buffered event;
  the test permits only that documented close race and records its retry count.
  Other task failure causes and task timeouts fail the suite.
- Workflow cancellation cancels timers and requests cancellation of running work.
- A running standalone Activity acknowledges heartbeat-delivered cancellation.
- The same worker PIDs process successful new work after failures/cancellations.
- SIGTERM cancels/drains an in-flight cooperative Activity and releases workflow state.

The restart suite starts two independent workflows, waits for each first Activity
completion and durable timer, and SIGKILLs both worker processes. New processes
start with empty caches and the same binary. Logs show both initial Activity and
timer commands reconstructed under `replay=true` for each run. The test checks the
preserved history prefix and distinct results, exactly one timer and two Activity
executions per run, and that neither first Activity executes again. It rejects
workflow task failures and execution timeouts. Sticky queue schedule-to-start
failover timeouts are legitimate after SIGKILL and are counted separately.

The client suite uses the V client for all workflow operations. It verifies signals
buffered before a worker starts, query state/arguments, error isolation and history
immutability, RPC status codes/deadlines, typed failed/cancelled results, and queries
after completion. It SIGKILLs the worker with two open counter runs, sends further
signals while it is down, then verifies replayed signal state and distinct results
from a fresh worker. The CLI reads history independently.

Evidence under `../artifacts/v-sdk-spike/`:

- `milestone-results.json`, `milestone-histories/`, `logs/milestone-*.log`.
- `restart-results.json`, `restart-history-before-{0,1}.json`,
  `restart-history-after-{0,1}.json`, `logs/restart-*.log`.
- `smoke-results.json`, `logs/protocol-tests.log`.
- `client-results.json`, `client-histories/`, `logs/client-worker-*.log`.

## Schemas and native boundary

`build.sh` generates `temporal/core_pb.v` from the original
`temporal/sdk/core/core_interface.proto` and all imports (over 109K generated lines).
The generator patch fixes oneof wrapper/message name collisions and one-member
oneof aliases. Protocol tests cover nested failure causes against known wire bytes,
unknown-field preservation and the previously conflicting callback types.

`python3 scripts/probe_schemas.py` reproduces the original unpatched generator
failures. Both upstream and third-party generators reject annotation extensions
in the public WorkflowService schema. The client uses the request/response types
already imported by Core's schema and Rust's RPC dispatcher. Generating the entire
service declaration remains blocked, but it is not needed for these client methods.

`native/core_shim.c` handles Rust's async callbacks in C using condition variables;
V allocation does not run on Tokio threads. V clones incoming bytes on its own
threads and releases Core buffers. The V workers use bundled Boehm GC.

## Orchestration API

`UpdatableWorkflow` optionally adds readonly `update_names() []string` and
`validate_update(name string, arguments []string, view sdk.WorkflowContextView) !`. After validation, `on_event` receives
`.update_received` with the protocol instance in `event.id`. Complete it with
`context.complete_update(event.id, result_json)!` or reject with
`context.fail_update(event.id, error)!`. A handler can keep the ID in its workflow
state and complete later after a durable timer or Activity. Accepted Updates replay
without rerunning the validator. Check `context.all_handlers_finished()` before
closing when the application must finish every Update. Otherwise closure abandons
unfinished Updates according to their configured policy. Returned handler errors
reject only that Update; a panic fails its workflow task by default.

```v
update := client.start_update(handle, sdk.UpdateRequest{
    name: 'add'
    arguments: '{"amount":5}'
    id: 'business-operation-123'
})!
result := client.update_result(update, 30_000)!
```

Keep the returned `UpdateHandle`: validation rejection can arrive as an immediate
outcome without a durable Update that another process can poll. Reusing an Update
ID deduplicates the operation within the execution. After acceptance, an independent
client can poll using its workflow handle and Update ID.

`context.child(type, workflow_id, input_json, sdk.ChildOptions{})!` returns a sequence for
`.child_started`, `.child_completed`, `.child_failed`, and `.child_cancelled` events.
Children use the same namespace/task queue and request-cancel parent-close policy.
Child options configure namespace/queue, timeouts, retries, cancellation, ID reuse,
parent-close policy, headers, memo and versioning options.
`context.cancel_child(sequence, reason)!` is idempotent.
`context.continue_as_new(input_json, sdk.ContinueOptions{})!` starts the next run
with the same type/queue and inherited run/task timeouts unless overridden. `client.get_result` remains run-scoped;
`client.get_chain_result` follows continuation/retry run IDs with one overall
deadline and a limit of 1,000 runs.

`context.info()` exposes recorded workflow time in milliseconds and execution
identity. `context.patched(id)!` records a patch marker on new histories and uses
historical patch notifications during replay. `context.deprecate_patch(id)!` supports removing a patch in a later code version.
The patch probe tests legacy → patched → deprecated → removed workflow code; SDK
release-to-release compatibility remains a separate requirement.

The orchestration probe kills a worker after an asynchronous Update is accepted
and again after a patch marker and timer are committed. Fresh workers must replay
both histories and complete with the expected state. It also checks child failure
propagation, Update rejection/deduplication, and a four-run continuation chain.

## Production release requirements

The requested release scope is **full Temporal SDK feature parity** for
**Linux amd64 and arm64 containers**. This implementation does not meet that scope
and must not be released as production ready.

[release/readiness.json](release/readiness.json) tracks missing and partially
implemented requirements against the pinned Rust SDK. Run the audit with:

```sh
python3 scripts/release_gate.py
```

It intentionally exits nonzero while requirements are incomplete. Passing the
integration suite verifies only its documented scenarios, not full SDK parity.
Remaining work includes broader option/edge-case coverage, workflow execution
ergonomics, SDK release compatibility, native failure injection/sanitizers,
sustained resource/outage tests, and release supply-chain verification. The current
source checklist is [release/readiness.json](release/readiness.json).

The multi-stage `Dockerfile` defines toolchain, build, validation, and nonroot
runtime stages. `SPIKE_BUILD_PROFILE=release` enables Rust release and V `-prod`
builds. To verify each target on a working Docker installation:

```sh
docker buildx build --platform linux/amd64 --target validation --load -t temporal-v-validation:amd64 .
docker buildx build --platform linux/arm64 --target validation --load -t temporal-v-validation:arm64 .
```

Linux arm64 and the host passed all 37 reports for the per-type versioning source
`198b5e3134d49e5829db2c64459910c7d8182096ce6ba863dadb9e115a7fe632`.
Linux amd64 and the host passed all 36 reports for the standalone Activity source
`50a9cd1be301fe9e3d656921dc5c8d3b62d76e68816d4ea687ea9c1b2534d7e7`.
Earlier nonroot, read-only runtime images for both architectures passed execution,
SIGKILL/restart replay and graceful shutdown. Evidence is under
`../artifacts/v-sdk-spike/linux-{arm64,amd64}-evidence/` and
`container-{arm64,amd64}-results.json`. Newer dynamic-handler changes require
fresh checks on both targets; runtime reports record their exact image IDs.
The runtime base is pinned by digest. OS packages still need immutable release
pinning and a dependency/license audit.

Activity cleanup now drains handler threads and the poller before freeing Core,
including errors; handlers must cooperate with cancellation. Worker signal handlers
are shared across worker instances and restored after the last closes. Client calls now hold a shared lifetime lock; close is idempotent and waits for
in-flight RPCs. A concurrent-close integration test covers twelve client loops.
`native_sanitizer_probe.py` instruments V-generated C, the shim and bundled Boehm C with Address
and UB sanitizers. It exercises real RPCs, callbacks, buffer handling and concurrent
close. Rust Core is not instrumented. ASAN does not track individual objects in
Boehm's custom heap, and GC leak detection is disabled. Linux amd64 sanitizer
builds give Boehm a supported high-memory address hint: an unhinted minimal V
allocation reproduces a GC-initialization ASAN failure under local amd64 emulation,
while the hinted build passes without suppressing checks. The full target must
still pass its sanitizer gate.
Panics outside the documented callback boundaries, Rust sanitizer builds and
broader native failure injection remain.

Crash tests cover committed Activity results and durable operations. They do not
establish exactly-once Activity side effects across mid-Activity crashes. External
side effects still require application-level idempotency.

## Argument boundaries

`WorkflowStart.arguments` and `UpdateRequest.arguments` accept `sdk.Arguments`,
a string for one value or `[]string` for an explicit list. Their default is zero
arguments. Activity, child, continue-as-new, signal/query and external-signal APIs
accept the same type. For example, `sdk.Arguments(['1', '"two"'])` sends two JSON
payloads with the default converter; the binary converter preserves each string's
bytes, including NUL and invalid UTF-8.

Workflow inputs/signals/Updates expose `event.arguments`. Activity handlers, queries,
validators and their interceptors receive `[]string` directly. Composed codecs
process the complete payload list, preserving count and boundaries. Multi-argument
inputs are not repacked into JSON arrays. Existing single-value workflow handlers
may read `event.data`; use `sdk.single_argument` when validating exact arity.

## Propagated Nexus workflow results

`WorkflowStart.propagated_nexus_serialization_context` supplies a result target for
a workflow backing a Nexus operation. `NexusContext.start_workflow` sets it from
the incoming operation. Inputs and interactions keep workflow scope; terminal
results, failures and cancellation details use the propagated Nexus scope.
`context.result_serialization_context()` exposes that distinction.

Client result decoding retains the server's propagated context across history
pages; continuations inherit it. `result_context_probe.py` checks independent wire
metadata, failures, cancellation and both continuation histories through offline
replay. These fields require a supporting server: stable 1.32.2 drops them. The
probe uses the checksum-pinned CLI `1.9.4-time-skipping` (embedded server 1.33.0,
revision `69c692e8773e`) from `locks/protocol-server.json`. The Activity result-context probe uses the same supporting server; other probes use 1.32.2.
Build verification includes both server executable hashes. Legacy completion paths
can lack propagated context; contextual codecs still need self-describing payloads
for those paths.

## Workflow close details and event identity

`context.cancel()` is fallible: use `context.cancel()!` or provide
`details: sdk.Arguments([...])`. Cancellation can return multiple converted detail
values. `client.terminate(handle, reason: ..., details: ...)!` also accepts an
optional first-execution-run-ID guard and event links.

`WorkflowResult.details` decodes cancellation/termination details;
`WorkflowResult.retry_state` retains failure/timeout retry state. Termination's
reason remains in `message`. The cancellation event exposes the caller's reason
through `event.message`. Signal events expose `sender_identity` (supplied by the
sender) and `originating_event_id` from history; both survive replay.

`lifecycle_probe.py` verifies scoped binary details, the termination chain guard,
server retry state, signal identity, worker restart and offline replay. All known
cancellation callers use the fallible API, including the packaged consumer.

## Standalone Nexus executions

`client.start_nexus_operation(NexusExecutionStart{...})!` starts an operation
without a calling workflow. Options include the endpoint/service/operation, input,
timeouts, plain Nexus headers, ID policies, metadata, callbacks and links.
`nexus_operation_started(handle, timeout)!` waits for its token or completion;
`nexus_operation_result(handle, timeout)!` returns the exact server status and
converted result/failure. Wait deadlines leave the operation running.

`describe_nexus_operation(handle)!` retains the raw response alongside decoded
input, outcome and last-attempt failure. Result decoding resolves the original
stored target, including when use-existing ignored a different requested operation.
The result deadline bounds both polling and that target lookup. Cancellation and
termination use `cancel_nexus_operation` and `terminate_nexus_operation` with
`NexusManagementOptions`.

`nexus_client_probe.py` verifies synchronous and workflow-backed asynchronous
operations, protected failures, conflict/use-existing identity, management statuses
and durable callback completion after both workers are killed. Native sanitizer
coverage includes these client paths.

## Standalone Activities

`client.start_activity(sdk.StandaloneActivityStart{...})!` starts an Activity
without a workflow. The returned `StandaloneActivityHandle` contains Activity/run
IDs and whether this call started a new execution. Start options include arguments,
headers, durations, retry/conflict/reuse policies, delay, priority, search attributes,
summary/details, callbacks and links.

`client.activity_result(handle, timeout)!` waits for a `StandaloneActivityResult`.
It retains result values, decoded failure, status and retry state. A wait deadline
leaves the Activity running. `client.describe_activity(handle)!` decodes inputs,
heartbeat details, headers, UI metadata and failures while retaining the full raw
response. Empty run IDs address the latest execution.

`cancel_activity`, `terminate_activity`, `pause_activity`, `unpause_activity` and
`reset_activity` accept `ActivityManagementOptions`, including an optional request
ID. Reset options include keeping it paused, discarding heartbeats and restoring
original options. A `StandaloneActivityHandle` is also an `ActivityIdentifier` for
external heartbeat/completion/failure/cancellation; task-token completion works too.
Handlers can inspect `ActivityInfo.is_standalone` and the standalone run ID.

`standalone_probe.py` runs without a workflow worker and exercises worker-loss
retry, contextual binary arguments, protected failures, management operations and
deferred completion by token and ID. Its verbose context fixture omits the summary
to stay within the server's summary payload limit.

## Dynamic handlers

`runtime.register_dynamic(factory)!` registers one workflow factory for types that
have no named registration. It accepts the same `WorkflowRegistrationOptions`,
including per-workflow versioning behavior. `activities.register_dynamic(handler)!`
and `runtime.register_dynamic_local_activity(handler)!` accept
`DynamicActivityHandler`, whose argument list is `[]RawValue`. They handle
unregistered remote/local/standalone Activity types after codec/storage decoding,
bypassing payload conversion. Duplicate dynamic registrations fail; named
registrations retain normal conversion and always take priority.

Dynamic workflows inspect the original name through `context.info().workflow_type`;
Activity handlers use `context.info().activity_type`. Dynamic Activity values are
owned and preserve unknown encodings. `context.decode_raw_value(value)!` explicitly
applies the configured input-scope payload converter without repeating codecs or
storage. `DataConverter.decode_raw_value(value)!` provides the same operation for
an explicitly scoped converter. The existing examples use this API when they
want converted strings. Interceptors, failures, retries, cancellation and replay
retain the shared Activity execution path. Replay retains dynamic registration
and completed results without redispatch.

`dynamic_probe.py` checks named-handler priority, actual type names, scoped payloads,
remote/local retry recovery, failure propagation and restart/offline replay without
redispatching completed Activities. Examples enable these registrations only with
`V_SDK_EXAMPLE_DYNAMIC_HANDLERS=true`.

## Workflow versioning behavior

Workflow registration accepts an optional behavior:

```v
runtime.register('Orders', new_orders, versioning_behavior: .pinned)!
runtime.register('Reports', new_reports, versioning_behavior: .auto_upgrade)!
```

An unspecified registration inherits the worker deployment default. Explicit
behaviors require an enabled versioned deployment; invalid configuration fails
before the worker connects. Each activation completion carries the workflow type's
behavior, including replay. `WorkflowInfo` exposes the current task's deployment
version, last SDK version and the target-deployment-change flag provided by Core.
These fields describe the activation, including historical values during replay.

`workflow_versioning_probe.py` tests mixed pinned/auto types under both worker
defaults, live deployment changes, worker SIGKILL/restart and offline replay.
Routing changes propagate asynchronously; the probe waits for server-observed
migration before asserting its destination.

## Serialization context

Every `PayloadConverter.to_payload/from_payload` and `PayloadCodec.encode/decode`
receives `sdk.SerializationContext` as its second argument. `DataConverter.with_context`
returns a bound copy. Workflow and Activity contexts also expose
`serialization_context()`. Workflow scope uses namespace and workflow ID, stable
across run chains. Activity scope adds Activity ID and locality. Nexus scope uses
endpoint, service and operation. Type and task-queue fields are informational and
can be absent when addressing an existing execution.

Calls bind the target actor: child inputs/results use the child's namespace and
ID; external signals use the target workflow; remote/local Activity payloads use
Activity scope. Captured failures retain their protocol kind, execution identity,
retry state and unknown fields. When crossing actor scopes, known detail/common
attribute payloads are decoded in the source scope and encoded in the target
scope. Unavailable decoders make this conversion fail explicitly. Byte-exact
rethrow applies when scope and conversion output remain the same.

Deferred `ActivityTaskToken` records carry optional serialization context; persist
the whole record for later completion. `ActivityExecutionIdentifier` also accepts
an explicit context. Opaque task tokens alone cannot reveal their target scope.
Workflow-backed asynchronous Nexus results may originate in workflow scope;
codecs supporting this case must carry enough information to decode those results.

`client.query` returns `sdk.QueryError` when Temporal supplies a structured query
failure. It exposes the decoded `failure` and original `rpc_failure`, including
status details. A query exception leaves the workflow running. Older server/SDK
responses without a structured failure retain their `RpcError` representation.

`context_probe.py` exercises converter and codec scope validation, protected common
attributes, cross-namespace child failures and signals, Nexus, deferred completion
after worker loss, and offline replay of both continuation runs and child histories.
The example codecs record scope and frame bytes; they provide no encryption.

## Interceptors, telemetry and testing

Workflows can set replay-reconstructed current details with
`context.set_current_details(...)!`. Temporal UI reads them through the reserved
metadata query; `client.workflow_metadata(handle)!` decodes that plain JSON
response independently of application codecs. `WorkflowInfo` also exposes history
length/size and Core's continue-as-new recommendation.
The response includes workflow type and sorted query/signal/Update definitions.
Implement `sdk.DescribedWorkflow.interaction_definitions()` to provide handler
names/descriptions; otherwise event-based signals and queries are reported as
dynamic handlers. Registered Update names are included automatically. The
`WorkflowMetadata` JSON representation uses Temporal's standard camelCase names.

`context.memo()!` and `context.search_attributes()!` read initialization plus
subsequent local upserts/deletions, including decisions in the current activation.
Search attributes are typed, sorted by name and independent of application codecs.
`WorkflowInfo` exposes initial timeouts, parent/root execution, retry policy,
priority, start/expiration timestamps and original run identity. Protocol duration
and timestamp fields retain their original precision and range.
`context.last_completion_result()!` and `context.continued_failure()` expose prior
run data supplied by Temporal.

`load_probe.py` defaults to three batches of 64 workflows, exceeding four cached
workflows and two Activity slots. It checks cancellation, restart under load,
sampled histories/offline replay, graceful drain and a configurable RSS ceiling.
Use `--workflows`, `--batches` and `--memory-limit-mib` for longer runs. Passing this
short gate does not replace a production-duration soak.

`WorkerOptions.custom_tuner` accepts separate `sdk.SlotSupplier` implementations
for workflows, remote/local Activities and Nexus. Unspecified kinds keep their
configured fixed/resource supplier. Implement cancellation-aware `reserve`,
nonblocking `try_reserve`, `mark_used`, `release` and optional-count
`available_slots`. Permit IDs must be nonzero and unique among live reservations.
Reservations run on V threads; lifecycle notifications run on a V dispatcher.
Suppliers shared across threads/workers must protect mutable state, and blocked
reservations must observe `SlotReservationCancellation.is_canceled()` so shutdown
can drain. Handler concurrency remains bounded by the worker's configured limits.
Core callbacks copy context into C-owned records and never invoke V on Rust
threads. Worker close frees Core, drains/joins dispatchers, then frees bridge
storage. `example_tuning/` demonstrates a mutable capacity policy; `tuner_probe.py`
tests all four slot kinds, capacity changes, cancellation races and replay.

`new_rpc_cancellation()` creates an explicitly owned cancellation token. Supply it
in `ClientOptions.cancellation` or `ClientRpcRequest.cancellation`, then call
`cancel()` to release in-flight waits. `close()` cancels and drains shared users
before freeing the token. Closing/cancelling a token does not cancel the workflow
or undo a server-accepted request. `call_rpc` exposes per-call deadlines, retries,
metadata and cancellation alongside the generated typed methods.

`ClientOptions.interceptors` wraps every unary RPC, including result long polls
and Update waits. Requests carry method, service, bytes, deadline, retry policy and
per-call ASCII/binary metadata. `rpc_retry` enables Core's retry policy; deadlines
still bound the call. `RpcError.status_details()` decodes server error details,
including typed duplicate-workflow information. Update RPC errors retain both
resumable Update identity and an optional RPC failure.

`TransportOptions.retry` configures initial/maximum backoff, jitter, multiplier,
elapsed-time budget and attempt limit for retry-enabled unary calls. The defaults
match pinned Core. `max_retries` counts total attempts: 1 disables retries and 0
removes that limit. A zero `max_elapsed_time` removes the elapsed retry limit;
the RPC deadline and cancellation still bound the whole call. Core worker polls
retain their separate retry policy. Durations use millisecond resolution.
`grpc_compression: .gzip` enables compressed requests and response decoding;
the default is `.none`. `dns_load_balancing` enables Core's periodic DNS resolver,
with a refresh interval of at least one second. Combining it with a CONNECT proxy
is rejected because Core would ignore the DNS setting. Single-host resolution is
tested; multiple-address redistribution and DNS changes still need validation.
`client_name` and `client_version` set printable ASCII SDK headers. Experimental
`payloads_warn_size` and `memo_warn_size` log oversized outbound fields without
rejecting them; zero disables the warning. These do not change server limits.
`scripts/transport_probe.py` verifies gRPC DATA compression, retry failures,
overall deadlines/cancellation and interoperability with a real Temporal server.

Metrics exporters use `TelemetryOptions.prometheus: PrometheusMetrics{...}` or
`opentelemetry: OpenTelemetryMetrics{...}`. Prometheus accepts counter/unit suffix
switches, seconds or milliseconds for durations, and per-metric histogram bucket
overrides. OTLP also accepts cumulative/delta temporality and explicit or base-2
exponential histograms. `ExponentialHistogramOptions` sets maximum bucket count,
scale and a required nonempty name suffix. Explicit bucket overrides cannot be
combined with exponential aggregation. Buckets must be finite and strictly
increasing; invalid settings are rejected before constructing a runtime.
`attach_service_name` controls the default service-name attribute. Existing
metrics environment variables select the same exporter defaults as before.
`scripts/metrics_options_probe.py` checks real Prometheus output and independently
parses OTLP histograms, units and temporality, including delta count resets.

`client.metric_meter()!` creates an owned user metric meter. Close it explicitly;
it keeps its runtime alive even after the client closes. Workflow, Activity and
Nexus contexts expose borrowed `metric_meter()` values that the SDK closes at
scope end. Workflow scope ends on eviction; handler scopes end after execution.
Do not close a borrowed context meter. `with_attributes(...)` binds additional
string, boolean, signed-integer or floating-point attributes; record attributes
override matching bound attributes. Context meters include namespace/task-queue
and workflow/Activity or Nexus service/operation labels.

`instrument(MetricOptions{ name: ..., kind: ... })!` supports integer counters,
integer/float/duration histograms, integer/float gauges and signed up/down
counters. Use `record_integer`, `record_float`, `record_duration` or `record_signed`
with an attribute map. Instruments are cached within their scope and owned by its
meter. User metric names do not receive the Core metric prefix. Duration records
have millisecond resolution. Invalid names, nonfinite values, type mismatches,
negative durations and closed handles return errors. With no exporter configured,
valid records are no-ops. Workflow records are suppressed during replay, including
cached instruments that become active again when live execution resumes.
`scripts/user_metrics_probe.py` verifies exported values, concurrent close,
runtime retention and restart/offline replay; its V/C paths also run under the
native sanitizer gate. Custom metric exporters are described in the exporter API
below.

`load_client_profile(ClientProfileLoadOptions{...})!` uses Core's TOML/environment
profile loader. It supports explicit file/inline data, profile selection, strict
unknown-key checking, environment overrides and disabling either source.
An explicit `environment` map supplies the complete environment snapshot; an
empty map isolates loading from process variables. `load_client_config(...)!`
loads the file's complete profile collection. Profile-only flags apply to the
single-profile loader.

Call `profile.client_options()!`, then customize the returned `ClientOptions`
before connecting. Addresses without a scheme infer HTTP or HTTPS from TLS.
The local SDK/worker/CLI default is now port `7233`. API-key presence enables TLS
unless the profile explicitly disables it. CA/client credentials support files
and inline bytes; empty sources and incomplete client key pairs are rejected.
Codec endpoint/auth metadata is returned for explicit converter setup. The pinned
Core TLS bridge cannot disable hostname verification; that profile flag is
preserved and explicitly rejected when applying TLS options. Errors redact parser
input by default; `ConfigurationError.diagnostic()` exposes the underlying text
on explicit request. The profile probe checks actual TLS/mTLS, client operations,
restart and offline replay, using only disposable configurations and certificates.

`WorkerOptions` accepts ordered workflow, command, Activity, query and Update
validator interceptor chains. The first registered interceptor enters first and
exits last. Each calls `next.execute(...)` to delegate. Workflow and command
interceptors also run during replay and must be deterministic. Command
interceptors see outbound workflow decisions and can propagate payload headers;
Activity interceptors cover local and remote handlers. Queries and validators
receive read-only information. Register the same workflow/command chains in
`ReplayOptions`. Activity options expose encoded headers; workflow events carry
raw payload headers and the data converter decodes them. Signal/query/Update
options can provide headers. Do not retain a Next object or its context past a handler call.
Interceptor implementations shared across concurrent calls must be thread-safe.
The fixture implementations are in `example_interceptors/`.

`run_worker(options, runtime, activities)!` runs workflow and remote/local
Activity polling on one Core worker. `run_workflows` and `run_activities` support
separate deployments. Combined workers can receive eager Activities with workflow
task completions. `max_eager_activity_reservations` bounds reservations per task;
Activity options can disable eager execution. Core retains the slot limits.
Optional `PollerAutoscaling` settings configure workflow, Activity and Nexus pollers
independently; fixed poller counts remain the default.

`Logger`, `ConsoleLogger` and `JsonLogger` provide worker/handler logs. Workflow
context logging suppresses replay duplicates. `TelemetryOptions` configures Core
logs, Prometheus or OTLP gRPC/HTTP metrics and global tags. Examples accept
`TEMPORAL_LOG_FORMAT=json`, `TEMPORAL_CORE_LOG_FILTER`,
`TEMPORAL_METRICS_ADDRESS`, `TEMPORAL_OTEL_METRICS_URL` and
`TEMPORAL_OTEL_PROTOCOL`. SDK/Core JSON records share an output lock. Exporters are configured per runtime; give independently
created runtimes separate Prometheus ports.

`start_test_environment` owns a Core-managed dev or time-skipping server and
client. Supply the pinned executable path; bootstrap installs both binaries.
Concurrent result waits balance automatic time-skipping locks. `WorkerControl`
shuts down a worker group programmatically. The test environment owns the server
and client, while application code owns worker shutdown.

The installed module includes generated schemas, the hardened protobuf runtime,
Core headers and native libraries. `scripts/dependency_probe.py` compiles a
separate application using plain `v`, without `VMODULES` or custom flags, and
checks execution and restart replay against Temporal. Maintainers prepare native
builds with `scripts/build_native.sh` / `Dockerfile.native` and record them with
`scripts/prepare_native_release.py`; consumers use `v install`.

## Pinned inputs

| Input | Revision |
| --- | --- |
| Temporal Rust SDK | `67174ea96e8a06bc33137036e73cfaf508aa9dd7` |
| V SDK compiler | `213eb9b920b91bcbb2d24c5b98a48d3f49137a42` |
| V formatter | `0.5.2`, source and bootstrap checksums in `locks/v-formatter.json` |
| V bootstrap C | `a5a8c9313a3e5ff07061dce993598ee2b326b4ea` |
| `we-be/protobuf.v` | `64811280130b2b4bda45eba707512412ace968ad` |
| Temporal CLI / dev server | `1.10.0` / `1.32.2` |
| Time-skipping test server | `1.40.0`, archive/binary checksums in `locks/test-server.json` |

Twelve copied-source Rust patches are applied during builds. The original bridge fixes include: binary metadata
avoids unchecked UTF-8 construction, OTLP preserves the selected protocol, and
RPC cancellation preserves gRPC CANCELLED status and bounds the entire retry
future with the RPC deadline. The query failure patch retains
structured failures for both normal and legacy query responses. Shared-runtime
borrows permit worker construction while blocking RPCs are in flight and avoid
unnecessary mutable runtime references during buffer/metric operations. Profile
serialization retains the hostname-verification flag instead of discarding it.
Further patches implement custom gRPC transport, eager standalone Activity slots,
custom metrics, legacy eager routing, ordered duplicate/binary per-call metadata,
and recorded SideEffect markers with replay lookahead. Reference checkouts are
not edited.

Keep the unstable Core C bridge header, library and schemas on the same revision.

## Propagated Nexus Activity results

`StandaloneActivityStart.propagated_nexus_serialization_context` binds Activity
results, failures and heartbeat checkpoints to the Nexus operation. Activity
inputs and headers retain Activity scope. `ActivityContext.result_serialization_context()`
exposes the output scope. Deferred completion tokens persist both contexts;
completion by standalone handle resolves the original result context from the
server. The live probe verifies retry after worker loss, deferred completion,
protected nested failures and server-generated heartbeat-timeout details.

## Atomic signal-with-start and query rejection

`client.signal_with_start(WorkflowStart{...}, signal_name, arguments)!` returns the
execution handle, whether it started a new execution, and server link/chain
metadata. Start and signal payloads use the workflow's serialization context.
A repeated explicit request ID deduplicates the signal. The default conflict
policy follows each RPC's server default (start fails on an existing execution;
signal-with-start uses it). Signal-with-start rejects the unsupported FAIL policy
and fields absent from its protocol instead of silently dropping them.

`client.query(handle, name, arguments, reject_condition: ...)!` supports the
server's open/clean-completion policies. A rejection returns `QueryRejectedError`
with the workflow handle and actual execution status. Query handler failures
continue to return decoded `QueryError`. Concurrent starts, deduplication, allowed
and rejected closed queries, worker restart and offline replay have live coverage.

## Nondeterminism policy

`WorkerOptions.nondeterminism_as_workflow_fail` opts all workflow types into
terminal execution failure for incompatible replay. Alternatively, list only
selected names in `nondeterminism_as_workflow_fail_for_types`. The default fails
the workflow task and leaves the execution available for a repaired deployment.
Live tests change a recorded timer into an Activity command, verify all three
policies and isolation of healthy workflows, then repair and replay blocked runs.

## Nexus links

`nexus_link_from_common` and `common_link_from_nexus` convert workflow events
(including request-ID references), workflow resources, standalone Activities,
standalone Nexus operations and callbacks. Escaped path segments, workflow reasons
and ordered repeated callback component paths are preserved. Malformed recognized
links fail conversion. Workflow-backed handlers propagate all recognized link
variants to starts and durable completion callbacks. Batch-job links have no Nexus
URL representation and return an explicit error.

## Activity-backed Nexus operations

`context.start_activity(client, StandaloneActivityStart{...})!` attaches the Nexus
request identity, links, propagated result context and a durable completion
callback before acknowledging an asynchronous start. `context.cancel_activity`
validates the operation token and cancels its exact Activity run. One invocation
may start only one asynchronous Activity or workflow. Checkpoint retry, external
completion, protected failures and cancellation survive worker loss; a workflow
caller also has restart/offline replay coverage.

This requires a server supporting propagated Activity result contexts and enabled
Activity callbacks. The disposable probe uses pinned server 1.33.0 with
`activity.enableCallbacks=true`; the SDK does not change server configuration.

## Per-Update result contexts and metadata

`UpdateRequest` supports execution-chain guards, request identity, links, completion
callbacks and propagated Nexus result context. Inputs and headers use workflow
scope; successful results, handler failures and validation rejections use the
individual Update's output scope. `context.update_result_serialization_context(id)!`
exposes it while an Update is pending. `UpdateHandle` retains response links and
its output scope; detached result polling uses the server-returned context.
Concurrent Updates with different scopes, worker loss, duplicate requests with a
different requested scope, protected binary failure details and offline replay
are tested against the supporting pinned server. Workflow output retains its own
scope. Update-backed Nexus helpers use these request and output-context fields.

## Nexus workflow interactions

`context.start_update(client, workflow, UpdateRequest{...})!` attaches durable
callbacks before acknowledging acceptance. It returns an async token for pending
Updates or a converted sync result for an already-completed Update. Protected
validation and execution failures fail the operation. The supporting server must
enable `history.enableUpdateCallbacks`; Updates have no general cancellation RPC,
so cancellation handlers remain an application policy.

`context.query_workflow(...)!` returns a synchronous Nexus result and the server's
workflow link. Query failures retain their structured cause while moving from
workflow to Nexus scope; query rejections fail only that operation.

`context.signal_workflow(...)!` and `context.signal_with_start(...)!` propagate
links and use the Nexus request ID to deduplicate retries. Only one signal may be
sent by an invocation, including a mix of those methods. Ordinary client signals
can also supply `request_id` and `links` through `MessageOptions`. Real handler
retries prove that an acknowledged signal is delivered once.

## Shared clients and eager workflow starts

Set `WorkerOptions.client` to a connected client to share its native transport and
runtime with one or more workers. The worker namespace must match the client;
worker connection/telemetry settings apply when creating an independent connection.
Each worker holds a native lifetime reference, so closing the public client rejects
new client calls/workers while already-running workers can finish and drain.

`WorkflowStart.request_eager_execution` asks Core to reserve a matching worker slot
and dispatch the workflow task returned with the start response. Without a matching
worker/available reservation, Core uses ordinary polling. Signal-with-start and
Update-with-start reject this unsupported flag. Live tests inspect actual eager
response presence, route two queues, close a shared client, drain workers
independently, and reconstruct completed histories in replacement workers/offline.

## Request identity generation

Automatic client request and Update IDs use UUIDv4 values from the operating
system random source. Generation failures propagate before the RPC; explicit IDs
are preserved for caller-controlled deduplication. Concurrent callers and separate
V processes are tested through independently observed committed signals and replay.
Workflow UUIDs continue to use their recorded deterministic seed.

## External payload storage

`DataConverter.external_storage` accepts a manager from `new_external_storage`.
Register thread-safe `StorageDriver` implementations with stable names, ordered
batch `store`/`retrieve` methods and a driver type for observability. The default
policy stores encoded payloads of at least 256 KiB in the first driver; an optional
`StorageSelector` can choose a registered driver or keep a payload inline. Store
and retrieval concurrency limits apply across calls sharing that manager.

References use Temporal's `json/protobuf` `ExternalStorageReference` format, with
original sizes in `external_payloads`. The reference is visible in history, so
claim data must contain object identifiers rather than credentials. Storage runs
after application codecs on send, and before decoding on receive. Search attributes
remain inline. Client unary RPCs, workflow commands, Activity completions and
heartbeats, local Activity result markers, Nexus completions and offline replay
share the same schema-aware traversal of the pinned Rust protobuf graph. Raw RPC
requests can set `raw_payloads` to preserve references for history export.

Storage runs outside workflow execution. Replayed commands do not upload again;
query replies do, including legacy queries. A query upload error fails that query.
Other workflow upload/retrieval failures report Temporal's external-storage WFT
failure cause; Activity storage failures report its retryable Activity cause.
Storage I/O runs outside polling coordinators. Drivers must honor the supplied
deadline and implement their own transport cancellation/timeouts; synchronous V
calls cannot be forcibly interrupted. The manager bounds permit waits and checks
returned calls against that deadline. Retention, access policy, object integrity,
orphan cleanup and durable acknowledgement belong to the driver.

`example_storage` is a disposable filesystem integration fixture, enabled with
`V_SDK_EXAMPLE_STORAGE_DIR`. It is not a production storage backend. The SDK does
not currently bundle a cloud storage driver. Linux validation of the new storage
implementation and broader storage transport fault coverage remain required.

## Recorded workflow clock

`WorkflowContext.now()` returns the recorded activation time with nanosecond
precision. Repeated reads within an activation return the same value; replay
reconstructs it from history. `WorkflowInfo.time_millis` and workflow log timestamps
are derived from that same clock. Use this API for workflow decisions; host wall
clocks are not deterministic during replay.

Storage ownership follows the receiving execution for child inputs/results,
external signals and continue-as-new. The operation context keeps the originating
run's identity for ordinary commands and uses an empty run ID when the receiving
run is not known yet. Configured driver types are forwarded to Core's worker
heartbeat metadata. Reference readers accept the standard protobuf JSON field
names and lower camel case names.

## Configuration plugins and worker definitions

`ClientOptions.plugins`, `WorkerOptions.plugins` and `ReplayOptions.plugins`
accept values implementing `Plugin.name()`. Optional `ConnectionConfigurator`,
`ClientConfigurator`, `WorkerConfigurator` and `ReplayConfigurator` interfaces
supply configuration hooks; `VersionedPlugin` supplies a reported version.
Registrations are captured before hooks run. Hook changes to the plugin list do
not recurse. Client plugins propagate their worker hooks to workers using that
client; explicit worker plugins run afterward. Errors retain plugin name, target
phase and the original cause. Duplicate worker plugin names warn and still apply.

`PluginBundle` installs converters, ordered interceptors and workflow/Activity/
Nexus registrations. Collection values append and a supplied converter replaces
its current value. Worker/replay setup uses isolated registration copies. Worker
creation takes a read-only `&Runtime`; callers pass `runtime` rather than
`mut runtime`. The same definition registry can create replacement workers without
retaining a previous worker's cache or accumulating plugin registrations.

`run_worker(options, runtime, activities, nexus: services)` also polls Nexus on the
same Core worker when services are registered. A plugin bundle can install those
services. Workflow-only, Activity-only and Nexus-only entrypoints poll their
respective task kinds. Shutdown drains every enabled handler loop before freeing
Core or the shared transport.

`TelemetryOptions.worker_heartbeat_interval` enables Core's worker heartbeats
(zero disables them). `runtime_info` describes the hosting runtimes; its default
is native with an unknown version. `disable_environment_info` suppresses the
runtime/platform/hosting environment section. Worker plugin names/versions and
storage-driver types reach Temporal's worker APIs when heartbeats are enabled.
`WorkerOptions.disable_payload_error_limit` forwards Core's option to let the
server enforce payload error limits.

### Custom gRPC transports

`TransportOptions.custom_grpc` accepts `CustomGrpcTransport` with a V
`GrpcTransport.call(context, request)!GrpcReply` implementation. Requests contain
service/method names, protobuf message bytes and HTTP header values; response
headers contain user metadata only. Binary request headers use HTTP base64
encoding. `corebridge.GrpcRequestSnapshot` and `GrpcReply` are the boundary types.

Core callbacks enqueue C-owned requests; application handlers run on V threads.
Separate bounded control, worker-poll and client-wait pools prevent long polls
from using all control capacity. Pending count/protobuf-byte limits return
RESOURCE_EXHAUSTED. Handlers must cooperate with `context.cancelled()` and finish;
the SDK cannot forcibly interrupt arbitrary V code. Closing the final Core owner
drains accepted calls. Shared workers retain the transport after client close.
Thrown handler errors produce a generic INTERNAL response; explicit reply status,
message and serialized `google.rpc.Status` details are retained.

A handler can use `context.forward(independent_client, request, timeout_millis)!`
to forward over an ordinary Core connection with cancellation propagation. This
helper rejects duplicate/combined metadata it cannot preserve. It does not expose
response metadata because the Core unary RPC bridge currently returns only body
and status details. The forwarding client must use a different transport. This helper re-enters
Core client middleware: its own worker registry can clear eager-start flags.
Use an independent HTTP/2 implementation when managed eager-start behavior must
be preserved exactly.
Custom transports own TLS, proxy, DNS, keep-alive and compression; conflicting
Core channel settings are rejected. See `grpc_demo/` and
`scripts/grpc_transport_probe.py` for the live restart/replay fixture.


`StandaloneActivityStart.request_eager_execution` opts into eager standalone
Activity execution. Core reserves a slot on a registered worker for the same
client, namespace and task queue before asking the server for an inline task.
It dispatches accepted tasks into the ordinary Activity handler path; declined
starts and RPC failures release the reservation. Missing/shut-down/full workers,
worker/task-queue throttling and deployment-versioned workers use normal polling.
The standalone request has no eager worker deployment routing field, so this
implementation conservatively excludes deployment-versioned workers.

Actual inline standalone dispatch is tested against the pinned supporting 1.33
protocol server with `activity.enableEagerStart=true`. The embedded 1.32 server
ignores this newer request field; that fallback is tested separately. The live
probe fills available slots using an explicit release gate and verifies heartbeat
recovery after killing the inline worker/client process.


### Custom metric exporters

`TelemetryOptions.custom_metrics` accepts `CustomMetricExporter` with a V
`MetricExporter.export(context, samples)!` implementation and an optional
`MetricExporterShutdown.shutdown(context)!` hook. It is exclusive with Prometheus
and OpenTelemetry on a runtime. Export receives typed raw Core/user records,
merged global/instrument attributes, u64 counters, signed up/down values, floats,
and `MetricDurationValue.milliseconds`. Histograms are raw observations; the
exporter chooses aggregation and delivery.

Native callbacks copy/retain immutable data into a bounded queue without running
V code or waiting for export I/O. The V dispatcher batches accepted points;
queue count and estimated byte limits drop excess records. Pending includes the
batch in flight. `client.metric_export_stats()` exposes accepted/exported/failed/
dropped points, allocation/export failures and completion. Errors returned by an
exporter remain private and do not fail workflow or Activity execution.

Handlers must return cooperatively and can call `context.check_cancelled()!` to
honor the configured export timeout; arbitrary V code cannot be interrupted.
Shared workers and explicit metric-meter leases retain their exporter after
client close. The final owner drains accepted points and invokes shutdown once.
`metric_export_demo/` and `scripts/metric_export_probe.py` exercise ownership,
failures and workflow restart/replay. Focused live Core tests passed, including failed startup cleanup, independent
runtimes, queue pressure, cooperative timeouts, self-close and restart/replay.
Full aggregate and current Linux verification remain pending.


### Typed deployment routing and overrides

`WorkflowStart.versioning_override` and `ChildOptions.versioning_override` use
`sdk.VersioningOverride`: `PinnedVersioningOverride`, `AutoUpgradeVersioningOverride`
and `OneTimeVersioningOverride`. Pinned/one-time inputs contain a
`WorkerDeploymentVersion { deployment_name, build_id }`; these SDK types are the
high-level API. Marshaling uses the canonical protobuf oneof fields.

`client.set_versioning_override(handle, override)!` and
`clear_versioning_override(handle)!` update only the override, preserving priority
and other execution options. `set_current_deployment(update)!` and
`set_ramping_deployment(update, percentage)!` return conflict tokens for guarded
subsequent updates. `describe_worker_deployment(name)!` and
`describe_worker_deployment_version(version)!` expose actual service snapshots.
A routing update's empty build ID selects unversioned workers. Invalid names and
non-finite/out-of-range ramp percentages fail before RPC interception.

`scripts/deployment_probe.py` tests 0/100 ramps, stale tokens, unknown targets,
pinned/auto/one-time routing, priority preservation, explicit cross-version child
routing, inherited continue-as-new pinning, SIGKILL/replacement and offline replay
against the pinned 1.32 and supporting 1.33 servers. Focused tests passed; full
aggregate and current Linux evidence for this candidate remain pending.


Legacy worker build-ID compatibility is exposed through
`update_build_id_compatibility(queue, update)!`, `build_id_compatibility(queue, max_sets)!`
and `build_id_reachability(options)!`. Updates accept `NewBuildIdSet`,
`CompatibleBuildId`, `PromoteBuildIdSet`, `PromoteBuildId` and `MergeBuildIdSets`.
The namespace must enable the server's legacy versioning APIs. The disposable
test fixture enables these APIs only for its own in-memory servers. Legacy
workers decline eager workflow reservations because eager-start requests cannot
identify a legacy compatibility set; server matching handles those starts.
The regression probe first proves ordinary default-C routing before requesting
eager execution with an incompatible local A worker.


The legacy versioning candidate passed all 62 host reports, including instrumented
routing, worker-loss and replay coverage. The current schedule/batch candidate
still requires its expanded aggregate and Linux validation.

`create_schedule(id, options)!` converts a `ScheduleDefinition` and its
`ScheduleWorkflow` action through the configured payload converter/codecs. Calendar,
interval, cron, exclusions, timezone, jitter and state/policy options retain their
generated schema types. `encode_schedule(definition)!` builds a replacement action.
`describe_schedule(id)!` returns the service snapshot; `update_schedule(id, update)!`
accepts that encoded schedule unchanged so editing state preserves payloads and
unknown fields. Omitted memo/search attributes preserve existing values; explicit
empty values request clearing. `ScheduleUpdate.conflict_token` is forwarded to the
service: the tested 1.32 V1 scheduler accepts stale tokens, while the tested 1.33
CHASM scheduler rejects them. Server configuration controls that enforcement.

`list_schedules(options)!` returns pagination tokens. `pause_schedule`,
`unpause_schedule`, `trigger_schedule`, `backfill_schedule`, `schedule_matching_times`,
`delete_schedule` and typed `patch_schedule` cover lifecycle operations. Triggers
accept an optional scheduled timestamp. Manual triggers/backfills can run while
a schedule is paused or has no remaining automatic actions.

Scheduled payload conversion uses the configured base workflow ID, matching the
upstream client convention. Tested servers append a timestamp to the actual ID
even when retaining `keep_original_workflow_id` in their response. Exact-ID-bound
codecs therefore need a server configuration verified to honor stable IDs, or a
codec designed for scheduled execution IDs. The SDK retains actual workflow scope
validation; the successful schedule matrix uses ordinary and composed payload codecs.

`start_batch(options)!` requires an explicit operation and exactly one visibility
query or target list; it returns a `BatchHandle` with an OS-random job ID unless
one was supplied. All pinned operation variants use the generated operation
oneof. `batch_signal`, `batch_termination` and `batch_cancellation` create common
operations with identity and converted payloads. Batch payloads use the unbound
client converter because one request targets multiple executions. `describe_batch`,
`wait_batch`, `list_batches` and `stop_batch` manage jobs. A wait deadline leaves
the job running; stopping is an explicit operation.

Batch signal headers and termination details require a supporting server: independent
histories and the server implementations confirm that 1.32.2 omits these fields,
while the pinned 1.33 server forwards them. Tests record that expectation in workflow
input so restarting/replaying does not depend on process environment.

The 1.32.2 batcher also ignores the per-request rate field; its namespace/server
rate limit controls execution. The supporting 1.33 implementation uses the request
rate. Stop/deadline tests apply a one-operation/second cap to the disposable server
so they verify a running job independently of that version difference.


The schedule/batch candidate passed all 64 host reports, including instrumented
two-server matrices. Linux validation of this source remains outstanding.

`ProtobufPayloadConverter[T]{message_type: 'your.package.Message'}` validates
already-serialized protobuf bytes using the concrete generated `T.decode` and
uses `binary/protobuf` metadata. Pass `message.encode().bytestr()` to existing
argument APIs and decode returned bytes with the generated message decoder. The
message type name is optional, matching Rust's unnamed `ProstSerializable` format;
a conflicting declared name is rejected when a name was configured. Unknown wire
bytes survive conversion. Configure matching converters on communicating actors.
All values passed to this converter must use that message's protobuf encoding;
Binary and JSON protobuf conversion are supported. Per-value arguments are described below.

The pinned protobuf runtime/generator includes verified patches for UTF-8, known
wire types, 64-bit varint overflow, message/group recursion bounds, matching group
tags and noncanonical tag interoperability. Decode nesting is bounded consistently
with the pinned Rust parser. `scripts/protobuf_probe.py` compares a deterministic
wire corpus with that actual Rust implementation and exercises the typed converter
against Temporal; instrumented validation uses the same worker binary for wire
vectors and live execution. Dependency patch application checks exact file hashes.


Use `TypedArguments{values: [...]}` to mix logical JSON strings, `BinaryValue`,
`NullValue` and `protobuf_value(message, message_type)!` in one call. Ordinary
string arguments still use the configured payload converter. Payload converters receive each typed value before the complete array passes
through the configured codecs and external storage. The default converter reads all four
encodings; a custom payload converter controls its own decoding behavior.

`Event.raw_arguments`, `ActivityContext.input_values()` and the query/Update
validation interceptor's `next.input_values()` expose codec-decoded `RawValue`
snapshots. `kind()`, `encoding()`, `message_type()`, `data()` and `as_value()`
distinguish null from binary bytes containing `null`. `to_payload()` and `data()`
return independent copies. Workflows must make deterministic decisions from these
values just as they do from `Event.arguments`.

`context.complete(...)` and `client.complete_activity(...)` accept `PayloadValue`.
Workflow, Update and Nexus result records expose optional `raw_value`; standalone
Activity results expose `raw_values`. `client.query_response(...)` returns both
logical data and its raw value; `client.query(...)` returns logical data.
Completion events expose optional `raw_result`. These views do not invoke codecs
again. Activity handlers, Activity/query interceptors and workflow query methods return
`!sdk.PayloadValue`; ordinary strings coerce into that sum type. Return a
`BinaryValue`, `NullValue` or `ProtobufValue` for an explicit wire encoding.
`context.complete_update(...)` accepts the same value type. Both remote and local
Activity dispatch use this return contract.

Remote Activity heartbeats have no timeout by default. Set `heartbeat_timeout`
when missed heartbeats should trigger a retry; deferred completion needs external
heartbeats when a heartbeat timeout is configured.


`ProtobufJsonPayloadConverter[T]` validates and canonicalizes a concrete generated
message as `json/protobuf`. Configure the matching converter on clients and
workers, or mix `protobuf_json_value(message, message_type)!` with other values
under the default converter. Binary protobuf retains unknown wire fields; JSON
protobuf omits unknown binary fields and rejects unknown JSON fields.

`json_value(value)!` creates explicit JSON from a V value. `JsonValue{data: ...}`
accepts already-serialized JSON. The built-in converters retain `json/plain` for these explicit values while
allowing configured preferences for ordinary strings. Custom converters receive
the complete typed value and control their own encoding policy. Codec-decoded
raw JSON views convert back to an explicit JSON value through `as_value()`.

The generated Temporal schemas expose `json()!` and `from_json(...)!`. The
maintained runtime/generator patch preserves numeric token spelling until the
schema selects a conversion, preventing loss at 64-bit boundaries. It rejects
duplicate keys and field aliases, invalid UTF-8, unknown fields and invalid
well-known types. The required JSON probe compares 592 cases with checked-in
expectations generated by Go `protojson` v1.36.6 over the exact pinned Rust
Temporal descriptors. Its oracle source and module hashes are recorded with the
corpus; normal and instrumented live tests also inspect server history.
This is bounded interoperability coverage, not full protobuf conformance.

A batch wait reports its overall deadline after a transport error when its wait
budget has elapsed. Errors arriving while time remains still propagate. The
Linux regression covers forty repeated one-millisecond waits and missing-job
errors against both server versions with codecs enabled and disabled.


Payload conversion now uses typed values in both directions:

```v
pub interface PayloadConverter {
    to_payload(value PayloadValue, context SerializationContext) !temporal.TemporalApiCommonV1_Payload
    from_payload(value temporal.TemporalApiCommonV1_Payload, context SerializationContext) !PayloadValue
}
```

This replaces the previous string-only converter contract. Custom converters
must return the decoded semantic type; `value.logical_data()` provides the
logical string view used by existing workflow/Activity handlers. All built-in
converters compose with the standard encodings. Concrete protobuf converters
retain a typed value's message name when no outgoing name was configured.
Codecs still run once on each complete array. `RawValue` is also a `PayloadValue`:
explicit raw forwarding preserves payload metadata and unknown fields.

`ApplicationError.details` and external Activity heartbeat/failure/cancellation
arguments accept `Arguments`, including `TypedArguments`. Captured `Failure`
records expose `detail_values` and `raw_details` alongside logical `details`.
Rethrowing in the same scope retains exact protocol bytes; crossing scopes uses
semantic values through the target converter and codecs. Custom failure
converters must preserve the typed details when retaining a captured failure.

Workflow/Update/Nexus results expose an optional typed `value`; query responses
expose a typed `value`. Cancellation and termination results expose
`detail_values`; standalone Activity results expose `typed_values`. Workflow
activation events expose `typed_arguments` and optional `typed_result`.
Activities expose `typed_input_values()!`, and Activity information includes
`ActivityContext.heartbeat_values()!` and raw checkpoint details. Query and Update validation
interceptors can inspect `next.typed_input_values()!` without rerunning conversion.

Nexus input/return values accept `PayloadValue`; a handler's `context.typed_input()!`
returns its decoded value and `input_payload()` exposes its codec-decoded raw
payload. Workflow and standalone Nexus calls preserve the same types. The Nexus
fixture uses a whole-payload envelope when composing framing codecs: Nexus's HTTP
serializer interprets standard encoding metadata, including `binary/null`, so a
codec that changes those contents must also describe its transformed encoding.
See the [pinned server serializer](https://github.com/temporalio/temporal/blob/v1.32.2/common/nexus/payload_serializer.go).

Payload codecs may change counts for argument collections. The SDK applies
each codec to the whole array, in registration order for encoding and reverse
order for decoding. `DataConverter.encode_value` and `decode_value` require one
encoded or decoded value at singular protocol boundaries and reject malformed
counts before indexing. External storage drivers still preserve payload counts.

The protobuf JSON corpus now records 13,682 unique Go-derived JSON and binary
message cases. It checks padded/unpadded standard and URL-safe base64, float
string grammar and overflow, wrappers, Timestamp/Duration bounds, reversible
FieldMask paths, finite/set `google.protobuf.Value` and resolvable/valid `Any`.
This is a bounded interoperability corpus; broader protobuf conformance remains.

Workflow/Activity/signal/query/Update headers and memo APIs use
`map[string]PayloadValue`. `DataConverter.encode_map` / `decode_map` apply the
configured converter and codecs per singular map value, with sorted keys.
`WorkflowContext.headers`, `event_headers`, `memo`, query/validator header views,
Activity information and standalone Activity descriptions retain semantic types.
`last_completion_result` returns `DecodedArguments`, including typed/raw views.
`ActivityContext.info` returns owned typed byte snapshots. `raw_value(payload)`
constructs an owned, wire-validated raw value for custom converter forwarding.

Memo updates follow the server's wire deletion rules: empty data, `null` and `[]`
are deletion markers regardless of payload encoding. A codec can preserve a typed
null or those binary bytes by producing a nonempty encoded representation. Start
history filters markers; update history retains command payloads while stored
memo state removes them. Independent raw server descriptions cover both cases.

`CancelledError` and `ActivityCancelledError` accept typed `details`. Thrown
workflow errors, Activity cancellation responses and explicit Nexus operation
cancellations retain them through codecs and failure rebinding. Pause/reset stay
retryable failures. Explicit operation cancellation is separate from acknowledgment
of a canceled Nexus polling task. Local Activity work delays incoming workflow
Signals while its Workflow Task executes; use short local work.

The memo deletion behavior follows the pinned [Temporal server implementation](https://github.com/temporalio/temporal/blob/v1.32.2/common/payload/payload.go). Local Activity signal latency follows [Temporal's Local Activity execution model](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/activities/local-activity.mdx).


Captured non-application failures returned by a Nexus handler are wrapped in an
application failure acceptable to Core. Their protocol kind and typed cause
remain available; application/cancellation roots retain their original shape.
`DataConverter.encode_nexus_failure` applies this policy and rejects custom
failure converters that cannot produce an accepted operation root. The
`nexus_failure_probe.py` matrix checks Activity/timeout/child causes through
codecs, query/Update propagation, worker replacement and offline replay.

JSON payload validation treats `_type` as ordinary application data, preserves
numeric token text and uses the existing bounded JSON parser. It does not invoke
V's generic sum-type decoder to validate a payload. The payload JSON unit tests
cover nested type fields, numeric extremes, malformed input and invalid UTF-8.


History APIs support raw protobuf and JSON History batches. Normal RPC responses
normalize batches before external-storage retrieval; result waits select terminal
events within raw close-event batches. `history()` exports all current pages with
wire payloads and storage references intact. `history_page()` uses its
`raw_payloads` option for the same behavior; normal pages retrieve references.
`history_from_response()` decodes batch containers without running converters or
retrieving stored payloads. Unknown binary fields survive export; mixed raw and
nonempty structured histories and unsupported encodings fail explicitly.

Workflow, Update, standalone Activity and Nexus result waits retain early
transport errors. Once their overall budget is spent, a failed wait reports
deadline status, including the Nexus result's final description RPC. Caller
cancellation remains distinct. One-millisecond waits do not cancel executions.


Per-call RPC metadata is an ordered `[]corebridge.RpcMetadataEntry`. Each entry
contains `name` and raw `value` bytes; names ending in `-bin` use binary metadata.
Repeated names and empty binary values are preserved. The custom gRPC forwarder
also handles comma-joined binary HTTP headers. The eleventh Core patch preserves
request/response multiplicity and metadata when the retry layer clones requests.
The older per-call ASCII/binary map fields have been removed from owned callers.

The 76-report host candidate passed at source
`f4504f079522d92af8ca4f5e139ad3b525d45aa1734f3edc66641fd6ac0a0135`.
The metadata candidate has focused normal/instrumented transport evidence and
215 Rust unit tests; its combined and platform gates remain pending. The full
pinned upstream Rust lint alias currently fails on existing compiler/derive
warnings. The separate cleanup experiment is not part of the metadata patch.

Workflow visibility supports `client.workflow_page(...)`, lazy
`client.workflow_pages(...)` and `client.count_workflows(query)`. Pages carry
workflow handles, typed memos and the original protobuf execution information.
`WorkflowListOptions` selects current, archived or scan listing, query, page size
and starting token. Archived listing requires server archival configuration.
`decode_memo: false` exposes memo wire payloads through `raw` without retrieving
external references. Decoding selects each execution's workflow scope separately.
Iterate until `pages.exhausted()`; RPC errors retain the token for retry, repeated
tokens fail explicitly and overlapping `next_page()` calls are rejected.

Workflow activation storage retrieval retains the initialized workflow identity
through later activations and removes it on eviction. Storage drivers can enforce
workflow scope for initialization memos, subsequent signals and offline replay.

Workflow callbacks for different runs can execute on separate threads, bounded by
`WorkerOptions.workflow_concurrency`. Activations for the same run remain serial,
including eviction and completion. Each run owns its workflow object and runtime
state; reusable interceptors, converters, factories, storage drivers and loggers
must synchronize mutable state shared across runs. Core polling and slot suppliers
can reduce the number of simultaneously active callbacks below the configured
limit. `WorkflowContext.is_replaying()` is available for suppressing nondurable
diagnostics; durable commands must be identical during live execution and replay.
A blocked callback still occupies its slot until it returns. Workflow callback
panics fail the current task by default. Native faults remain process failures.

`client.describe_workflow(handle)` returns execution information, a typed memo
and the original protobuf description. `decode_memo: false` avoids external
retrieval and leaves memo wire payloads in the raw execution information.
Other payload-bearing description fields remain in their original wire form.

`client.reset_workflow(handle, ...)` requires a workflow-task history event ID
greater than one. Options include a reason, request identity, excluded event
categories and post-reset operations. It returns the new run handle. Automatic
RPC retries are disabled for this operation, including on clients configured to
retry other calls: the tested Temporal 1.32 server created different runs for the
same reset ID, whereas 1.33 deduplicated it. A caller retry can reset again.
`reset_signal_operation(...)` encodes a signal and typed headers in the target
workflow's scope. Both tested servers reject that post-reset operation with
INVALID_ARGUMENT; the schema exposes it ahead of server support. Errors are
returned directly. Ordinary reset, signal exclusion and subsequent signals work.

`client.delete_workflow(handle)` requests asynchronous deletion of that execution.
Server processing and its close-task acknowledgement determine when it disappears.
External storage retention remains the storage driver's responsibility.

`client.pause_workflow(handle, ...)` and `client.unpause_workflow(handle, ...)`
accept a reason and optional request identity; otherwise an OS-random UUID is
created. The namespace must enable workflow pausing. The SDK preserves the server
error when it is disabled. Paused executions retain queued signals and
cancellation requests across worker replacement, then process them on resumption.

`runtime.replay_many(inputs, concurrency: 3)` replays a corpus without a server.
Each `ReplayInput` has binary history and its own `ReplayOptions`, including
workflow identity, queue, converter, interceptors and plugins. Every history gets
an isolated cache. Results retain input order and distinguish succeeded, failed
and skipped cases, with replay statistics or error details. `fail_fast: true`
stops starting cases after a failure and drains those already running. Batch
configuration errors fail the call; a history error becomes a per-history outcome.
The default concurrency is one, with a supported range of 1–32. Shared converter,
interceptor and plugin objects must synchronize mutable state when used in
parallel. A callback that does not return can still block its replay thread.


## Recorded workflow values

`WorkflowContext.side_effect(operation)` records a typed `PayloadValue` in workflow
history and returns the recorded value during replay. The callback takes no
workflow context and returns `!PayloadValue`. Use its returned value to update
workflow state outside the callback.

`WorkflowContext.mutable_side_effect(id, operation, equal)` records the first value
for a nonempty ID and subsequent unequal values. The equality function receives
the previous and current typed values. Replay restores recorded values and call
counters without invoking either callback. Different IDs keep independent state.
Both APIs use the workflow converter, codecs and external storage scope.

Callbacks can run again if a workflow task fails before its marker commits. They
must tolerate that repetition. A callback/conversion error fails the workflow task,
even if workflow code catches the returned error. Missing recorded values are
nondeterminism errors and respect global/per-type workflow failure policies.
A panic in a SideEffect computation or equality callback fails its workflow
task; native faults remain process failures.

These APIs require the maintained Core marker extension; the SDK checks its
protocol capability before connecting, starting workers or replaying. Rebuild
native libraries together with the vendored header and generated schemas.

`side_effect_probe.py` exercises ordinary markers and their interleaving with
patch/local Activity markers, count-changing codecs, storage, replacement and
offline replay. `side_effect_errors_probe.py` checks changed-code failure policies
and repair. `side_effect_go_probe.py` replays frozen Go SDK 1.35.0 histories without
requiring Go in the SDK runtime or Linux validation image. The optional generator
`generate_side_effect_go_corpus.py` uses `_tests/native/side_effect_go`; it verifies
Go-to-V worker handoff after explicitly clearing the dead Go worker's sticky queue,
and replays the completed histories with both implementations. This proves that
recorded Go values can be consumed by V; it does not establish arbitrary workflow
code migration between languages.


## Tracing

Create a `TraceProvider` with a custom `TraceExporter` or
`new_otlp_trace_exporter`, then set `ClientOptions.tracing` and
`WorkerOptions.tracing` to `TraceOptions{provider: provider}`. The OTLP exporter
supports gRPC, HTTP/protobuf and HTTP/JSON, explicit resource/header settings,
custom CA certificates and client certificate/key pairs through `TraceTlsOptions`.
TLS keeps hostname verification enabled. Treat private key strings as secrets.

Use `WorkflowContext.trace_context()` or `ActivityContext.trace_context()` to
obtain the active span context. `client.with_trace_parent(parent)` returns an
independently closable client sharing the connection and configured interceptors;
closing the original does not close retained clients or their workers. Providers
remain caller-owned. User-created spans use `provider.start_span` and explicit
parents; call `finish`, `force_flush` and `shutdown` as needed. Custom exporters
must obey their cooperative `TraceExportContext` deadline. Queue limits include
in-flight batches; asynchronous errors and drops are available through `stats`.
`record_errors: false` prevents automatic exception messages from being recorded.

Tracing owns `_tracer-data` in SDK header maps and keeps its JSON carrier outside
application converters/codecs. Application header APIs reject that reserved key;
application memo keys are unrestricted. Workflow tracing reconstructs identities
without consuming the workflow RNG and suppresses exports during replay.
Telemetry is best effort: process loss may drop queued spans or duplicate spans
exported before workflow-task acknowledgement. It is not durable application state.
Broader sampler/exporter coverage remains incomplete.
See `tracing_demo/` and the tracing probes for executable coverage.


`TraceOptions.baggage` carries immutable W3C baggage beside trace context.
`new_baggage` validates member/property snapshots; `parse_baggage` exposes partial
valid results, invalid-member counts and truncation. `PropagationContext` combines
an optional span with baggage. Use `client.with_propagation_context` to make an
independently closable scope, or `ClientRpcRequest.baggage` for one RPC. Workflow,
Activity, Nexus, query and Update validator contexts expose
`propagation_context()`. Signal and Update baggage applies to that callback and
its outbound commands; later workflow callbacks recover the workflow's baggage.
Baggage is propagated and is not automatically added to exported span attributes.

Trace and baggage carriers remain inline even when application payloads use
external storage. `temporal-v/propagation` is reserved payload metadata for SDK
carriers in header fields. Application memo remains unrestricted. Baggage is
plaintext propagation metadata; put only values appropriate for downstream
services there. The parser bounds encoded baggage to 64 members/8192 bytes and
applies additional 1 MiB input/1024-property resource limits.

Nexus handler spans propagate through SDK workflow/Activity/Update/query/signal
helpers without transferring ownership of the application's client. The required
Go tracing corpus verifies actual Go-to-V and V-to-Go worker handoffs and offline
replay with the official Go OpenTelemetry interceptor. Regeneration helpers and
Go sources live under `_tests/native/tracing_temporal_go`; they are test tooling
and are not linked into V workers.


`locks/core-source.json` binds the exact maintained Rust tree and all fourteen
patches. The build checks it before and after native compilation, including the
metrics protocol/HTTP client choices. Header-compatible stale tool caches are
rejected; update the pin, patches and manifest together when upgrading Core.

Trace providers support `AlwaysOnTraceSampler`, `AlwaysOffTraceSampler`,
`new_trace_id_ratio_sampler(fraction)` and `ParentBasedTraceSampler`. The default
parent-based policy honors remote and local parent flags and samples root spans.
Custom `TraceSampler` implementations receive immutable span attributes, links,
parent identity and baggage. They may return attributes and tracestate. Sampling
runs before a span is returned; avoid blocking in this callback.

`record_only` permits local recording and suppresses export; `drop` retains a
valid propagation identity without recording. `record_and_sample` records and
exports. Explicit `TraceSpanOptions.sampled` overrides the sampler decision.
Automatic workflow, Activity and Nexus propagation uses the sampled span context,
including producer spans whose decision differs from their parent. Sampling
changes telemetry only; workflow replay continues to suppress exports.

The sampling candidate has a pinned 19,020-case Go OpenTelemetry 1.43 oracle,
including exact ratio boundaries and parent policies. Full aggregate and Linux
validation remain pending.

On current upstream V, application callback panic handling uses native
`recover()` and runs pending deferred cleanup. Released V 0.5.2 has no panic
recovery and exits the process on a panic; returned errors remain supported. Workflow factories, event and
command interceptors, query handlers, Update validators/handlers, Activity
handlers/interceptors and Nexus start/cancellation handlers are covered.
`PanicError` retains the panic message and an SDK-owned recovery-boundary stack.
The SDK does not patch the consumer compiler. Native faults and callbacks that never return require
process supervision.

`WorkerOptions.workflow_panic_policy` defaults to `.block_workflow`: the failed
task is retried, its cached state is discarded and unrelated runs keep working.
Choose `.fail_workflow` to close the execution with an application `PanicError`.
Use the same `ReplayOptions.workflow_panic_policy` when replaying these histories.
Query and Update validator panics fail their calls. Update handler panics fail
the task, preserving the accepted Update for retry. Activity panics follow the
Activity retry policy; Nexus panics report handler errors and follow Nexus retry.

Focused normal and V/C sanitizer matrices covered 96 workflow/Activity histories
and 12 Nexus histories. The full aggregate and Linux gates remain pending.

Activity preparation, decoding, output/failure encoding and external storage also
run within a recovery boundary. A panic there produces one retryable `PanicError`
completion without re-entering the failing converter or storage driver. The
Activity metric scope closes on every preparation path. Storage permits unwind
through their existing deferred cleanup. Normal callback-returned errors preserve
their existing failure conversion behavior.

The focused extension matrix covers nine concurrent input/output/failure
conversion and storage faults, a subsequent successful Activity, worker
replacement and 44 offline histories across both servers and two codec profiles.
The integrated candidate requires 103 reports; Linux verification is pending.

The final extension matrix passed all 44 histories in normal, optimized and V/C
ASan/UBSan builds, including maximum-attempt retry state and one-slot storage
permit recovery. The ordinary and instrumented 96-history panic regression
matrices passed after the Activity ownership refactor. Source, compiler, native
libraries, executables and reports are bound in the focused validation receipt.
Aggregate and Linux validation remain pending.

`context.pending_updates()` returns sorted snapshots containing each accepted
Update's client ID, protocol instance ID, name and unfinished policy.
`context.all_handlers_finished()` becomes true after explicit completion or
rejection. Signal event callbacks in this API return synchronously; deferred
accepted Updates remain tracked after their initial event returns.

A workflow may implement `ConfiguredUpdatesWorkflow` with
`update_handler_options(name string) UpdateHandlerOptions` to choose each
handler's `unfinished_policy`: `.warn_and_abandon` (default) or `.abandon`.
Completion, cancellation and Continue-as-New allow unfinished Updates, with
replay-suppressed warnings for the default policy. Workflow failure abandons
Updates without these warnings. Temporal determines their final failed outcome;
the SDK does not synthesize a user rejection when closing the workflow.

The lifecycle matrix passed 28 histories in normal, optimized and V/C sanitizer
builds, including restart, explicit drain, handler-driven closure and both
policies. Actual Go SDK 1.35/V worker handoffs passed sixteen runs in both
directions plus four continued runs; both SDKs replayed all twenty histories.
The frozen corpus is verified and replayed by the required `handler_go_probe.py`
without installing Go. Regenerate it using `generate_handler_go_corpus.py` and
the pinned reference in `_tests/native/handler_temporal_go`. All twenty also
passed optimized and instrumented V replay. The integrated candidate requires
105 reports and now binds the time-skipping test server executable as well.
Linux validation remains blocked; release_ready remains false.

The preceding tracing/baggage/Core14 source
9419cbc55de28c99849db6ec04f3b2f159f4d7335cbcdb7e3c95845e1733993d
passed all 97 host reports including the complete V/C sanitizer matrix. Its exact
source, reports and 40 bound artifacts are frozen in host-evidence-97-9419cbc.
The newer sampling/panic/handler candidate requires its own 105-report gate;
Linux readiness is still unverified.

`context.get_version(change_id, min_supported, max_supported)!` records the first
live choice and retains it during later calls and replay. New runs select the
maximum; unmarked older histories select `sdk.default_version` (-1). Zero is a
normal version. Choose a supported range that includes all histories the worker
may receive. An incompatible recorded choice is a retained `NondeterminismError`,
so catching the returned error cannot commit a different command path. Existing
workflow-task/global/per-type nondeterminism policies apply.

Numeric choices use Core patch markers in a reserved hashed namespace and do not
consume workflow RNG state. Ordinary `patched`/`deprecate_patch` calls reject
that namespace. After all older choices retire, replace the numeric call with
`context.deprecate_version(change_id, last_supported)!`. This validates the final
remaining choice and emits a deprecated Core marker. Remove that call only once
histories requiring the earlier nondeprecated marker have retired. Change IDs
must remain unique to their logical change.

Normal and optimized matrices passed 60 histories across both servers, three
codecs and storage on/off, with 72 rejected incompatible replays. The matrix
checks old/unmarked, zero and later choices, changed maximums, swallowed errors,
unrelated progress, repair, stable RNG, SIGKILL, deprecation and safe removal.
Existing SideEffect regressions passed 144 histories/672 markers, eight
changed-code histories/24 rejections and two actual Go reference histories.
The expanded instrumented matrix and integrated 106-report gate remain pending.

The complete numeric matrix passed 60 histories and 72 incompatible rejections
in all three normal/optimized/V-C-instrumented builds. Bounded query polling
recorded transient transport resets/deadlines during deliberate task failure and
repair. Independent raw-history audit verified actual external storage references
versus inline payloads. Source/compiler/native/executable bindings were preserved.
The integrated 106-report gate and Linux validation remain outstanding.

## Dynamic Update handlers

Implement `DynamicUpdatesWorkflow.validate_dynamic_update(name string, arguments []RawValue)!`
to accept Update names without an explicit registration. The workflow receives the
Update through its ordinary `on_event` callback, with owned `RawValue` entries in
`context.typed_arguments` and `context.raw_arguments`. Codec and external-storage
decoding run before dispatch; the payload converter is bypassed for dynamic
arguments, preserving unknown encodings and arbitrary bytes. Named registrations
take priority and retain normal payload conversion; conversion failures do not
fall through to the dynamic handler.

Dynamic validators obey the existing read-only validation and panic isolation
contracts. Validators do not rerun while replaying an accepted Update. Deferred
completion, per-handler unfinished policies, duplicate Update IDs and replay use
the same lifecycle as named handlers. Workflow metadata advertises the dynamic
handler with an empty name. Empty or duplicate named registrations are rejected.

## Recovered panic stacks

Current SDK consumers use unmodified V. Released V 0.5.2 terminates the worker on
panic. Upstream V with native `recover()` isolates the documented callback
boundaries and captures up to 64 surviving recovery-boundary frames, bounded to
16 KiB. These traces are explicitly labelled and do not claim to contain the
original panic-site stack. Failure conversion and tracing preserve them.

The frozen `replay-baseline-v1` fixture still requires its historical compiler
and panic-stack extension; this is test infrastructure, not an SDK dependency.

`context.force_task_fail(cause)` explicitly retries the current workflow task.
Its first request wins over later callback errors, panics and queued completion,
and all activation commands are discarded. It also overrides the worker's
fail-workflow panic policy. Execution resumes when the callback stops requesting
that failure; deployments that always request failure continue to block that run.

`context.random_stream(name)` returns a deterministic named RNG. Repeated lookup
and copied handles share progress, while different names and the default RNG
remain independent. Handles follow Temporal seed updates after workflow reset.
Keep names stable across replay. Streams provide `random_u64()`, `random_uuid()`,
`random_bool()` and `random_f64()` in `[0, 1)`; these values are not cryptographic.
Default RNG float/bool methods also use the existing default stream. The V-specific
SHA-256 seed derivation and SplitMix64 sequence are pinned replay contracts, with
384 frozen Python reference values. They do not promise identical random values
to another language's SDK.

## Raw dynamic signals and queries

`DynamicQueriesWorkflow.query_dynamic(name, arguments []RawValue)!PayloadValue`
handles query names without a named registration. A workflow that combines it
with `QueryableWorkflow` must implement `NamedQueriesWorkflow.query_names()`.
Named queries use the configured payload converter and keep priority, including
conversion errors. Queries retain the existing read-only callback contract.

`DynamicSignalsWorkflow.signal_dynamic(event Event, mut context WorkflowContext)!`
receives signal events with owned raw arguments, typed `RawValue` entries, sender
identity, originating event ID and headers. `NamedSignalsWorkflow.signal_names()`
selects signals that use ordinary converted `on_event` dispatch. All other names
reach the dynamic callback. Dynamic arguments pass through codecs and external
storage decoding while bypassing the payload converter. Unknown encodings and
arbitrary bytes are preserved. Existing workflow/query interceptors surround
both paths. Query panics fail the query; signal panics follow workflow panic
policy. Reserved workflow metadata includes empty-name dynamic definitions.
Empty or duplicate named declarations and ambiguous mixed query dispatch are
rejected before workflow initialization completes.

Custom gRPC transport and metric/trace exporter callbacks isolate V panics through
their existing error paths. RPC panics return a redacted INTERNAL response.
Exporter panics fail the affected batch or shutdown, update failure statistics,
and release native queued resources. Later export/RPC calls can continue, and
shutdown completes. Trace details remain available through provider statistics;
private callback diagnostics are not emitted to ordinary output. Applications
still own any state changes or external side effects made before a callback failed.

`WorkflowContext.is_replaying()` reports Core's activation replay flag.
`is_replaying_history_events()` excludes query-only activations, following the
pinned Rust SDK's distinction. Read-only query interceptors can inspect both
through the corresponding `QueryNext` methods. Workflow logs, metric replay
guards and tracing suppress historical event replay while keeping live cached
query observations. Query-only activations may have an activation replay flag
of true. This distinction is for nondurable diagnostics, not workflow decisions.

Activity checkpoint payload conversion is explicit. Use
`context.raw_heartbeat_details()` (also in `context.info().raw_heartbeat_details`)
for owned values after codec/storage decoding. `context.heartbeat_values()!`
applies the configured payload converter in the Activity result scope;
`context.heartbeat_details()!` returns their logical string representations.
These calls may fail without consuming or changing the retained raw values.
The eager `ActivityInfo.heartbeat_values` and `heartbeat_details` fields have
been removed; all repository callers use the context methods. Unknown payload
encodings can now reach a named or dynamic handler. Malformed codecs/storage
references still fail during preparation. Conversion happens on each explicit
call, so application converters should be repeatable.

Dynamic workflow factories receive start arguments as owned `RawValue` entries
in `Event.raw_arguments` and `typed_arguments`, after codecs and external
storage. They bypass the payload converter, including empty and opaque argument
lists. Use `context.decode_raw_value(value)!` to apply the scoped payload
converter explicitly. Named registrations take precedence and retain normal
conversion. The repository's existing dynamic workflow example now converts
its JSON input explicitly. Dynamic signal/query/Update routing remains
independently declared by the workflow interfaces.

`ActivityContext.client()!` returns an independently owned client using the
worker's existing connection and namespace. Close each returned handle; closing
it leaves the worker and sibling clients usable. A retained handle may outlive
the Activity and worker. Shared-client workers preserve that client's configured
converter, interceptors, plugins and RPC options without rerunning construction
hooks. Otherwise the client uses the worker's converter, transport, telemetry
and namespace. Activity tracing supplies the current span and baggage for client
calls. Retaining a new handle after the worker connection has closed fails.

`NexusContext.client()!` shares the Nexus worker connection with independent
ownership, just as the Activity accessor does. Shared-client configuration and
plugin snapshots are preserved; current Nexus trace context/baggage apply to
client calls. Close returned clients. Start and cancel callbacks can use these
clients with the existing workflow/Activity/Update/query/signal helpers. The
Nexus examples now reuse this accessor instead of connecting per request.

`ActivityInfo.task_token` exposes an owned copy of the opaque task token without
changing completion mode. It is omitted from JSON serialization. Use
`defer_completion()` when completion will occur outside the handler; merely
reading the token does not defer completion. `ActivityInfo.deadline` is the
earlier of the original schedule-to-close deadline and this attempt's
start-to-close deadline. A zero schedule-to-close timeout is unlimited. Missing
or invalid task timestamp/timeout fields leave the deadline absent. Seconds and
nanoseconds are added separately to avoid Unix-nanosecond overflow.


Experimental Event Groups associate commands with history correlation metadata.
Enable `WorkerOptions.experimental_event_groups` and the same flag in
`ReplayOptions` for histories produced with this feature. Create a group with
`new_event_group(id)!`, optionally call `.with_label(label)!`, and pass it in
command options' `event_groups`. IDs are plaintext in history; labels start as
`json/plain` strings and pass through payload codecs, bypassing the application
payload converter. Empty IDs and labels are rejected.

`context.with_event_group(group, callback)!` and `with_event_groups` scope all
commands queued by a synchronous callback. Nested scopes compose. Duplicate IDs
keep their first position and use their last value; direct command options take
precedence. Capture `current_event_groups()` and re-enter that scope explicitly
when an event-driven handler resumes in a later callback. Signal and Update
handlers start with an implicit group using the originating history event ID or
the client's Update ID. Each handler's implicit scope replaces the enclosing
scope. Groups do not propagate into a child workflow or a new run.

Timers, remote/local Activities, child workflows, Nexus operations, external
signals/cancellation, continuation, memo/search upserts and patch markers accept
groups. Completion/failure/cancellation inherit the current scope; workflow
cancellation can also take direct groups. Core preserves the original command's
groups for cancellation of scheduled work. Local durable backoff retains its
captured groups. Query and Update protocol replies are not grouped. Calling the
experimental API with the flag disabled fails the workflow task, so a correctly
configured replacement worker can repair it.

`scripts/event_groups_probe.py` verifies actual Temporal history metadata and
offline replay across both pinned server versions, codecs and storage. It also
checks continuation, external commands, scope restoration, panic repair and
worker configuration repair. The API follows the pinned Rust SDK's experimental
Event Groups semantics; integrated host and Linux validation remain required.


Workflow context values are runtime-only bindings for deterministic data. Use
`with_context_value(key, value, callback)!` to install any V value under a
namespaced string key for a synchronous callback. Read it with
`context_value[ValueType](key)`; absent keys and mismatched types return none.
Nested scopes inherit other keys, shadow the same key and restore on return or
error. `capture_context_values()` produces a snapshot of bindings; re-enter it
with `with_context_values(snapshot, callback)!` when an event-driven branch
resumes. Snapshot copies isolate bindings; referenced application objects retain
their normal V ownership and mutation semantics.

These values are not history, payloads or automatic headers. Reconstruct them
deterministically during replay. Outbound command interceptors read each
command's captured scope with `WorkflowInfo.context_value[ValueType](key)`.
Local Activity retry and durable backoff preserve the scheduling scope. Query
and Update validator interceptors can read the current scope through their
`next.context_value[ValueType](key)`. Runtime bindings are excluded from
WorkflowInfo JSON. Use the existing explicit headers for durable propagation.


`context.wait_condition(predicate, options)!` registers a deterministic read-only
predicate over workflow state and `WorkflowInfo`, returning a sequence handle.
The runtime evaluates pending conditions in registration order after workflow
callbacks and after condition timers fire. A ready wait produces
`condition_satisfied`; cancellation produces `condition_cancelled`; a positive
`timeout` produces `condition_timed_out`. The event's sequence identifies the
wait. A ready predicate takes precedence over scope cancellation or timeout.
New waits registered by resolution callbacks are evaluated before the workflow
task completes. Predicates must return promptly and must not mutate state or
perform external work. Predicate panics follow the workflow panic policy.

Wait options use the existing cancellation `scope`, including shielding. The
runtime lazily starts a timeout timer only for a pending, uncancelled condition;
internal timer resolutions do not reach `on_event`. `cancel_condition(sequence)!`
cancels a wait explicitly and is idempotent. Resolving the wait cancels its
pending timeout. A scope with a pending wait cannot be released. Captured context
values and Event Groups are restored while evaluating the predicate and while
calling the resolution handler. Ordinary waits require no experimental flag;
explicit Event Groups retain their existing opt-in requirement.

`cancel_scope(id, reason: 'cause')!` records the first cancellation cause for the
scope and unshielded descendants. `scope_cancel_reason(id)!` reads it. Newly
created unshielded descendants inherit their parent's cancellation and cause.
Root cancellation retains Temporal's reason; condition cancellation events carry
that reason in `Event.message`. Runtime waits and scoped values are reconstructed
by replay and are not recorded as application payloads.


Activity application headers now use the same explicit conversion model as
heartbeat details. `context.raw_headers()` and `ActivityInfo.raw_headers` expose
owned maps of codec/storage-decoded `RawValue` entries. Unknown encodings and
arbitrary binary data/metadata reach named and dynamic handlers. Use
`context.header_values()!` to convert the map with the Activity input scope, or
`context.decode_raw_value(raw)!` to convert one entry. Conversion errors leave
retained raw values unchanged. Each explicit call repeats payload conversion.
Malformed codecs or storage references still fail preparation. Propagator-owned
`_tracer-data` stays separate from application headers.

The eager `ActivityInfo.headers` field has been removed. All repository callers
now request conversion explicitly. Raw header snapshots are omitted from
ActivityInfo JSON. Changes to a returned map, data buffer or `to_payload()`
result do not change the retained context.


`ActivityContext.workflow_handle()` returns the parent workflow's execution IDs
for workflow-backed remote/local Activities, and none for standalone Activities.
Use the independently owned `context.client()!` for client operations on that
handle. `ActivityInfo.namespace` always identifies the Activity worker's
namespace, including standalone Activities whose workflow namespace is absent.
The existing workflow metadata continues to describe the invoking workflow.
When replaying a non-default namespace, set `ReplayOptions.namespace` to match
its serialization scope; the panic/feature example now honors its configured
namespace during replay.


Workflow code can read owned application headers before payload conversion with
`context.raw_headers()!` for the start, or `raw_event_headers(event)!` for an
inbound event. Query and Update validator interceptors use
`next.raw_headers()!`. These explicit methods perform codec/storage decoding and
omit propagator-owned headers. They preserve unknown payload encodings and
binary metadata. The existing `headers()` and `event_headers()` methods request
conversion; `decode_raw_value(raw)!` converts one entry with the same scope.
Query and validator `next` values now expose that individual conversion method.
Explicit getter calls may repeat codec/storage work; converters/codecs should be
repeatable. Returned maps/buffers/payload copies do not mutate retained inputs.


Payload codec boundaries now take deep snapshots of complete protobuf payloads.
A codec may mutate nested fields in its local payload structs without changing
retained wire inputs, a previous converter/codec's cached buffers, or sibling
returned payloads. Data, metadata byte values, external payload details and
unknown protobuf fields are preserved. The same pipeline handles codec-only
Event Group labels. This corrects an observed case where two reads of one wire
payload returned different values because a codec's nested mutation escaped.

The ownership probe includes shared-source multithreaded checks and live tests
with an intentionally mutating reversible codec. That test codec is enabled only
by `V_CODEC_OWNERSHIP=true` in the example converter; worker, client and replay
must use the same codec configuration. Callback implementations still own their
thread safety and any external state they intentionally mutate.


Experimental `WorkerOptions.patch_activation_callback` can decide whether a
fresh live `patched(id)` call activates a rollout. The callback receives
`PatchActivationInput` with an owned, read-only workflow information snapshot
and patch ID. Its raw header/memo getters apply codecs/storage without payload
conversion; search attributes retain their indexed JSON payloads. Use
`decode_raw_value` for explicit scoped conversion. Callbacks must be thread safe.
Commands cannot be scheduled through a captured workflow context while the
callback runs.

Both true and false decisions are cached for the run. Replay and already
notified markers follow history; deprecation bypasses rollout policy and
preserves an already cached decision. The policy does not control internal
numeric `get_version` markers. A declined patch has no marker, so replay treats
it as inactive. Supply the callback through `ReplayOptions` when reproducing
worker configuration; completed historical decisions never consult it. Callback
panics follow the configured workflow panic policy. Protobuf fields returned in
workflow/Activity information snapshots now own their nested arrays and unknown
fields, so changing a snapshot cannot alter retained metadata.


Persistent V/C sanitizer executables are bound in the sanitizer report with their
compiler hash and compile/link commands. Final verification rechecks these images.
After a successful exact-source check, run `python3 scripts/archive_verified_check.py`
with the same `SPIKE_ARTIFACTS` to retain source, reports, normal build images and
instrumented images together. This archive requires the current aggregate gate
to pass; focused results alone do not qualify.


Worker middleware is registered in `WorkerOptions.worker_interceptors` (or
`ReplayOptions.worker_interceptors`). A value implements `WorkerInterceptor.name`
and any of `WorkerRunInterceptor`, `WorkerReplayInterceptor`, or
`WorkerShutdownInterceptor`. Run hooks receive the plugin-configured `WorkerSetup`
and call `next.execute(setup)` synchronously to continue; replay hooks similarly
receive `ReplaySetup` and history bytes and return a `ReplayResult`. They may
short-circuit, transform setup/options, or propagate typed errors. Continuations
reject duplicate/concurrent use and use after their middleware returns. The worker
role and registered interceptor chain are fixed.

Callbacks may be shared across workers and must synchronize their own state.
Shutdown notifications run in registration order after polling and handlers drain,
before the Core owner closes. They receive role, replay status, namespace and task
queue. Hook panics are logged and isolated so remaining notifications and cleanup
continue; middleware panics become `PanicError` with a recovery-boundary stack.
A worker that is short-circuited or fails before Core construction has no shutdown
notification. OS kill cannot run middleware cleanup. Worker activation inspection is described below.


A worker interceptor may also implement `WorkerCompletionInterceptor` with
`on_workflow_activation_completion(completion temporal.CoresdkWorkflowCompletion_WorkflowActivationCompletion)`.
It observes the exact encoded/stored completion immediately before submission to
Core, including failed tasks, draining and replay. Each observer receives a
separate snapshot; mutations have no effect on subsequent observers or Core.
Observers may run concurrently for different workflow runs. Panics are logged and
isolated. The same observer interface is available through replay options.


`WorkerActivationInterceptor.on_workflow_activation(activation temporal.CoresdkWorkflowActivation_WorkflowActivation) !`
receives a codec/storage-decoded snapshot before workflow factories and handlers
run. Application payload conversion has not occurred. Batched payload fields
retain their decoded cardinality; search attributes and trace propagation headers
remain unchanged. Activity, child and Nexus results use their recorded target
serialization scopes. Each observer gets an independent snapshot and cannot
change the activation executed by the workflow. This optional inspection performs
an additional codec-only traversal; codecs must be deterministic and thread safe.

An activation observer error or panic stops the worker, drains its handlers and
returns `WorkerInterceptorError` with phase `activation` and the original typed
cause. Panic causes include a recovery-boundary stack. Later activation observers
are skipped. Payload preparation failures fail the affected workflow task so a
configuration repair can retry it. Completion observers still run when the failed
completion is submitted. The interfaces also apply during offline/batch replay.


`ActivityContext.is_cancelled()` polls cancellation without throwing;
`wait_cancelled()!` waits and returns `ActivityCancelInfo`. The wait preserves
existing heartbeat details and continues remote heartbeats. Local waits send no
heartbeats. Transport failures propagate. `check_cancelled()` remains the throwing
operation. The first observed reason is stable and cancellation metadata snapshots
are owned. Core may report `not_found` after a server timeout; the SDK preserves
that reason rather than inferring a different cause.

Cooperative local Activities also receive worker shutdown after the configured
grace period, through OS signals or `WorkerControl.shutdown()`. Returning their
shutdown cancellation interrupts the workflow task so replacement replay can
retry it without recording an application cancellation. Native faults and
callbacks that never cooperate still require process supervision.


Queries and Update validators receive `WorkflowContextView` as their final argument,
including raw dynamic handlers. This owned snapshot exposes workflow information,
deterministic time, replay flags, details, memo, search attributes, initialization,
start inputs, request inputs and headers. Payload conversion remains explicit:
`raw_request_headers()` and `input_values()` preserve raw values; `request_headers()`
and `decode_raw_value()` use the workflow's scoped converter. Search attributes
bypass application codecs. Snapshot fields are private and omitted from JSON.

`context.view()!` captures the current workflow context for later inspection.
Snapshots retain the values present when captured, including interceptor context
bindings. Query and validator continuations support `with_context_value()` and
`with_context_values()` without changing the run's context or sibling calls.
Context values retain the SDK's existing application-owned object semantics;
applications must not mutate shared reference values from read-only handlers.

Queries and validators cannot schedule workflow commands. Captured mutable
contexts are guarded for the duration of the callback, including failed and
panicking callbacks. This is command exclusion, not a sandbox for application
memory or external I/O. Query/validator code must remain deterministic and avoid
mutating workflow state. Accepted Update validators are not invoked during replay.


Workflow factories receive `(view sdk.WorkflowContextView, inputs []sdk.PayloadValue)`
and return `!sdk.Workflow`. The view exposes initialization metadata, headers,
memo and deterministic time before the first event. Named registrations receive
converted inputs; dynamic fallback receives raw values. Constructor inputs are
owned independently of the started event. Factory errors retry the workflow task;
factory panics follow the configured workflow panic policy.


A captured Linux replay regression now covers local-Activity heartbeat boundaries:
Core can record the Activity result in one workflow task and its following
SideEffect in the next. The fixed Core preloads marker values across task and
page boundaries before replaying the callback. The source-locked history is
replayed by both current and archived `replay-baseline-v1` V implementations on
corrected Core. This baseline is a frozen test fixture, not a published SDK release.
See `release/API-POLICY.md` for replay memory behavior and supported rollback images.
Workers reject native libraries without the marker-lookahead capability.

The test client accepts `--input-file` for large JSON values. Integration fixtures
use it above 64 KiB so the full payload reaches the SDK without exceeding Linux's
per-argument limit. All owned V files must pass the separately pinned V 0.5.2 formatter.
Bootstrap installs it with `python3 scripts/prepare_formatter.py`; format checks
and all generators use `scripts/format_v.py`, independently of the SDK compiler.
`python3 scripts/format_v.py .` writes formatting and adding `--check` verifies
without rewriting. A local V 0.5.2 installation's `v fmt -w .` produces the same
format. Generated output is formatted to a verified fixed point before exact
byte comparisons; generation checks never rewrite repository sources.
Run `python3 scripts/formatter_test.py` for the focused formatter/generator
regressions without building the SDK or starting Temporal.
