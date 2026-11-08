# API and workflow-history compatibility

The public V API is frozen for major version 1. The integration candidate is
`1.0.0-rc.1`; this identifies an API contract, not a completed production release.
`api-contract.json` records exported declarations, including option fields,
callback signatures and construction attributes. Builds and the full check suite
verify the lock. Whitespace and formatter layout do not change the API; token
boundaries, quoted defaults, types and attributes remain contractual. Builds
never regenerate the lock. All V files must pass the separately pinned V 0.5.2 formatter check
(`locks/v-formatter.json`), also used by every generator. The SDK compiler
and runtime retain their independent pin. Breaking public changes require a
new major contract and explicit review; do not add old-signature fallback paths.

Query and Update-validator callbacks take `WorkflowContextView` as their final
argument. Workflow factories take `(WorkflowContextView, []PayloadValue)
and return `!Workflow`. Inputs are decoded once using named/dynamic registration
semantics. Initializer input buffers are independent of the started event; raw
views retain opaque encodings. Factory errors fail the workflow task for retry;
on upstream V with native recovery, factory panics obey the workflow panic
policy and retain a labelled recovery-boundary stack. Released V 0.5.2 exits the
worker process on panic; it does not provide callback panic recovery.

Source API compatibility and workflow-history compatibility are separate gates.
`replay-baseline-v1` is a frozen regression-test fixture, not a published SDK
release. Its archive and metadata live in `release/compatibility/`.
`build_replay_baseline_v1.py` builds its worker, Activity, and replay executables.
`upgrade_replay_probe.py` runs the archived V implementation on each target.
It checks SIGKILL handoffs in both directions, accepted Updates,
deduplication, signals, remote/local Activities, protected failures, child
workflows, cancellation, patches and continue-as-new. Both V implementations
replay the resulting histories. An incompatible implementation must fail replay. This
establishes compatibility for this corpus, not for arbitrary application changes.

Before each deployment, replay exported histories of the application's own
long-lived workflows with the exact proposed worker binary. Application command
changes still require patch/version decisions. Keep the previous worker image
and its configuration until rollback is verified. Do not change payload codecs,
external-storage access or workflow registration names during a worker upgrade
without validating those changes against the retained histories.

Native faults terminate the process. Callbacks must return or cooperate with
cancellation; process supervision is required for hung callbacks. Shutdown grace
signals cooperative local Activities and leaves interrupted tasks retryable on
a replacement worker. Activities with external side effects require application
idempotency across attempts and worker loss.

Replay preloads recorded SideEffect values across workflow-task boundaries. A
local Activity result can be recorded by a heartbeat before the language callback
runs in the next task. When local-Activity or language markers are present, Core
fetches the remaining history pages before exposing replay callbacks and sends
each marker once. These workflows retain more history during replay; use
continue-as-new to bound long-lived histories.

Workers require the native marker-lookahead capability and reject older native
libraries. The `replay-baseline-v1` compatibility image compiles the unchanged
prior V API against the same corrected Core. This is an API-compatibility test
fixture, not an application deployment image. Production rollback images must include the
native replay fix and the shutdown fixes required by their application. Core14
images have the captured replay defect and are not rollback candidates.
The compatibility matrix verifies V SDK upgrades on the pinned, corrected Core;
it does not certify arbitrary changes of the upstream Core revision.

Validation disables the pinned V compiler's module cache with `-nocache` to
avoid stateful cache-header differences. This does not disable its auxiliary
generic-instance checker. That checker can exceed a six-GiB Linux build VM;
validation keeps it enabled on a larger isolated builder and rejects OOM events
even when the compiler reports success. The compiler and runtime source pins
remain unchanged.
