# Go marker reference generator

This test-only module pins Temporal Go SDK 1.35.0. SDK workers do not depend on Go.
The Go workflow records three binary SideEffects and two changed-only
MutableSideEffects with unchanged intermediate calls. The generator replaces its
worker with the V worker after clearing the original sticky queue, checks the
result, and replays the history using both SDKs.

After building the V SDK, build this reference binary into the same artifact root:

```sh
go -C _tests/native/side_effect_go build -trimpath -o "$SPIKE_ARTIFACTS/tools/side-effect-go-oracle" .
python3 scripts/generate_side_effect_go_corpus.py
```

The generated manifest binds source/module checksums and both history formats.
The required side_effect_go_probe.py only replays frozen histories; Linux builds
and deployed V workers do not need this generator or its Go dependencies.
