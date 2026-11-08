# Native libraries bundled for V consumers

`v install` fetches these libraries with the module; consumers do not build Rust.
Targets: macOS arm64 and Linux amd64/arm64 with glibc 2.36 or newer.
Each target manifest records library hashes, the Rust Core source manifest,
C header and Cargo locks. Linux libraries are release builds; the initial macOS
build uses the debug profile with debug symbols stripped. No compiler runtime
patch is required by current SDK consumers.

Maintainers build Linux targets with `Dockerfile.native` and macOS with
`scripts/build_native.sh`, then run `scripts/prepare_native_release.py`.
The frozen replay-baseline-v1 fixture and the full SDK production-readiness gate
remain separate from this dependency distribution.
