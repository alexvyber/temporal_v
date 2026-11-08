module corebridge

#flag -ltemporalio_sdk_core_c_bridge
#flag -ltemporal_v_payloads

$if macos && arm64 {
	#flag -L @DIR/../native/prebuilt/darwin-arm64
	#flag -Wl,-rpath,@DIR/../native/prebuilt/darwin-arm64
} $else $if linux && amd64 {
	#flag -L @DIR/../native/prebuilt/linux-amd64
	#flag -Wl,-rpath,@DIR/../native/prebuilt/linux-amd64
} $else $if linux && arm64 {
	#flag -L @DIR/../native/prebuilt/linux-arm64
	#flag -Wl,-rpath,@DIR/../native/prebuilt/linux-arm64
} $else {
	$compile_error('Temporal V supports macOS arm64 and Linux amd64/arm64 with glibc.')
}
