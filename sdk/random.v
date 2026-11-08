module sdk

// SplitMix64 is deliberately fixed as part of the workflow replay contract.
// The seed comes from InitializeWorkflow/UpdateRandomSeed, never wall-clock time.
// This generator is for deterministic workflow decisions, not cryptography.
pub fn (mut context WorkflowContext) random_u64() !u64 {
	context.ensure_open()!
	context.random_state += u64(0x9e3779b97f4a7c15)
	return splitmix64_value(context.random_state)
}

fn splitmix64_value(state u64) u64 {
	mut value := state
	value = (value ^ (value >> 30)) * u64(0xbf58476d1ce4e5b9)
	value = (value ^ (value >> 27)) * u64(0x94d049bb133111eb)
	return value ^ (value >> 31)
}

pub fn (mut context WorkflowContext) random_uuid() !string {
	upper := context.random_u64()!
	lower := context.random_u64()!
	return random_uuid_value(upper, lower)
}

fn random_uuid_value(upper u64, lower u64) string {
	hex := '${upper:016x}${lower:016x}'
	variant := ((lower >> 60) & 3) | 8
	return '${hex[..8]}-${hex[8..12]}-4${hex[13..16]}-${variant:x}${hex[17..20]}-${hex[20..]}'
}

// random_f64 returns a deterministic value in [0, 1).
pub fn (mut context WorkflowContext) random_f64() !f64 {
	value := context.random_u64()!
	return f64(value >> 11) * (1.0 / 9007199254740992.0)
}

pub fn (mut context WorkflowContext) random_bool() !bool {
	return context.random_u64()! & 1 != 0
}
