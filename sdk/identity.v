module sdk

import crypto.rand as crypto_rand

// Request identities must remain independent across processes and concurrent
// callers. An entropy failure is an API error, never a weaker random fallback.
fn random_request_id() !string {
	mut bytes := crypto_rand.bytes(16)!
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	hex := bytes.hex()
	return '${hex[..8]}-${hex[8..12]}-${hex[12..16]}-${hex[16..20]}-${hex[20..]}'
}
