module sdk

import encoding.utf8
import strings

// Error text can include arbitrary payload bytes. Protobuf string fields and JSON
// log records require UTF-8, while payload data itself must remain byte-for-byte.
fn protocol_text(value string) string {
	if utf8.validate_str(value) { return value }
	mut result := strings.new_builder(value.len)
	mut offset := 0
	for offset < value.len {
		_, size := utf8.decode_rune_at(value, offset)
		if size == 1 && value[offset] >= 128 {
			result.write_string('\\x' + value[offset].hex())
		} else {
			result.write_string(value[offset..offset + size])
		}
		offset += size
	}
	return result.str()
}

fn protocol_fields(values map[string]string) map[string]string {
	mut result := map[string]string{}
	for key, value in values {
		result[protocol_text(key)] = protocol_text(value)
	}
	return result
}
