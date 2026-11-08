module sdk

import encoding.utf8
import strings

pub struct BaggageProperty {
pub:
	key       string
	value     string
	has_value bool
}

pub struct BaggageMember {
pub:
	key        string
	value      string
	properties []BaggageProperty
}

// Immutable snapshots keep caller mutation out of concurrent propagation.
pub struct Baggage {
	entries map[string]BaggageMember
}

pub struct BaggageParseResult {
pub:
	baggage         Baggage
	invalid_members int
	truncated       bool
}

fn baggage_key(value string) bool {
	if value == '' { return false }
	for character in value.bytes() {
		if (character >= `a` && character <= `z`)
			|| (character >= `A` && character <= `Z`)
			|| (character >= `0` && character <= `9`)
			|| character in "!#$%&'*+-.^_`|~".bytes() {
			continue
		}
		return false
	}
	return true
}

fn baggage_value_byte(character u8) bool {
	return character >= 0x21 && character <= 0x7e && character !in [u8(0x22), 0x2c, 0x3b, 0x5c]
}

fn baggage_escape(value string) string {
	mut result := strings.new_builder(value.len)
	for character in value.bytes() {
		if baggage_value_byte(character) && character != `%` {
			result.write_u8(character)
		} else {
			result.write_string('%' + character.hex().to_upper())
		}
	}
	return result.str()
}

fn baggage_hex(character u8) !u8 {
	if character >= `0` && character <= `9` { return u8(character - `0`) }
	if character >= `a` && character <= `f` { return u8(character - `a` + 10) }
	if character >= `A` && character <= `F` { return u8(character - `A` + 10) }
	return error('invalid baggage percent encoding')
}

fn baggage_unescape(value string) !string {
	mut bytes := []u8{cap: value.len}
	mut offset := 0
	for offset < value.len {
		character := value[offset]
		if !baggage_value_byte(character) { return error('invalid baggage value') }
		if character == `%` {
			if offset + 2 >= value.len { return error('incomplete baggage percent encoding') }
			high := baggage_hex(value[offset + 1])!
			low := baggage_hex(value[offset + 2])!
			bytes << (high << 4) | low
			offset += 3
		} else {
			bytes << character
			offset++
		}
	}
	raw := bytes.bytestr()
	if utf8.validate_str(raw) { return raw }
	mut result := strings.new_builder(raw.len)
	offset = 0
	for offset < raw.len {
		_, size := baggage_decode_rune_at(raw, offset)
		if size == 1 && raw[offset] >= 128 {
			result.write_string('�')
		} else {
			result.write_string(raw[offset..offset + size])
		}
		offset += size
	}
	return result.str()
}

fn baggage_property(value string) !BaggageProperty {
	if value == '' { return BaggageProperty{} }
	raw := value.trim(' \t')
	pair := raw.split_nth('=', 2)
	key := pair[0].trim(' \t')
	if !baggage_key(key) { return error('invalid baggage property key') }
	if pair.len == 1 {
		return BaggageProperty{
			key: key
		}
	}
	return BaggageProperty{
		key:       key
		value:     baggage_unescape(pair[1].trim(' \t'))!
		has_value: true
	}
}

fn baggage_member(value string) !BaggageMember {
	parts := value.split(';')
	if parts.len > 1025 { return error('baggage member exceeds the property resource limit') }
	pair := parts[0].split_nth('=', 2)
	if pair.len != 2 { return error('baggage member requires a key and value') }
	key := baggage_trim_space(pair[0])
	if !baggage_key(key) { return error('invalid baggage key') }
	decoded := baggage_unescape(baggage_trim_space(pair[1]))!
	mut properties := []BaggageProperty{}
	for property in parts[1..] {
		properties << baggage_property(property)!
	}
	return BaggageMember{
		key:        key
		value:      decoded
		properties: properties
	}
}

fn (member BaggageMember) header() string {
	mut result := member.key + '=' + baggage_escape(member.value)
	if member.properties.len > 0 {
		mut properties := []string{}
		for property in member.properties {
			if property.key == '' {
				continue
			}
			properties << if property.has_value {
				property.key + '=' + baggage_escape(property.value)
			} else {
				property.key
			}
		}
		result += ';' + properties.join(';')
	}
	return result
}

pub fn (baggage Baggage) members() []BaggageMember {
	mut keys := baggage.entries.keys()
	keys.sort()
	mut result := []BaggageMember{}
	for key in keys {
		member := baggage.entries[key] or { continue }
		result << BaggageMember{
			...member
			properties: member.properties.clone()
		}
	}
	return result
}

pub fn (baggage Baggage) member(key string) ?BaggageMember {
	member := baggage.entries[key] or { return none }
	return BaggageMember{
		...member
		properties: member.properties.clone()
	}
}

pub fn (baggage Baggage) header() string {
	mut values := []string{}
	for member in baggage.members() {
		values << member.header()
	}
	return values.join(',')
}

pub fn new_baggage(members []BaggageMember) !Baggage {
	if members.len > 1024 { return error('baggage input exceeds member limit') }
	mut entries := map[string]BaggageMember{}
	for member in members {
		if !baggage_key(member.key) || !utf8.validate_str(member.value) {
			return error('invalid baggage member')
		}
		if member.properties.len > 1024 { return error('too many baggage properties') }
		for property in member.properties {
			if !baggage_key(property.key) || !utf8.validate_str(property.value)
				|| (!property.has_value && property.value != '') {
				return error('invalid baggage property')
			}
		}
		entries[member.key] = BaggageMember{
			...member
			properties: member.properties.clone()
		}
	}
	baggage := Baggage{
		entries: entries
	}
	if entries.len > 64 || baggage.header().len > 8192 {
		return error('baggage exceeds 64 members or 8192 encoded bytes')
	}
	return baggage
}

pub fn (baggage Baggage) with_member(member BaggageMember) !Baggage {
	mut members := baggage.members()
	members << member
	return new_baggage(members)!
}

pub fn (baggage Baggage) without_member(key string) Baggage {
	mut entries := baggage.entries.clone()
	entries.delete(key)
	return Baggage{
		entries: entries
	}
}

// Invalid members are isolated; valid members before a size/count limit survive.
pub fn parse_baggage(value string) BaggageParseResult {
	if value == '' { return BaggageParseResult{} }
	if value.len > 1024 * 1024 {
		return BaggageParseResult{
			truncated: true
		}
	}
	mut entries := map[string]BaggageMember{}
	mut invalid := 0
	mut truncated := false
	mut offset := 0
	for offset <= value.len {
		if entries.len >= 64 {
			truncated = true
			break
		}
		rest := value[offset..]
		delimiter := rest.index(',') or { rest.len }
		member := baggage_member(rest[..delimiter]) or {
			invalid++
			offset += delimiter + 1
			continue
		}
		mut selected := entries.clone()
		selected[member.key] = member
		if (Baggage{
			entries: selected
		}).header().len > 8192 {
			truncated = true
			break
		}
		entries = selected.move()
		offset += delimiter + 1
	}
	return BaggageParseResult{
		baggage:         Baggage{
			entries: entries
		}
		invalid_members: invalid
		truncated:       truncated
	}
}

pub fn extract_baggage(headers map[string]string) Baggage {
	mut values := []string{}
	mut keys := headers.keys()
	keys.sort()
	for key in keys {
		if key.to_lower() == 'baggage' { values << headers[key] }
	}
	return parse_baggage(values.join(',')).baggage
}

fn baggage_space(character rune) bool {
	return
		character in [rune(9), 10, 11, 12, 13, 32, 0x85, 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000]
		|| (character >= 0x2000 && character <= 0x200a)
}

fn baggage_trim_space(value string) string {
	mut first := 0
	mut last := value.len
	for first < last {
		character, size := baggage_decode_rune_at(value, first)
		if !baggage_space(character) {
			break
		}
		first += size
	}
	for last > first {
		mut start := last - 1
		for start > first && (value[start] & u8(0xc0)) == u8(0x80) {
			start--
		}
		character, _ := baggage_decode_rune_at(value, start)
		if !baggage_space(character) {
			break
		}
		last = start
	}
	return value[first..last]
}

// get_rune is public in both released V 0.5.2 and current upstream V.
fn baggage_decode_rune_at(value string, offset int) (rune, int) {
	if offset < 0 || offset >= value.len { return 0, 0 }
	character := utf8.get_rune(value, offset)
	encoded := character.str()
	if offset + encoded.len > value.len || value[offset..offset + encoded.len] != encoded {
		return rune(0xfffd), 1
	}
	return character, encoded.len
}
