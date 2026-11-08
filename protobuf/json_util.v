module protobuf

import x.json2
import encoding.base64
import math
import strconv
import time
import encoding.utf8

// Numeric tokens retain their exact spelling until a schema-selected conversion.
pub struct JsonNumber {
	text string
}

pub type JsonValue = JsonNumber
	| []JsonValue
	| map[string]JsonValue
	| string
	| bool
	| json2.Null
	| int
	| i8
	| i16
	| i32
	| i64
	| u8
	| u16
	| u32
	| u64
	| f32
	| f64

pub fn (value JsonValue) json_str() !string {
	return match value {
		JsonNumber {
			value.text
		}
		[]JsonValue {
			mut items := []string{}
			for item in value {
				items << item.json_str()!
			}
			'[' + items.join(',') + ']'
		}
		map[string]JsonValue {
			mut items := []string{}
			mut keys := value.keys()
			keys.sort()
			for key in keys {
				if !utf8.validate_str(key) { return error('protojson: invalid UTF-8 map key') }
				item := value[key] or { return error('protojson: missing object key') }
				items << json2.encode(key) + ':' + item.json_str()!
			}
			'{' + items.join(',') + '}'
		}
		json2.Null {
			'null'
		}
		string {
			if !utf8.validate_str(value) { return error('protojson: invalid UTF-8 string') }
			json2.encode(value)
		}
		bool {
			json2.encode(value)
		}
		int, i8, i16, i32, i64, u8, u16, u32, u64 {
			value.str()
		}
		f32, f64 {
			if math.is_nan(f64(value)) || math.is_inf(f64(value), 0) {
				return error('protojson: non-finite JSON number')
			}
			value.str()
		}
	}
}

// Runtime support for the generated canonical-JSON (protojson) methods.
// Parsers are tolerant the way the spec demands: 64-bit ints arrive as
// numbers or strings, floats as numbers or "NaN"/"Infinity" strings,
// bytes as standard or URL-safe base64.

// deeper nesting than any real message, far below the ~10k depth where
// json2's recursive parser segfaults (upstream issue)
pub const json_max_depth = 1000

pub fn json_parse(src string) !JsonValue {
	if !utf8.validate_str(src) { return error('protojson: invalid UTF-8 document') }
	json_precheck(src)!
	mut scanner := json2.new_scanner(src)
	value, next := json_read_value(mut scanner, scanner.next()!, 0)!
	if !next.is_eof() { return error('protojson: trailing document data') }
	return value
}

// iterative structural pre-scan, guarding two json2 pathologies on
// hostile input: its recursive parser segfaults around 10k nesting, and
// an unterminated array inside an object (`{"a":[1,21`) loops forever.
// Reject both classes before json2 sees them.
fn json_precheck(src string) ! {
	mut depth := 0
	mut in_str := false
	mut esc := false
	for c in src {
		if in_str {
			if esc {
				esc = false
			} else if c == `\\` {
				esc = true
			} else if c == `"` {
				in_str = false
			}
			continue
		}
		match c {
			`"` {
				in_str = true
			}
			`{`, `[` {
				depth++
				if depth > json_max_depth {
					return error('protojson: nesting deeper than ${json_max_depth}')
				}
			}
			`}`, `]` {
				depth--
				if depth < 0 {
					return error('protojson: unbalanced brackets')
				}
			}
			else {}
		}
	}
	if depth != 0 || in_str {
		return error('protojson: truncated document')
	}
	// A bare top-level scalar (number / true / false / null) or an empty
	// document can drive json2 past EOF and crash its error formatter (a
	// substr underflow, json2 check.v). Containers and strings were just
	// validated as balanced+terminated, so json2 stops at their close; only
	// bare scalars and the empty document need a full check here.
	body := src.trim_space()
	if body == '' {
		return error('protojson: empty JSON document')
	}
	c0 := body[0]
	if c0 != `{` && c0 != `[` && c0 != `"` {
		if body != 'true' && body != 'false' && body != 'null' && !is_json_number(body) {
			return error('protojson: not a valid JSON value')
		}
	}
}

// validates the JSON number grammar: -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?
fn is_json_number(s string) bool {
	mut i := 0
	n := s.len
	if i < n && s[i] == `-` {
		i++
	}
	if i >= n {
		return false
	}
	if s[i] == `0` {
		i++
	} else if s[i] >= `1` && s[i] <= `9` {
		for i < n && s[i] >= `0` && s[i] <= `9` {
			i++
		}
	} else {
		return false
	}
	if i < n && s[i] == `.` {
		i++
		if i >= n || !(s[i] >= `0` && s[i] <= `9`) {
			return false
		}
		for i < n && s[i] >= `0` && s[i] <= `9` {
			i++
		}
	}
	if i < n && (s[i] == `e` || s[i] == `E`) {
		i++
		if i < n && (s[i] == `+` || s[i] == `-`) {
			i++
		}
		if i >= n || !(s[i] >= `0` && s[i] <= `9`) {
			return false
		}
		for i < n && s[i] >= `0` && s[i] <= `9` {
			i++
		}
	}
	return i == n
}

pub fn json_object(a JsonValue) !map[string]JsonValue {
	if a is map[string]JsonValue {
		return a
	}
	return error('protojson: expected object, got ${a.type_name()}')
}

pub fn json_array(a JsonValue) ![]JsonValue {
	if a is []JsonValue {
		return a
	}
	return error('protojson: expected array, got ${a.type_name()}')
}

pub fn json_stringv(a JsonValue) !string {
	if a is string {
		// clone: json2's string values can be windows into a decode buffer that
		// is later reused, which corrupts map/repeated string values once the
		// parse returns (observed on macOS/arm64). An owned copy is safe.
		return a.clone()
	}
	return error('protojson: expected string, got ${a.type_name()}')
}

pub fn json_boolv(a JsonValue) !bool {
	if a is bool {
		return a
	}
	return error('protojson: expected bool, got ${a.type_name()}')
}

pub fn json_intv(a JsonValue) !i64 {
	match a {
		JsonNumber {
			return json_key_i64(json_integral_text(a.text)!)
		}
		i64 {
			return a
		}
		int {
			return a
		}
		i32 {
			return i64(a)
		}
		i16 {
			return i64(a)
		}
		i8 {
			return i64(a)
		}
		u8 {
			return i64(a)
		}
		u16 {
			return i64(a)
		}
		u32 {
			return i64(a)
		}
		u64 {
			if a > 9223372036854775807 {
				return error('protojson: integer out of i64 range')
			}
			return i64(a)
		}
		f64 {
			// casting non-finite or out-of-range floats to i64 is UB in C
			if math.is_nan(a) || math.is_inf(a, 0) || a < -9.223372036854776e18
				|| a >= 9.223372036854776e18 {
				return error('protojson: number out of integer range')
			}
			if a != f64(i64(a)) {
				return error('protojson: expected integer, got fraction')
			}
			return i64(a)
		}
		f32 {
			return json_intv(JsonValue(f64(a)))
		}
		string {
			return json_key_i64(json_integral_text(a)!)
		}
		else {
			return error('protojson: expected integer, got ${a.type_name()}')
		}
	}
}

pub fn json_uintv(a JsonValue) !u64 {
	match a {
		JsonNumber {
			return json_key_u64(json_integral_text(a.text)!)
		}
		u64 {
			return a
		}
		u32 {
			return u64(a)
		}
		u16 {
			return u64(a)
		}
		u8 {
			return u64(a)
		}
		i64, int, i32, i16, i8 {
			v := json_intv(a)!
			if v < 0 {
				return error('protojson: negative value for unsigned field')
			}
			return u64(v)
		}
		f64 {
			if math.is_nan(a) || math.is_inf(a, 0) || a < 0 || a >= 1.8446744073709552e19 {
				return error('protojson: number out of unsigned range')
			}
			if a != f64(u64(a)) {
				return error('protojson: expected unsigned integer')
			}
			return u64(a)
		}
		f32 {
			return json_uintv(JsonValue(f64(a)))
		}
		string {
			return json_key_u64(json_integral_text(a)!)
		}
		else {
			return error('protojson: expected unsigned integer, got ${a.type_name()}')
		}
	}
}

pub fn json_floatv(a JsonValue) !f64 {
	match a {
		JsonNumber {
			value := strconv.atof64(a.text)!
			if math.is_nan(value) || math.is_inf(value, 0) {
				return error('protojson: floating point overflow')
			}
			return value
		}
		f64 {
			return a
		}
		f32 {
			return f64(a)
		}
		i64 {
			return f64(a)
		}
		int {
			return f64(a)
		}
		i32 {
			return f64(a)
		}
		i16 {
			return f64(a)
		}
		i8 {
			return f64(a)
		}
		u64 {
			return f64(a)
		}
		u32 {
			return f64(a)
		}
		u16 {
			return f64(a)
		}
		u8 {
			return f64(a)
		}
		string {
			match a {
				'NaN' {
					return math.nan()
				}
				'Infinity' {
					return math.inf(1)
				}
				'-Infinity' {
					return math.inf(-1)
				}
				else {
					if a == '' || a.trim_space() != a {
						return error('protojson: invalid float string')
					}
					// The reference reads the first numeric JSON token in a quoted
					// number. Match its delimiters, then validate that token exactly.
					mut end := 0
					for end < a.len && a[end] !in [u8(9), 10, 13, 32, 44, 58, 91, 93, 123, 125] {
						end++
					}
					text := a[..end]
					if !is_json_number(text) {
						return error('protojson: invalid float number grammar')
					}
					return json_floatv(JsonValue(JsonNumber{ text: text }))!
				}
			}
		}
		else {
			return error('protojson: expected number, got ${a.type_name()}')
		}
	}
}

// float fields reject a finite JSON number that overflows f32; only the
// explicit "Infinity"/"-Infinity"/"NaN" strings may yield a non-finite float
pub fn json_f32v(a JsonValue) !f32 {
	v := json_floatv(a)!
	f := f32(v)
	if math.is_inf(f, 0) && !math.is_inf(v, 0) {
		return error('protojson: value out of float range')
	}
	return f
}

// double fields: a non-finite result from a JSON number (as opposed to the
// string spellings) means the literal overflowed the double range
pub fn json_f64v(a JsonValue) !f64 {
	if a is string {
		return json_floatv(a)!
	}
	v := json_floatv(a)!
	if math.is_nan(v) || math.is_inf(v, 0) {
		return error('protojson: value out of double range')
	}
	return v
}

// Strict RFC3339 for a protojson Timestamp: requires the uppercase `T`
// date/time separator and an uppercase `Z` (or a numeric offset), and enforces
// the 0001-01-01..9999-12-31 range. Returns (seconds, nanos). protoc's parser
// rejects lowercase t/z and a space separator, so we do too.
pub fn parse_timestamp_rfc3339(s string) !(i64, int) {
	if s.len < 20 || s[10] != `T` {
		return error('protojson: timestamp needs the uppercase T separator')
	}
	if s.ends_with('z') {
		return error('protojson: timestamp zone must be uppercase Z')
	}
	if s.len > 19 && s[19] == `.` {
		mut end := 20
		for end < s.len && s[end].is_digit() {
			end++
		}
		if end - 20 < 1 || end - 20 > 9 { return error('protojson: invalid timestamp fraction') }
	}
	t := time.parse_rfc3339(s) or { return error('protojson: bad timestamp `${s}`') }
	secs := t.unix()
	if secs < -62135596800 || secs > 253402300799 {
		return error('protojson: timestamp out of range `${s}`')
	}
	return secs, t.nanosecond
}

pub fn json_bytesv(a JsonValue) ![]u8 {
	s := json_stringv(a)!
	// ProtoJSON accepts either alphabet and optional padding. Select padding
	// from the original token, as Go protobuf does; only CR/LF may be ignored.
	url := s.contains('-') || s.contains('_')
	padded := s.len % 4 == 0
	clean := s.replace('\r', '').replace('\n', '')
	mut end := clean.len
	for end > 0 && clean[end - 1] == `=` {
		end--
	}
	padding := clean.len - end
	if padding > 2 || (!padded && padding > 0) || (padded && clean.len % 4 != 0) || end % 4 == 1 {
		return error('protojson: invalid base64')
	}
	if padding > 0 && ((padding == 1 && end % 4 != 3) || (padding == 2 && end % 4 != 2)) {
		return error('protojson: invalid base64 padding')
	}
	for c in clean[..end] {
		if (c >= `A` && c <= `Z`) || (c >= `a` && c <= `z`) || (c >= `0` && c <= `9`) { continue
		 }
		if (url && c in [`-`, `_`]) || (!url && c in [`+`, `/`]) { continue
		 }
		return error('protojson: invalid base64 character or mixed alphabets')
	}
	canonical := clean[..end].replace('-', '+').replace('_', '/') + '='.repeat((4 - end % 4) % 4)
	return base64.decode(canonical)
}

pub fn json_int32v(a JsonValue) !int {
	v := json_intv(a)!
	if v < -2147483648 || v > 2147483647 {
		return error('protojson: value ${v} out of int32 range')
	}
	return int(v)
}

pub fn json_uint32v(a JsonValue) !u32 {
	v := json_uintv(a)!
	if v > 4294967295 {
		return error('protojson: value ${v} out of uint32 range')
	}
	return u32(v)
}

// FieldMask paths: snake_case segments <-> lowerCamel segments
pub fn json_camel_path(p string) string {
	mut segs := []string{}
	for seg in p.split('.') {
		mut out := ''
		mut up := false
		for c in seg {
			if c == `_` {
				up = true
			} else if up {
				out += c.ascii_str().to_upper()
				up = false
			} else {
				out += c.ascii_str()
			}
		}
		segs << out
	}
	return segs.join('.')
}

pub fn json_fieldmask_path(p string) !string {
	for segment in p.split('.') {
		if segment.len == 0 { return error('protojson: empty FieldMask path segment') }
		for index, c in segment {
			if (c >= `A` && c <= `Z`) || (c >= `a` && c <= `z`) || c == `_` { continue
			 }
			if index > 0 && c >= `0` && c <= `9` { continue
			 }
			return error('protojson: invalid FieldMask path')
		}
	}
	camel := json_camel_path(p)
	if json_snake_path(camel) != p { return error('protojson: irreversible FieldMask path') }
	return camel
}

pub fn json_snake_path(p string) string {
	mut segs := []string{}
	for seg in p.split('.') {
		mut out := ''
		for c in seg {
			if c >= `A` && c <= `Z` {
				out += '_' + c.ascii_str().to_lower()
			} else {
				out += c.ascii_str()
			}
		}
		segs << out
	}
	return segs.join('.')
}

// map keys arrive as strings regardless of the proto key type
pub fn json_key_i64(k string) !i64 {
	if k == '-0' { return 0 }
	if k == '' {
		return error('protojson: empty integer string')
	}
	v := strconv.parse_int(k, 10, 64) or { return error('protojson: bad integer `${k}`') }
	// V's parse_int silently clamps on overflow; round-trip to catch it
	if v.str() != k {
		return error('protojson: integer out of range `${k}`')
	}
	return v
}

pub fn json_key_u64(k string) !u64 {
	if k == '-0' { return 0 }
	if k == '' {
		return error('protojson: empty integer string')
	}
	v := strconv.parse_uint(k, 10, 64) or { return error('protojson: bad unsigned `${k}`') }
	if v.str() != k {
		return error('protojson: unsigned out of range `${k}`')
	}
	return v
}

// emission: 64-bit ints are JSON strings (JS numbers lose precision),
// non-finite floats are the spec's string spellings
pub fn json_i64(v i64) JsonValue {
	return JsonValue(v.str())
}

pub fn json_u64(v u64) JsonValue {
	return JsonValue(v.str())
}

// google.protobuf.Value numbers cannot use string forms for nonfinite floats.
pub fn json_value_number(v f64) !JsonValue {
	if math.is_nan(v) || math.is_inf(v, 1) || math.is_inf(v, -1) {
		return error('protojson: nonfinite Value number')
	}
	return JsonValue(v)
}

pub fn json_f64(v f64) JsonValue {
	if math.is_nan(v) {
		return JsonValue('NaN')
	}
	if math.is_inf(v, 1) {
		return JsonValue('Infinity')
	}
	if math.is_inf(v, -1) {
		return JsonValue('-Infinity')
	}
	return JsonValue(v)
}

pub fn json_f32(v f32) JsonValue {
	if math.is_nan(f64(v)) {
		return JsonValue('NaN')
	}
	if math.is_inf(f64(v), 1) {
		return JsonValue('Infinity')
	}
	if math.is_inf(f64(v), -1) {
		return JsonValue('-Infinity')
	}
	return JsonValue(v)
}

pub fn json_b64(b []u8) JsonValue {
	return JsonValue(base64.encode(b))
}

fn json_read_value(mut scanner json2.Scanner, token json2.Token, depth int) !(JsonValue, json2.Token) {
	if depth > json_max_depth { return error('protojson: excessive nesting') }
	match token.kind {
		.lcbr {
			mut result := map[string]JsonValue{}
			mut key := scanner.next()!
			if key.kind == .rcbr { return JsonValue(result), scanner.next()! }
			for {
				if key.kind != .str { return error('protojson: expected object key') }
				name := key.literal().clone()
				if name in result { return error('protojson: duplicate object key') }
				if scanner.next()!.kind != .colon { return error('protojson: expected colon') }
				value, next := json_read_value(mut scanner, scanner.next()!, depth + 1)!
				result[name] = value
				if next.kind == .rcbr { return JsonValue(result), scanner.next()! }
				if next.kind != .comma { return error('protojson: expected object separator') }
				key = scanner.next()!
			}
		}
		.lsbr {
			mut result := []JsonValue{}
			mut first := scanner.next()!
			if first.kind == .rsbr { return JsonValue(result), scanner.next()! }
			for {
				value, next := json_read_value(mut scanner, first, depth + 1)!
				result << value
				if next.kind == .rsbr { return JsonValue(result), scanner.next()! }
				if next.kind != .comma { return error('protojson: expected array separator') }
				first = scanner.next()!
			}
		}
		.str {
			return JsonValue(token.literal().clone()), scanner.next()!
		}
		.null {
			return JsonValue(json2.Null{}), scanner.next()!
		}
		.bool {
			return JsonValue(token.literal() == 'true'), scanner.next()!
		}
		.int, .float {
			return JsonValue(JsonNumber{
				text: token.literal().clone()
			}), scanner.next()!
		}
		else {
			return error('protojson: expected value')
		}
	}

	return error('protojson: expected value')
}

pub fn json_duration_parts(text string) !(i64, int) {
	if !text.ends_with('s') || text.len < 2 { return error('protojson: invalid duration') }
	negative := text.starts_with('-')
	body := text[if negative { 1 } else { 0 }..text.len - 1]
	parts := body.split('.')
	if parts.len > 2 || parts[0].len == 0 { return error('protojson: invalid duration') }
	for part in parts {
		if part.len == 0 { return error('protojson: invalid duration') }
		for digit in part {
			if !digit.is_digit() { return error('protojson: invalid duration') }
		}
	}
	mut seconds := strconv.parse_int(parts[0], 10, 64)!
	if seconds < 0 || seconds > 315576000000 { return error('protojson: duration out of range') }
	mut nanos := 0
	if parts.len == 2 {
		if parts[1].len > 9 { return error('protojson: invalid duration fraction') }
		nanos = (parts[1] + '0'.repeat(9 - parts[1].len)).int()
	}
	if negative {
		seconds = -seconds
		nanos = -nanos
	}
	return seconds, nanos
}

pub fn json_mask_path(path string) !string {
	if path == '' { return error('protojson: empty field mask path') }
	for segment in path.split('.') {
		if segment == '' { return error('protojson: empty field mask segment') }
		for index, c in segment {
			if !(c >= `a` && c <= `z`) && !(c >= `A` && c <= `Z`) && !(index > 0 && c.is_digit()) {
				return error('protojson: invalid field mask segment')
			}
		}
	}
	return json_snake_path(path)
}

// Convert decimal/exponent integer spellings exactly, without an intermediate
// float that can erase fractions or round across a 64-bit boundary.
fn json_integral_text(text string) !string {
	if !is_json_number(text) { return error('protojson: invalid integer spelling') }
	negative := text.starts_with('-')
	body := if negative { text[1..] } else { text }
	parts := body.to_lower().split('e')
	coefficient := parts[0]
	decimal := coefficient.index('.') or { coefficient.len }
	digits := coefficient.replace('.', '')
	if digits.trim_left('0') == '' { return '0' }
	mut exponent := 0
	if parts.len == 2 {
		value := parts[1]
		if value.trim_left('+-0').len > 3 {
			return error('protojson: integer exponent out of range')
		}
		exponent = value.int()
		if exponent < -1000 || exponent > 1000 {
			return error('protojson: integer exponent out of range')
		}
	}
	position := decimal + exponent
	if position <= 0 { return error('protojson: fractional integer') }
	mut integer := ''
	if position < digits.len {
		if digits[position..].trim_left('0') != '' { return error('protojson: fractional integer') }
		integer = digits[..position]
	} else {
		if position - digits.len > 20 { return error('protojson: integer out of range') }
		integer = digits + '0'.repeat(position - digits.len)
	}
	integer = integer.trim_left('0')
	if integer.len > 20 { return error('protojson: integer out of range') }
	return if negative { '-' + integer } else { integer }
}
