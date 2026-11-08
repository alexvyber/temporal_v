module main

import temporal_v.sdk
import os
import x.json2

struct BaggageResult {
	members   []sdk.BaggageMember
	header    string
	has_error bool @[json: 'error']
}

fn baggage_contexts() ! {
	cases := json2.decode[[]string](os.read_file(os.args[2])!)!
	for input in cases {
		result := sdk.parse_baggage(input)
		println(json2.encode(BaggageResult{
			members:   result.baggage.members()
			header:    result.baggage.header()
			has_error: result.invalid_members > 0 || result.truncated
		}))
	}
}

fn baggage_validation() ! {
	mut properties := [sdk.BaggageProperty{ key: 'flag' }, sdk.BaggageProperty{
		key:       'empty'
		has_value: true
	}, sdk.BaggageProperty{ key: 'metadata', value: '世界 ;,%', has_value: true }]
	initial := sdk.new_baggage([
		sdk.BaggageMember{ key: 'tenant', value: 'one', properties: properties },
	])!
	properties[0] = sdk.BaggageProperty{
		key: 'changed'
	}
	if initial.header() != 'tenant=one;flag;empty=;metadata=%E4%B8%96%E7%95%8C%20%3B%2C%25' {
		return error('baggage snapshot or escaping changed')
	}
	changed := initial.with_member(sdk.BaggageMember{ key: 'tenant', value: 'two' })!
	if initial.member('tenant') or { return error('initial baggage missing') }.value != 'one' || changed.member('tenant') or {
		return error('changed baggage missing')
	}.value != 'two' {
		return error('baggage update mutated its parent')
	}
	if changed.without_member('tenant').header() != '' || changed.header() != 'tenant=two' {
		return error('baggage deletion mutated its parent')
	}
	for member in [sdk.BaggageMember{ key: 'bad key', value: 'one' },
		sdk.BaggageMember{
			key:        'key'
			properties: [
				sdk.BaggageProperty{
					key:   'property'
					value: 'data'
				},
			]
		}] {
		if _ := sdk.new_baggage([member]) { return error('invalid baggage member accepted') }
	}
	if _ := sdk.new_baggage([sdk.BaggageMember{ key: 'key', value: 'x'.repeat(8192) }]) {
		return error('oversized baggage admitted')
	}
	partial := sdk.parse_baggage('first=valid,bad,second=valid')
	if partial.invalid_members != 1 || partial.baggage.members().len != 2 {
		return error('invalid baggage member affected valid neighbors')
	}
	if sdk.extract_baggage({
		'Baggage': 'first=one'
		'baggage': 'second=two'
	}).header() != 'first=one,second=two' {
		return error('case-insensitive baggage extraction changed')
	}
	println('BAGGAGE_OK')
}
