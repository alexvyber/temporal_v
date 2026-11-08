module main

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import sync
import time

@[heap]
struct RetrievalDriver {
mut:
	mutex  &sync.Mutex = sync.new_mutex()
	active int
	peak   int
	mode   string
}

fn (driver &RetrievalDriver) name() string {
	return 'test'
}

fn (driver &RetrievalDriver) driver_type() string {
	return 'test.memory'
}

fn (driver &RetrievalDriver) store(context sdk.StorageContext, values []temporal.TemporalApiCommonV1_Payload) ![]sdk.StorageClaim {
	return error('unexpected store')
}

fn (driver &RetrievalDriver) retrieve(context sdk.StorageContext, claims []sdk.StorageClaim) ![]temporal.TemporalApiCommonV1_Payload {
	mut state := unsafe { driver }
	state.mutex.lock()
	state.active++
	if state.active > state.peak { state.peak = state.active }
	state.mutex.unlock()
	defer {
		state.mutex.lock()
		state.active--
		state.mutex.unlock()
	}
	time.sleep(10 * time.millisecond)
	if driver.mode == 'failure' { return error('private-claim-credentials') }
	if driver.mode == 'count' { return []temporal.TemporalApiCommonV1_Payload{} }
	if driver.mode == 'nested' { return [reference('{"driverName":"test"}')] }
	return claims.map(temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding': 'json/plain'.bytes()
		}
		data:     it.data['value'].bytes()
	})
}

fn reference(data string) temporal.TemporalApiCommonV1_Payload {
	return temporal.TemporalApiCommonV1_Payload{
		metadata: {
			'encoding':    'json/protobuf'.bytes()
			'messageType': 'temporal.api.sdk.v1.ExternalStorageReference'.bytes()
		}
		data:     data.bytes()
	}
}

fn decode_reference(converter sdk.DataConverter, results chan bool) {
	values := converter.decode_values([
		reference('{"claimData":{"value":"42"},"driverName":"test"}'),
	]) or {
		results <- false
		return
	}
	results <- (values == ['42'])
}

fn test_external_reference_retrieval_limits_and_permit_recovery() ! {
	mut driver := &RetrievalDriver{}
	storage := sdk.new_external_storage(
		drivers:                   [sdk.StorageDriver(driver)]
		max_concurrent_retrievals: 2
	)!
	converter := sdk.DataConverter{
		external_storage: storage
	}
	results := chan bool{cap: 24}
	for _ in 0 .. 24 {
		spawn decode_reference(converter, results)
	}
	for _ in 0 .. 24 {
		assert <-results
	}
	assert driver.peak == 2
	assert driver.active == 0
	for mode in ['failure', 'count', 'nested'] {
		driver.mode = mode
		if _ := converter.decode_values([
			reference('{"driverName":"test","claimData":{}}'),
		])
		{
			panic('invalid driver response accepted')
		} else {
			assert err is sdk.ExternalStorageError
			assert !err.msg().contains('private-claim')
		}
	}
	driver.mode = ''
	assert converter.decode_values([
		reference('{"driverName":"test","claimData":{"value":"true"}}'),
	])! == ['true']
	assert converter.decode_values([
		reference('{"driver_name":"test","claim_data":{"value":"true"}}'),
	])! == ['true']
	assert converter.decode_values([
		reference('{"driverName":"test","claimData":{"_type":"DriverClaim","value":"true"}}'),
	])! == ['true']
	assert driver.active == 0
}

fn test_missing_drivers_and_malformed_references_are_not_application_payloads() ! {
	plain := sdk.DataConverter{}
	if _ := plain.decode_values([reference('{"driverName":"test"}')]) {
		panic('missing external storage accepted')
	}
	storage := sdk.new_external_storage(drivers: [sdk.StorageDriver(&RetrievalDriver{})])!
	converter := sdk.DataConverter{
		external_storage: storage
	}
	for malformed in [[u8(255)].bytestr(), '{"driverName":"test","driver_name":"test"}',
		'{"driverName":"test","claimData":{},"claim_data":{}}', 'null', '[]', '{}',
		'{"driverName":"test","driverName":"test"}',
		'{"driverName":"test","claimData":{"value":"true","value":"false"}}',
		'{"driverName":"test","claimData":[1,2', '{"driverName":5}', '{"driverName":""}',
		'{"driverName":"test","claimData":{"secret":5}}', '{"driverName":"unknown"}'] {
		if _ := converter.decode_values([reference(malformed)]) {
			panic('malformed reference accepted: ${malformed}')
		}
	}
}
