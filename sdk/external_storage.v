module sdk

import corebridge
import encoding.utf8
import json2
import protobuf
import temporal
import time

// Drivers must be safe for concurrent calls, preserve payload metadata, and
// return results in input order. References are durable history: the driver owns
// retention, access control, integrity checks and its network timeout.
pub interface StorageDriver {
	name() string
	driver_type() string
	store(context StorageContext, values []temporal.TemporalApiCommonV1_Payload) ![]StorageClaim
	retrieve(context StorageContext, claims []StorageClaim) ![]temporal.TemporalApiCommonV1_Payload
}

pub struct StorageClaim {
pub:
	data map[string]string
}

pub struct StorageContext {
pub:
	target      SerializationContext
	run_id      string
	field_paths []string
	deadline    time.Time
}

// Returning an empty driver name keeps a payload inline. The default policy
// offloads payloads at the threshold to the first registered driver.
pub interface StorageSelector {
	select_driver(context StorageContext, payload temporal.TemporalApiCommonV1_Payload) !string
}

@[params]
pub struct ExternalStorageOptions {
pub:
	drivers                   []StorageDriver
	selector                  ?StorageSelector
	threshold_bytes           int           = 256 * 1024
	max_concurrent_stores     int           = 16
	max_concurrent_retrievals int           = 16
	operation_timeout         time.Duration = 30 * time.second
}

@[heap]
pub struct ExternalStorage {
	options        ExternalStorageOptions
	drivers        map[string]StorageDriver
	store_slots    chan bool
	retrieve_slots chan bool
}

pub struct ExternalStorageError {
pub:
	phase       string
	driver_name string
	message     string
}

pub fn (err ExternalStorageError) msg() string {
	return 'TMPRL1105 external storage ${err.phase}: ${err.message}'
}

pub fn (err ExternalStorageError) code() int {
	return 500
}

pub fn new_external_storage(options ExternalStorageOptions) !&ExternalStorage {
	if options.drivers.len == 0 { return error('external storage requires at least one driver') }
	if options.threshold_bytes < 1 || options.max_concurrent_stores < 1
		|| options.max_concurrent_retrievals < 1 || options.operation_timeout <= 0 {
		return error('external storage limits must be positive')
	}
	mut drivers := map[string]StorageDriver{}
	for driver in options.drivers {
		name := driver.name()
		if !utf8.validate_str(name) || name.contains('\x00') {
			return error('invalid storage driver name')
		}
		if !utf8.validate_str(driver.driver_type()) || driver.driver_type().contains('\x00') {
			return error('invalid storage driver type')
		}
		if name == '' || driver.driver_type() == '' || name in drivers {
			return error('invalid or duplicate storage driver')
		}
		drivers[name] = driver
	}
	return &ExternalStorage{
		options:        options
		drivers:        drivers
		store_slots:    chan bool{cap: options.max_concurrent_stores}
		retrieve_slots: chan bool{cap: options.max_concurrent_retrievals}
	}
}

struct ExternalStorageReference {
	driver_name string            @[json: driverName]
	claim_data  map[string]string @[json: claimData]
}

fn is_external_reference(payload temporal.TemporalApiCommonV1_Payload) bool {
	return payload.metadata['encoding'].bytestr() == 'json/protobuf'
		&& payload.metadata['messageType'].bytestr() == 'temporal.api.sdk.v1.ExternalStorageReference'
}

fn parse_external_reference(payload temporal.TemporalApiCommonV1_Payload) !ExternalStorageReference {
	if !is_external_reference(payload) {
		return ExternalStorageError{
			phase:   'reference'
			message: 'expected a reference payload'
		}
	}
	// Decode as JSON first so malformed or incorrectly typed claim maps cannot
	// be defaulted into a driver request. Do not include claim data in errors.
	if !utf8.validate_str(payload.data.bytestr()) {
		return ExternalStorageError{
			phase:   'reference'
			message: 'invalid reference UTF-8'
		}
	}
	value := protobuf.json_parse(payload.data.bytestr()) or {
		return ExternalStorageError{
			phase:   'reference'
			message: 'invalid reference JSON'
		}
	}
	if value !is map[string]protobuf.JsonValue {
		return ExternalStorageError{
			phase:   'reference'
			message: 'expected a reference object'
		}
	}
	object := value as map[string]protobuf.JsonValue
	if 'driverName' in object && 'driver_name' in object {
		return ExternalStorageError{
			phase:   'reference'
			message: 'duplicate driver name'
		}
	}
	if 'claimData' in object && 'claim_data' in object {
		return ExternalStorageError{
			phase:   'reference'
			message: 'duplicate claim map'
		}
	}
	name := object['driverName'] or {
		object['driver_name'] or {
			return ExternalStorageError{
				phase:   'reference'
				message: 'missing driver name'
			}
		}
	}
	if name !is string {
		return ExternalStorageError{
			phase:   'reference'
			message: 'invalid driver name'
		}
	}
	if name == '' {
		return ExternalStorageError{
			phase:   'reference'
			message: 'empty driver name'
		}
	}
	mut claims := map[string]string{}
	claim := object['claimData'] or {
		object['claim_data'] or { protobuf.JsonValue(map[string]protobuf.JsonValue{}) }
	}
	if claim !is map[string]protobuf.JsonValue {
		return ExternalStorageError{
			phase:   'reference'
			message: 'invalid claim map'
		}
	}
	for key, item in claim as map[string]protobuf.JsonValue {
		if item !is string {
			return ExternalStorageError{
				phase:   'reference'
				message: 'invalid claim value'
			}
		}
		claims[key] = item as string
	}
	return ExternalStorageReference{
		driver_name: name as string
		claim_data:  claims
	}
}

fn storage_permit(slots chan bool, deadline time.Time) ! {
	remaining := deadline - time.now()
	if remaining <= 0 {
		return ExternalStorageError{
			phase:   'limit'
			message: 'operation deadline exceeded'
		}
	}
	select {
		slots <- true {}
		remaining {
			return ExternalStorageError{
				phase:   'limit'
				message: 'operation deadline exceeded while waiting for a permit'
			}
		}
	}
}

fn (storage &ExternalStorage) context(context StorageContext) StorageContext {
	deadline := time.now().add(storage.options.operation_timeout)
	if context.deadline.unix() > 0 && context.deadline < deadline { return context }
	return StorageContext{
		...context
		deadline: deadline
	}
}

fn (storage &ExternalStorage) store_payloads(values []temporal.TemporalApiCommonV1_Payload, initial_context StorageContext) ![]temporal.TemporalApiCommonV1_Payload {
	context := storage.context(initial_context)
	mut result := values.clone()
	mut groups := map[string][]int{}
	for index, payload in values {
		if is_external_reference(payload) {
			parse_external_reference(payload)!
			continue
		}
		mut name := ''
		if selector := storage.options.selector {
			name = selector.select_driver(context, payload) or {
				return ExternalStorageError{
					phase:   'select'
					message: 'driver selection failed'
				}
			}
		} else if payload.encoded_size() >= storage.options.threshold_bytes {
			name = storage.options.drivers[0].name()
		}
		if name == '' {
			continue
		}
		if name !in storage.drivers {
			return ExternalStorageError{
				phase:       'select'
				driver_name: name
				message:     'selected driver is not registered'
			}
		}
		groups[name] << index
	}
	for name, indexes in groups {
		driver := storage.drivers[name] or {
			return ExternalStorageError{
				phase:   'store'
				message: 'missing driver'
			}
		}
		mut batch := []temporal.TemporalApiCommonV1_Payload{}
		mut paths := []string{}
		for index in indexes {
			batch << values[index]
			if index < context.field_paths.len { paths << context.field_paths[index] }
		}
		storage_permit(storage.store_slots, context.deadline)!
		claims := storage.store_batch(driver, StorageContext{ ...context, field_paths: paths },
			batch)!
		if claims.len != indexes.len {
			return ExternalStorageError{
				phase:       'store'
				driver_name: name
				message:     'driver changed payload count'
			}
		}
		for offset, index in indexes {
			result[index] = temporal.TemporalApiCommonV1_Payload{
				metadata:          {
					'encoding':    'json/protobuf'.bytes()
					'messageType': 'temporal.api.sdk.v1.ExternalStorageReference'.bytes()
				}
				data:              json2.encode(ExternalStorageReference{
					driver_name: name
					claim_data:  claims[offset].data
				}).bytes()
				external_payloads: [
					temporal.TemporalApiCommonV1_Payload_ExternalPayloadDetails{
						size_bytes: i64(values[index].encoded_size())
					},
				]
			}
		}
	}
	return result
}

fn (storage &ExternalStorage) store_batch(driver StorageDriver, context StorageContext, values []temporal.TemporalApiCommonV1_Payload) ![]StorageClaim {
	defer { _ := <-storage.store_slots }
	claims := driver.store(context, values) or {
		return ExternalStorageError{
			phase:       'store'
			driver_name: driver.name()
			message:     'driver failed'
		}
	}
	if time.now() > context.deadline {
		return ExternalStorageError{
			phase:       'store'
			driver_name: driver.name()
			message:     'operation deadline exceeded'
		}
	}
	return claims
}

fn (storage &ExternalStorage) retrieve_payloads(values []temporal.TemporalApiCommonV1_Payload, initial_context StorageContext) ![]temporal.TemporalApiCommonV1_Payload {
	context := storage.context(initial_context)
	mut result := values.clone()
	mut groups := map[string][]int{}
	mut references := map[int]ExternalStorageReference{}
	for index, payload in values {
		if !is_external_reference(payload) {
			continue
		}
		reference := parse_external_reference(payload)!
		if reference.driver_name !in storage.drivers {
			return ExternalStorageError{
				phase:       'retrieve'
				driver_name: reference.driver_name
				message:     'reference driver is not registered'
			}
		}
		references[index] = reference
		groups[reference.driver_name] << index
	}
	for name, indexes in groups {
		driver := storage.drivers[name] or {
			return ExternalStorageError{
				phase:   'retrieve'
				message: 'missing driver'
			}
		}
		mut claims := []StorageClaim{}
		mut paths := []string{}
		for index in indexes {
			claims << StorageClaim{
				data: references[index].claim_data
			}
			if index < context.field_paths.len { paths << context.field_paths[index] }
		}
		storage_permit(storage.retrieve_slots, context.deadline)!
		batch := storage.retrieve_batch(driver, StorageContext{ ...context, field_paths: paths },
			claims)!
		if batch.len != indexes.len {
			return ExternalStorageError{
				phase:       'retrieve'
				driver_name: name
				message:     'driver changed payload count'
			}
		}
		for offset, index in indexes {
			if is_external_reference(batch[offset]) {
				return ExternalStorageError{
					phase:       'retrieve'
					driver_name: name
					message:     'driver returned a nested reference'
				}
			}
			result[index] = batch[offset]
		}
	}
	return result
}

fn (storage &ExternalStorage) retrieve_batch(driver StorageDriver, context StorageContext, claims []StorageClaim) ![]temporal.TemporalApiCommonV1_Payload {
	defer { _ := <-storage.retrieve_slots }
	batch := driver.retrieve(context, claims) or {
		return ExternalStorageError{
			phase:       'retrieve'
			driver_name: driver.name()
			message:     'driver failed'
		}
	}
	if time.now() > context.deadline {
		return ExternalStorageError{
			phase:       'retrieve'
			driver_name: driver.name()
			message:     'operation deadline exceeded'
		}
	}
	return batch
}

fn (converter DataConverter) retrieve_payloads(values []temporal.TemporalApiCommonV1_Payload) ![]temporal.TemporalApiCommonV1_Payload {
	if storage := converter.external_storage {
		return storage.retrieve_payloads(values, StorageContext{
			target: converter.serialization_context
		})!
	}
	for payload in values {
		if is_external_reference(payload) {
			return ExternalStorageError{
				phase:   'retrieve'
				message: 'external storage is not configured'
			}
		}
	}
	return values
}

fn (converter DataConverter) storage_message(kind corebridge.PayloadMessageKind, input []u8, store bool, context StorageContext, options corebridge.PayloadMessageOptions) ![]u8 {
	if store && converter.external_storage == none { return input }
	message := corebridge.extract_payloads(kind, input, options)!
	if message.values.len == 0 { return input }
	values := if storage := converter.external_storage {
		if store {
			storage.store_message_payloads(message.values, message.paths, context)!
		} else {
			storage.retrieve_payloads(message.values, StorageContext{
				...context
				field_paths: message.paths
			})!
		}
	} else {
		converter.retrieve_payloads(message.values)!
	}
	if values == message.values { return input }
	return corebridge.replace_payloads(kind, message, values, options)!
}

// Propagation must remain readable by the next SDK before application storage
// retrieval. The marker is reserved metadata and applies only to header fields;
// application memo values retain ordinary storage behavior.
fn (storage &ExternalStorage) store_message_payloads(values []temporal.TemporalApiCommonV1_Payload, paths []string, context StorageContext) ![]temporal.TemporalApiCommonV1_Payload {
	if values.len != paths.len { return error('payload visitor path count differs from values') }
	mut selected := []temporal.TemporalApiCommonV1_Payload{}
	mut selected_paths := []string{}
	mut positions := []int{}
	for index, value in values {
		path := paths[index]
		header := path.ends_with('.Header.fields') || path.ends_with('.headers')
			|| path.ends_with('.header_fields')
		if header && value.metadata['temporal-v/propagation'].bytestr() == '1'
			&& value.metadata['encoding'].bytestr() == 'json/plain' {
			continue
		}
		selected << value
		selected_paths << path
		positions << index
	}
	if selected.len == 0 { return values }
	stored := storage.store_payloads(selected, StorageContext{
		...context
		field_paths: selected_paths
	})!
	mut result := values.clone()
	for index, position in positions {
		result[position] = stored[index]
	}
	return result
}
