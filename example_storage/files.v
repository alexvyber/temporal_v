module example_storage

import alexvyber.temporal_v.sdk
import alexvyber.temporal_v.temporal
import os
import x.json2
import crypto.sha256
import crypto.rand
import time

// Disposable integration fixture, not a production filesystem storage service.
pub struct FileStorageDriver {
pub:
	root string
}

fn (driver FileStorageDriver) name() string {
	return 'fixture.files'
}

fn (driver FileStorageDriver) driver_type() string {
	return 'fixture.filesystem'
}

fn (driver FileStorageDriver) store(context sdk.StorageContext, values []temporal.TemporalApiCommonV1_Payload) ![]sdk.StorageClaim {
	driver.record('store-start', context, values.len)!
	defer { driver.record('store-end', context, values.len) or {} }
	if os.exists(os.join_path(driver.root, 'fail-store'))
		|| (os.exists(os.join_path(driver.root, 'fail-activity-store'))
		&& context.field_paths.any(it.contains('activity_result.Success.result')))
		|| (os.exists(os.join_path(driver.root, 'fail-workflow-store'))
		&& context.field_paths.any(it.contains('CompleteWorkflowExecution.result'))) {
		return error('disposable storage outage with private-claim-credentials')
	}
	delay := os.getenv('V_SDK_EXAMPLE_STORAGE_DELAY_MS').int()
	if delay > 0 { time.sleep(delay * time.millisecond) }
	mut result := []sdk.StorageClaim{}
	for value in values {
		bytes := value.encode()
		key := sha256.sum(bytes).hex()
		path := os.join_path(driver.root, key)
		if !os.exists(path) {
			temporary := path + '.pending-' + rand.bytes(16)!.hex()
			defer { os.rm(temporary) or {} }
			os.write_file(temporary, bytes.bytestr())!
			os.chmod(temporary, 0o600)!
			os.rename(temporary, path)!
		}
		result << sdk.StorageClaim{
			data: {
				'key': key
			}
		}
	}
	mut log := os.open_append(os.join_path(driver.root, 'stores.log'))!
	defer { log.close() }
	log.writeln('${values.len}')!
	return result
}

fn (driver FileStorageDriver) retrieve(context sdk.StorageContext, claims []sdk.StorageClaim) ![]temporal.TemporalApiCommonV1_Payload {
	if os.exists(os.join_path(driver.root, 'fail-retrieve')) {
		return error('disposable retrieval outage with private-claim-credentials')
	}
	mut result := []temporal.TemporalApiCommonV1_Payload{}
	for claim in claims {
		key := claim.data['key']
		if key.len != 64 || key.bytes().any(!((it >= `0` && it <= `9`) || (it >= `a` && it <= `f`))) {
			return error('invalid fixture storage key')
		}
		bytes := os.read_bytes(os.join_path(driver.root, key))!
		if sha256.sum(bytes).hex() != key { return error('fixture storage checksum mismatch') }
		result << temporal.TemporalApiCommonV1_Payload.decode(bytes)!
	}
	return result
}

pub fn from_env(converter sdk.DataConverter) !sdk.DataConverter {
	directory := os.getenv('V_SDK_EXAMPLE_STORAGE_DIR')
	if directory == '' { return converter }
	os.mkdir_all(directory) or {
		// Separate workers and clients can initialize the shared fixture path
		// concurrently. Success requires the directory to exist after the call.
		if !os.is_dir(directory) { return err }
	}
	storage := sdk.new_external_storage(
		drivers:                   [
			sdk.StorageDriver(FileStorageDriver{
				root: directory
			}),
		]
		threshold_bytes:           256 * 1024
		max_concurrent_stores:     2
		max_concurrent_retrievals: 2
	)!
	return sdk.DataConverter{
		...converter
		external_storage: storage
	}
}

fn (driver FileStorageDriver) record(action string, context sdk.StorageContext, count int) ! {
	line := json2.encode({
		'action':        json2.Any(action)
		'pid':           json2.Any(os.getpid())
		'count':         json2.Any(count)
		'namespace':     json2.Any(context.target.namespace)
		'workflow_id':   json2.Any(context.target.workflow_id)
		'workflow_type': json2.Any(context.target.workflow_type)
		'run_id':        json2.Any(context.run_id)
		'paths':         json2.Any(context.field_paths.map(json2.Any(it)))
	})
	mut file := os.open_append(os.join_path(driver.root, 'operations.log'))!
	defer { file.close() }
	file.write_string(line + '\n')!
}
