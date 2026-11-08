module main

import sdk
import temporal
import os
import example_converter
import example_storage

struct PanicCodec {}

@[noinline]
fn (codec PanicCodec) encode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	if context.kind == .activity {
		for value in values {
			if value.data.bytestr().trim('"') == 'converter-output' { panic('output codec panic') }
		}
	}
	return values
}

@[noinline]
fn (codec PanicCodec) decode(values []temporal.TemporalApiCommonV1_Payload, context sdk.SerializationContext) ![]temporal.TemporalApiCommonV1_Payload {
	if context.kind == .activity {
		for value in values {
			if value.data.bytestr().trim('"').ends_with('-input') { panic('input codec panic') }
		}
	}
	return values
}

struct PanicFailureConverter {
	inner sdk.FailureConverter
}

@[noinline]
fn (encoder PanicFailureConverter) to_failure(err IError, converter sdk.DataConverter, category string) !temporal.TemporalApiFailureV1_Failure {
	if converter.serialization_context.kind == .activity && err.msg() == 'failure converter input' {
		panic('failure converter panic')
	}
	return encoder.inner.to_failure(err, converter, category)!
}

fn (decoder PanicFailureConverter) from_failure(value temporal.TemporalApiFailureV1_Failure, converter sdk.DataConverter) sdk.Failure {
	return decoder.inner.from_failure(value, converter)
}

fn panic_converter() !sdk.DataConverter {
	mut converter := example_converter.from_env()!
	if os.getenv('V_PANIC_STORAGE_DRIVER') == 'true' {
		driver := sdk.StorageDriver(example_storage.FileStorageDriver{
			root: os.getenv('V_SDK_EXAMPLE_STORAGE_DIR')
		})
		storage := sdk.new_external_storage(
			drivers:                   [
				sdk.StorageDriver(PanicStorageDriver{
					inner: driver
				}),
			]
			threshold_bytes:           1
			max_concurrent_stores:     1
			max_concurrent_retrievals: 1
		)!
		converter = sdk.DataConverter{
			...converter
			external_storage: storage
		}
	}
	if os.getenv('V_PANIC_EXTENSIONS') != 'true' { return converter }
	mut codecs := [sdk.PayloadCodec(PanicCodec{})]
	codecs << converter.codecs
	return sdk.DataConverter{
		...converter
		codecs:            codecs
		failure_converter: PanicFailureConverter{
			inner: converter.failure_converter
		}
	}
}

struct PanicStorageDriver {
	inner sdk.StorageDriver
}

fn (driver PanicStorageDriver) name() string {
	return driver.inner.name()
}

fn (driver PanicStorageDriver) driver_type() string {
	return driver.inner.driver_type()
}

@[noinline]
fn (driver PanicStorageDriver) store(context sdk.StorageContext, values []temporal.TemporalApiCommonV1_Payload) ![]sdk.StorageClaim {
	if context.target.workflow_id.ends_with('-store')
		&& context.field_paths.any(it.contains('activity_result.Success.result')) {
		panic('storage store panic')
	}
	return driver.inner.store(context, values)!
}

@[noinline]
fn (driver PanicStorageDriver) retrieve(context sdk.StorageContext, claims []sdk.StorageClaim) ![]temporal.TemporalApiCommonV1_Payload {
	if context.target.activity_id != '' && context.target.workflow_id.ends_with('-retrieve') {
		panic('storage retrieve panic')
	}
	return driver.inner.retrieve(context, claims)!
}
