module main

import sdk
import temporal
import os
import example_storage
import example_converter

struct SideEffectStorage {
	root string
}

fn (driver SideEffectStorage) name() string {
	return 'side-effect.files'
}

fn (driver SideEffectStorage) driver_type() string {
	return 'side-effect.fixture'
}

fn (driver SideEffectStorage) store(context sdk.StorageContext, values []temporal.TemporalApiCommonV1_Payload) ![]sdk.StorageClaim {
	if context.target.workflow_id == '' {
		return error('side effect storage write has no workflow scope')
	}
	delegate := sdk.StorageDriver(example_storage.FileStorageDriver{
		root: driver.root
	})
	claims := delegate.store(context, values)!
	mut result := []sdk.StorageClaim{}
	for claim in claims {
		mut data := claim.data.clone()
		data['workflow'] = context.target.workflow_id
		data['namespace'] = context.target.namespace
		data['_type'] = 'SideEffectClaim'
		result << sdk.StorageClaim{
			data: data
		}
	}
	return result
}

fn (driver SideEffectStorage) retrieve(context sdk.StorageContext, claims []sdk.StorageClaim) ![]temporal.TemporalApiCommonV1_Payload {
	for claim in claims {
		if context.target.kind != .workflow || context.target.workflow_id == ''
			|| context.target.workflow_id != claim.data['workflow']
			|| context.target.namespace != claim.data['namespace'] {
			return error('side effect storage read used a different workflow scope')
		}
	}
	mut file := os.open_append(os.join_path(driver.root, 'retrievals.log'))!
	file.writeln(context.target.workflow_id)!
	file.close()
	delegate := sdk.StorageDriver(example_storage.FileStorageDriver{
		root: driver.root
	})
	return delegate.retrieve(context, claims)!
}

fn converter() !sdk.DataConverter {
	mut base := example_converter.from_env()!
	profile := os.getenv('V_EFFECT_COUNTS')
	if profile == 'pack' {
		base = sdk.DataConverter{
			...base
			codecs: [sdk.PayloadCodec(PackCodec{})]
		}
	}
	if profile == 'split-pack' {
		base = sdk.DataConverter{
			...base
			codecs: [sdk.PayloadCodec(SplitCodec{}), sdk.PayloadCodec(PackCodec{})]
		}
	}
	if profile == 'pack-split-pack' {
		base = sdk.DataConverter{
			...base
			codecs: [sdk.PayloadCodec(PackCodec{}), sdk.PayloadCodec(SplitCodec{}),
				sdk.PayloadCodec(PackCodec{})]
		}
	}
	root := os.getenv('V_SDK_EXAMPLE_STORAGE_DIR')
	if root == '' { return base }
	storage := sdk.new_external_storage(
		drivers: [
			sdk.StorageDriver(SideEffectStorage{
				root: root
			}),
		]
	)!
	return sdk.DataConverter{
		...base
		external_storage: storage
	}
}
