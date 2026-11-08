module sdk

import crypto.sha256
import encoding.utf8
import strconv

pub const default_version = -1
const numeric_version_patch_prefix = '__temporal_v_version/v1/'

fn version_patch_prefix(change_id string) string {
	return numeric_version_patch_prefix + sha256.sum(change_id.bytes()).hex() + '/'
}

fn (context &WorkflowContext) recorded_patch_versions(change_id string) ![]int {
	prefix := version_patch_prefix(change_id)
	mut versions := []int{}
	for id, present in context.known_patches {
		if !present || !id.starts_with(prefix) {
			continue
		}
		encoded := id[prefix.len..]
		parsed := strconv.parse_int(encoded, 10, 32) or {
			return NondeterminismError{
				message: 'invalid recorded version for change ' + change_id
			}
		}
		version := int(parsed)
		if version < default_version || version.str() != encoded {
			return NondeterminismError{
				message: 'invalid recorded version for change ' + change_id
			}
		}
		versions << version
	}
	versions.sort()
	if versions.len > 1 {
		return NondeterminismError{
			message: 'multiple recorded versions for change ' + change_id
		}
	}
	return versions
}

// The first live call records max_supported; replay without a marker uses
// default_version. Existing recorded choices survive later supported ranges.
pub fn (mut context WorkflowContext) get_version(change_id string, min_supported int, max_supported int) !int {
	context.ensure_open()!
	if previous := context.recorded_value_error { return previous }
	if change_id == '' || !utf8.validate_str(change_id) {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'version change ID must be nonempty UTF-8'
		})
	}
	if min_supported < default_version || max_supported < min_supported {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'invalid workflow version range'
		})
	}
	if context.retired_version_changes[change_id] {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'cannot reuse a deprecated version change: ' + change_id
		})
	}
	if recorded := context.recorded_versions[change_id] {
		context.validate_version_range(change_id, recorded, min_supported, max_supported)!
		return recorded
	}
	versions := context.recorded_patch_versions(change_id) or {
		return context.retain_recorded_value_error(err)
	}
	selected := if versions.len == 1 {
		versions[0]
	} else if context.replaying {
		default_version
	} else {
		max_supported
	}
	context.validate_version_range(change_id, selected, min_supported, max_supported)!
	patch_id := version_patch_prefix(change_id) + selected.str()
	// An unmarked replay chooses the default without introducing a command.
	if versions.len == 1 || !context.replaying {
		if !context.patch_decision(patch_id, false)! {
			return context.retain_recorded_value_error(NondeterminismError{
				message: 'recorded version marker unavailable for change ' + change_id
			})
		}
	}
	context.recorded_versions[change_id] = selected
	return selected
}

fn (mut context WorkflowContext) validate_version_range(change_id string, recorded int, minimum int, maximum int) ! {
	if recorded < minimum || recorded > maximum {
		return context.retain_recorded_value_error(NondeterminismError{
			message: 'recorded version ${recorded} for ${change_id} is outside supported range [${minimum}, ${maximum}]'
		})
	}
}

// Retire a recorded numeric choice using Core's ordinary patch-deprecation
// rules. Remove this call only after histories needing the old marker retire.
pub fn (mut context WorkflowContext) deprecate_version(change_id string, last_supported int) ! {
	context.ensure_open()!
	if previous := context.recorded_value_error { return previous }
	if change_id == '' || !utf8.validate_str(change_id) {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'version change ID must be nonempty UTF-8'
		})
	}
	if last_supported < default_version {
		return context.retain_recorded_value_error(WorkflowTaskError{
			message: 'invalid deprecated workflow version'
		})
	}
	versions := context.recorded_patch_versions(change_id) or {
		return context.retain_recorded_value_error(err)
	}
	selected := if recorded := context.recorded_versions[change_id] {
		recorded
	} else if versions.len == 1 {
		versions[0]
	} else if context.replaying {
		default_version
	} else {
		last_supported
	}
	context.validate_version_range(change_id, selected, last_supported, last_supported)!
	if context.retired_version_changes[change_id] { return }
	context.deprecate_patch_marker(version_patch_prefix(change_id) + selected.str())!
	context.recorded_versions[change_id] = selected
	context.retired_version_changes[change_id] = true
}
