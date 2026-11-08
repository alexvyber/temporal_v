module sdk

import json2
import temporal

struct CommandMetadata {
	value ?temporal.TemporalApiSdkV1_UserMetadata
}

fn (converter DataConverter) command_metadata(summary string, details string) !CommandMetadata {
	if summary == '' && details == '' { return CommandMetadata{} }
	mut metadata := temporal.TemporalApiSdkV1_UserMetadata{}
	if summary != '' { metadata.summary = converter.encode_value(json2.encode(summary))! }
	if details != '' { metadata.details = converter.encode_value(json2.encode(details))! }
	return CommandMetadata{
		value: metadata
	}
}

fn (context &WorkflowContext) local_command_metadata(sequence u32) ?temporal.TemporalApiSdkV1_UserMetadata {
	metadata := context.local_activity_metadata[sequence] or { return none }
	return metadata
}
