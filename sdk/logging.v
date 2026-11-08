module sdk

import x.json2
import alexvyber.temporal_v.corebridge
import time
import os

pub enum LogLevel {
	debug
	info
	warn
	error
}

pub struct LogEntry {
pub:
	level            LogLevel
	message          string
	fields           map[string]string
	timestamp_millis i64
}

// Implementations may be called concurrently by Activity/Nexus threads.
pub interface Logger {
	log(entry LogEntry)
}

pub struct ConsoleLogger {}

pub fn (logger ConsoleLogger) log(entry LogEntry) {
	mut message := protocol_text(entry.message)
	mut keys := entry.fields.keys()
	keys.sort()
	for key in keys {
		message += ' ${protocol_text(key)}=${protocol_text(entry.fields[key])}'
	}
	if entry.level in [.warn, .error] {
		eprintln(message)
	} else {
		println(message)
		flush_stdout()
	}
}

pub struct JsonLogger {}

pub fn logger_from_env() !Logger {
	return match os.getenv('TEMPORAL_LOG_FORMAT') {
		'', 'text' { Logger(ConsoleLogger{}) }
		'json' { Logger(JsonLogger{}) }
		else { return error('TEMPORAL_LOG_FORMAT must be text or json') }
	}
}

pub fn (logger JsonLogger) log(entry LogEntry) {
	corebridge.log_line(json2.encode(LogEntry{
		...entry
		message: protocol_text(entry.message)
		fields:  protocol_fields(entry.fields)
	}))
}

fn emit_log(logger Logger, level LogLevel, message string) {
	logger.log(LogEntry{ level: level, message: message, timestamp_millis: time.now().unix_milli() })
}

pub fn (context &WorkflowContext) log(level LogLevel, message string, attributes map[string]string) {
	if context.replaying_history { return }
	mut fields := attributes.clone()
	fields['namespace'] = context.namespace
	fields['workflow_id'] = context.workflow_id
	fields['run_id'] = context.run_id
	fields['workflow_type'] = context.workflow_type
	fields['task_queue'] = context.queue
	context.logger.log(LogEntry{
		level:            level
		message:          message
		fields:           fields
		timestamp_millis: context.now().unix_milli()
	})
}

pub fn (context &ActivityContext) log(level LogLevel, message string, attributes map[string]string) {
	mut fields := attributes.clone()
	fields['activity_id'] = context.information.activity_id
	fields['activity_type'] = context.information.activity_type
	fields['workflow_id'] = context.information.workflow_id
	fields['run_id'] = context.information.run_id
	fields['attempt'] = context.information.attempt.str()
	fields['is_local'] = context.information.is_local.str()
	context.logger.log(LogEntry{
		level:            level
		message:          message
		fields:           fields
		timestamp_millis: time.now().unix_milli()
	})
}

pub fn (context &NexusContext) log(level LogLevel, message string, attributes map[string]string) {
	mut fields := attributes.clone()
	fields['service'] = context.information.service
	fields['operation'] = context.information.operation
	fields['request_id'] = context.information.request_id
	context.logger.log(LogEntry{
		level:            level
		message:          message
		fields:           fields
		timestamp_millis: time.now().unix_milli()
	})
}
