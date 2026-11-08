module example_tuning

import alexvyber.temporal_v.sdk
import sync
import time
import x.json2
import alexvyber.temporal_v.workflows
import alexvyber.temporal_v.example_activities

struct SupplierState {
mut:
	mutex                &sync.Mutex = sync.new_mutex()
	capacity             int
	kind                 sdk.SlotKind
	queue                string
	next                 u64
	live                 map[u64]bool
	reserved             int
	released             int
	marked               int
	maximum              int
	issue                string
	unknown_availability bool
}

struct Supplier {
	state &SupplierState
}

fn new_supplier(kind sdk.SlotKind, queue string, capacity int) Supplier {
	return Supplier{
		state: &SupplierState{
			kind:                 kind
			queue:                queue
			capacity:             capacity
			unknown_availability: kind == .nexus
		}
	}
}

fn (supplier Supplier) reserve(context sdk.SlotReservationContext, cancellation &sdk.SlotReservationCancellation) ?sdk.SlotPermit {
	for !cancellation.is_canceled() {
		if permit := supplier.try_reserve(context) { return permit }
		time.sleep(10 * time.millisecond)
	}
	return none
}

fn (supplier Supplier) try_reserve(context sdk.SlotReservationContext) ?sdk.SlotPermit {
	mut state := supplier.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if context.kind != state.kind || context.task_queue != state.queue
		|| context.worker_identity == '' || context.worker_build_id == '' {
		state.issue = 'reservation context was not preserved'
		return none
	}
	if state.live.len >= state.capacity { return none }
	state.next++
	state.live[state.next] = false
	state.reserved++
	if state.live.len > state.maximum { state.maximum = state.live.len }
	return sdk.SlotPermit{
		id: state.next
	}
}

fn (supplier Supplier) mark_used(permit sdk.SlotPermit, info sdk.SlotInfo) {
	mut state := supplier.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if permit.id !in state.live || state.live[permit.id] {
		state.issue = 'unknown or reused permit'
	}
	if info.kind != state.kind { state.issue = 'slot kind was not preserved' }
	if info.kind == .workflow && info.workflow_type == '' {
		state.issue = 'missing workflow slot information'
	}
	if info.kind in [.activity, .local_activity] && info.activity_type == '' {
		state.issue = 'missing Activity slot information'
	}
	if info.kind == .nexus && (info.service != 'v-service' || info.operation != 'echo') {
		state.issue = 'missing Nexus slot information'
	}
	state.live[permit.id] = true
	state.marked++
}

fn (supplier Supplier) release(permit sdk.SlotPermit, info ?sdk.SlotInfo) {
	mut state := supplier.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if permit.id !in state.live { state.issue = 'permit released twice or never reserved' }
	if information := info {
		if information.kind != state.kind {
			state.issue = 'release information kind was not preserved'
		}
	}
	state.live.delete(permit.id)
	state.released++
}

fn (supplier Supplier) available_slots() ?u64 {
	mut state := supplier.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.unknown_availability { return none }
	return u64(if state.capacity > state.live.len {
		state.capacity - state.live.len
	} else {
		0
	})
}

fn (supplier Supplier) capacity(limit int) {
	mut state := supplier.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	state.capacity = limit
}

fn (supplier Supplier) verify() ! {
	mut state := supplier.state
	state.mutex.lock()
	defer { state.mutex.unlock() }
	if state.issue != '' { return error(state.issue) }
	if state.live.len != 0 || state.reserved != state.released {
		return error('custom slot permits did not drain')
	}
	if state.marked == 0 { return error('custom slot supplier never marked a task') }
	if state.kind != .workflow && state.maximum > 1 {
		return error('custom slot capacity was exceeded')
	}
}

fn combined(options sdk.WorkerOptions, done chan string) {
	mut runtime := sdk.Runtime{}
	workflows.register(mut runtime) or {
		done <- err.msg()
		return
	}
	runtime.register_local_activity('v.attempt', example_activities.attempt) or {
		done <- err.msg()
		return
	}
	mut activities := sdk.Activities{}
	activities.register('v.echo', example_activities.echo) or {
		done <- err.msg()
		return
	}
	activities.register('v.delay', example_activities.delay) or {
		done <- err.msg()
		return
	}
	sdk.run_worker(options, runtime, activities) or {
		done <- err.msg()
		return
	}
	done <- ''
}

fn echo(mut context sdk.NexusContext, input string) !sdk.NexusResult {
	context.check_cancelled()!
	return sdk.NexusResult{
		value: '"custom Nexus slots"'
	}
}

fn nexus(options sdk.WorkerOptions, done chan string) {
	mut services := sdk.NexusServices{}
	services.register('v-service', 'echo', sdk.NexusOperation{ start: echo }) or {
		done <- err.msg()
		return
	}
	sdk.run_nexus_worker(options, services) or {
		done <- err.msg()
		return
	}
	done <- ''
}

struct TestWorker {
	control &sdk.WorkerControl
	done    chan string
mut:
	stopped bool
}

fn start_worker(options sdk.WorkerOptions, is_nexus bool) &TestWorker {
	control := &sdk.WorkerControl{}
	done := chan string{cap: 1}
	configured := sdk.WorkerOptions{
		...options
		control: control
	}
	if is_nexus {
		spawn nexus(configured, done)
	} else {
		spawn combined(configured, done)
	}
	return &TestWorker{
		control: control
		done:    done
	}
}

fn (mut worker TestWorker) stop() ! {
	if worker.stopped { return }
	worker.control.shutdown()
	select {
		message := <-worker.done {
			worker.stopped = true
			if message != '' { return error(message) }
		}
		15 * time.second {
			return error('custom tuner worker shutdown timed out')
		}
	}
}

fn timer_recorded(client &sdk.Client, handle sdk.WorkflowHandle) !bool {
	for event in client.history(handle)!.events {
		if event.event_type == .event_type_timer_started { return true }
	}
	return false
}

pub fn exercise(client &sdk.Client, base sdk.WorkerOptions) ![]string {
	workflow := new_supplier(.workflow, base.task_queue, 4)
	activity := new_supplier(.activity, base.task_queue, 0)
	local := new_supplier(.local_activity, base.task_queue, 1)
	nexus_slots := new_supplier(.nexus, base.task_queue, 1)
	options := sdk.WorkerOptions{
		...base
		activity_concurrency:       4
		local_activity_concurrency: 2
		workflow_concurrency:       4
		max_cached_workflows:       4
		custom_tuner:               sdk.CustomTuner{
			workflow:       workflow
			activity:       activity
			local_activity: local
			nexus:          nexus_slots
		}
	}
	mut first := start_worker(options, false)
	mut nexus_worker := start_worker(options, true)
	defer {
		first.stop() or { eprintln(err) }
		nexus_worker.stop() or { eprintln(err) }
	}
	blocked := client.start(sdk.WorkflowStart{
		workflow_id:   base.task_queue + '-custom-blocked'
		workflow_type: 'v.parallel'
		task_queue:    base.task_queue
		arguments:     '{"name":"custom","delay_ms":50}'
	})!
	counter := client.start(sdk.WorkflowStart{
		workflow_id:   base.task_queue + '-custom-counter'
		workflow_type: 'v.counter'
		task_queue:    base.task_queue
		arguments:     '{"initial":1}'
	})!
	client.query(counter, 'state', sdk.Arguments(sdk.TypedArguments{}))!
	time.sleep(100 * time.millisecond)
	for event in client.history(blocked)!.events {
		if event.event_type == .event_type_activity_task_started {
			return error('zero-capacity custom supplier executed an Activity')
		}
	}
	client.signal(counter, 'finish', sdk.Arguments(sdk.TypedArguments{}))!
	if client.get_result(counter, 10000)!.status != .completed {
		return error('workflow custom slots did not progress independently')
	}
	activity.capacity(1)
	mut handles := [blocked]
	for index in 0 .. 4 {
		handles << client.start(sdk.WorkflowStart{
			workflow_id:   base.task_queue + '-custom-' + index.str()
			workflow_type: 'v.parallel'
			task_queue:    base.task_queue
			arguments:     '{"name":"custom","delay_ms":50}'
		})!
	}
	for handle in handles {
		if client.get_result(handle, 10000)!.status != .completed {
			return error('custom Activity slots did not resume work')
		}
	}
	local_run := client.start(sdk.WorkflowStart{
		workflow_id:   base.task_queue + '-custom-local'
		workflow_type: 'v.activity-options'
		task_queue:    base.task_queue
		arguments:     '{"local":true,"fail_until":1}'
	})!
	local_result := client.get_result(local_run, 10000)!
	if local_result.status != .completed || !local_result.data.contains('"attempt":2') {
		return error('custom local retry slots failed')
	}
	nexus_run := client.start(sdk.WorkflowStart{
		workflow_id:   base.task_queue + '-custom-nexus'
		workflow_type: 'v.nexus'
		task_queue:    base.task_queue
		arguments:     json2.encode({
			'endpoint': base.task_queue + '-endpoint'
		})
	})!
	if client.get_result(nexus_run, 10000)!.data != '"custom Nexus slots"' {
		return error('custom Nexus slots did not execute')
	}
	restarted := client.start(sdk.WorkflowStart{
		workflow_id:   base.task_queue + '-custom-restart'
		workflow_type: 'v.restart'
		task_queue:    base.task_queue
		arguments:     '{"name":"slots","delay_seconds":2}'
	})!
	watch := time.new_stopwatch()
	for !timer_recorded(client, restarted)! {
		if watch.elapsed() > 10 * time.second {
			return error('custom worker did not reach durable timer')
		}
		time.sleep(20 * time.millisecond)
	}
	first.stop()!
	mut replacement := start_worker(options, false)
	defer { replacement.stop() or { eprintln(err) } }
	if client.get_result(restarted, 10000)!.status != .completed {
		return error('custom worker restart failed')
	}
	replacement.stop()!
	nexus_worker.stop()!
	for supplier in [workflow, activity, local, nexus_slots] {
		supplier.verify()!
	}
	return [
		'Custom workflow/remote/local/Nexus suppliers received reservation and task context on V dispatcher threads',
		'Zero remote capacity blocked Activities while workflows progressed; changing capacity resumed queued work within a one-slot bound',
		'Local retry, Nexus execution and restart replay succeeded; every reserved permit was released and all dispatchers drained',
	]
}
