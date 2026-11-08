"""Reusable schedules against legacy and CHASM scheduler implementations."""
RESULT_REPORT = 'schedule-results.json'
import argparse
import json
from pathlib import Path
import subprocess
import time
from integration import ARTIFACTS, ROOT, TemporalFixture

BINARY = ARTIFACTS / 'tools/v-schedule'


def invoke(fixture, mode, *arguments, success=True):
    process = subprocess.run([str(BINARY), mode, *map(str, arguments)], cwd=ROOT, env=fixture.env,
                             capture_output=True, text=True, timeout=25)
    (ARTIFACTS / f'logs/{fixture.name}-{mode}.log').write_text(process.stdout + process.stderr)
    if not success: return process
    assert process.returncode == 0, process.stderr
    output = process.stdout.strip()
    return output if output in ('OK', 'VALIDATION_OK') else json.loads(output)


def recent(fixture, identity):
    return invoke(fixture, 'describe', identity)['info'].get('recent_actions', [])


def handle(action):
    return action['start_workflow_result']


def exercise(label, server, modern, codec):
    replays = []
    flags = ('--dynamic-config-value', 'history.enableCHASMSchedulerCreation=true',
             '--dynamic-config-value', 'history.chasmSchedulerCreationRolloutPercent=100',
             '--dynamic-config-value', 'history.enableChasm=true') if modern else ()
    with TemporalFixture(f'schedule-{label}-{codec}', server_binary=server, server_args=flags) as fixture:
        if codec: fixture.env['V_SDK_EXAMPLE_CODEC'] = 'prefix'
        assert invoke(fixture, 'validation') == 'VALIDATION_OK'
        worker = fixture.launch(BINARY, 'worker', ('worker',))
        identity = fixture.queue
        invoke(fixture, 'create', identity)
        duplicate = invoke(fixture, 'create', identity, success=False)
        assert duplicate.returncode != 0 and 'code: 6' in duplicate.stderr, duplicate.stderr
        original = invoke(fixture, 'describe', identity)
        assert original['schedule']['state']['paused']
        action = original['schedule']['action']
        workflow = action['action']['value']
        assert workflow['workflow_id'] == identity + '-workflow' and len(workflow['input']['payloads']) == 2
        assert workflow['priority']['priority_key'] == 2 and workflow['header']['fields']['flow']
        assert workflow['memo']['fields']['action'] and workflow['user_metadata']['summary']
        invoke(fixture, 'update', identity)
        updated = invoke(fixture, 'describe', identity)
        assert updated['schedule']['state']['notes'] == 'updated'
        assert updated['schedule']['action'] == action, 'state update changed an encoded action'
        assert updated['memo'] == original['memo'], 'omitted memo was not preserved'
        stale = invoke(fixture, 'update', identity, json.dumps(original['conflict_token']), success=False)
        if modern:
            assert stale.returncode != 0 and 'code: 9' in stale.stderr, stale.stderr
            invoke(fixture, 'clear-memo', identity)
            assert not invoke(fixture, 'describe', identity).get('memo', {}).get('fields'), 'explicit empty memo was not cleared'
        else:
            # This pinned V1 scheduler accepts stale tokens; the SDK still forwards
            # them, and the newer scheduler above verifies actual enforcement.
            assert stale.returncode == 0, stale.stderr
        invoke(fixture, 'unpause', identity)
        assert not invoke(fixture, 'describe', identity)['schedule']['state']['paused']
        invoke(fixture, 'pause', identity)
        assert invoke(fixture, 'describe', identity)['schedule']['state']['paused']
        # Backfill includes the start boundary; MatchingTimes excludes it.
        # Put both endpoints between minute ticks so both select the same two.
        minute = int(time.time()) // 60 * 60
        window_start = minute - 180 + 1
        window_end = minute - 60 + 1
        matching = invoke(fixture, 'matching', identity, window_start, window_end)
        assert len(matching) == 2, matching
        invoke(fixture, 'trigger', identity)
        fixture.wait_for(lambda: len(recent(fixture, identity)) == 1)
        first = handle(recent(fixture, identity)[0])
        assert json.loads(fixture.call('query', first, name='state')['data']) == 'scheduled'
        invoke(fixture, 'trigger', identity)
        fixture.wait_for(lambda: invoke(fixture, 'describe', identity)['info']['overlap_skipped'] >= 1)
        worker.kill()
        assert worker.wait(timeout=10) == -9
        worker = fixture.launch(BINARY, 'replacement', ('worker',))
        assert json.loads(fixture.call('query', first, name='state')['data']) == 'scheduled'

        def finish(execution, name):
            fixture.call('signal', execution, name='finish')
            result = fixture.call('result', execution)
            assert result['status'] == 'completed' and json.loads(result['data']) == 'scheduled', result
            fixture.assert_clean_history(execution)
            path = ARTIFACTS / f'binary-histories/{fixture.name}-{name}.bin'
            fixture.call('history', execution, file=path)
            replays.append((path, execution['workflow_id'], fixture.queue, fixture.env.copy()))

        finish(first, 'restart')
        invoke(fixture, 'backfill', identity, window_start, window_end)
        fixture.wait_for(lambda: len(recent(fixture, identity)) == 3)
        backfills = [handle(action) for action in recent(fixture, identity)
                     if handle(action)['run_id'] != first['run_id']]
        assert len(backfills) == 2
        for execution in backfills:
            assert json.loads(fixture.call('query', execution, name='state')['data']) == 'scheduled'
        for index, execution in enumerate(backfills): finish(execution, 'backfill-' + str(index))
        second = identity + '-second'
        invoke(fixture, 'create', second)
        found = set()
        token = []

        def listed():
            nonlocal token
            page = invoke(fixture, 'list', json.dumps(token) if token else '')
            found.update(item['schedule_id'] for item in page.get('schedules', []))
            token = page.get('next_page_token', [])
            return {identity, second} <= found

        fixture.wait_for(listed)
        for schedule in (identity, second):
            invoke(fixture, 'delete', schedule)
            missing = invoke(fixture, 'describe', schedule, success=False)
            assert missing.returncode != 0 and 'code: 5' in missing.stderr, missing.stderr
        worker.terminate()
        assert worker.wait(timeout=20) == 0
    for path, workflow_id, queue, environment in replays:
        replay = subprocess.run([str(BINARY), 'replay', str(path), workflow_id, queue], cwd=ROOT,
                                env=environment, capture_output=True, text=True, timeout=40)
        (ARTIFACTS / f'logs/{path.stem}-offline.log').write_text(replay.stdout + replay.stderr)
        assert replay.returncode == 0, replay.stderr


def main(binary=BINARY, report_name=RESULT_REPORT):
    global BINARY
    BINARY = binary
    report = {'passed': False, 'cases': []}
    try:
        for label, server, modern in [('legacy-1-32', ARTIFACTS / 'tools/temporal', False),
                                      ('chasm-1-33', ARTIFACTS / 'tools/temporal-protocol', True)]:
            for codec in (False, True):
                exercise(label, server, modern, codec)
                report['cases'].append(f'{label} codec={codec}: validated before RPC; create/duplicate, encoded-action-preserving update, memo preservation, pause/unpause, matching times, manual trigger/overlap skip, two concurrent backfills, pagination and delete passed; SIGKILL/replacement and all three histories replayed offline')
            if modern: report['cases'].append('CHASM scheduler rejected stale update tokens and cleared explicit empty memo; pinned legacy scheduler accepts stale tokens')
        report['passed'] = True
    finally:
        (ARTIFACTS / report_name).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=BINARY)
    parser.add_argument('--report', choices=(RESULT_REPORT, 'schedule-sanitized-results.json'), default=RESULT_REPORT)
    arguments = parser.parse_args()
    main(arguments.binary, arguments.report)
