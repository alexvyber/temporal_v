"""Activity deadlines match independent server history and tokens stay owned/private."""
RESULT_REPORT='activity-deadline-results.json'
import argparse
import calendar
import datetime
from decimal import Decimal
import json
from pathlib import Path
import re
import subprocess
from integration import ARTIFACTS,ROOT,TemporalFixture,counts


def nanos(value):
    return value['seconds']*1000000000+value['nanos']


def timestamp(value):
    match=re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z',value)
    assert match,value
    seconds=calendar.timegm(datetime.datetime.strptime(match[1],'%Y-%m-%dT%H:%M:%S').timetuple())
    return seconds*1000000000+int((match[2] or '').ljust(9,'0'))


def duration(value):
    assert value.endswith('s'),value
    return int(Decimal(value[:-1])*1000000000)


def main(binary,report_name,quick=False):
    report={'passed':False,'cases':[],'histories':0,'remote_deadlines_checked':0,'local_deadlines_checked':0}
    try:
        profiles=[(version,server,codec,stored) for version,server in [('1-32','temporal'),('1-33','temporal-protocol')]
                  for codec in ['', 'prefix', 'context'] for stored in [False,True]]
        if quick:profiles=profiles[:1]
        for version,server,codec,stored in profiles:
            with TemporalFixture('activity-metadata-'+version+'-'+(codec or 'default')+'-'+str(stored)+'-'+binary.name,
                                 server_binary=ARTIFACTS/'tools'/server) as fixture:
                directory=fixture.histories/fixture.queue;directory.mkdir()
                fixture.env['V_SDK_EXAMPLE_CODEC']=codec
                if stored:fixture.env.update(V_PANIC_STORAGE_DRIVER='true',V_SDK_EXAMPLE_STORAGE_DIR=str(directory/'storage'))
                worker=fixture.launch(binary,'before',('worker',));handle=fixture.start('bulk','v.activity-deadline',{})
                def state():return json.loads(fixture.call('query',handle,name='state',rpc_ms=20000)['data'])
                fixture.wait_for(lambda:len(state())==8)
                saved=state();history=fixture.history(handle)
                scheduled={event['activityTaskScheduledEventAttributes']['activityId']:event for event in history['events'] if 'activityTaskScheduledEventAttributes' in event}
                remote_tokens=[]
                for label,encoded in saved.items():
                    info=json.loads(encoded);assert info['label']==label and info['token_length']>0,info
                    assert re.fullmatch('[0-9a-f]{64}',info['token_digest']),info
                    assert 'task_token' not in json.loads(info['information']),'task token escaped JSON redaction'
                    assert int(info['attempt'])==(2 if label.startswith('retry') else 1),info
                    if not info['local']:
                        event=scheduled[info['activity_id']];attributes=event['activityTaskScheduledEventAttributes']
                        started=next(item for item in history['events'] if item.get('activityTaskStartedEventAttributes',{}).get('scheduledEventId')==event['eventId'])
                        assert nanos(info['scheduled'])==timestamp(event['eventTime']),info
                        assert nanos(info['started'])==timestamp(started['eventTime']),info
                        start_deadline=timestamp(started['eventTime'])+duration(attributes['startToCloseTimeout'])
                        schedule_timeout=duration(attributes.get('scheduleToCloseTimeout','0s'))
                        expected=min(start_deadline,timestamp(event['eventTime'])+schedule_timeout) if schedule_timeout>0 else start_deadline
                        assert nanos(info['deadline'])==expected,(label,info['deadline'],expected)
                        remote_tokens.append(info['token_digest']);report['remote_deadlines_checked']+=1
                    else:
                        if info['scheduled'] is not None and info['started'] is not None:
                            start=(90 if label.startswith('schedule') else 30)*1000000000+123456789
                            schedule=(60 if label.startswith('schedule') else 120)*1000000000+987654321
                            expected=nanos(info['started'])+start if label.startswith('unlimited') else min(nanos(info['started'])+start,nanos(info['scheduled'])+schedule)
                            assert nanos(info['deadline'])==expected,info
                            report['local_deadlines_checked']+=1
                        else:assert info['deadline'] is None,info
                assert len(set(remote_tokens))==4,'remote Activity tokens were reused'
                worker.kill();assert worker.wait(timeout=10)==-9;worker=fixture.launch(binary,'after',('worker',))
                assert state()==saved
                fixture.call('signal',handle,name='finish');assert fixture.call('result',handle)['status']=='completed'
                assert counts(fixture.history(handle))['workflowtaskfailed']==0
                file=directory/'history.bin';fixture.call('history',handle,file=file)
                worker.terminate();assert worker.wait(timeout=20)==0
                replay=subprocess.run([str(binary),'replay',str(file),handle['workflow_id'],fixture.queue],cwd=ROOT,env=fixture.env,capture_output=True,text=True,timeout=45)
                assert replay.returncode==0 and replay.stdout.strip()=='OK',replay.stdout+replay.stderr
                report['histories']+=1
                report['cases'].append(f'{version}/{codec or "default"}/storage={stored}: owned JSON-redacted task tokens authenticated real heartbeat RPCs and retry progress; remote deadlines matched independent server timestamps/durations to the nanosecond, local bounds and both deadline minima passed; SIGKILL/replacement and offline replay retained eight results')
        report['passed']=True
    finally:(ARTIFACTS/report_name).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=ARTIFACTS/'tools/v-panic');parser.add_argument('--report',default=RESULT_REPORT);parser.add_argument('--quick',action='store_true');args=parser.parse_args();main(args.binary,args.report,args.quick)
