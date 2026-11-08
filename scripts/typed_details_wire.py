"""Independent Temporal history payload inspection for rich typed details."""
import base64,json,os
ENCODINGS=['json/plain','binary/plain','binary/protobuf','json/protobuf','binary/null','binary/plain','json/plain']
def varint(data,offset):
 value=0;shift=0
 while True:
  part=data[offset];offset+=1;value|=(part&127)<<shift
  if part<128:return value,offset
  shift+=7
  assert shift<70

def fields(data):
 offset=0;result={}
 while offset<len(data):
  tag,offset=varint(data,offset);kind=tag&7;number=tag>>3
  if kind==2:
   size,offset=varint(data,offset);value=data[offset:offset+size];offset+=size
  elif kind==0:value,offset=varint(data,offset)
  else:raise AssertionError(kind)
  result.setdefault(number,[]).append(value)
 return result

def wire_payload(data):
 value=fields(data)
 return {'metadata':{fields(entry)[1][0].decode():base64.b64encode(fields(entry)[2][0]).decode() for entry in value.get(1,[])},'data':base64.b64encode(value.get(2,[b''])[0]).decode()}

def unpack_payloads(payloads):
 while payloads:
  encoding=base64.b64decode(payloads[0]['metadata']['encoding'])
  if encoding==b'binary/test-payloads':
   assert len(payloads)==1,len(payloads)
   payloads=[wire_payload(data) for data in fields(base64.b64decode(payloads[0].get('data',''))).get(1,[])]
  elif encoding==b'binary/test-split':
   assert len(payloads)%2==0
   merged=[]
   for index in range(0,len(payloads),2):
    for part in range(2):
     metadata=payloads[index+part]['metadata']
     assert base64.b64decode(metadata['encoding'])==encoding and base64.b64decode(metadata['part'])==str(part).encode()
    merged.append(wire_payload(b''.join(base64.b64decode(payload.get('data','')) for payload in payloads[index:index+2])))
   payloads=merged
  else:break
 return payloads

def unwrap(payload,profile,handle,scope,endpoint=None,enveloped=False,operation='echo'):
 metadata={key:base64.b64decode(value) for key,value in payload['metadata'].items()}
 data=base64.b64decode(payload.get('data',''))
 if enveloped:
  assert metadata['encoding']==b'binary/test-envelope'
  inner=fields(data);metadata={fields(entry)[1][0].decode():fields(entry)[2][0] for entry in inner.get(1,[])};data=inner.get(2,[b''])[0]
 if metadata['encoding']==b'binary/test-payloads':
  restored=unpack_payloads([{'metadata':{key:base64.b64encode(value).decode() for key,value in metadata.items()},'data':base64.b64encode(data).decode()}]);assert len(restored)==1
  metadata={key:base64.b64decode(value) for key,value in restored[0]['metadata'].items()};data=base64.b64decode(restored[0].get('data',''))
 if profile in ('prefix','context','binary-prefix'):
  assert data[:6]==bytes([0,66,255,0,65,255]);data=data[6:]
 if profile in ('opaque','proto-binary','proto-json'):
  assert data[:6]==bytes([255,0,165])*2;data=data[6:]
  actor='activity-1' if scope=='activity' else ''
  expected='|'.join([scope,'default',handle['workflow_id'],actor,'false','','',''] if scope!='nexus' else [scope,'','','','false',endpoint,'typed.details',operation]).encode()
  assert metadata['codec-scope-65']==metadata['codec-scope-66']==expected
  if profile=='opaque':
   assert metadata['encoding']==b'test/opaque' and metadata['payload-scope']==expected
   inner=fields(data);metadata={fields(entry)[1][0].decode():fields(entry)[2][0] for entry in inner.get(1,[])};data=inner.get(2,[b''])[0]
 if profile=='context':
  for key in ('temporal-v/value-context','temporal-v/codec-context'):
   actual=json.loads(metadata[key]);assert actual['kind']==scope and actual['namespace']==('' if scope=='nexus' else 'default') and actual['workflow_id']==('' if scope=='nexus' else handle['workflow_id']),actual
   if scope=='nexus':assert actual['endpoint']==endpoint and actual['service']=='typed.details' and actual['operation']==operation,actual
   assert actual['activity_id']==('activity-1' if scope=='activity' else ''),actual
 return metadata,data

def details(payloads,profile,handle,scope,packed_array=True,endpoint=None,operation='echo'):
 if packed_array and os.getenv('V_SDK_DETAILS_PACK')=='true':
  assert len(payloads)==1 and base64.b64decode(payloads[0]['metadata']['encoding'])==b'binary/test-payloads'
  payloads=unpack_payloads(payloads)
 assert len(payloads)==7,len(payloads)
 values=[unwrap(payload,profile,handle,scope,endpoint=endpoint,operation=operation) for payload in payloads]
 assert [meta['encoding'].decode() for meta,data in values]==ENCODINGS
 assert values[0][1]==b'{"number":7}' and values[1][1]==bytes([0,255,128])
 assert values[2][1].endswith(bytes([160,6,7])) and 'details-世界'.encode() in values[2][1]
 assert json.loads(values[3][1])=={'workflowId':'details-世界'}
 assert values[4][1]==b'' and values[5][1]==b'null' and values[6][1]==b'"null"'
 for index in (2,3):assert values[index][0]['messageType']==b'temporal.api.common.v1.WorkflowExecution'
 return 1

def failure(value,profile,handle,scope):
 count=0
 for key,attribute in value.items():
  if key in ('applicationFailureInfo','canceledFailureInfo'):
   payloads=attribute.get('details',{}).get('payloads',[])
   if payloads:count+=details(payloads,profile,handle,scope)
 if value.get('cause'):count+=failure(value['cause'],profile,handle,scope)
 return count

def history_details(history,profile,handle,mode):
 count=0;updates=0
 for event in history['events']:
  for key,attribute in event.items():
   if key=='activityTaskFailedEventAttributes':count+=failure(attribute['failure'],profile,handle,'activity')
   if key=='workflowExecutionFailedEventAttributes':count+=failure(attribute['failure'],profile,handle,'workflow')
   if key=='workflowExecutionUpdateCompletedEventAttributes':
    updates+=1
    count+=failure(attribute['outcome']['failure'],profile,handle,'workflow')
   if key in ('activityTaskCanceledEventAttributes','workflowExecutionCanceledEventAttributes','workflowExecutionTerminatedEventAttributes'):
    count+=details(attribute['details']['payloads'],profile,handle,'activity' if key.startswith('activity') else 'workflow')
 expected={'failure':4,'rethrow':6,'defer-fail':1,'defer-cancel':1,'wf-cancel':1,'terminate':1}.get(mode,0)
 assert count==expected,(mode,count,expected)
 assert updates==(1 if mode in ('failure','rethrow') else 0),(mode,updates)
 return count

def one_payload(payload,profile,handle,index,scope,endpoint):
 metadata,data=unwrap(payload,profile,handle,scope,endpoint,profile!='default')
 assert metadata['encoding'].decode()==ENCODINGS[index],metadata
 if index==0:assert data==b'{"number":7}'
 if index==1:assert data==bytes([0,255,128])
 if index==2:assert data.endswith(bytes([160,6,7])) and 'details-世界'.encode() in data
 if index==3:assert json.loads(data)=={'workflowId':'details-世界'}
 if index==4:assert data==b''
 if index==5:assert data==b'null'
 if index==6:assert data==b'"null"'
 if index in (2,3):assert metadata['messageType']==b'temporal.api.common.v1.WorkflowExecution'

def nexus_history(history,profile,handle,index,endpoint):
 found=0
 for event in history['events']:
  if 'nexusOperationScheduledEventAttributes' in event:
   one_payload(event['nexusOperationScheduledEventAttributes']['input'],profile,handle,index,'nexus',endpoint);found+=1
  if 'nexusOperationCompletedEventAttributes' in event:
   one_payload(event['nexusOperationCompletedEventAttributes']['result'],profile,handle,index,'nexus',endpoint);found+=1
  if 'workflowExecutionCompletedEventAttributes' in event:
   one_payload(event['workflowExecutionCompletedEventAttributes']['result']['payloads'][0],profile,handle,index,'workflow',endpoint);found+=1
 assert found==3,found
