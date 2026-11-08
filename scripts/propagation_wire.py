"""Independent raw history checks for inline propagation and application storage references."""
import base64,json

def carriers(value,path=()):
    if isinstance(value,dict):
        for key,item in value.items():
            if key=='_tracer-data' and path[-2:]==('header','fields'):yield item
            yield from carriers(item,path+(key,))
    elif isinstance(value,list):
        for item in value:yield from carriers(item,path)

def inspect_propagation(history,trace_id,baggage=None,require_memo_storage=False):
    found=list(carriers(history));assert found,'history contains no propagation headers'
    for payload in found:
        metadata={key:base64.b64decode(value) for key,value in payload['metadata'].items()}
        assert metadata['encoding']==b'json/plain' and metadata['temporal-v/propagation']==b'1',metadata
        carrier=json.loads(base64.b64decode(payload['data']))
        assert carrier['traceparent'].split('-')[1]==trace_id,carrier
        if baggage is not None:assert carrier.get('baggage','')==baggage,carrier
    if require_memo_storage:
        started=next(event['workflowExecutionStartedEventAttributes'] for event in history['events'] if 'workflowExecutionStartedEventAttributes' in event)
        memo=started['memo']['fields']['_tracer-data'];metadata={key:base64.b64decode(value) for key,value in memo['metadata'].items()}
        assert metadata['encoding']==b'json/protobuf' and metadata['messageType']==b'temporal.api.sdk.v1.ExternalStorageReference',metadata
    return len(found)
