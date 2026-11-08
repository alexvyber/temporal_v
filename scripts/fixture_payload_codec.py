"""Independent wire decoder for the probes' deliberately mutating test codec."""
import base64,os

def decode_fixture_payload(payload,codec):
 metadata={key:base64.b64decode(value) for key,value in payload['metadata'].items()}
 data=base64.b64decode(payload['data'])
 if codec in ['prefix','context']:
  assert data[:6]==bytes([0,66,255,0,65,255]);data=data[6:]
 if os.environ.get('V_CODEC_OWNERSHIP')=='true':
  assert metadata['encoding']==b'binary/owned-fixture',metadata
  data=bytes(value^170 for value in data)
  metadata['encoding']=metadata.pop('temporal-v/owned-source-encoding')
 return metadata,data
