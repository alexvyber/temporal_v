"""Compile the frozen replay-baseline-v1 fixture without adapting its V APIs."""
from pathlib import Path
import hashlib,json,os,platform,subprocess,tarfile
ROOT=Path(__file__).resolve().parent.parent
ART=Path(os.environ.get('SPIKE_ARTIFACTS',ROOT.parent/'artifacts/v-sdk-spike')).resolve()

def main():
 metadata=json.loads((ROOT/'release/compatibility/replay-baseline-v1.json').read_text())
 archive=ROOT/'release/compatibility/replay-baseline-v1-source.tar.gz'
 assert hashlib.sha256(archive.read_bytes()).hexdigest()==metadata['archive_sha256']
 destination=ART/'replay-baseline-v1';destination.mkdir(exist_ok=True)
 files={}
 with tarfile.open(archive) as source:
  for member in source.getmembers():
   name=Path(member.name);assert not name.is_absolute() and '..' not in name.parts
   assert member.isfile(),member.name
   assert member.name.startswith(metadata['archive_prefix'])
   target=destination/Path(*name.parts[1:]);target.parent.mkdir(parents=True,exist_ok=True)
   content=source.extractfile(member).read()
   files[str(Path(*name.parts[1:]))]=hashlib.sha256(content).hexdigest()
   if target.exists():assert target.read_bytes()==content,'baseline source changed: '+str(target)
   else:target.write_bytes(content)
 assert hashlib.sha256(json.dumps(files,sort_keys=True).encode()).hexdigest()==metadata['source_sha256'],'baseline source fingerprint mismatch'
 profile=os.environ.get('SPIKE_BUILD_PROFILE','debug');lib=ART/'rust-target'/profile
 flags=([str(lib/'libtemporalio_sdk_core_c_bridge.dylib'),str(lib/'libtemporal_v_payloads.dylib')] if platform.system()=='Darwin' else ['-L'+str(lib),'-ltemporalio_sdk_core_c_bridge','-ltemporal_v_payloads'])+['-Wl,-rpath,'+str(lib)]
 commands=[];images={}
 for program,name in [('workflow_worker','worker'),('worker','activities'),('replay_demo','replay'),('side_effect_demo','side-effects')]:
  binary=ART/('tools/v-replay-baseline-v1-'+name)
  command=[str(ART/'tools/v/v'),'-d','use_bundled_libgc',*(['-prod'] if profile=='release' else []),'-ldflags',' '.join(flags),'-o',str(binary),program]
  with (ART/('logs/replay-baseline-v1-'+name+'-build.log')).open('w') as output:
   subprocess.run(command,cwd=destination,stdout=output,stderr=subprocess.STDOUT,check=True)
  commands.append(command);images[str(binary)]=hashlib.sha256(binary.read_bytes()).hexdigest()
 record={'passed':True,'baseline':metadata,'platform':platform.system(),'architecture':platform.machine(),'commands':commands,'images':images,'libraries':{str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in lib.glob('libtemporal*.dylib' if platform.system()=='Darwin' else 'libtemporal*.so')}}
 (ART/'replay-baseline-v1-build.json').write_text(json.dumps(record,indent=2)+'\n')
if __name__=='__main__':main()
