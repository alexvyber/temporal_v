"""Archive a completed exact-source check with normal and instrumented images."""
import json,platform,shutil,stat,subprocess,tarfile
from pathlib import Path
import verification
from validation_artifacts import verify_instrumented

def copy_evidence(source,target):
 target.parent.mkdir(parents=True,exist_ok=True)
 if platform.system()=='Darwin':
  subprocess.run(['cp','-c',str(source),str(target)],check=True)
  if not source.stat().st_flags & getattr(stat,'UF_COMPRESSED',0x20):shutil.copystat(source,target)
 else:shutil.copy2(source,target)
 if verification.digest(source)!=verification.digest(target):raise ValueError('archive copy differs: '+str(source))

def main():
 root=verification.ROOT;artifacts=verification.ARTIFACTS
 check=json.loads((artifacts/'check-verification.json').read_text())
 if check.get('passed') is not True or check.get('errors'):raise ValueError('cannot archive an incomplete or failed check')
 if check['source']!=verification.source_snapshot():raise ValueError('source differs from completed check')
 sanitizer=json.loads((artifacts/'sanitizer-results.json').read_text())
 instrumented=verify_instrumented(artifacts,sanitizer['instrumented_builds'],check['build']['binaries']['tools/v/v'])
 if check.get('validation_binaries')!=instrumented:raise ValueError('final check did not bind these instrumented images')
 normal=check['build']['binaries'];images=normal.copy()
 for name,digest in instrumented.items():
  if name in images and images[name]!=digest:raise ValueError('conflicting archive image: '+name)
  images[name]=digest
 source=check['source']['sha256'];destination=artifacts/f'host-evidence-{len(check["reports"])}-{source[:7]}'
 if destination.exists():raise ValueError('archive already exists: '+str(destination))
 destination.mkdir()
 with tarfile.open(destination/'source.tar.gz','w:gz') as output:
  for name,expected in check['source']['files'].items():
   path=root/name
   if verification.digest(path)!=expected:raise ValueError('source changed while archiving: '+name)
   output.add(path,arcname='v-sdk/'+name,recursive=False)
 for name in ['build-verification.json','check-verification.json',*check['reports']]:
  if name in check['reports'] and verification.digest(artifacts/name)!=check['reports'][name]['sha256']:raise ValueError('report changed: '+name)
  copy_evidence(artifacts/name,destination/name)
 for path in artifacts.glob('*-sanitized-results.json'):copy_evidence(path,destination/path.name)
 for name,expected in images.items():
  path=artifacts/name
  if verification.digest(path)!=expected:raise ValueError('bound image changed while archiving: '+name)
  copy_evidence(path,destination/'artifacts'/name)
 manifest={'schema_version':2,'source':source,'reports':len(check['reports']),'normal_artifacts':normal,'validation_artifacts':instrumented,'artifacts':images,'source_archive':verification.digest(destination/'source.tar.gz')}
 (destination/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');print(destination)
if __name__=='__main__':main()
