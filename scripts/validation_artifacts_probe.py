"""Reject instrumented image/command/compiler tampering, including output escapes."""
RESULT_REPORT='validation-artifacts-results.json'
import copy,json,subprocess,tempfile
from pathlib import Path
from unittest.mock import patch
from integration import ARTIFACTS,ROOT
from validation_artifacts import InstrumentedBuilds,verify_instrumented,digest,SANITIZERS

def main():
 report={'passed':False,'cases':[]}
 try:
  with tempfile.TemporaryDirectory(prefix='v-validation-artifacts-') as temporary:
   directory=Path(temporary);root=directory/'artifacts';(root/'tools/v').mkdir(parents=True)
   compiler=directory/'pinned-v';compiler.write_bytes(b'pinned compiler fixture')
   (root/'tools/v/v').symlink_to(compiler);output=root/'tools/instrumented';flags=' '.join(SANITIZERS)
   command=[str(root/'tools/v/v'),'-cc','clang','-cflags',flags,'-ldflags',flags,'-o',str(output),'fixture.v']
   native_compiler=directory/'clang';native_compiler.write_bytes(b'pinned native compiler fixture');native_compiler.chmod(0o755)
   command[command.index('-cc')+1]=str(native_compiler)
   tracker=InstrumentedBuilds(root,root/'tools/v/v',native_compiler)
   calls=[]
   def compile_fixture(argv,**options):
    calls.append(argv);Path(argv[argv.index('-o')+1]).write_bytes(b'completed instrumented executable')
    return subprocess.CompletedProcess(argv,0)
   with patch('validation_artifacts.subprocess.run',side_effect=compile_fixture):
    assert tracker.compile(command).returncode==0
   manifest=tracker.finish();expected_images={**manifest['binaries'],manifest['native_compiler']['path']:digest(native_compiler)}
   assert verify_instrumented(root,manifest,digest(compiler))==expected_images
   report['cases'].append('Valid compiler alias, explicit sanitizer compile/link flags and produced image were bound immediately and rechecked')
   def rejected(label,operation):
    try:operation()
    except (ValueError,FileNotFoundError):report['cases'].append(label);return
    raise AssertionError('accepted invalid evidence: '+label)
   original=output.read_bytes();output.write_bytes(b'tampered executable')
   rejected('Changed instrumented executable rejected',lambda:verify_instrumented(root,manifest));output.write_bytes(original)
   original_compiler=compiler.read_bytes();compiler.write_bytes(b'tampered compiler')
   rejected('Changed compiler rejected even through prepared-toolchain symlink',lambda:verify_instrumented(root,manifest));compiler.write_bytes(original_compiler)
   rejected('Compiler mismatch with normal build rejected',lambda:verify_instrumented(root,manifest,'0'*64))
   for option in ['-cflags','-ldflags']:
    changed=copy.deepcopy(manifest);changed['commands']['tools/instrumented'][command.index(option)+1]='-fno-omit-frame-pointer'
    rejected('Missing sanitizer flags rejected in '+option,lambda:verify_instrumented(root,changed))
   changed=copy.deepcopy(manifest);changed['commands']['tools/instrumented'][0]='/different/compiler'
   rejected('Forged command compiler rejected',lambda:verify_instrumented(root,changed))
   changed=copy.deepcopy(manifest);changed['commands']['tools/instrumented'][command.index('-o')+1]=str(root/'tools/other')
   rejected('Command/output mismatch rejected',lambda:verify_instrumented(root,changed))
   changed=copy.deepcopy(manifest);changed['binaries']['../outside']=changed['binaries'].pop('tools/instrumented');changed['commands']['../outside']=changed['commands'].pop('tools/instrumented')
   rejected('Traversal manifest rejected',lambda:verify_instrumented(root,changed))
   outside=directory/'outside';outside.write_bytes(b'preserve outside data');escape=root/'tools/escape';escape.symlink_to(outside)
   escaping=command.copy();escaping[escaping.index('-o')+1]=str(escape)
   rejected('Escaping compile symlink rejected before invoking compiler',lambda:tracker.compile(escaping));assert outside.read_bytes()==b'preserve outside data' and len(calls)==1
   rejected('Duplicate instrumented output rejected',lambda:tracker.compile(command))
   changed=copy.deepcopy(manifest);changed['binaries']={};changed['commands']={}
   rejected('Empty persistent-image manifest rejected',lambda:verify_instrumented(root,changed))
   for invalid in [None,[],{},dict(manifest,schema_version=99),dict(manifest,compiler=[])]:
    rejected('Malformed manifest rejected',lambda:verify_instrumented(root,invalid))
   for option in ['-o','-cflags','-ldflags']:
    changed=copy.deepcopy(manifest);argv=changed['commands']['tools/instrumented'];index=argv.index(option);del argv[index:index+2];argv.append(option)
    rejected('Missing option value rejected for '+option,lambda:verify_instrumented(root,changed))
   # Exercise final verification with a passing report, then a changed image.
   import verification,time
   (root/'scripts').mkdir()
   (root/'scripts/check.sh').write_text('python3 "$SPIKE_ROOT/scripts/sanitizer.py"\n')
   (root/'scripts/sanitizer.py').write_text("RESULT_REPORT='sanitizer-results.json'\n")
   snapshot={'sha256':'fixture-source','files':{}}
   state={'source':snapshot,'build':{'binaries':{'tools/v/v':digest(compiler)}},'started_at':time.time()-10}
   (root/'check-verification-state.json').write_text(json.dumps(state))
   (root/'build-verification.json').write_text(json.dumps(state['build']))
   (root/'sanitizer-results.json').write_text(json.dumps({'passed':True,'instrumented_builds':manifest}))
   with patch.object(verification,'ROOT',root),patch.object(verification,'ARTIFACTS',root),patch.object(verification,'source_snapshot',return_value=snapshot):
    assert verification.finish(0)==0
    accepted=json.loads((root/'check-verification.json').read_text());assert accepted['validation_binaries']==expected_images
    output.write_bytes(b'changed after sanitizer success')
    assert verification.finish(0)==1
    denied=json.loads((root/'check-verification.json').read_text());assert not denied['passed'] and any('instrumented binary changed' in error for error in denied['errors'])
    output.write_bytes(original)
    assert verification.finish(0)==0
    import archive_verified_check
    archive_verified_check.main()
    archive=root/'host-evidence-1-fixture'
    saved=json.loads((archive/'manifest.json').read_text())
    assert saved['normal_artifacts']==state['build']['binaries'] and saved['validation_artifacts']==expected_images
    output.write_bytes(b'later rebuild replaces original')
    archived=archive/'artifacts/tools/instrumented'
    assert archived.read_bytes()==original and digest(archived)==manifest['binaries']['tools/instrumented']
    output.write_bytes(original)
   report['cases'].append('Archive retained exact normal/instrumented images and stayed immutable after original-image replacement')
   report['cases'].append('Final verification rejected a modified image despite a passing sanitizer report and unchanged normal build/compiler')
   output.unlink();rejected('Missing image rejected',lambda:verify_instrumented(root,manifest))
   # Standalone C binaries use Clang's driver flags directly. Retain the
   # compiler bytes so exported/archived checks do not depend on host Clang.
   native_output=root/'tools/native-instrumented'
   native_command=[str(native_compiler),*SANITIZERS,'fixture.c','-o',str(native_output)]
   with patch('validation_artifacts.subprocess.run',side_effect=compile_fixture):
    assert tracker.compile(native_command).returncode==0
   output.write_bytes(original);native_manifest=tracker.finish()
   images=verify_instrumented(root,native_manifest,digest(compiler))
   assert images['tools/native-instrumented']==digest(native_output)
   assert images['tools/validation-compilers/clang']==digest(native_compiler)
   report['cases'].append('Standalone C compile/link flags, compiler snapshot and executable were bound and rechecked')
   changed=copy.deepcopy(native_manifest);changed['commands']['tools/native-instrumented'].remove(SANITIZERS[0])
   rejected('Missing standalone C sanitizer flags rejected',lambda:verify_instrumented(root,changed))
   changed=copy.deepcopy(native_manifest);changed['commands']['tools/native-instrumented'][0]='/different/clang'
   rejected('Forged standalone C compiler rejected',lambda:verify_instrumented(root,changed))
   snapshot=root/'tools/validation-compilers/clang';native_bytes=snapshot.read_bytes();snapshot.write_bytes(b'tampered snapshot')
   rejected('Changed archived native compiler rejected',lambda:verify_instrumented(root,native_manifest));snapshot.write_bytes(native_bytes)
   native_bytes=native_compiler.read_bytes();native_compiler.write_bytes(b'changed native compiler')
   rejected('Changed live native compiler rejected',tracker.finish);native_compiler.write_bytes(native_bytes)
   portable=directory/'exported';import shutil;shutil.copytree(root,portable,symlinks=False)
   # Compiler invocation identities belong to the build host; their retained
   # bytes and all output images are sufficient for portable verification.
   exported=copy.deepcopy(native_manifest)
   for argv in exported['commands'].values():
    if argv[0]==str(root/'tools/v/v'):argv[0]=str(portable/'tools/v/v')
    argv[argv.index('-o')+1]=str(portable/Path(argv[argv.index('-o')+1]).relative_to(root))
   native_compiler.unlink()
   assert verify_instrumented(portable,exported,digest(compiler))
   report['cases'].append('Exported evidence verifies from retained Clang bytes without the build-host compiler path')
   # Every persistent sanitizer compiler call must use the tracker.
   import ast
   tree=ast.parse((ROOT/'scripts/native_sanitizer_probe.py').read_text())
   compiles=[node for node in ast.walk(tree) if isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id=='compiled' for target in node.targets)]
   assert len(compiles)>=21 and all(isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Attribute) and node.value.func.attr=='compile' and isinstance(node.value.func.value,ast.Name) and node.value.func.value.id=='builds' for node in compiles)
   report['cases'].append(f'All {len(compiles)} persistent sanitizer compilation sites use image tracking')
  report['passed']=True
 finally:(ARTIFACTS/RESULT_REPORT).write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
if __name__=='__main__':main()
