"""Bind persistent instrumented executables to the invocations that built them."""
import hashlib
from pathlib import Path,PurePosixPath
import subprocess
import shutil
import os

SANITIZERS=('-fsanitize=address,undefined','-fno-sanitize-recover=all','-fno-omit-frame-pointer')

def digest(path):
 result=hashlib.sha256()
 with Path(path).open('rb') as source:
  for block in iter(lambda:source.read(1024*1024),b''):result.update(block)
 return result.hexdigest()

def artifact_path(artifacts,name):
 if not isinstance(name,str):raise ValueError('validation artifact path must be a string')
 relative=PurePosixPath(name)
 if not name or relative.is_absolute() or '..' in relative.parts or str(relative)!=name:
  raise ValueError('invalid validation artifact path: '+name)
 path=Path(artifacts)/name
 if not path.resolve().is_relative_to(Path(artifacts).resolve()):
  raise ValueError('validation artifact escapes artifact root: '+name)
 return path

def command_option(command,option):
 if command.count(option)!=1 or command.index(option)+1>=len(command):
  raise ValueError('instrumented command requires one value for '+option)
 return command[command.index(option)+1]

class InstrumentedBuilds:
 def __init__(self,artifacts,compiler,native_compiler='clang'):
  self.artifacts=Path(artifacts).absolute();self.compiler=Path(compiler).absolute()
  self.compiler_name=str(self.compiler.relative_to(self.artifacts))
  if self.compiler_name!='tools/v/v':raise ValueError('expected normal build compiler alias')
  self.compiler_sha256=digest(self.compiler);self.binaries={};self.commands={}
  resolved=shutil.which(str(native_compiler))
  if not resolved:raise ValueError('native sanitizer compiler not found')
  self.native_compiler=Path(resolved).absolute()
  self.native_compiler_sha256=digest(self.native_compiler)
  self.native_compiler_name='tools/validation-compilers/clang'
  snapshot=artifact_path(self.artifacts,self.native_compiler_name)
  snapshot.parent.mkdir(parents=True,exist_ok=True)
  if snapshot.exists():
   if digest(snapshot)!=self.native_compiler_sha256:raise ValueError('native compiler snapshot changed')
  else:shutil.copy2(self.native_compiler,snapshot)
 def compile(self,command,**options):
  command=[str(part) for part in command]
  if not command:raise ValueError('missing instrumented compiler')
  if Path(command[0]).absolute()==self.compiler:
   for option in ['-cflags','-ldflags']:
    flags=command_option(command,option).split()
    if not all(flag in flags for flag in SANITIZERS):raise ValueError('missing sanitizer flag in '+option)
   selected=command_option(command,'-cc')
   if shutil.which(selected,path=options.get('env',os.environ).get('PATH'))!=str(self.native_compiler):raise ValueError('unexpected V native sanitizer compiler')
  elif shutil.which(command[0])==str(self.native_compiler):
   command[0]=str(self.native_compiler)
   if not all(command.count(flag)==1 for flag in SANITIZERS):raise ValueError('missing native compile/link sanitizer flags')
  else:raise ValueError('unexpected instrumented compiler')
  output=Path(command_option(command,'-o')).absolute()
  name=str(output.relative_to(self.artifacts));artifact_path(self.artifacts,name)
  if name==self.native_compiler_name:raise ValueError('instrumented output overwrites compiler snapshot')
  if name in self.binaries:raise ValueError('instrumented output compiled twice: '+name)
  if digest(self.compiler)!=self.compiler_sha256:raise ValueError('instrumented compiler changed')
  if digest(self.native_compiler)!=self.native_compiler_sha256:raise ValueError('native sanitizer compiler changed')
  result=subprocess.run(command,**options)
  if result.returncode==0:
   if not output.is_file():raise ValueError('instrumented compiler did not produce '+name)
   self.binaries[name]=digest(output);self.commands[name]=command
  return result
 def finish(self):
  if digest(self.native_compiler)!=self.native_compiler_sha256:raise ValueError('native sanitizer compiler changed')
  manifest={'schema_version':2,'compiler':{'path':self.compiler_name,'sha256':self.compiler_sha256},'native_compiler':{'path':self.native_compiler_name,'invocation':str(self.native_compiler),'sha256':self.native_compiler_sha256},'binaries':self.binaries.copy(),'commands':self.commands.copy()}
  verify_instrumented(self.artifacts,manifest)
  return manifest

def verify_instrumented(artifacts,manifest,expected_compiler=None):
 if not isinstance(manifest,dict):raise ValueError('invalid instrumented manifest')
 if manifest.get('schema_version')!=2:raise ValueError('unsupported instrumented manifest schema')
 compiler=manifest.get('compiler',{})
 if not isinstance(compiler,dict):raise ValueError('invalid instrumented compiler record')
 name=compiler.get('path','')
 if name!='tools/v/v':raise ValueError('invalid instrumented compiler alias')
 path=Path(artifacts)/name
 # The pinned V compiler can be a prepared toolchain symlink outside the
 # artifact directory. Its alias is already a normal build-bound artifact.
 if digest(path)!=compiler.get('sha256'):raise ValueError('instrumented compiler changed')
 if expected_compiler is not None and compiler.get('sha256')!=expected_compiler:raise ValueError('instrumented compiler does not match normal build')
 native=manifest.get('native_compiler',{})
 if not isinstance(native,dict) or native.get('path')!='tools/validation-compilers/clang' or not isinstance(native.get('invocation'),str) or not Path(native['invocation']).is_absolute():raise ValueError('invalid native sanitizer compiler record')
 native_path=artifact_path(artifacts,native['path'])
 if digest(native_path)!=native.get('sha256'):raise ValueError('native sanitizer compiler snapshot changed')
 binaries=manifest.get('binaries');commands=manifest.get('commands')
 if not isinstance(binaries,dict) or not binaries or not isinstance(commands,dict) or set(commands)!=set(binaries):
  raise ValueError('instrumented manifest must bind every command/output')
 for name,expected in binaries.items():
  if name==native['path']:raise ValueError('instrumented output overwrites compiler snapshot')
  path=artifact_path(artifacts,name)
  if not path.is_file() or digest(path)!=expected:raise ValueError('instrumented binary changed: '+name)
  command=commands[name]
  if not isinstance(command,list) or not all(isinstance(part,str) for part in command) or command.count('-o')!=1:raise ValueError('invalid instrumented command: '+name)
  is_v=Path(command[0]).absolute()==Path(artifacts).absolute()/compiler['path']
  if not is_v and command[0]!=native['invocation']:raise ValueError('instrumented command compiler mismatch: '+name)
  if Path(command_option(command,'-o')).absolute()!=Path(artifacts).absolute()/name:raise ValueError('instrumented command output mismatch: '+name)
  if is_v:
   if command_option(command,'-cc') not in ('clang',native['invocation']):raise ValueError('instrumented native compiler mismatch: '+name)
   for option in ['-cflags','-ldflags']:
    if command.count(option)!=1 or not all(flag in command_option(command,option).split() for flag in SANITIZERS):raise ValueError('instrumented flags missing: '+name)
  elif not all(command.count(flag)==1 for flag in SANITIZERS):raise ValueError('native instrumented flags missing: '+name)
 return {**binaries,native['path']:native['sha256']}
