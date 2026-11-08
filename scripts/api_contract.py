"""Freeze exported V declarations; implementation-only function changes remain allowed."""
import argparse,hashlib,json,re
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent

def masked(text):
 # Keep offsets/newlines while removing comments and quoted-string contents.
 pattern=r'//[^\n]*|/\*[\s\S]*?\*/|\x27(?:\\.|[^\x27\\])*\x27|"(?:\\.|[^"\\])*"'
 return re.sub(pattern,lambda match:re.sub(r'[^\n]',' ',match[0]),text)

def declarations(text):
 scan=masked(text);result=[]
 for match in re.finditer(r'^pub (fn|struct|enum|interface|type|const)\b',scan,re.M):
  kind=match[1];start=match.start();cursor=match.end()
  if kind in ('fn','struct','enum','interface'):
   opening=scan.find('{',cursor)
   if opening<0:raise ValueError('public declaration has no body')
   if kind=='fn':end=opening
   else:
    depth=1;end=opening+1
    while depth and end<len(scan):
     depth+=(scan[end]=='{')-(scan[end]=='}');end+=1
    if depth:raise ValueError('unclosed public declaration')
  else:
   end=scan.find('\n',cursor)
   if end<0:end=len(scan)
   while end<len(scan) and re.match(r'\n[ \t]+\|',scan[end:]):
    following=scan.find('\n',end+1);end=following if following>=0 else len(scan)
  # Attributes affect construction/calling semantics (e.g. @[params]).
  previous=text[:start].rstrip('\n').splitlines()
  attrs=[]
  while previous and previous[-1].startswith('@['):attrs.insert(0,previous.pop())
  body=text[start:end].strip()
  if kind=='struct':
   header,inner=body.split('{',1);inner=inner.rsplit('}',1)[0]
   public=False;fields=[]
   for line in inner.splitlines():
    section=re.fullmatch(r'\s*(pub(?: mut)?|mut|global|__global):\s*',line)
    if section:
     public=section[1].startswith('pub')
     if public:fields.append(line)
    elif public:fields.append(line)
   body=header+'{\n'+'\n'.join(fields).strip()+'\n}'
  declaration='\n'.join(attrs+[body])
  # Strip comments but retain string literals/default values in the contract.
  declaration=re.sub(r'''//[^\n]*|/\*[\s\S]*?\*/|'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"''',lambda match: '' if match[0].startswith(('//','/*')) else match[0],declaration)
  result.append(declaration.strip())
 return result

def canonical(declaration):
 # Token boundaries and quoted values are contractual; layout is not.
 return re.findall(r"'(?:(?:\\.)|[^'\\])*'|\"(?:(?:\\.)|[^\"\\])*\"|[A-Za-z_][A-Za-z_0-9]*|[0-9]+|[^\s]",declaration)

def canonical_map(values):
 return {name:[canonical(item) for item in items] for name,items in values.items()}

def snapshot(root=ROOT):
 return {str(path.relative_to(root)):declarations(path.read_text()) for path in sorted((root/'sdk').glob('*.v')) if declarations(path.read_text())}

def verify(root=ROOT):
 expected=json.loads((root/'release/api-contract.json').read_text())
 actual=snapshot(root)
 if canonical_map(expected['declarations'])!=canonical_map(actual):
  changed=sorted(key for key in set(actual)|set(expected['declarations']) if [canonical(item) for item in actual.get(key,[])]!=[canonical(item) for item in expected['declarations'].get(key,[])])
  raise ValueError('frozen SDK API changed: '+', '.join(changed))
 if expected['module_version'] not in (root/'v.mod').read_text():raise ValueError('module version differs from API contract')
 return expected

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--write',action='store_true');args=parser.parse_args()
 if args.write:
  value={'schema_version':1,'api_major':1,'module_version':'1.0.0-rc.1','policy':'Public declarations frozen. Breaking public changes require a new major contract; never regenerate this lock during a build.','declarations':snapshot()}
  (ROOT/'release/api-contract.json').write_text(json.dumps(value,indent=2)+'\n')
 else:verify()
