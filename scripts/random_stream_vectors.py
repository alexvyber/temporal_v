"""Independent fixed vectors for the V-specific named-stream replay contract."""
import hashlib
import json
from pathlib import Path
MASK=(1<<64)-1

def values(name,count):
    state=int.from_bytes(hashlib.sha256(b'temporal-v-workflow-random-stream/v1\0'+bytes(8)+name.encode()).digest()[:8],'big')
    result=[]
    for _ in range(count):
        state=(state+0x9e3779b97f4a7c15)&MASK
        value=state
        value=((value^(value>>30))*0xbf58476d1ce4e5b9)&MASK
        value=((value^(value>>27))*0x94d049bb133111eb)&MASK
        result.append(value^(value>>31))
    return result

def main():
    names=['','alpha','beta','orders/alpha','orders/beta','a\0b','a','b','🌍/заказы','插件/随机','x'*4096,'default']
    vectors=[{'name':name,'values':values(name,32)} for name in names]
    destination=Path(__file__).resolve().parent.parent/'locks/random-stream-vectors.json'
    destination.write_text(json.dumps(vectors,ensure_ascii=False,indent=2)+'\n')
if __name__=='__main__':main()
