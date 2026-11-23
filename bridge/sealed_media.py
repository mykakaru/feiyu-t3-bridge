import json,sys,os,hashlib
from pathlib import Path
from bridge_control import protect
ROOT=Path(__file__).resolve().parent
MAX=2*1024*1024

def target(msgid):
 if not isinstance(msgid,str) or not 0<len(msgid)<=1024:raise ValueError('invalid_id')
 return ROOT/'media-sealed'/(hashlib.sha256(msgid.encode()).hexdigest()+'.dpapi')

def main():
 action=sys.argv[1]
 if action=='seal':
  raw=sys.stdin.buffer.read(MAX+1)
  if len(raw)>MAX:raise ValueError('size_limit')
  body=json.loads(raw); p=target(body['record']['msgid'])
  if not body.get('media'):raise ValueError('no_media')
  p.parent.mkdir(exist_ok=True)
  encrypted=protect(raw)
  # Atomic durable file, no plaintext intermediate or command-line secrets.
  tmp=p.with_suffix('.tmp')
  with tmp.open('wb') as f:f.write(encrypted);f.flush();os.fsync(f.fileno())
  os.replace(tmp,p)
 elif action=='unseal':
  p=target(sys.argv[2]); raw=protect(p.read_bytes(),decrypt=True)
  if len(raw)>MAX:raise ValueError('size_limit')
  body=json.loads(raw)
  if body['record']['msgid']!=sys.argv[2]:raise ValueError('id_mismatch')
  sys.stdout.buffer.write(raw)
 else:raise ValueError('invalid_action')

if __name__=='__main__':
 try:main()
 except Exception:
  sys.stderr.write('sealed_media_operation_failed\n');sys.exit(1)
