"""Official SDK receiver lifecycle; credentials use an independent Windows DPAPI store."""
from pathlib import Path
import argparse
from contextlib import nullcontext
import ctypes
from ctypes import wintypes
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent
import shutil as _shutil
LEGACY = Path(os.environ.get('FEIYU_LEGACY_RECEIVER_DIR', str(Path.home() / 'AppData/Local/wecom-aibot-receiver')))
_node_env = os.environ.get('FEIYU_NODE_EXE') or _shutil.which('node') or 'node'
NODE = Path(_node_env)
SCRIPT = Path(__file__).with_name('wecom_receiver.cjs')

class Blob(ctypes.Structure):
    _fields_ = [('cbData', wintypes.DWORD), ('pbData', ctypes.POINTER(ctypes.c_ubyte))]

def protect(data, decrypt=False):
    buf = ctypes.create_string_buffer(data)
    source = Blob(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)))
    output = Blob()
    api = ctypes.windll.crypt32.CryptUnprotectData if decrypt else ctypes.windll.crypt32.CryptProtectData
    ok = api(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(output))
    if not ok: raise ctypes.WinError()
    try: return ctypes.string_at(output.pbData, output.cbData)
    finally: ctypes.windll.kernel32.LocalFree(output.pbData)

def read_state():
    from win_runtime import read_json_retry
    return read_json_retry(ROOT / 'status.json')

def live(state):
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(state['updated_at'])).total_seconds()
        if not 0 <= age < 8 or state.get('state') == 'stopped': return False
        kernel=ctypes.windll.kernel32
        kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD]
        kernel.OpenProcess.restype=wintypes.HANDLE
        kernel.GetExitCodeProcess.argtypes=[wintypes.HANDLE,ctypes.POINTER(wintypes.DWORD)]
        kernel.CloseHandle.argtypes=[wintypes.HANDLE]
        handle=kernel.OpenProcess(0x1000,False,int(state['pid']))
        if not handle:return False
        try:
            code=wintypes.DWORD()
            return bool(kernel.GetExitCodeProcess(handle,ctypes.byref(code))) and code.value==259
        finally:kernel.CloseHandle(handle)
    except (KeyError, ValueError): return False

def pending(limit):
    if not 1 <= limit <= 100: raise ValueError('limit must be 1..100')
    rows,errors=[],[]
    for f in (ROOT/'queue').glob('*.json'):
        try:
            row=json.loads(f.read_text(encoding='utf-8'))
            if not isinstance(row, dict) or not isinstance(row.get('received_at'),str) or not isinstance(row.get('msgid'),str):
                raise ValueError('invalid_queue_record')
            if row.get('delivery')=='pending':rows.append(row)
        except (OSError,ValueError,TypeError,AttributeError):
            errors.append({'file':f.name,'reason':'unreadable_queue_record'})
    rows.sort(key=lambda row:(row.get('received_at',''),row.get('msgid','')))
    from policy import decision
    for row in rows[:limit]:
        row['routing_policy']=decision(row,root=ROOT)
        row['owner_verified']=row['routing_policy']['sender_is_owner']
    return {'messages':rows[:limit],'pending_total':len(rows),'has_more':len(rows)>limit,'queue_errors':errors,'untrusted_external_content':True}

def main():
    if sys.stdout is not None:sys.stdout.reconfigure(encoding='utf-8')
    p=argparse.ArgumentParser()
    p.add_argument('action',choices=['start','status','read','stop','worker'])
    p.add_argument('--limit',type=int,default=10)
    args=p.parse_args()
    from win_runtime import SingleInstance
    guard = SingleInstance('Receiver'+args.action, ROOT) if args.action in ('start','worker') else nullcontext()
    with guard as instance:
        if instance is not None and not instance.acquired:
            print(json.dumps({'already_running_or_starting':True}))
            return 0
        return execute(args)

def execute(args):
    ROOT.mkdir(parents=True,exist_ok=True)
    cred=LEGACY/'receiver-credentials.dpapi'
    if args.action=='worker':
        config=protect(cred.read_bytes(),decrypt=True)
        for attempt in range(7):
            with (ROOT/'worker.stderr.log').open('ab') as log:
                child=subprocess.Popen([str(NODE),str(SCRIPT),str(ROOT)],stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=log,shell=False,creationflags=subprocess.CREATE_NO_WINDOW)
                child.communicate(config)
            state=read_state()
            if child.returncode==0 or state.get('error_type')=='WSAuthFailureError' or 'replaced' in state.get('reason','').lower():
                return child.returncode
            for _ in range(min(30,2**attempt)):
                if (ROOT/'stop.request').exists(): return 0
                time.sleep(1)
        return child.returncode
    elif args.action=='start':
        state=read_state()
        delegated_via=None
        if not live(state):
            if not cred.is_file(): raise RuntimeError('Receiver credentials not configured')
            (ROOT/'stop.request').unlink(missing_ok=True)
            launched_at=datetime.now(timezone.utc)
            from win_runtime import spawn_detached_or_delegate
            with (ROOT/'launcher.stderr.log').open('ab') as log:
                cmd=[sys.executable,str(Path(__file__).resolve()),'worker']
                mode, via = spawn_detached_or_delegate(
                    cmd, cwd=ROOT, root=ROOT, service_name='feiyu_receiver',
                    stdout=subprocess.DEVNULL, stderr=log,
                )
            if mode == 'blocked':
                print(json.dumps({'running':False,'error':via,**state},ensure_ascii=False))
                return 1
            if mode == 'delegated':
                delegated_via = via
            for _ in range(65):
                time.sleep(.5)
                state=read_state()
                try:
                    if datetime.fromisoformat(state['started_at']) < launched_at:
                        continue
                except (KeyError, ValueError):
                    continue
                if live(state) and state.get('state')=='authenticated': break
                if state.get('state')=='stopped' and state.get('reason') not in ['local stop request']: break
        out={'running':live(state),**state}
        if delegated_via:
            out['delegated_via']=delegated_via
        print(json.dumps(out,ensure_ascii=False))
        return 0 if live(state) and state.get('state')=='authenticated' else 1
    elif args.action=='status':
        state=read_state()
        print(json.dumps({'running':live(state),**state},ensure_ascii=False))
    elif args.action=='read':
        print(json.dumps(pending(args.limit),ensure_ascii=False))
    elif args.action=='stop':
        (ROOT/'stop.request').touch()
        for _ in range(15):
            if not live(read_state()):break
            time.sleep(.3)
        state=read_state()
        print(json.dumps({'stopped':not live(state),**state},ensure_ascii=False))
        return 0 if not live(state) else 1
    return 0

if __name__=='__main__':
    raise SystemExit(main())
