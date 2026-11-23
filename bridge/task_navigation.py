"""Place an existing bridge execution in the owner's desktop sidebar. Never start or resume a turn."""
import asyncio
import argparse
from contextlib import closing
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import re
import subprocess
import sqlite3
import sys
import threading
import uuid


class MetadataError(Exception):
    pass


class _Lines(asyncio.SubprocessProtocol):
    def __init__(self):
        self.stdout=asyncio.StreamReader(limit=2**21)
        self.exited=asyncio.get_running_loop().create_future()

    def pipe_data_received(self, fd, data):
        if fd==1: self.stdout.feed_data(data)

    def pipe_connection_lost(self, fd, exc):
        if fd==1: self.stdout.feed_eof()

    def process_exited(self):
        if not self.exited.done(): self.exited.set_result(True)


def native_uuid(value):
    return isinstance(value, str) and bool(re.fullmatch(r'[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}', value))


@asynccontextmanager
async def desktop(exe):
    # Use only the shipped stdio metadata protocol; do not connect a new service,
    # acquire the existing conversation, or pass bridge transport credentials.
    env = {k:v for k,v in os.environ.items() if not k.upper().startswith(('XIAOAI_', 'CONTROL_PLANE_', 'MCP_EXTRA_HEADERS', 'MCP_DISCOVERY_EXTRA_HEADERS'))}
    p, io = await asyncio.get_running_loop().subprocess_exec(_Lines,str(exe), 'app-server', '--stdio',
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        env=env, creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    writer=p.get_pipe_transport(0)
    index = 0
    async def send(packet):
        writer.write(json.dumps(packet, ensure_ascii=True).encode('ascii') + b'\n')
    async def call(method, args):
        nonlocal index
        index += 1
        await send({'id':index, 'method':method, 'params':args})
        while True:
            line = await io.stdout.readline()
            if not line:
                raise MetadataError('metadata_process_closed')
            packet = json.loads(line)
            if packet.get('id') == index and 'method' not in packet:
                if 'error' in packet:
                    raise MetadataError(method + ':' + str(packet['error'].get('code','error')))
                return packet['result']
    try:
        await call('initialize',{'clientInfo':{'name':'feiyu_task_navigation','version':'1.0'}})
        await send({'method':'initialized'})
        yield call
    finally:
        # An auxiliary process can inherit a Win32 stdout handle. The reliable
        # lifecycle boundary is our own child's process exit, not stdout EOF.
        writer.close()
        try:
            try: await asyncio.wait_for(asyncio.shield(io.exited),2)
            except asyncio.TimeoutError:
                if p.get_returncode() is None:
                    try: p.terminate()
                    except ProcessLookupError: pass
                await asyncio.wait_for(asyncio.shield(io.exited),2)
        finally:
            p.close()
            await asyncio.sleep(0)


def same_path(left, right):
    if not isinstance(left,str) or not left or not Path(left).is_absolute(): return False
    return Path(left).resolve() == Path(right).resolve()


def sidebar_title(manifest, case):
    # Only the authenticated direct request, never a quote, attachment or full
    # task prompt. Keep links, handles, control/format characters out of titles.
    anchor = manifest.get('anchor_msgid')
    direct = next((item.get('direct','') for item in manifest.get('members',[])
                   if item.get('msgid') == anchor), '')
    if not isinstance(direct,str): direct=''
    label = re.sub(r'https?://\S+|[A-Za-z]:[\\/][^\s]+|\b[a-fA-F0-9]{24,}\b',' ',direct)
    label = re.sub(r'^(?:\s*【[^】]{1,12}】|\s*@大肥鱼)+','',label,flags=re.I)
    label = ' '.join(''.join(ch if ch.isprintable() else ' ' for ch in label).split()).strip(' •:：，,。')[:38].strip()
    return '小爱 · '+(label or ('任务 '+case[-7:]))


async def arrange(call, native, work, target, title, resume):
    args = {'threadId':native, 'includeTurns':False}
    for delay in (0, .25, .65, 1.25, 2):
        if delay: await asyncio.sleep(delay)
        try:
            thread = (await call('thread/read',args))['thread']
            break
        except MetadataError:
            if delay == 2: raise
    if thread.get('id') != native or thread.get('ephemeral'):
        raise MetadataError('thread_identity_mismatch')
    # A resumed execution may point at a parent workspace. Never move or rename
    # it: use the same actual thread and respect its existing filing choices.
    if not resume and (thread.get('source') != 'exec' or not same_path(thread.get('cwd'), work)):
        raise MetadataError('new_job_workspace_mismatch')
    section = thread.get('section') or {}
    if not resume and not section:
        available = (await call('threadSection/list',{'limit':100})).get('data',[])
        if not any(x.get('id') == target for x in available):
            raise MetadataError('configured_section_unavailable')
        if not thread.get('name'):
            await call('thread/name/set',{'threadId':native,'name':title})
        await call('thread/section/move',{'threadId':native,'sectionId':target})
        thread = (await call('thread/read',args))['thread']
        if thread.get('id') != native or (thread.get('section') or {}).get('id') != target:
            raise MetadataError('section_readback_failed')
    section = thread.get('section') or {}
    return {'thread_title':thread.get('name'), 'section_id':section.get('id'),
        'section_name':section.get('name'), 'sidebar_verified':bool(section.get('id')),
        'status':'in_sidebar' if section.get('id') else 'original_thread_link'}


def store_receipt(path, data):
    tmp = path.with_name('.'+path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n', encoding='utf-8')
        os.replace(tmp,path)
    finally:
        tmp.unlink(missing_ok=True)


def start(root, work, exe, case, native, manifest):
    if not native_uuid(native) or manifest.get('case_id') != case: return None
    resume=manifest.get('resume_native')
    if resume and (not isinstance(resume,dict) or resume.get('native_id')!=native): return None
    # The native-ID commit has already enrolled this notice. The signal only reduces latency.
    try:
        from local_dispatch import wake
        wake(Path(root) / 'task_link_ready')
    except OSError:
        pass
    receipt = {'case_id':case,'native_thread_id':native,'url':'codex://threads/'+native,
               'status':'link_ready','sidebar_verified':False}
    path = Path(work)/'task-navigation.json'
    store_receipt(path,receipt)
    async def perform():
        config=json.loads((Path(root)/'task-navigation-settings.json').read_text(encoding='utf-8'))
        if not native_uuid(config.get('sidebar_section_id')): raise MetadataError('invalid_section_config')
        resumed=bool(manifest.get('resume_native'))
        async with desktop(exe) as call:
            return await arrange(call,native,work,config['sidebar_section_id'],sidebar_title(manifest,case),resumed)
    def run():
        try: result=asyncio.run(asyncio.wait_for(perform(),timeout=12))
        except Exception as exc:
            result={'status':'link_only','error':str(exc) if isinstance(exc,MetadataError) else type(exc).__name__}
        try: store_receipt(path,dict(receipt,**result))
        except OSError: pass
    # This short-lived metadata-only helper never blocks reading the CLI pipe.
    # A non-daemon thread also preserves the receipt on very short business turns.
    t=threading.Thread(name='bridge-sidebar-navigation',target=run,daemon=False)
    t.start()
    return t


def lookup(root, case):
    """Owner-side, exact known case only. No enumeration, claims, deliveries or turns."""
    if not isinstance(case,str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,128}',case):
        raise ValueError('expected an exact bridge case id')
    root=Path(root)
    with closing(sqlite3.connect((root/'local-tasks.sqlite3').resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        row=db.execute('SELECT native_id,thread_id,state FROM jobs WHERE msgid=?',(case,)).fetchone()
    if not row or not native_uuid(row[0] or row[1]):
        return {'case_id':case,'status':'no_native_id_yet' if row else 'no_native_job_record','url':None}
    native=row[0] or row[1]
    if row[0] and row[1] and row[0]!=row[1]: raise MetadataError('conflicting_native_identity')
    result={'case_id':case,'job_state':row[2],'native_thread_id':native,'url':'codex://threads/'+native,'sidebar_verified':False}
    async def current():
        exe=json.loads((root/'command-routing.json').read_text(encoding='utf-8'))['codex_exe']
        async with desktop(exe) as call:
            th=(await call('thread/read',{'threadId':native,'includeTurns':False}))['thread']
            if th.get('id')!=native or th.get('ephemeral'): raise MetadataError('thread_identity_mismatch')
            sec=th.get('section') or {}
            return {'status':'desktop_read','thread_title':th.get('name'),'section_name':sec.get('name'),'sidebar_verified':bool(sec.get('id'))}
    try: result.update(asyncio.run(asyncio.wait_for(current(),10)))
    except (OSError,MetadataError,asyncio.TimeoutError,ValueError,KeyError) as e:
        result.update(status='registered_link_only',error=type(e).__name__)
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description='Read the private desktop link for one known Feiyu bridge case.')
    p.add_argument('--case',required=True)
    sys.stdout.reconfigure(encoding='utf-8')
    print(json.dumps(lookup(Path(__file__).resolve().parent,p.parse_args().case),ensure_ascii=False))
