"""Apply reviewed data-only plans to dev1 via the shared checked entry; never submit."""
import ast
import contextlib
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import re
import shutil
import subprocess
import sys
import time

WORKSPACE=Path(os.environ.get('FEIYU_P4_WORKSPACE', str(Path.home() / 'Workspace')))
PROJECT_SUBPATH=os.environ.get('FEIYU_P4_PROJECT_SUBPATH', 'Client/Projects/ExampleProject')
BASE=WORKSPACE/PROJECT_SUBPATH
CHECKED=Path(os.environ.get('FEIYU_P4_CHECKED_SCRIPT', str(WORKSPACE/'scripts/checked_p4.py')))
P4_CLIENT=os.environ.get('FEIYU_P4_CLIENT', 'example_p4_client')
P4_USER=os.environ.get('FEIYU_P4_USER', 'example_p4_user')
P4=Path('C:/Program Files/Perforce/p4.exe')
DIRS=('Config','Content','Docs','Source','Plugins','Scripts','Tests','Tools')
PATH=re.compile(r'(?<![\w./\\:])(?:Client/Projects/ExampleProject/)?((?:'+ '|'.join(DIRS)+r')/(?:[\w.()+-]+/)*[\w.()+-]+\.[a-zA-Z0-9]{1,12})(?![\w./\\:])')
TEXT={'.ini','.json','.csv','.xml','.txt','.md','.lua','.cpp','.c','.h','.hpp','.cs','.py','.yaml','.yml','.usf','.ush','.tsv'}
HIDDEN={'saved','intermediate','binaries','deriveddatacache','node_modules','privatekeys','credentials','secrets'}
SENSITIVE=re.compile(r'(?i)(?:private key-----|(?:password|secret|apikey|api_key|token|privateexponent|encryptionkey)\s*["\x27]?\s*[:=]\s*["\x27]?[^\s"\x27,;]{4,})')
MAX=96000
class Refused(ValueError):pass
def digest(data):return hashlib.sha256(data).hexdigest()
def project_paths(text):
    normalized=text.replace('\\','/').replace('Client/Projects/ExampleProject/','Client/Projects/ExampleProject/')
    return list(dict.fromkeys(m.group(1) for m in PATH.finditer(normalized)))
def requested_ops(text):
    verbs={'add':r'新增|添加|新建|创建|增删|增改|\b(?:add|create)\b','edit':r'修[改订]|调整|编辑|更改|改[为成动]|替换|增删改|增改|删改|\b(?:edit|modify|replace)\b','delete':r'删除|移除|增删|删改|删掉|\b(?:delete|remove)\b'}
    return {op for op,verb in verbs.items() if re.search(verb,text,re.I) and not re.search(r'(?:不要|禁止|不得|不能|不许|不允许|不可|勿|不)(?:执行|进行)?(?:'+verb+')',text,re.I)}
def permitted(grant):
    paths=set(project_paths(grant['scope']))
    request=set(project_paths(grant['request']))
    allowed=paths & request if paths and request else (paths or request)
    ops=requested_ops(grant['request'])
    restriction=requested_ops(grant['scope'])
    if restriction:ops&=restriction
    denies={'add':'增|建|添加|创建','edit':'改|编辑|调整','delete':'删|移除'}
    for action,verbs in denies.items():
        if re.search('(?:不得|不要|不能|禁止|不许|不允许|不可|勿|不)(?:再|做|执行)?(?:'+verbs+')',grant['scope']):ops.discard(action)
    return allowed,ops
def safe_path(relative):
    if (not isinstance(relative,str) or project_paths(relative)!=[relative] or not PATH.fullmatch(relative)
        or len(relative)>250 or any(x.startswith('.') or x.endswith('.') or PureWindowsPath(x).is_reserved() or x.lower() in HIDDEN for x in relative.split('/'))
        or re.search(r'(?i)(?:credential|secret|password|defaultcrypto|defaultencryption|privatekey|authtoken)',relative)):
        raise Refused('文件不在可处理的工程路径范围')
    path=BASE/relative
    if not path.resolve().is_relative_to(BASE.resolve()) or path.resolve()!=path.absolute():
        raise Refused('路径映射已变化')
    for p in [path,*path.parents]:
        if p==WORKSPACE.parent:break
        if p.is_symlink() or p.is_junction():raise Refused('不处理链接路径')
    if path.exists() and (not path.is_file() or path.stat().st_nlink!=1):
        raise Refused('不是独立普通文件')
    return path
def snapshot(rel):
    p=safe_path(rel)
    if not p.exists():return {'path':rel,'sha256':'absent'}
    if p.suffix.lower() not in TEXT or p.stat().st_size>MAX:return {'path':rel,'inspection':'需相应资产或大文件读取工具'}
    data=p.read_bytes()
    try:body=data.decode('utf-8-sig')
    except UnicodeError:raise Refused('文件编码不支持')
    if '\x00' in body or SENSITIVE.search(body):raise Refused('该文件不适合在群里读取')
    return {'path':rel,'sha256':digest(data),'text':body}
def snapshots(paths):
    result=[]
    for rel in sorted(paths)[:12]:
        try:result.append(snapshot(rel))
        except (OSError,ValueError):result.append({'path':rel,'inspection':'无法读取'})
        if sum(len(r.get('text','').encode()) for r in result)>MAX:break
    return result
def supported_delete():
    # Use delete only when the owning checked tool actually advertises it.
    tree=ast.parse(CHECKED.read_text(encoding='utf-8-sig'))
    for call in ast.walk(tree):
        if isinstance(call,ast.Call) and call.args and isinstance(call.args[0],ast.Constant) and call.args[0].value=='operation':
            for arg in call.keywords:
                if arg.arg=='choices' and isinstance(arg.value,(ast.List,ast.Tuple)):
                    return 'delete' in ast.literal_eval(arg.value)
    return False
def validate(actions,grant):
    allowed,ops=permitted(grant)
    if not isinstance(actions,list) or not 1<=len(actions)<=24:raise Refused('需要具体文件操作清单')
    seen=set()
    for action in actions:
        if not isinstance(action,dict):raise Refused('操作格式错误')
        op=action.get('op');rel=action.get('path')
        if not isinstance(op,str) or not isinstance(rel,str):raise Refused('操作格式错误')
        if op not in ('add','edit','delete') or op not in ops or rel not in allowed:raise Refused('操作或文件超出本人授权')
        if set(action)!=({'op','path','sha256'} if op=='delete' else {'op','path','sha256','text'}):raise Refused('操作格式错误')
        p=safe_path(rel)
        if rel.casefold() in seen:raise Refused('同一文件重复')
        seen.add(rel.casefold())
        if op=='delete' and not supported_delete():raise Refused('当前工程受检 P4 入口未提供整文件 delete，原件保留')
        if p.suffix.lower() not in TEXT:raise Refused('该文件需要受检 UE 资产写入入口')
        if p.suffix.lower() in ('.cpp','.c','.h','.hpp') and not re.search(r'(?i)c\+\+|\.cpp\b|\.hpp\b',grant['scope']):
            raise Refused('工程 C++ 需要本人明确指定')
        old=snapshot(rel)
        if 'sha256' not in old or old['sha256']!=action['sha256'] or (op=='add')!=(old['sha256']=='absent'):
            raise Refused('文件基线已变化')
        if op!='delete' and (not isinstance(action['text'],str) or len(action['text'].encode())>MAX or '\x00' in action['text'] or SENSITIVE.search(action['text'])):
            raise Refused('新内容格式或大小不支持')
    return actions

def process(args,**kwargs):
    env=dict(os.environ,P4CONFIG='p4config.txt',P4CHARSET='utf8',PYTHONUTF8='1')
    return subprocess.run(args,cwd=WORKSPACE,env=env,capture_output=True,timeout=45,creationflags=subprocess.CREATE_NO_WINDOW,shell=False,**kwargs)
def query(*args,empty=False):
    response=process([str(P4),'-ztag','-Mj',*args])
    rows=[json.loads(line) for line in response.stdout.splitlines() if line.strip()]
    errors=[r for r in rows if int(r.get('severity',0))>=3]
    if (response.returncode or errors) and not (empty and errors and all(str(r.get('generic'))=='17' for r in errors)):
        raise Refused('P4 状态核验失败')
    return [r for r in rows if 'severity' not in r]
def identity():
    info=query('info')[0];client=query('client','-o',P4_CLIENT)[0];login=query('login','-s')[0]
    if (info.get('clientName')!=P4_CLIENT or Path(info.get('clientRoot','')).resolve()!=WORKSPACE.resolve()
        or client.get('Owner')!=login.get('User') or login.get('User')!=P4_USER or client.get('Client')!=info['clientName']):
        raise Refused('当前 dev1 工作区身份不符')
    return login['User'],info['clientName']
def checked(op,change,paths,preview=False):
    if op not in ('check-path','edit','add','delete') or (op=='delete' and not supported_delete()):raise Refused('没有对应的受检 P4 入口')
    args=[sys.executable,'-B',str(CHECKED),op]
    if change:args+=['--change',str(change)]
    if preview:args+=['--dry-run']
    if process([*args,'--',*[str(p) for p in paths]]).returncode:raise Refused('工程受检 P4 操作未通过')
def create_change(cid):
    user,client=identity()
    description='企微群员授权任务，仅限本地待处理修改 '+cid
    spec=f'Change: new\nClient: {client}\nUser: {user}\nStatus: new\nDescription:\n\t{description}\nFiles:\n'
    result=process([str(P4),'change','-i'],input=spec.encode())
    match=re.search(rb'\bChange (\d+) created\b',result.stdout)
    if result.returncode or not match:raise Refused('新建独立 CL 的结果待核查')
    return int(match[1])
def verify_cl(cl,cid):
    form=query('change','-o',str(cl))[0]
    if (form.get('Status')!='pending' or form.get('Client')!=P4_CLIENT or form.get('User')!=P4_USER
        or form.get('Description','').strip()!='企微群员授权任务，仅限本地待处理修改 '+cid):
        raise Refused('专用 CL 的状态或归属改变')
def inspect(rel):
    path=safe_path(rel)
    mapping=query('where',str(path))
    if len(mapping)!=1 or 'unmap' in mapping[0] or Path(mapping[0].get('path','')).resolve()!=path.resolve():raise Refused('P4 映射不唯一')
    rows=query('fstat','-Ol',str(path),empty=True)
    if len(rows)>1:raise Refused('文件映射不唯一')
    return mapping[0]['depotFile'],rows[0] if rows else {}
def preflight(plan):
    identity()
    checked('check-path',None,[safe_path(a['path']) for a in plan])
    for a in plan:
        dep,stat=inspect(a['path'])
        if stat.get('action') or stat.get('otherOpen') or stat.get('otherLock'):raise Refused('文件已有打开或锁定记录')
        if a['op']=='add':
            if stat.get('headRev') and stat.get('headAction') not in ('delete','move/delete'):raise Refused('仓库已有同名文件')
        elif (not stat.get('haveRev') or stat.get('haveRev')!=stat.get('headRev') or stat.get('headAction') in ('delete','move/delete')
              or query('diff','-se',str(safe_path(a['path'])),empty=True)):raise Refused('仓库版本或本地未检出差异待处理')
def write_json(path,obj):
    tmp=path.with_suffix('.tmp')
    with tmp.open('w',encoding='utf-8') as stream:
        json.dump(obj,stream,ensure_ascii=False,indent=2);stream.flush();os.fsync(stream.fileno())
    os.replace(tmp,path)
def apply(root,cid,plan,grant,still_allowed):
    from win_runtime import SingleInstance
    with SingleInstance('PeerPendingDev1',WORKSPACE) as lock:
        if not lock.acquired:raise Refused('授权写入入口正在办理另一项修改')
        validate(plan,grant);preflight(plan)
        ledger=Path(root)/'peer-change-recovery'/cid
        ledger.mkdir(parents=True,exist_ok=False)
        receipt={'case':cid,'files':{},'state':'preparing','pending_cl':None}
        for i,a in enumerate(plan):
            p=safe_path(a['path'])
            if p.exists():
                backup=ledger/(str(i)+'.original')
                shutil.copyfile(p,backup)
                if digest(backup.read_bytes())!=a['sha256']:raise Refused('原件备份校验失败')
            receipt['files'][a['path']]={'before':a['sha256'],'backup':str(i)+'.original' if p.exists() else None,'op':a['op']}
        write_json(ledger/'receipt.json',receipt)
        still_allowed()
        cl=create_change(cid)
        receipt.update(state='pending',pending_cl=cl);write_json(ledger/'receipt.json',receipt)
        def opened():return {r['depotFile']:r['action'] for r in query('opened','-c',str(cl),empty=True)}
        expected={}
        for a in plan:
            still_allowed();verify_cl(cl,cid)
            if opened()!=expected:raise Refused('专用 CL 文件集发生变化')
            validate([a],grant);preflight([a])
            rel,op=a['path'],a['op'];p=safe_path(rel);dep,_=inspect(rel)
            if op!='add':
                checked('edit',cl,[p],True);checked('edit',cl,[p])
                if opened()!={**expected,dep:'edit'}:raise Refused('未取得本 CL 的 edit')
                if digest(p.read_bytes())!=a['sha256']:raise Refused('检出后原件发生变化')
            if op=='delete':
                checked('delete',cl,[p],True);checked('delete',cl,[p])
            else:
                new=a['text']
                if op=='edit':
                    old=p.read_bytes()
                    if b'\r\n' in old and b'\n' not in old.replace(b'\r\n',b''):new=new.replace('\r\n','\n').replace('\n','\r\n')
                    data=(b'\xef\xbb\xbf' if old.startswith(b'\xef\xbb\xbf') else b'')+new.lstrip('\ufeff').encode()
                    if opened()!={**expected,dep:'edit'}:raise Refused('文件不在本 CL')
                    with p.open('r+b') as f:
                        if digest(f.read())!=a['sha256']:raise Refused('写前原件发生变化')
                        f.seek(0);f.write(data);f.truncate();f.flush();os.fsync(f.fileno())
                else:
                    data=new.encode()
                    # Stage the bytes privately and remember newly created parents so
                    # a rejected checked ADD can restore its original absent state.
                    staged=ledger/(str(list(receipt['files']).index(rel))+'.new')
                    staged.write_bytes(data)
                    missing=[]
                    parent=p.parent
                    while not parent.exists():
                        missing.append(parent);parent=parent.parent
                    p.parent.mkdir(parents=True,exist_ok=True)
                    created=False
                    try:
                        with p.open('xb') as f:
                            created=True;f.write(data);f.flush();os.fsync(f.fileno())
                        checked('add',cl,[p],True);checked('add',cl,[p])
                    except Exception:
                        if created and not inspect(rel)[1].get('action') and safe_path(rel)==p and p.read_bytes()==data:
                            p.unlink()
                            for directory in missing:
                                try:directory.rmdir()
                                except OSError:break
                            receipt['files'][rel]['restored_absent']=True
                            write_json(ledger/'receipt.json',receipt)
                        raise
                if digest(p.read_bytes())!=digest(data):raise Refused('写后哈希校验失败')
                receipt['files'][rel]['after']=digest(data)
            expected[dep]=op
            verify_cl(cl,cid)
            if opened()!=expected or (op=='delete' and p.exists()):raise Refused('写后 CL 清单核验失败')
            write_json(ledger/'receipt.json',receipt)
        receipt['state']='verified_pending';write_json(ledger/'receipt.json',receipt)
        return cl,len(expected)
