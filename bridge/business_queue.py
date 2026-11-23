"""Durable conversation-scoped work orders. Raw callback files are an input journal."""
import hashlib
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager, closing
from datetime import datetime, timezone
from pathlib import Path

from command_routing import enabled, private_default, select
from identity import resolve
import group_replies
import peer_permissions
import pending_expiry


def utc(value=None):
    return datetime.fromtimestamp(time.time() if value is None else value, timezone.utc).isoformat()


def key(value):
    return 'w_' + hashlib.sha256(value.encode()).hexdigest()[:32]


def dumped(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def load(path, default):
    try:
        return json.loads(path.read_text(encoding='utf-8-sig'))
    except (OSError, UnicodeError, ValueError):
        return default


@contextmanager
def db(root, write=False):
    conn = sqlite3.connect(Path(root) / 'business.sqlite3', timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('PRAGMA foreign_keys=ON')
        if write:
            conn.execute('BEGIN IMMEDIATE')
        yield conn
        conn.commit()
    finally:
        conn.close()


def initialize(root):
    with db(root) as c:
        c.executescript("""
          PRAGMA journal_mode=WAL;
          CREATE TABLE IF NOT EXISTS cases(
            id TEXT PRIMARY KEY, anchor TEXT UNIQUE, chat TEXT, sender TEXT, at TEXT,
            epoch REAL, title TEXT, mode TEXT, expected INTEGER, category TEXT, note TEXT,
            related TEXT DEFAULT '[]', frozen TEXT, updated REAL NOT NULL);
          CREATE INDEX IF NOT EXISTS nearby ON cases(chat,sender,epoch);
          CREATE TABLE IF NOT EXISTS sources(
            mid TEXT PRIMARY KEY, chat TEXT, sender TEXT, epoch REAL, item TEXT, case_id TEXT REFERENCES cases(id));
          CREATE INDEX IF NOT EXISTS materials ON sources(chat,sender,epoch,case_id);
          CREATE TABLE IF NOT EXISTS lanes(
            case_id TEXT REFERENCES cases(id), target TEXT, state TEXT, since REAL,
            changed REAL, detail TEXT DEFAULT '', result TEXT DEFAULT '', evidence TEXT DEFAULT '[]',
            PRIMARY KEY(case_id,target));
          CREATE TABLE IF NOT EXISTS imports(name TEXT PRIMARY KEY);
          CREATE TABLE IF NOT EXISTS intake_files(name TEXT PRIMARY KEY,signature TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS intake_errors(source TEXT PRIMARY KEY,at REAL,reason TEXT);
          CREATE TABLE IF NOT EXISTS observations(case_id TEXT REFERENCES cases(id),source TEXT,at REAL,detail TEXT,
            PRIMARY KEY(case_id,source));
        """)
        from chat_feedback import install
        install(c)
        peer_permissions.install(c)
        import message_batches
        message_batches.install(c)
        import task_controls
        task_controls.install(c)
        import owner_review
        owner_review.install(c)
        import clarifications
        clarifications.install(c)
        import conclusion_notes
        conclusion_notes.install(c)
        import group_relay
        group_relay.install(c)
        import agent_relay
        agent_relay.install(c)
        if 'bot_channel' not in {r[1] for r in c.execute('PRAGMA table_info(cases)')}:
            c.execute("ALTER TABLE cases ADD COLUMN bot_channel TEXT NOT NULL DEFAULT 'feiyu'")


def direct(row):
    return '\n'.join(p['text'] for p in row.get('parts', []) if not p.get('quoted')
                     and p.get('type') in ('text','voice_transcript') and isinstance(p.get('text'),str)).strip()


def summarize(root, row):
    mid = row['msgid']
    evidence = load(Path(root)/'business-evidence'/(mid + '.json'), {}) if re.fullmatch('[a-zA-Z0-9_-]{1,128}',mid) else {}
    files = []
    for a in row.get('attachments', []):
        info = {k:a[k] for k in ('type','state','quoted','bytes','sha256','display_name','reason') if k in a}
        if evidence.get('msgid') == mid and evidence.get('sender') == row.get('sender') and evidence.get('msgtype') == 'file' and not a.get('quoted'):
            if evidence.get('error',{}).get('http_status') == 403 and a.get('state') != 'downloaded':
                info.update(state='unavailable', reason='HTTP 403 ' + str(evidence['error'].get('service_code','')), attempts=evidence.get('attempts'))
        elif isinstance(a.get('failure'),dict):
            info['failure'] = {k:v for k,v in a['failure'].items() if k in ('http_status','service_code','name')}
        if a.get('state') == 'downloaded' and isinstance(a.get('relative_path'),str):
            base=(Path(root)/'media').resolve()
            path=(Path(root)/a['relative_path']).resolve()
            if path.is_relative_to(base) and path.is_file():
                info['local_path']=str(path)
            else:
                info.update(state='unavailable',reason='local_file_missing')
        files.append(info)
    return {'msgid':mid,'chatid':row['chatid'],'sender':row['sender'],'received_at':row['received_at'],
            'chattype':row['chattype'],'msgtype':row['msgtype'],'direct':direct(row),
            'bot_channel':row.get('bot_channel') or 'feiyu',
            'quotes':[p['text'] for p in row.get('parts',[]) if p.get('quoted') and isinstance(p.get('text'),str)],
            'attachments':files,'delivery_at_import':row.get('delivery','unknown')}


def with_media(root,row):
    if not row.get('msgid'):return row
    filename=hashlib.sha256(row['msgid'].encode()).hexdigest()+'.json'
    media=load(Path(root)/'media-state'/filename,{})
    if media.get('schema')!=1 or any(media.get(k)!=row.get(k) for k in ('msgid','sender','chatid','received_at')):return row
    merged={**row,**{k:media[k] for k in ('attachments','attachment_count','media_status','processed_at','media_source_sealed',
             'media_persistence_error','processing_interrupted','media_gap') if k in media}}
    if merged.get('delivery')=='processing' and merged.get('media_status') in ('completed','failed','interrupted','none'):merged['delivery']='pending'
    return merged


def file_count(items):
    return sum(1 for item in items for a in item['attachments'] if a.get('type')=='file' and not a.get('quoted'))


PRIVATE_FILE_REFERENCE = re.compile(r'(?:这个|这份|刚发的|刚才发的|随后发的|接下来发的)(?:.{0,4})(?:附件|文件)')
OUTPUT_AS_ATTACHMENT = re.compile(r'(?:把|将)?(?:这份|这个|该)(?:原件|成品|生成的文件|文档|文件)(?:作为|当作|用作)(?:一[个份])?附件')


def incoming_private_reference(text):
    # A request to generate something and return that original as an attachment
    # is not a promise to upload an input. Other input references still count.
    if re.search(r'生成|新建|制作|导出|创建',text):
        text = OUTPUT_AS_ATTACHMENT.sub('',text)
    return bool(PRIVATE_FILE_REFERENCE.search(text))


def correct_outbound_file_wait(c, root, case):
    """Reclassify only an unsettled, single-source owner request for its own generated output."""
    if case['expected'] != 1 or case['chat'] != case['sender'] or json.loads(case['related'] or '[]'):
        return case
    items = members(c,case['id'])
    if len(items) != 1:
        return case
    item = items[0]
    text = item['direct']
    if (item['msgid'] != case['anchor'] or item['chattype'] != 'single' or item['sender'] != case['sender']
            or item['chatid'] != case['chat'] or item['attachments'] or item['quotes']
            or not OUTPUT_AS_ATTACHMENT.search(text) or not PRIVATE_FILE_REFERENCE.search(text)
            or expected(text) or incoming_private_reference(text) or not resolve(root,case['sender'])['is_owner']
            or c.execute("SELECT 1 FROM message_batches WHERE cid=?", (case['id'],)).fetchone()):
        return case
    c.execute("UPDATE cases SET expected=0 WHERE id=? AND category='work' AND frozen IS NULL AND expected=1",
              (case['id'],))
    return c.execute("SELECT * FROM cases WHERE id=?", (case['id'],)).fetchone()


def expected(text, *, declared_batch=False):
    if not declared_batch and not re.search(r'发|传|附上|下面|以下|随后|接下来|这|分析|合并|整合|整理|评估|检查|处理|attach|send|review|analy[sz]e',text,re.I):
        return 0
    match = re.search(r'([1-9][0-9]?|两|二|三|一|四|五)\s*(?:个|份|条)?\s*(?:不同的?)?\s*(?:附件|文件|材料|files?\b)',text,re.I)
    if not match:
        match=re.search(r'(?:我|随后|接下来).{0,5}(?:发|传)\s*([1-9][0-9]?|两|二|三|一|四|五)\s*版(?:方案|稿子|文档|设计稿)',text)
    if not match:return 0
    digits={'一':1,'二':2,'两':2,'三':3,'四':4,'五':5}
    value=match[1]
    return int(value) if value.isdecimal() else digits[value]


def mode_for(root, row):
    verified = resolve(root, row['sender'])['is_owner']
    if not verified:
        return 'colleague_followup'
    return 't3'


def make_case(c, item, mode, count=0, category='work', note='', related=(), bot_channel=None):
    mid=item['msgid']; cid=key(mid); t=datetime.fromisoformat(item['received_at'].replace('Z','+00:00')).timestamp()
    ch = bot_channel or item.get('bot_channel') or 'feiyu'
    c.execute('INSERT OR IGNORE INTO cases(id,anchor,chat,sender,at,epoch,title,mode,expected,category,note,related,updated,bot_channel) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
              (cid,mid,item['chatid'],item['sender'],item['received_at'],t,item['direct'][:100] or '单独收到的材料',mode,count,category,note,dumped(related),time.time(),ch))
    c.execute('UPDATE sources SET case_id=? WHERE mid=?',(cid,mid))
    return cid


def members(c,cid):
    return [json.loads(row[0]) for row in c.execute('SELECT item FROM sources WHERE case_id=? ORDER BY epoch,rowid',(cid,))]


def add_lane(c,cid,target,now):
    c.execute("INSERT OR IGNORE INTO lanes(case_id,target,state,since,changed) VALUES(?,?,'queued',?,?)",(cid,target,now,now))


def insert_source(c,item):
    epoch=datetime.fromisoformat(item['received_at'].replace('Z','+00:00')).timestamp()
    c.execute('INSERT INTO sources(mid,chat,sender,epoch,item) VALUES(?,?,?,?,?)',(item['msgid'],item['chatid'],item['sender'],epoch,dumped(item)))
    return epoch


BOT_QUOTE_CASE_TAG = re.compile(
    r'^(?:Windows 接手 · [^\n]*\n)?'
    r'(?:(?:追加|补发)?办理结果|已收到(?:确认)?|已收齐|正在收集|需要确认|追问|提醒[：:]\s*事项|事项|已授权)'
    r'〔([0-9a-f]{7})〕',
    re.I,
)
BOT_QUOTE_DRAFT_ID = re.compile(r'^【待确认群答复\s*·\s*(D-[0-9a-f]{4,8})】', re.I)


def quote_links(c,item):
    targets=[]
    for quote in item['quotes']:
        if not isinstance(quote,str):continue
        q=quote.strip()
        if not q:continue
        rows=c.execute("""SELECT DISTINCT case_id FROM sources WHERE chat=? AND sender=? AND case_id IS NOT NULL
                          AND json_extract(item,'$.direct')=? AND mid<>? LIMIT 2""",
                       (item['chatid'],item['sender'],q,item['msgid'])).fetchall()
        if len(rows)==1 and rows[0][0] not in targets:
            targets.append(rows[0][0])
            continue
        m_tag=BOT_QUOTE_CASE_TAG.match(q)
        if m_tag:
            tag=m_tag.group(1).lower()
            if (item.get('chattype')=='single' and item.get('sender')==item.get('chatid')
                    and (item.get('bot_channel') or 'feiyu')=='feiyu'
                    and c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='request_routes'").fetchone()):
                crows=c.execute(
                    """SELECT id FROM cases WHERE sender=? AND lower(substr(id,-7))=?
                       AND (chat=? OR EXISTS (SELECT 1 FROM request_routes r WHERE r.case_id=cases.id AND r.origin_kind='group'))
                       AND anchor<>? AND category<>'control' LIMIT 2""",
                    (item['sender'],tag,item['chatid'],item['msgid']),
                ).fetchall()
            else:
                crows=c.execute(
                    """SELECT id FROM cases WHERE chat=? AND sender=? AND lower(substr(id,-7))=?
                       AND anchor<>? AND category<>'control' LIMIT 2""",
                    (item['chatid'],item['sender'],tag,item['msgid']),
                ).fetchall()
            if len(crows)==1 and crows[0][0] not in targets:
                targets.append(crows[0][0])
                continue
        m_draft=BOT_QUOTE_DRAFT_ID.match(q)
        if m_draft and c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='group_drafts'").fetchone():
            drows=c.execute(
                """SELECT DISTINCT case_id FROM group_drafts
                   WHERE owner_chat=? AND owner_sender=? AND upper(draft_id)=? LIMIT 2""",
                (item['chatid'],item['sender'],m_draft.group(1).upper()),
            ).fetchall()
            if len(drows)==1 and drows[0][0] not in targets:
                targets.append(drows[0][0])
    return targets


def attach_available(c,cid,anchor,num,root):
    if num <= 0:return
    owned=members(c,cid)
    missing=num-file_count(owned)
    if missing<=0:return
    rows=c.execute('SELECT * FROM sources WHERE chat=? AND sender=? AND case_id IS NULL AND epoch BETWEEN ? AND ? ORDER BY epoch,mid',
                   (anchor['chat'],anchor['sender'],anchor['epoch']-120,anchor['epoch']+900)).fetchall()
    single_owner=(resolve(root,anchor['sender'])['is_owner'] and anchor['sender']==anchor['chat'])
    if single_owner and num==1:
        files=[r for r in rows if file_count([json.loads(r['item'])]) and not json.loads(r['item'])['direct']]
        # If multiple uploads fit one instruction, an exact filename must select it.
        if len(files)>1:
            named=[r for r in files if any(a.get('display_name') and a['display_name'] in anchor['title'] for a in json.loads(r['item'])['attachments'])]
            if len(named)!=1:return
            rows=named
    for row in rows:
        item=json.loads(row['item'])
        n=file_count([item])
        if item['direct'] or not n or n>missing:continue
        possible=[]
        for other in c.execute("""SELECT * FROM cases WHERE chat=? AND sender=? AND expected>0 AND category='work'
                   AND epoch BETWEEN ? AND ? AND frozen IS NULL""",(row['chat'],row['sender'],row['epoch']-900,row['epoch']+120)):
            if file_count(members(c,other['id']))<other['expected']:possible.append(other['id'])
        if possible!=[cid]:continue
        c.execute('UPDATE sources SET case_id=? WHERE mid=? AND case_id IS NULL',(cid,row['mid']))
        missing-=n
        if not missing:break


def ingest_new(c,root,row,item,epoch):
    import agent_relay
    import clarifications
    import conclusion_notes
    import group_relay
    import message_batches
    import request_flow
    import task_controls

    bot_channel = item.get('bot_channel') or row.get('bot_channel') or 'feiyu'
    # 1. Colleague group mentions are strictly zero-LLM static conclusion lookups / private owner push.
    if request_flow.member_group(root, item):
        conclusion_notes.handle_colleague_group_message(c, root, item, epoch)
        return
    # 2. Unbound private callbacks are source records; they have no task authority.
    if item['chattype']=='single':
        if item['sender']!=item['chatid'] or not resolve(root,item['sender'])['is_owner']:
            return
        # 2.5 Zero-LLM owner private group-relay & draft confirmation commands (【直回】/【直发群】/发/【改发】/不发)
        if group_relay.handle_owner_private_command(c, root, item, epoch, bot_channel=bot_channel):
            return
    elif item['chattype']!='group':
        return
    else:
        if not group_replies.addressed_to_bot(item):
            return
        # Owner group commands for publishing/updating/deleting static colleague notes
        if conclusion_notes.handle_owner_group_command(c, root, item, epoch):
            return
    # 3. Clarification answers take priority so replies never get eaten by batches or new tasks.
    if clarifications.intake_answer(c, root, item, epoch):
        return
    # 4. Targeted task controls (【追加】/【取消】 with case ID) take priority before open batches.
    if task_controls.intake(c, root, item, epoch):
        return
    # 4.5 Agent-initiated ask replies (quoted footer or A-XXXX / threadId) route back to the originating T3 thread.
    if agent_relay.intake_reply(c, root, item, epoch):
        return
    # 5. Private batch collection (【开始收集】..【收尾】)
    if message_batches.handle(c, root, row, item, epoch) or not item['direct']:
        return
    if item['chattype']=='group' and group_replies.owner_mention(root,item) and item.get('quotes'):
        conclusion_notes.resolve_matching_open_followup_in_chat(c, item['chatid'], item['quotes'], now=epoch)
    refs=quote_links(c,item)
    mode=mode_for(root,row)
    text=item['direct']
    count=expected(text)
    owner_private=(row['chattype']=='single' and row['chatid']==row['sender'] and resolve(root,row['sender'])['is_owner'])
    if owner_private:
        if re.search(r'(?:生成|新建|制作|导出|创建)(?:.{0,8})(?:文件|文档|附件)',text) and not re.search(r'我.{0,6}(?:会发|发了|已发|会传|上传给你)|(?:用|根据|读取|处理)(?:.{0,6})(?:附件|文件)',text):
            count=0
        elif not count and not item['attachments'] and not refs and incoming_private_reference(text):
            count=1
    if count and not refs and re.search(r'刚才|之前|前面|上面|先前|已经发|已发|有没有.{0,5}跟办',item['direct']):
        candidates=[r['id'] for r in c.execute("""SELECT id FROM cases WHERE chat=? AND sender=? AND expected=?
            AND epoch BETWEEN ? AND ? ORDER BY epoch DESC""",(item['chatid'],item['sender'],count,epoch-7200,epoch))
            if file_count(members(c,r['id']))==count]
        if len(candidates)==1:refs=candidates
    if refs and file_count([m for ref in refs for m in members(c,ref)])>=count:count=0
    case_note=''
    quote_receipt_tag=task_controls.quoted_receipt_tag(item.get('quotes'))
    if quote_receipt_tag:
        case_note=(
            f'原事项〔{quote_receipt_tag}〕已结束，已携引用上下文作为新任务办理。'
            if refs else
            f'引用历史收据〔{quote_receipt_tag}〕上下文，已作为新任务办理。'
        )
    cid=make_case(c,item,mode,count,note=case_note,related=refs)
    from chat_feedback import enroll
    enroll(c,root,cid,item,epoch)


def migrate(c,root,rows,plan):
    """Only the specifically reviewed historical case is commissioned for new execution."""
    if not plan or c.execute("SELECT 1 FROM imports WHERE name='known_history'").fetchone():return
    by_id={r['msgid']:r for r in rows}
    for group in plan.get('groups',[]):
        ids=group['sources']
        if not ids or any(mid not in by_id for mid in ids):raise ValueError('missing_migration_source')
        row=by_id[ids[0]]
        if row['chattype']!='single' or not resolve(root,row['sender'])['is_owner'] or any(
                by_id[m]['chatid']!=row['chatid'] or by_id[m]['sender']!=row['sender'] for m in ids):
            raise ValueError('migration_origin_mismatch')
        items=[summarize(root,by_id[m]) for m in ids]
        for item in items:insert_source(c,item)
        if group.get('required_files') is not None and file_count(items)!=group['required_files']:
            raise ValueError('migration_file_mismatch')
        related=[key(m) for m in group.get('related',[])]
        cid=make_case(c,items[0],group['mode'],category=group['category'],note=group['note'],related=related)
        for m in ids[1:]:c.execute('UPDATE sources SET case_id=? WHERE mid=?',(cid,m))
    for mid in plan['historical_ids']:
        if mid not in by_id or c.execute('SELECT 1 FROM sources WHERE mid=?',(mid,)).fetchone():continue
        item=summarize(root,by_id[mid]);insert_source(c,item)
        if item['delivery_at_import']=='pending' and item['direct']:
            make_case(c,item,'history',category='historical',note='升级前的未关闭事项；保留原执行记录，等待核对实际结果。')
    c.execute("INSERT INTO imports VALUES('known_history')")


class PreGateClarification(Exception):
    def __init__(self, check):
        super().__init__(check.get('reason', 'clarification_required'))
        self.check = check


def package(c,root,case):
    import ambiguity_gate
    import clarifications
    import owner_review
    adoption = owner_review.adoption(c, case['id'])
    own=members(c,case['id']); linked=[]; seen={case['id']};todo=list(json.loads(case['related']))
    has_controls=bool(c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='owner_task_controls'").fetchone())
    while todo and len(linked)<16:
        target=todo.pop(0)
        if target in seen:continue
        seen.add(target)
        other=c.execute('SELECT * FROM cases WHERE id=? AND chat=? AND sender=?',(target,case['chat'],case['sender'])).fetchone()
        if other:
            entry={
                'case_id':target,
                'title':other['title'],
                'at':other['at'],
                'category':other['category'],
                'note':other['note'],
                'members':members(c,target),
            }
            if len(linked)<4:
                lane_results=[
                    {'target':lr['target'],'state':lr['state'],'result':lr['result'][:12000]}
                    for lr in c.execute('SELECT target,state,result FROM lanes WHERE case_id=? ORDER BY target',(target,))
                    if lr['result']
                ]
                if lane_results:
                    entry['results']=lane_results
                rel_clar=[
                    {k:ch[k] for k in ('qid','round_no','reason','answer','state') if k in ch}
                    for ch in clarifications.history(c,target)
                ]
                if rel_clar:
                    entry['clarifications']=rel_clar
                if has_controls:
                    rel_appends=[
                        {'change_id':ar['cid'],'text':ar['body'],'state':ar['result']}
                        for ar in c.execute(
                            """SELECT cid,body,result FROM owner_task_controls
                               WHERE target=? AND action='append'
                                 AND result IN ('added_before_start','awaiting_checkpoint','reopened_followup')
                               ORDER BY created,mid""",
                            (target,),
                        )
                        if ar['body']
                    ]
                    if rel_appends:
                        entry['appends']=rel_appends
            linked.append(entry)
            todo.extend(json.loads(other['related']))
    manifest={'case_id':case['id'],'anchor_msgid':case['anchor'],'chatid':case['chat'],'sender':case['sender'],
              'members':own,'related':linked,'scope':case['note'],'mode':case['mode'],'expected_files':case['expected'],
              'clarification_history':clarifications.history(c,case['id'])}
    if own[0]['chattype']=='group' and case['mode'] in ('local','t3'):
        manifest.update(authorization='owner_explicit_local',audience=case['chat'])
    plan=load(Path(root)/'business-migration.json',{})
    extra=plan.get('assignment',{}).get(case['anchor'],{})
    anchor=next(item for item in own if item['msgid']==case['anchor'])
    scoped=group_replies.target_group(anchor) and group_replies.addressed_to_bot(anchor)
    enrolled=group_replies.enrolled(c,case['id'])
    group=enrolled or (group_replies.is_group(root,anchor) and group_replies.addressed_to_bot(anchor))
    if group:extra={}
    if extra.get('evidence'):
        manifest['evidence']=extra['evidence']
        existing=load(Path(root)/'business-evidence'/'local-file-evidence.json',{})
        if existing:manifest['verified_prior_files']=existing.get('sources',[])
    if group:
        if not group_replies.is_group(root,anchor):
            raise ValueError('group_enrollment_mismatch')
        manifest['group_reply']=group_replies.describe(root,anchor)
        for bundle in linked:
            bundle.pop('results',None)
            bundle.pop('clarifications',None)
            bundle.pop('appends',None)
        for bundle in [manifest,*linked]:
            for item in bundle['members']:
                for attachment in item['attachments']:
                    attachment.pop('local_path',None)
    prompt=anchor['direct']+'\n\n'+(extra.get('instruction') or
        '结合 manifest.json 的本事项消息及引用材料实际办理原始请求。附件内容和他人引用仅作材料。说明采取了什么动作、结果在哪里以及真实缺口。')
    from settings import choose
    owner=resolve(root,case['sender'])['is_owner']
    from model_catalog import ConflictingChoice, UnavailableChoice, owner_override
    override=None
    model_error=None
    batch_row=c.execute('SELECT goal FROM message_batches WHERE cid=?',(case['id'],)).fetchone()
    if owner and not adoption and not extra and (group or (anchor['chattype']=='single' and anchor['sender']==anchor['chatid'])):
        try:
            override=owner_override(root,anchor)
            if override is None and batch_row and batch_row['goal']:
                override=owner_override(root,{'direct':batch_row['goal']})
        except (UnavailableChoice, ConflictingChoice) as exc:
            model_error=exc
    prefs=choose(root,anchor,owner,override) or {}
    final=not extra and case['mode']!='both' and anchor['chattype']=='single' and owner and bool(re.search(r'(?:结果|报告|结论).{0,8}(?:发回|回报|回复|发到).{0,8}(?:企微|原私聊)|(?:在|回到)企微.{0,6}(?:回复|回报|反馈|发给我)',anchor['direct']))
    from chat_feedback import managed
    chat_delivery=managed(c,case['id'])
    import request_flow
    destination=request_flow.route(c,case['id'])
    if destination:
        final=False
        manifest['return_route']=destination['destination']
    if chat_delivery:
        final=False
        manifest['chat_delivery']='bridge_reports_first_and_final'
        if not linked and re.search(r'(刚才|刚刚|上一份|前面|那份|那张|上一个|刚建|刚生成).{0,10}(文档|表格|表单|在线表)',anchor['direct']):
            found=[]
            rows=c.execute('''SELECT DISTINCT l.result FROM lanes l JOIN cases prev ON l.case_id=prev.id
                WHERE prev.chat=? AND prev.sender=? AND prev.epoch BETWEEN ? AND ? AND prev.id<>?
                AND l.state='completed' AND l.result<>'' ORDER BY prev.epoch DESC LIMIT 5''',
                (case['chat'],case['sender'],case['epoch']-3600,case['epoch'],case['id']))
            for past in rows:
                for url in re.findall(r'https://(?:doc|page)\.weixin\.qq\.com/(?:doc|sheet|smartsheet|smartpage)/[A-Za-z0-9_-]+',past[0]):
                    if url not in found:found.append(url)
            if found:manifest['recent_document_candidates']=found[:8]
            if len(found)==1:prompt+='\n同一私聊近期办理结果中唯一匹配的企微文档链接见 manifest.json 的 recent_document_candidates；先核对文档标题及本次动作。'
            elif len(found)>1:prompt+='\n同一私聊近期存在多份候选文档。通过本条描述唯一核对，仍有歧义时请用户指定。'
        prompt+='\n消息桥负责首次确认及按已核验目的地回报。执行者只返回可复核的业务结果。'
    if (owner and (
            (anchor['chattype']=='single' and anchor['sender']==anchor['chatid'] and (chat_delivery or final)
             and (not destination or destination['destination'] in ('original','cross_group_draft','cross_group_direct')))
            or (anchor['chattype']=='group' and destination and destination['destination'] in ('original','native_original')))):
        prompt+=('\n私聊或本人授权的原群/跨群需交付 ≤19.5 MB 本地文件或视频时，将要交付的原件放入本事项 outputs，以 [文件名](<outputs/文件名>) 在最终结果中标明。'
                 '消息桥会上传、回下载核对并发送原生「文件附件（file）」卡片，严禁在群内直接裸发无法打开的本地链接或采用未验证的视频发送方式。既有企微在线文档沿用真实分享链接。'
                 '执行环境拿不到原件时请说明缺口。如果已经直接发过同一附件，只报告真实回执，避免重复投递。')
        if (anchor['chattype']=='single' and not linked and re.search(r'刚才|刚刚|上一份|上一个|刚发|刚做|刚改|前面|方才',anchor['direct'])
                and re.search(r'文件|附件|链接|原件|[Pp][Dd][Ff]',anchor['direct'])):
            from private_files import recent_sources
            candidates=recent_sources(c,root,case)
            if candidates:
                manifest['recent_private_files']=candidates
                prompt+=('\nmanifest.json 的 recent_private_files 来自同一私聊上一次含文件的结果。'
                         '按本条要求核对版本，唯一匹配时将原件复制到本事项 outputs 交付；多份无法区分时再问。')
    import message_batches
    prompt=message_batches.decorate(c,case,manifest,prompt)
    # Resolve cross-group target: fast-path deterministic match, or hand live group clues to T3 model.
    if case['mode']=='t3' and owner and manifest.get('return_route') in ('cross_group_draft','cross_group_direct'):
        import group_relay
        resolved=group_relay.resolve_target_for_case(c,root,anchor,manifest.get('clarification_history') or [])
        if resolved:
            if resolved.get('clarification'):
                raise PreGateClarification(resolved['clarification'])
            if resolved.get('cancel_group'):
                manifest['return_route']='original'
                c.execute("UPDATE request_routes SET destination='original' WHERE case_id=?",(case['id'],))
            elif resolved.get('pending_model_selection'):
                manifest['group_target_resolution']=resolved['pending_model_selection']
            else:
                manifest['target_group']=resolved
    # Deterministic Layer-1 pre-gate BEFORE creating any T3 thread!
    if case['mode']=='t3' and owner:
        check=ambiguity_gate.deterministic_check(
            c, root, anchor, manifest,
            model_error=model_error,
            return_route=manifest.get('return_route','original'),
        )
        if check is not None:
            raise PreGateClarification(check)
    elif model_error is not None:
        raise model_error
    readonly=(manifest['group_reply']['role']!='owner') if group else (not owner or bool(extra.get('read_only',own[0]['chattype']!='single')))
    return {'case_id':case['id'],'manifest':manifest,'prompt':prompt,
            'provider':prefs.get('provider'),
            'model':extra.get('model',prefs.get('model')),
            'reasoning_effort':extra.get('effort',prefs.get('effort',prefs.get('reasoning_effort'))),
            'explicit_model':override,
            'read_only':readonly,
            'report_mode':extra.get('report_mode','final' if final else 'panel'),'final_report_authorized':bool(final)}


def private_handoff(root, context):
    """Render the owner's request at the handoff without changing its frozen authority."""
    manifest = context['manifest']
    return_text = {
        'original': '结果由大肥鱼原路回传。',
        'owner_private': '结果由大肥鱼发回本人企微私聊。',
        'quiet': '结果留在本任务。',
        'needs_destination': '结果留在本任务，等确认回传去向。',
    }.get(manifest.get('return_route'))
    if not return_text or context.get('read_only') or manifest.get('group_reply'):
        return None
    anchor = next((m for m in manifest.get('members', []) if m['msgid'] == manifest.get('anchor_msgid')), None)
    if (not anchor or anchor['chattype'] != 'single'
            or not (anchor['chatid'] == anchor['sender'] == manifest['chatid'] == manifest['sender'])
            or not resolve(root, anchor['sender'])['is_owner']):
        return None
    if load(Path(root) / 'business-migration.json', {}).get('assignment', {}).get(anchor['msgid']):
        return None
    batch = manifest.get('batch', {})
    lines = [batch.get('confirmed_request') or anchor['direct']]
    if (batch or manifest.get('related') or manifest.get('recent_document_candidates')
            or manifest.get('recent_private_files')
            or any(m.get('attachments') or m.get('quotes') for m in manifest['members'])):
        lines.append('相关材料见 manifest.json，仅作资料。')
    lines.append(return_text)
    return '\n\n'.join(lines)


def as_native_row(root,case,context):
    mid=case['anchor']
    from owner_review import is_review_source, native_source
    if is_review_source(mid):
        row = native_source(root, mid, case['chat'])
    else:
        row=load(Path(root)/'queue'/(hashlib.sha256(mid.encode()).hexdigest()+'.json'),{})
    if not row or row.get('msgid')!=mid or row.get('sender')!=case['sender'] or row.get('chatid')!=case['chat']:return {}
    return {**row,'msgid':case['id']}


_sweep=threading.Lock()
def scan_changes(root, c):
    rows=[]; signatures={}; created={}
    first=not c.execute("SELECT 1 FROM imports WHERE name='known_history'").fetchone()
    for path in (root/'queue').glob('*.json'):
        try:
            info=path.stat()
            side=root/'media-state'/path.name
            try:media=side.stat()
            except FileNotFoundError:media=None
            signature=f"{info.st_mtime_ns}:{info.st_size}:{(media.st_mtime_ns,media.st_size) if media else ''}"
            old=c.execute('SELECT signature FROM intake_files WHERE name=?',(path.name,)).fetchone()
            if not first and old and old[0]==signature:continue
            row=load(path,{})
            if not isinstance(row,dict) or not all(isinstance(row.get(k),str) and row[k] for k in ('msgid','sender','chatid','chattype','received_at','msgtype')):continue
            if path.stem!=hashlib.sha256(row['msgid'].encode()).hexdigest():continue
            created[row['msgid']]=getattr(info,'st_birthtime_ns',info.st_ctime_ns)
            rows.append(with_media(root,row))
            signatures[row['msgid']]=(path.name,signature)
        except OSError:continue
    return sorted(rows,key=lambda r:(r['received_at'],created[r['msgid']],r['msgid'])),signatures


def _package_pending_work(c, root, now):
    for case in c.execute("SELECT * FROM cases WHERE category='work' AND frozen IS NULL ORDER BY epoch").fetchall():
        if not enabled(root):break
        # Never auto-replay pre-cutover historical Dot cases into T3.
        if case['mode'] in ('dot','dot_then_local','both'):continue
        case=correct_outbound_file_wait(c,root,case)
        attach_available(c,case['id'],case,case['expected'],root)
        items=members(c,case['id'])
        if file_count(items)<case['expected']:continue
        settling=any(a.get('state') in ('processing','pending','downloading','queued') for m in items for a in m['attachments'])
        if settling:continue
        from model_catalog import ConflictingChoice, UnavailableChoice
        import clarifications
        import request_flow as rflow
        try:
            context=package(c,root,case)
        except PreGateClarification as pre:
            dest_row=rflow.route(c,case['id'])
            quiet_flag=bool(dest_row and dest_row['destination']=='quiet')
            add_lane(c,case['id'],'local',now)
            clarifications.open_question(
                c, case['id'],
                thread_id=None,
                chat=case['chat'],
                sender=case['sender'],
                origin=pre.check.get('origin','pregate'),
                reason=pre.check.get('reason','指令存在歧义或冲突'),
                questions=pre.check.get('questions') or [],
                summary=pre.check.get('summary') or '',
                now=now,
                quiet=quiet_flag,
            )
            continue
        except (UnavailableChoice, ConflictingChoice) as reason:
            c.execute("UPDATE cases SET category='blocked',note=?,updated=? WHERE id=?",(str(reason),now,case['id']))
            add_lane(c,case['id'],'local',now)
            c.execute("UPDATE lanes SET state='blocked',result=?,detail=? WHERE case_id=? AND target='local'",
                      (str(reason),'model_choice_unavailable',case['id']))
            continue
        c.execute('UPDATE cases SET frozen=?,updated=? WHERE id=?',(dumped(context),now,case['id']))
        add_lane(c,case['id'],'local',now)
    if enabled(root):
        return c.execute("""SELECT c.*, l.state AS lane_state FROM cases c JOIN lanes l ON c.id=l.case_id
                            WHERE c.mode='t3' AND c.category='work' AND c.frozen IS NOT NULL
                            AND l.target='local' AND l.state IN ('queued','resume_queued')""").fetchall()
    return []


def reconcile(root,now=None):
    root=Path(root); now=time.time() if now is None else now
    if not _sweep.acquire(blocking=False):return
    try:
        initialize(root)
        plan=load(root/'business-migration.json',{})
        local=[]
        with db(root,True) as c:
            rows,signatures=scan_changes(root,c)
            migrate(c,root,rows,plan)
            for row in rows:
                if row.get('delivery')=='processing' and not row.get('attachments'):continue
                mid=row['msgid']
                c.execute('SAVEPOINT message_intake')
                try:
                    if not isinstance(row.get('parts',[]),list) or not isinstance(row.get('attachments',[]),list) or not all(
                            isinstance(v,dict) for v in row.get('parts',[])+row.get('attachments',[])):
                        raise ValueError('malformed_message_parts')
                    epoch=datetime.fromisoformat(row['received_at'].replace('Z','+00:00'))
                    if epoch.tzinfo is None:raise ValueError('missing_timestamp_zone')
                    old=c.execute('SELECT item FROM sources WHERE mid=?',(mid,)).fetchone()
                    item=summarize(root,row)
                    if old:
                        if json.loads(old['item'])['attachments']!=item['attachments']:
                            c.execute('UPDATE sources SET item=? WHERE mid=?',(dumped(item),mid))
                    else:
                        numeric_epoch=insert_source(c,item)
                        if row.get('delivery') in ('pending','processing'):ingest_new(c,root,row,item,numeric_epoch)
                    c.execute('DELETE FROM intake_errors WHERE source=?',(mid,))
                except (ValueError,TypeError,OverflowError,KeyError,AttributeError):
                    c.execute('ROLLBACK TO message_intake')
                    c.execute('INSERT OR REPLACE INTO intake_errors VALUES(?,?,?)',(mid,now,'原始记录的时间、正文或附件格式异常'))
                finally:c.execute('RELEASE message_intake')
                c.execute('INSERT OR REPLACE INTO intake_files(name,signature) VALUES(?,?)',signatures[mid])
            import conclusion_notes
            import message_batches
            pending_expiry.sweep(c,root,now)
            conclusion_notes.sweep_expired_followups(c,now)
            message_batches.prepare(c,now,root)
            local=_package_pending_work(c,root,now)
        import agent_relay
        relay_summary=agent_relay.process_pending(root,now=now)
        if relay_summary.get('fallback'):
            with db(root,True) as c:
                local=_package_pending_work(c,root,now)
        import t3_dispatch
        if rows and enabled(root):
            # Controls can reopen a completed T3 job directly in the intake transaction.
            t3_dispatch.wake(root)
        for case in local:
            try:
                ok=t3_dispatch.enqueue_business(root,as_native_row(root,case,json.loads(case['frozen'])))
            except (sqlite3.Error,OSError):
                continue
            with db(root,True) as c:
                c.execute("""UPDATE lanes SET state=?,changed=?,detail=? WHERE case_id=? AND target='local' AND state IN ('queued','resume_queued')""",
                          ('submitted' if ok else 'blocked',now,'已派入 T3 调度队列' if ok else '原始身份或工作包校验未通过',case['id']))
        sync_t3(root)
    finally:_sweep.release()


def local_context(root,case_id):
    with db(root) as c:
        row=c.execute("SELECT frozen FROM cases JOIN lanes ON id=case_id WHERE id=? AND category='work' AND target='local'",(case_id,)).fetchone()
        return json.loads(row[0]) if row and row[0] else None


def authorized_final(root,case_id,chatid,text):
    with db(root) as c:
        row=c.execute("SELECT frozen,result FROM cases JOIN lanes ON id=case_id WHERE id=? AND chat=? AND state='completed' AND target='dot'",(case_id,chatid)).fetchone()
        if not row or not row['frozen']:return False
        context=json.loads(row['frozen'])
        return context.get('report_mode')=='final' and context.get('final_report_authorized') is True and bool(text) and text==row['result']


def case_for_message(root,msgid,chatid):
    with db(root) as c:
        row=c.execute('SELECT case_id FROM sources WHERE mid=? AND chat=?',(msgid,chatid)).fetchone()
        return row[0] if row else None


def dot_events(root):
    from request_flow import member_group
    with db(root) as c:
        rows=c.execute("""SELECT * FROM cases c JOIN lanes l ON l.case_id=c.id
                     WHERE l.target='dot' AND l.state='queued' ORDER BY c.epoch""").fetchall()
        result=[]
        for row in rows:
            manifest=json.loads(row['frozen'])['manifest'];items=manifest['members']+[m for l in manifest['related'] for m in l['members']]
            anchor = next(m for m in manifest['members'] if m['msgid'] == row['anchor'])
            if member_group(root, anchor):
                continue
            result.append({'case_id':row['id'],'msgid':row['anchor'],'chatid':row['chat'],'msgtype':next(m['msgtype'] for m in items if m['msgid']==row['anchor']),
                'received_at':row['at'],'title':row['title'],'member_count':len(items),'attachment_count':file_count(items),
                'sender_is_owner':resolve(root,row['sender'])['is_owner']})
            if group_replies.needs_review(manifest) and not result[-1]['sender_is_owner']:
                result[-1]['required_handling']=group_replies.MEMBER_REVIEW
        return result


def dot_read(root,case_id,chatid,claim=False):
    with db(root,claim) as c:
        case=c.execute('SELECT * FROM cases WHERE id=? AND chat=?',(case_id,chatid)).fetchone()
        if not case:raise ValueError('unknown_case_in_chat')
        if case['category'] in ('collecting','cancelled'):raise ValueError('material_batch_not_assigned')
        if case['category']=='owner_review':raise ValueError('owner_review_required')
        if case['category']=='expired' or (claim and pending_expiry.expired(c,root,case_id)):
            return pending_expiry.terminal(case_id)
        if case['category']=='blocked' and not case['frozen']:
            return {'case_id':case_id,'claimed':False,'state':'blocked','read_only':True,
                    'manifest':{},'instruction':case['note']}
        lane=c.execute("SELECT * FROM lanes WHERE case_id=? AND target='dot'",(case_id,)).fetchone()
        if claim and lane and lane['state']=='queued':
            from request_flow import member_group
            if member_group(root, group_replies.original(c,case_id,chatid)):
                raise ValueError('owner_review_required')
        allowed=bool(lane and lane['state'] in ('queued','running'))
        if claim and allowed:
            c.execute("INSERT OR IGNORE INTO observations VALUES(?,'signed_subscription_read',?,'已用本事项原事件凭据读取工作包')",(case_id,time.time()))
            c.execute("UPDATE lanes SET state='running',changed=?,detail='小爱已凭本事项签名事件读取工作包' WHERE case_id=? AND target='dot' AND state='queued'",(time.time(),case_id))
        context=json.loads(case['frozen']) if case['frozen'] else package(c,root,case)
        response={'case_id':case_id,'claimed':bool(claim and allowed),'state':'running' if claim and allowed else lane['state'] if lane else 'not_assigned',
                'instruction':private_handoff(root,context) or context['prompt'],'manifest':context['manifest'],
                'record_result_with':'finish_business_work','report_mode':context['report_mode']}
        if claim and allowed:
            import task_controls
            changes=task_controls.mailbox(c,root,case_id)
            response['owner_changes']=changes
            if changes:
                response['instruction']+='\n请在安全停点检查 owner_changes；继续沿用原授权和工具确认。安全停止后再确认取消。'
        if group_replies.needs_review(context['manifest']) and lane:
            received=c.execute("SELECT 1 FROM observations WHERE case_id=? AND source='signed_subscription_read'",(case_id,)).fetchone()
            review={'required_by':'dot_main_thread','signed_pickup':bool(received),'decision':None}
            if received and lane['detail']=='signed_subscription' and lane['state'] in ('completed','blocked'):
                review['decision']=group_replies.decision_of(json.loads(lane['result']))
            response['member_review']=review
        receipt=group_replies.delivery_status(c,root,case_id,chatid,case['anchor'])
        response['read_only']=context['read_only']
        if context.get('model'):
            response['model_preference']={'model':context['model'],'reasoning_effort':context.get('reasoning_effort')}
        if context['manifest'].get('peer_permission'):
            grant=peer_permissions.binding(c,root,case['anchor'])
            response['project_access']={'direct_write':False,'may_submit':False,'independent_pending_cl':True,
                'may_request_checked_changes':bool(grant and grant['state']=='bound' and grant['expires']>time.time())}
        if receipt:
            response['group_delivery']=receipt
            if receipt['origin']=='owner_chat_direct':
                response['instruction']+='\n本事项已有本人私聊授权的原群发送回执。已被服务端接受的部分不要重发。'
        return response


def delegate(root,case_id,chatid):
    with db(root,True) as c:
        case=c.execute('SELECT * FROM cases WHERE id=? AND chat=?',(case_id,chatid)).fetchone()
        if not case or not case['frozen']:raise ValueError('unknown_case')
        manifest=json.loads(case['frozen'])['manifest']
        if manifest['members'][0]['chattype']!='single' or case['sender']!=chatid or not resolve(root,case['sender'])['is_owner']:
            raise ValueError('private_owner_required')
        lane=c.execute("SELECT state FROM lanes WHERE case_id=? AND target='dot'",(case_id,)).fetchone()
        if not lane or lane[0] not in ('queued','running','handed_off'):raise ValueError('no_current_dot_assignment')
        if pending_expiry.expired(c,root,case_id):
            c.commit()
            raise ValueError('unstarted_work_expired')
        if case['mode']!='both':
            c.execute("UPDATE lanes SET state='handed_off',changed=?,detail='小爱已将此业务转交本机' WHERE case_id=? AND target='dot'",(time.time(),case_id))
        add_lane(c,case_id,'local',time.time())
    return {'case_id':case_id,'target':'local','state':'assigned'}


def finish_dot(root,case_id,chatid,result,evidence,disposition,*,origin='signed_subscription'):
    if disposition not in ('completed','blocked') or not isinstance(result,str) or not 30<=len(result.strip())<=120000:
        raise ValueError('substantive_business_result_required')
    if not isinstance(evidence,list) or not 1<=len(evidence)<=30 or any(not isinstance(v,str) or not 1<=len(v)<=1200 for v in evidence):
        raise ValueError('concrete_evidence_list_required')
    with db(root,True) as c:
        case=c.execute("""SELECT c.frozen,l.state,EXISTS(
                SELECT 1 FROM observations o WHERE o.case_id=c.id AND o.source='signed_subscription_read')
            FROM cases c JOIN lanes l ON l.case_id=c.id AND l.target='dot'
            WHERE c.id=? AND c.chat=?""",(case_id,chatid)).fetchone()
        if not case:raise ValueError('no_dot_lane')
        if pending_expiry.expired(c,root,case_id):return pending_expiry.terminal(case_id)
        manifest=json.loads(case[0])['manifest'] if case[0] else {}
    member=group_replies.needs_review(manifest)
    if member:
        if origin!='signed_subscription' or not case[2] or case[1] not in ('running','completed','blocked'):
            raise ValueError('member_requires_signed_dot_pickup')
        result,disposition=group_replies.member_result(root,case_id,chatid,manifest,result,disposition)
    elif origin=='signed_subscription':
        result,disposition=peer_permissions.pending_result(root,case_id,chatid,result,disposition)
    with db(root,True) as c:
        lane=c.execute("SELECT l.* FROM lanes l JOIN cases c ON c.id=l.case_id WHERE c.id=? AND c.chat=? AND target='dot'",(case_id,chatid)).fetchone()
        if not lane:raise ValueError('no_dot_lane')
        if pending_expiry.expired(c,root,case_id):return pending_expiry.terminal(case_id)
        if (member and manifest.get('peer_permission') and json.loads(result)['member_decision']!='change_plan'
                and c.execute('SELECT 1 FROM peer_changes WHERE case_id=?',(case_id,)).fetchone()):
            raise ValueError('immutable_pending_request')
        if lane['state'] in ('completed','blocked') and lane['result']==result and json.loads(lane['evidence'])==evidence:return {'state':lane['state'],'case_id':case_id}
        if lane['state'] not in (('queued','running') if origin=='observed_original_dot_chat' else ('running',)):raise ValueError('dot_claim_required_or_final_already_recorded')
        if origin=='signed_subscription':
            group_replies.queue_answer(c,root,case_id,chatid,result)
        c.execute("UPDATE lanes SET state=?,changed=?,result=?,evidence=?,detail=? WHERE case_id=? AND target='dot'",
                  (disposition,time.time(),result,dumped(evidence),origin,case_id))
    write_result(root,case_id)
    return {'state':disposition,'case_id':case_id}


def observe(root,case_id,source,detail):
    """Local operator input from a verified real task or original Dot conversation."""
    if source not in ('original_dot_chat','verified_material_outputs'):raise ValueError('unknown_evidence_source')
    if not isinstance(detail,str) or not detail.strip() or len(detail)>6000:raise ValueError('invalid_observation')
    with db(root,True) as c:
        c.execute('INSERT INTO observations VALUES(?,?,?,?) ON CONFLICT(case_id,source) DO UPDATE SET at=excluded.at,detail=excluded.detail WHERE detail<>excluded.detail',(case_id,source,time.time(),detail))


_native_progress={}


def native_progress(root,job):
    if not job['workspace'] or job['state']!='running':return ''
    config=load(Path(root)/'command-routing.json',{})
    if not config.get('tasks_root'):return ''
    workspace=Path(job['workspace']).resolve()
    if not workspace.is_relative_to(Path(config['tasks_root']).resolve()):return ''
    path=workspace/'events.jsonl';identity=(root,job['msgid'])
    old=_native_progress.get(identity,(None,''))
    try:
        stat=path.stat();marker=(stat.st_mtime_ns,stat.st_size)
        if old[0]==marker:return old[1]
        with path.open('rb') as stream:
            stream.seek(max(0,stat.st_size-196608))
            tail=stream.read(196608)
        value=old[1]
        for line in reversed(tail.splitlines()):
            try:event=json.loads(line)
            except (UnicodeDecodeError,ValueError):continue
            item=event.get('item',{})
            if event.get('type')=='item.completed' and item.get('type')=='agent_message' and isinstance(item.get('text'),str):
                value=' '.join(item['text'].split())[:260];break
        if len(_native_progress)>=64 and identity not in _native_progress:
            _native_progress.pop(next(iter(_native_progress)))
        _native_progress[identity]=(marker,value)
        return value
    except OSError:return old[1]


def sync_t3(root):
    """Synchronize T3 job statuses from local-tasks.sqlite3 back to business.sqlite3 lanes."""
    path=Path(root)/'local-tasks.sqlite3'
    if not path.is_file():return
    with db(root) as c:
        ids=[r[0] for r in c.execute("SELECT l.case_id FROM lanes l JOIN cases c ON c.id=l.case_id WHERE c.mode='t3' AND l.target='local' AND l.state IN ('submitted','running','uncertain','awaiting_clarification','resume_queued','heavy_queued','heavy_resume_queued')")]
    if not ids:return
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as jobs:
        jobs.row_factory=sqlite3.Row
        if not jobs.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='t3_jobs'").fetchone():return
        for cid in ids:
            job=jobs.execute('SELECT * FROM t3_jobs WHERE msgid=?',(cid,)).fetchone()
            if not job:continue
            st_map={
                'completed':'completed','failed':'blocked','blocked':'blocked','uncertain':'uncertain',
                'cancelled':'cancelled','awaiting_clarification':'awaiting_clarification',
                'running':'running','launching':'running','starting':'running',
                'resume_queued':'resume_queued','heavy_queued':'heavy_queued','heavy_resume_queued':'heavy_resume_queued',
            }
            state=st_map.get(job['state'],'submitted')
            tid=job['t3_thread_id'] or ''
            model_desc=' / '.join(str(x) for x in (job['route_kind'],job['requested_model'],job['requested_effort']) if x)
            details=' / '.join(str(x) for x in (job['state'],model_desc,tid,job['detail']) if x)
            evidence=[v for v in (tid,job['t3_link'],job['t3_triage_thread_id']) if v]
            answer=''
            if state=='completed' and job['workspace']:
                work=Path(job['workspace']); receipt=load(work/'completion.json',{})
                try:answer=(work/'result.txt').read_text(encoding='utf-8').strip()
                except OSError:pass
                if not answer or receipt.get('case_id')!=cid or hashlib.sha256(answer.encode('utf-8')).hexdigest()!=receipt.get('result_hash'):
                    state='uncertain';answer='';details+=' / 结果与持久凭证不匹配'
                else:
                    evidence.append(str(work/'completion.json'))
            elif state=='blocked' and job['workspace'] and (Path(job['workspace'])/'result.txt').is_file():
                try:answer=(Path(job['workspace'])/'result.txt').read_text(encoding='utf-8').strip()
                except OSError:pass
            with db(root,True) as c:
                case=c.execute('SELECT category FROM cases WHERE id=?',(cid,)).fetchone()
                if not case or case['category'] in ('expired','cancelled'):continue
                if state=='awaiting_clarification' and case['category']!='awaiting_clarification':continue
                if not pending_expiry.attach_native(c,root):continue
                if not c.execute('SELECT 1 FROM expiry_native.t3_jobs WHERE msgid=? AND state=? AND updated=?',
                                 (cid,job['state'],job['updated'])).fetchone():continue
                if state=='completed' and answer and group_replies.enrolled(c,cid):
                    chat=c.execute('SELECT chat FROM cases WHERE id=?',(cid,)).fetchone()[0]
                    payload=answer
                    try:
                        parsed=json.loads(answer)
                        if not isinstance(parsed,dict) or 'group_reply' not in parsed:
                            payload=json.dumps({'group_reply':answer},ensure_ascii=False)
                    except ValueError:
                        payload=json.dumps({'group_reply':answer},ensure_ascii=False)
                    try:
                        group_replies.queue_answer(c,root,cid,chat,payload,native=True)
                    except ValueError as error:
                        c.execute("UPDATE chat_receipts SET state='needs_public_review',detail=? WHERE case_id=? AND phase=? AND state='waiting'",(str(error),cid,group_replies.PHASE))
                c.execute("UPDATE lanes SET state=?,detail=?,result=?,evidence=?,changed=? WHERE case_id=? AND target='local' AND (state<>? OR detail<>? OR result<>?)",
                          (state,details,answer,dumped(evidence),time.time(),cid,state,details,answer))
            if answer:write_result(root,cid)


def sync_native(root):
    path=Path(root)/'local-tasks.sqlite3'
    if not path.is_file():return
    with db(root) as c:
        ids=[r[0] for r in c.execute("SELECT case_id FROM lanes WHERE target='local' AND state IN ('submitted','running','uncertain')")]
    if not ids:return
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as jobs:
        jobs.row_factory=sqlite3.Row
        if 'kind' not in {r[1] for r in jobs.execute('PRAGMA table_info(jobs)')}:return
        for cid in ids:
            job=jobs.execute("SELECT * FROM jobs WHERE msgid=? AND kind='business'",(cid,)).fetchone()
            if not job:continue
            state={'completed':'completed','failed':'blocked','uncertain':'uncertain','running':'running','launching':'running','starting':'running'}.get(job['state'],'submitted')
            s=job['native_id'] or job['thread_id'] or ''
            details=' / '.join(str(x) for x in (job['state'],s,job['last_tool'] or job['last_event'],job['detail'],job['model_detail']) if x)
            narrative=native_progress(root,job)
            if narrative:details+=' / '+narrative
            evidence=[s] if s else []
            answer=''
            if state=='completed':
                work=Path(job['workspace']); receipt=load(work/'completion.json',{})
                try:answer=(work/'result.txt').read_text(encoding='utf-8').strip()
                except OSError:pass
                if not answer or receipt.get('case_id')!=cid or receipt.get('native_id')!=s or hashlib.sha256(answer.encode()).hexdigest()!=receipt.get('result_hash'):
                    state='uncertain';answer='';details+=' / 结果与持久凭证不匹配'
                else:evidence+=['native turn.completed = '+str(receipt.get('turns')),str(work/'completion.json')]
            with db(root,True) as c:
                if c.execute("SELECT 1 FROM cases WHERE id=? AND category IN ('expired','cancelled')",(cid,)).fetchone():continue
                if state=='completed' and answer and job['context_json'] and 'return_route' in json.loads(job['context_json'])['manifest']:
                    import task_controls
                    from local_dispatch import _completion_proof
                    if (work/'task-control-ack.json').is_file() and _completion_proof(job) and task_controls.native_checkpoint(c,root,cid,work):
                        state='cancelled'
                        details+=' / 本机在安全停点结束，已有完成凭证及本人取消指令。'
                if state=='completed' and answer and group_replies.enrolled(c,cid):
                    chat=c.execute('SELECT chat FROM cases WHERE id=?',(cid,)).fetchone()[0]
                    try:group_replies.queue_answer(c,root,cid,chat,answer,native=True)
                    except ValueError as error:
                        from group_reply_only import tracked
                        if not tracked(c,cid):
                            c.execute("UPDATE chat_receipts SET state='needs_public_review',detail=? WHERE case_id=? AND phase=? AND state='waiting'",(str(error),cid,group_replies.PHASE))
                c.execute("UPDATE lanes SET state=?,detail=?,result=?,evidence=?,changed=? WHERE case_id=? AND target='local' AND (state<>? OR detail<>?)",
                          (state,details,answer,dumped(evidence),time.time(),cid,state,details))
            if state in ('completed','blocked','uncertain'):
                _native_progress.pop((root,cid),None)
            if answer:write_result(root,cid)


def write_result(root,cid):
    with db(root,True) as c:
        case=c.execute('SELECT * FROM cases WHERE id=?',(cid,)).fetchone()
        lines=['# '+case['title'],'','事项 '+cid,'']
        for lane in c.execute('SELECT * FROM lanes WHERE case_id=? ORDER BY target',(cid,)):
            heading='T3 执行器' if case['mode']=='t3' else ('历史小爱' if lane['target']=='dot' else '历史本机')
            lines+=['## '+heading,'',
                    lane['state']+' · '+lane['detail'],'',lane['result'] or '尚无执行结果。','']
            lines.extend('- '+v for v in json.loads(lane['evidence']))
            lines.append('')
        directory=Path(root)/'business-results';directory.mkdir(exist_ok=True)
        tmp=directory/(cid+'.tmp');tmp.write_text('\n'.join(lines),encoding='utf-8');os.replace(tmp,directory/(cid+'.md'))


def panel_cases(root):
    root=Path(root)
    if not (root/'business.sqlite3').exists():return []
    now=time.time();result=[]; t3_map={}
    tasks_path=root/'local-tasks.sqlite3'
    if tasks_path.is_file():
        try:
            with closing(sqlite3.connect(tasks_path.resolve().as_uri()+'?mode=ro',uri=True)) as tdb:
                tdb.row_factory=sqlite3.Row
                if tdb.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='t3_jobs'").fetchone():
                    for r in tdb.execute('SELECT * FROM t3_jobs'):
                        t3_map[r['msgid']]=dict(r)
        except sqlite3.Error:pass
    with db(root) as c:
        has_flow=bool(c.execute("SELECT 1 FROM sqlite_master WHERE name='feedback_alerts'").fetchone())
        has_clar=bool(c.execute("SELECT 1 FROM sqlite_master WHERE name='clarifications'").fetchone())
        import request_flow
        for case in c.execute("SELECT * FROM cases WHERE category<>'folded' ORDER BY epoch DESC"):
            own=members(c,case['id']);all_items=list(own)
            for cid in json.loads(case['related']):
                all_items.extend(members(c,cid))
            items=list({m['msgid']:m for m in all_items}.values())
            lanes={r['target']:r for r in c.execute('SELECT * FROM lanes WHERE case_id=?',(case['id'],))}
            states=[l['state'] for l in lanes.values() if l['state']!='handed_off']
            observations={r['source']:r['detail'] for r in c.execute('SELECT * FROM observations WHERE case_id=?',(case['id'],))}
            count=file_count(items);note='\n'.join(s for s in [case['note'],*observations.values()] if s)
            receipts=[]
            if has_flow:
                alerts=[r[0] for r in c.execute('SELECT reason FROM feedback_alerts WHERE case_id=? AND resolved IS NULL',(case['id'],))]
                if alerts:note+='\n回报待处理 · '+', '.join(alerts)
                receipts=request_flow.feedback_state(c,case['id'])
                if receipts:note+='\n回报记录 · '+' / '.join(r['phase']+' '+r['state'] for r in receipts)
            clar_rows=[]
            if has_clar:
                clar_rows=[dict(r) for r in c.execute('SELECT qid,round_no,reason,answer_text,state FROM clarifications WHERE case_id=? ORDER BY round_no',(case['id'],))]
            if pending_expiry.AUDIT in observations:state='expired'
            elif case['category']=='historical':state='historical'
            elif case['category']=='awaiting_clarification':state='awaiting_clarification'
            elif case['category']=='colleague_followup':state='colleague_followup'
            elif case['category']=='collecting':
                sealed=c.execute("SELECT 1 FROM message_batches WHERE cid=? AND state='sealed_waiting'",(case['id'],)).fetchone()
                state='blocked' if sealed else 'collecting'
            elif case['category']=='cancelled':state='cancelled'
            elif case['category']=='agent_relay':state='running'
            elif case['category']=='control':state='completed'
            elif case['category'] in (request_flow.OWNER_REVIEW,'owner_adopted'):state='historical'
            elif case['category']!='work':state='blocked'
            elif states and all(v in ('cancelled','completed') for v in states):state='cancelled' if 'cancelled' in states else 'completed'
            elif any(v=='awaiting_clarification' for v in states):state='awaiting_clarification'
            elif any(v in ('heavy_queued','heavy_resume_queued','resume_queued') for v in states):
                state=next(v for v in states if v in ('heavy_queued','heavy_resume_queued','resume_queued'))
            elif 'running' in states:state='running'
            elif any(v in ('uncertain','blocked') for v in states):state='uncertain' if 'uncertain' in states else 'blocked'
            elif not states and case['expected']>count and now-case['epoch']>30:state='blocked';note+=' 等待已声明的文件（%s/%s）。'%(count,case['expected'])
            else:state='queued'
            tj=t3_map.get(case['id'],{})
            t3_thread_id=tj.get('t3_thread_id') or ''
            t3_link=tj.get('t3_link') or ''
            route_kind=tj.get('route_kind') or case['mode']
            model_summary=''
            if tj:
                m_str=tj.get('requested_model') or ''
                e_str=tj.get('requested_effort') or ''
                rk={'triage_hydrogen':'Hydrogen 入口','escalated_neon':'重槽升级','escalated_light':'轻槽升级','explicit_direct':'显式直达','explicit_passthrough':'显式直通'}.get(route_kind,route_kind)
                model_summary=f'{rk} · {m_str}'+(f' ({e_str})' if e_str else '')
            else:
                model_summary={'t3':'T3 待派发','conclusion_note':'群静态说明(0模型)','colleague_followup':'同事待补充(0模型)','agent_relay':'Agent传话(0模型)','control':'指令控制'}.get(case['mode'],'历史记录')
            def label(target):
                l=lanes.get(target)
                if not l:return '未指派'
                names={'queued':'等待派发','submitted':'已入队','running':'执行中','resume_queued':'等待轻槽续办',
                       'heavy_queued':'等待重槽','heavy_resume_queued':'等待重槽续办',
                       'awaiting_clarification':'等待主人确认','completed':'已完成','blocked':'受阻','uncertain':'待核实',
                       'handed_off':'已转交','expired':'已过期','cancelled':'已取消'}
                return names.get(l['state'],l['state'])+' / '+utc(l['changed'])+(' / '+l['detail'] if l['detail'] else '')
            files=[]
            for m in items:
                for a in m['attachments']:
                    typ=a.get('type','media')
                    files.append(f"{m['msgid']} · {'引用' if a.get('quoted') else '直接'} {typ} · {a.get('display_name','')} {a.get('state','')} {a.get('reason','')}".strip())
            path=root/'business-results'/(case['id']+'.md')
            result.append({'id':case['id'],'at':case['at'],'updated':utc(max([case['updated']]+[l['changed'] for l in lanes.values()])),
                 'title':case['title'],'state':state,'mode':case['mode'],'route_kind':route_kind,'model_summary':model_summary,
                 't3_thread_id':t3_thread_id,'t3_link':t3_link,'workspace':tj.get('workspace') or '',
                 'execution_slot':tj.get('execution_slot') or '',
                 'clarifications':clar_rows,'member_count':len(items),'attachment_count':count,
                 'files':files,'source_ids':[m['msgid'] for m in items],'own_source_ids':[m['msgid'] for m in own],
                 'lane_states':{target:lane['state'] for target,lane in lanes.items()},'feedback':receipts,
                 'expired_source_ids':[m['msgid'] for m in own] if state=='expired' else [],
                 'local':label('local'),
                 'evidence':note+'\n'+'\n'.join(v for l in lanes.values() for v in json.loads(l['evidence'])),'result_path':str(path) if path.is_file() else ''})
        for row in c.execute("SELECT * FROM sources WHERE case_id IS NULL AND json_extract(item,'$.delivery_at_import') IN ('pending','processing')"):
            m=json.loads(row['item'])
            if m['attachments'] and not m['direct']:
                result.append({'id':'material_'+key(m['msgid']),'at':m['received_at'],'updated':m['received_at'],'title':'待关联的单独材料',
                  'state':'material','mode':'材料','route_kind':'material','model_summary':'单独材料','t3_thread_id':'','t3_link':'','workspace':'',
                  'clarifications':[],'member_count':1,'attachment_count':file_count([m]),'files':[a.get('display_name') or a['type'] for a in m['attachments']],
                  'source_ids':[m['msgid']],'local':'未指派','evidence':'同一对话中尚无可唯一关联的任务。','result_path':''})
        for err in c.execute('SELECT * FROM intake_errors'):
            result.append({'id':'error_'+key(err['source']),'at':utc(err['at']),'updated':utc(err['at']),'title':'收件格式需检查',
                'state':'blocked','mode':'收件','route_kind':'error','model_summary':'格式异常','t3_thread_id':'','t3_link':'','workspace':'',
                'clarifications':[],'member_count':1,'attachment_count':0,'files':[],'source_ids':[err['source']],
                'local':'未指派','evidence':err['reason'],'result_path':''})
    return sorted(result,key=lambda r:r['at'],reverse=True)
