"""Owner task amendments. Authority always comes from an original SDK message."""
import hashlib
import json
import re
import sqlite3
import time
from pathlib import Path

import group_replies
import pending_expiry
import request_flow
from command_routing import enabled
from identity import resolve

PREFIX = re.compile(r'^(?:[@＠]大肥鱼[\s\u2005\u200b]*)?', re.I)
COMMAND = re.compile(r'^【(追加(?:任务)?|取消(?:任务)?|确认追加|确认取消)】\s*(\S+)(?:\s+([\s\S]+))?$')
RESERVED = re.compile(r'^【(?:追加(?:任务)?|取消任务|确认追加|确认取消)】|^【取消】\s*\S')
RECEIPT_QUOTE = re.compile(r'^(?:已收到(?:确认)?|已收齐)〔([0-9a-f]{7})〕', re.I)
CLARIFY_CARD_QUOTE = re.compile(r'^(?:需要确认|追问)〔([0-9a-f]{7})〕', re.I)
OTHER_COMMAND = re.compile(
    r'^【(?:开始收集|收集|合并|合并处理|合批|开始合并|批量开始|合并开始|开始|收尾|结束|提交|完成|ok|要求|取消收集|'
    r'答复|回复|确认回复|直回|直发群|改发|修改并发送|发布说明|补充说明|更新说明|删除说明|说明|进度)(?:[\s：:】]|$)',
    re.I,
)
QUOTED_CANCEL = re.compile(r'^(?:【取消(?:任务)?】|取消(?:任务|这个任务|该任务|本任务|该事项|本事项)?)[。！!]*$')
QUOTED_APPEND_PREFIX = re.compile(r'^【追加(?:任务)?】[\s：:]*')
TARGET = re.compile(r'(?:w_[a-f0-9]{32}|[a-f0-9]{7})')
PROPOSAL = re.compile(r'p_[a-f0-9]{24}')
MESSAGES = {
    'cancelled':'该事项已在启动前取消。',
    'added_before_start':'已登记追加要求，将随原事项交给执行者。',
    'awaiting_checkpoint':'已登记，等待原执行者在安全停点核对。',
    'reopened_followup':'原事项尚在 10 分钟续办窗口内，已登记追加要求并交回原 T3 线程继续办理。',
    'inactive':'原事项已结束或已过期。',
    'expired':'这条控制请求或待确认提议已过期。',
    'unknown_target':'未找到同一发件人和会话下的唯一事项。',
    'invalid':'请提供事项编号。追加需要正文，取消只需编号。',
    'target_mismatch':'引用收据中的事项编号与正文指定编号不一致，未执行。',
    'unsupported_attachment':'暂不支持通过引用收据直接追加附件，请仅用文字/语音追加，或另发新消息附带文件。',
    'confirmation_mismatch':'待确认提议、动作或原会话不匹配。',
    'unmanaged':'该事项没有启用这版运行中控制，可在原执行会话处理。',
}


def install(c):
    c.execute("""CREATE TABLE IF NOT EXISTS task_proposals(id TEXT PRIMARY KEY, target TEXT NOT NULL,
        chat TEXT NOT NULL,sender TEXT NOT NULL,action TEXT NOT NULL,body TEXT NOT NULL,expires REAL NOT NULL,
        confirmed_mid TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS owner_task_controls(mid TEXT PRIMARY KEY, cid TEXT UNIQUE,
        target TEXT, action TEXT NOT NULL, body TEXT NOT NULL, result TEXT NOT NULL,
        created REAL NOT NULL,dot_seen REAL, native_seen REAL)""")
    c.execute('CREATE INDEX IF NOT EXISTS task_control_target ON owner_task_controls(target,created)')


def owner_origin(root,item):
    return (enabled(root) and resolve(root,item.get('sender'))['is_owner'] and
            ((item.get('chattype')=='single' and item['sender']==item.get('chatid')) or group_replies.owner_mention(root,item)))


def target(c,item,label):
    label=label.strip('〔〕')
    if not TARGET.fullmatch(label):return None
    if (item.get('chattype')=='single' and item.get('sender')==item.get('chatid')
            and (item.get('bot_channel') or 'feiyu')=='feiyu'
            and c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='request_routes'").fetchone()):
        matches=c.execute("""SELECT * FROM cases WHERE sender=? AND (id=? OR substr(id,-7)=?)
            AND (chat=? OR EXISTS (SELECT 1 FROM request_routes r WHERE r.case_id=cases.id AND r.origin_kind='group'))
            AND category IN ('work','collecting','awaiting_clarification','expired','cancelled','blocked') LIMIT 2""",
            (item['sender'],label,label,item['chatid'])).fetchall()
    else:
        matches=c.execute("""SELECT * FROM cases WHERE chat=? AND sender=? AND
            (id=? OR substr(id,-7)=?) AND category IN ('work','collecting','awaiting_clarification','expired','cancelled','blocked') LIMIT 2""",
            (item['chatid'],item['sender'],label,label)).fetchall()
    return matches[0] if len(matches)==1 else None


def request(c,root,case,action):
    cid=case['id']
    if pending_expiry.expired(c,root,cid) or case['category'] not in ('work','collecting','awaiting_clarification'):
        return 'inactive'
    lanes=list(c.execute('SELECT state FROM lanes WHERE case_id=?',(cid,)))
    if lanes and all(r[0] in ('completed','blocked','cancelled','expired','handed_off') for r in lanes):
        # Allow owner 【追加】 within the 10-minute unarchived T3 lifecycle window to resume the exact same T3 thread.
        if action=='append' and any(r[0]=='completed' for r in lanes) and case['mode']=='t3' and pending_expiry.attach_native(c,root):
            if c.execute("SELECT 1 FROM expiry_native.sqlite_master WHERE type='table' AND name='t3_jobs'").fetchone():
                cols={r[1] for r in c.execute('PRAGMA expiry_native.table_info(t3_jobs)')}
                if {'t3_archived_at','t3_archive_after'} <= cols:
                    tj=c.execute(
                        'SELECT state,execution_slot,route_kind,t3_thread_id,t3_archived_at,t3_archive_after FROM expiry_native.t3_jobs WHERE msgid=?',
                        (cid,),
                    ).fetchone()
                    now=time.time()
                    if (tj and tj['state']=='completed' and tj['t3_thread_id'] and not tj['t3_archived_at']
                            and tj['t3_archive_after'] is not None and float(tj['t3_archive_after']) > now):
                        from t3_dispatch import resume_queue_state
                        state=resume_queue_state(tj)
                        slot='regular' if state=='heavy_resume_queued' else 'light'
                        c.execute(
                            "UPDATE lanes SET state=?,changed=?,detail='10 分钟续办窗口内收到本人追加，交回原 T3 线程续办' WHERE case_id=? AND target='local'",
                            (state,now,cid),
                        )
                        c.execute(
                            "UPDATE expiry_native.t3_jobs SET state=?,execution_slot=?,lease=NULL,pid=NULL,worker_identity=NULL,"
                            "t3_archive_after=NULL,updated=?,detail='post_completion_append_queued' WHERE msgid=? AND state='completed'",
                            (state,slot,datetime_iso(now),cid),
                        )
                        return 'reopened_followup'
        return 'inactive'
    if case['category']=='awaiting_clarification':
        now=time.time()
        if action=='append':
            return 'awaiting_checkpoint'
        c.execute("UPDATE clarifications SET state='cancelled' WHERE case_id=? AND state='open'",(cid,))
        c.execute("UPDATE cases SET category='cancelled',updated=?,note='本人已在等待确认阶段取消。' WHERE id=?",(now,cid))
        c.execute("UPDATE lanes SET state='cancelled',changed=? WHERE case_id=?",(now,cid))
        if pending_expiry.attach_native(c,root) and c.execute("SELECT 1 FROM expiry_native.sqlite_master WHERE type='table' AND name='t3_jobs'").fetchone():
            c.execute(f"UPDATE expiry_native.t3_jobs SET state='cancelled',updated=? WHERE msgid=? "
                      f"AND (state='awaiting_clarification' OR state IN {pending_expiry.T3_QUEUE_SQL})",(datetime_iso(now),cid))
        c.execute("UPDATE chat_receipts SET state='disabled',detail='owner_cancelled',finished=? WHERE case_id=? AND state IN ('ready','waiting')",(now,cid))
        return 'cancelled'
    queued=bool(c.execute('SELECT 1 FROM cases c WHERE c.id=? AND '+pending_expiry.PENDING_CASE,(cid,)).fetchone())
    jobs=[]; t3_jobs=[]
    if pending_expiry.attach_native(c,root):
        if c.execute("SELECT 1 FROM expiry_native.sqlite_master WHERE type='table' AND name='jobs'").fetchone():
            jobs=c.execute('SELECT * FROM expiry_native.jobs WHERE msgid IN (?,?)',(cid,case['anchor'])).fetchall()
            queued=queued and all(j['msgid']==cid and j['kind']=='business' and j['state']=='queued'
                and not any(j[k] for k in pending_expiry.START_FIELDS if k in j.keys()) for j in jobs)
        if c.execute("SELECT 1 FROM expiry_native.sqlite_master WHERE type='table' AND name='t3_jobs'").fetchone():
            t3_jobs=c.execute('SELECT * FROM expiry_native.t3_jobs WHERE msgid=?',(cid,)).fetchall()
            queued=queued and all(j['state']=='queued' and not j['lease'] and not j['t3_thread_id'] for j in t3_jobs)
    if not jobs and t3_jobs and all(j['state'] in pending_expiry.T3_QUEUE_STATES and not j['lease'] for j in t3_jobs):
        if action=='append':
            return 'added_before_start' if request_flow.installed(root,case['epoch']) else 'unmanaged'
        from t3_dispatch import _archive_deadline
        now=time.time()
        c.execute("UPDATE expiry_native.t3_jobs SET state='cancelled',updated=?,t3_archive_after=?,detail='owner_cancelled' WHERE msgid=?",
                  (datetime_iso(now),_archive_deadline(root,now),cid))
        queued=True
    if not queued:
        if not request_flow.installed(root,case['epoch']):return 'unmanaged'
        if action=='cancel':
            c.execute("UPDATE lanes SET state='cancelled',changed=? WHERE case_id=? AND target='dot' AND state='queued'",(time.time(),cid))
            local_unstarted=all(j['msgid']==cid and j['kind']=='business' and j['state']=='queued'
                and not any(j[k] for k in pending_expiry.START_FIELDS if k in j.keys()) for j in jobs)
            t3_unstarted=all(j['state']=='queued' and not j['lease'] and not j['t3_thread_id'] for j in t3_jobs)
            if local_unstarted and t3_unstarted:
                c.execute("UPDATE lanes SET state='cancelled',changed=? WHERE case_id=? AND target='local' AND state IN ('submitted','queued')",(time.time(),cid))
                if jobs:c.execute("UPDATE expiry_native.jobs SET state='cancelled',status='cancelled' WHERE msgid=? AND state='queued' AND kind='business'",(cid,))
                if t3_jobs:c.execute("UPDATE expiry_native.t3_jobs SET state='cancelled',updated=? WHERE msgid=? AND state='queued'",(datetime_iso(time.time()),cid))
        return 'awaiting_checkpoint'
    if action=='append':return 'added_before_start' if request_flow.installed(root,case['epoch']) else 'unmanaged'
    now=time.time()
    c.execute("UPDATE cases SET category='cancelled',updated=?,note='本人已在启动前取消。' WHERE id=?",(now,cid))
    c.execute("UPDATE lanes SET state='cancelled',changed=? WHERE case_id=?",(now,cid))
    if jobs:
        c.execute("UPDATE expiry_native.jobs SET state='cancelled',status='cancelled' WHERE msgid=? AND kind='business' AND state='queued'",(cid,))
    if t3_jobs:
        c.execute("UPDATE expiry_native.t3_jobs SET state='cancelled',updated=? WHERE msgid=? AND state='queued'",(datetime_iso(now),cid))
    c.execute("UPDATE message_batches SET state='cancelled' WHERE cid=? AND state IN ('collecting','sealing','sealed_waiting')",(cid,))
    c.execute("UPDATE chat_receipts SET state='disabled',detail='owner_cancelled',finished=? WHERE case_id=? AND state IN ('ready','waiting')",(now,cid))
    c.execute("UPDATE group_reply_notes SET state='cancelled' WHERE case_id=? AND state IN ('waiting','claimed','bridge_ack','authored')",(cid,))
    return 'cancelled'


def datetime_iso(ts):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def quoted_receipt_tag(quotes, *, include_clarify=False):
    for q in (quotes or ()):
        if not isinstance(q, str):
            continue
        s = q.strip()
        match = RECEIPT_QUOTE.match(s)
        if match:
            return match.group(1).lower()
        if include_clarify:
            cmatch = CLARIFY_CARD_QUOTE.match(s)
            if cmatch:
                return cmatch.group(1).lower()
    return None


def is_appendable(c, root, case):
    """Read-only check: True iff `case` can still accept an append right now (active or within 10-min grace window)."""
    if not case:
        return False
    cid = case['id']
    if pending_expiry.expired(c, root, cid) or case['category'] not in ('work', 'collecting', 'awaiting_clarification'):
        return False
    lanes = list(c.execute('SELECT state FROM lanes WHERE case_id=?', (cid,)))
    if lanes and all(r[0] in ('completed', 'blocked', 'cancelled', 'expired', 'handed_off') for r in lanes):
        if any(r[0] == 'completed' for r in lanes) and case['mode'] == 't3' and pending_expiry.attach_native(c, root):
            if c.execute("SELECT 1 FROM expiry_native.sqlite_master WHERE type='table' AND name='t3_jobs'").fetchone():
                cols = {r[1] for r in c.execute('PRAGMA expiry_native.table_info(t3_jobs)')}
                if {'t3_archived_at', 't3_archive_after'} <= cols:
                    tj = c.execute(
                        'SELECT state,t3_thread_id,t3_archived_at,t3_archive_after FROM expiry_native.t3_jobs WHERE msgid=?',
                        (cid,),
                    ).fetchone()
                    now = time.time()
                    return bool(
                        tj and tj['state'] == 'completed' and tj['t3_thread_id']
                        and not tj['t3_archived_at']
                        and tj['t3_archive_after'] is not None
                        and float(tj['t3_archive_after']) > now
                    )
        return False
    return True


def intake(c,root,item,epoch):
    from business_queue import make_case
    if not request_flow.installed(root,epoch):return False
    text=PREFIX.sub('',(item.get('direct') or '').strip(),count=1).strip()
    quotes=item.get('quotes') or []
    quote_tag=quoted_receipt_tag(quotes)
    is_reserved=bool(RESERVED.match(text))
    if not quote_tag and (is_reserved or QUOTED_CANCEL.fullmatch(text)):
        quote_tag=quoted_receipt_tag(quotes, include_clarify=True)
    if not is_reserved and not quote_tag:
        return False
    if not is_reserved and quote_tag and OTHER_COMMAND.match(text):
        return False
    direct_attachments=[a for a in (item.get('attachments') or []) if not a.get('quoted')]
    is_cancel_quote=bool(quote_tag and QUOTED_CANCEL.fullmatch(text))
    natural_quote=bool(quote_tag and not is_reserved and not is_cancel_quote)
    case=None; body=''; action='invalid'; result='invalid'; apply_control=False
    if is_reserved and not quote_tag:
        if not owner_origin(root,item) or quotes or direct_attachments or item['msgtype']!='text':
            return True
        match=COMMAND.fullmatch(text)
        if match:
            op,ref,body=match.groups(); body=(body or '').strip()
            action='append' if '追加' in op else 'cancel'
            if op.startswith('确认'):
                proposal=c.execute('SELECT * FROM task_proposals WHERE id=? AND chat=? AND sender=? AND action=?',
                                   (ref,item['chatid'],item['sender'],action)).fetchone() if PROPOSAL.fullmatch(ref) else None
                if not proposal or body or proposal['confirmed_mid']:result='confirmation_mismatch'
                elif time.time()>=proposal['expires']:result='expired'
                else:
                    case=target(c,item,proposal['target']); body=proposal['body']
                    apply_control=bool(case)
                    c.execute('UPDATE task_proposals SET confirmed_mid=? WHERE id=? AND confirmed_mid IS NULL',(item['msgid'],ref))
            elif ((action=='cancel' and not body) or (action=='append' and 0<len(body)<=4000)):
                case=target(c,item,ref)
                if not case:result='unknown_target'
                else:apply_control=True
    else:
        if not owner_origin(root,item):
            return False
        if item.get('msgtype') not in ('text','voice','mixed') and not direct_attachments:
            return False
        if direct_attachments:
            case=target(c,item,quote_tag)
            if natural_quote and not is_appendable(c,root,case):
                # Original task already ended/expired: let the message open a new task with attachment + quoted context.
                return False
            action='append'
            result='unsupported_attachment'
        elif is_reserved:
            match=COMMAND.fullmatch(text)
            if match:
                op,ref,rest=match.groups(); rest=(rest or '').strip()
                if op.startswith('确认'):
                    return True
                action='append' if '追加' in op else 'cancel'
                ref_clean=ref.strip('〔〕').lower()
                if TARGET.fullmatch(ref_clean):
                    if ref_clean!=quote_tag and not (ref_clean.startswith('w_') and ref_clean[-7:]==quote_tag):
                        result='target_mismatch'
                        case=target(c,item,quote_tag)
                    else:
                        body=rest
                        if (action=='cancel' and not body) or (action=='append' and 0<len(body)<=4000):
                            case=target(c,item,ref_clean)
                            if not case:result='unknown_target'
                            else:apply_control=True
                        else:
                            case=target(c,item,quote_tag)
                elif action=='append':
                    body=QUOTED_APPEND_PREFIX.sub('',text,count=1).strip()
                    if 0<len(body)<=4000:
                        case=target(c,item,quote_tag)
                        if not case:result='unknown_target'
                        else:apply_control=True
                    else:
                        case=target(c,item,quote_tag)
            elif QUOTED_CANCEL.fullmatch(text):
                action='cancel'; body=''
                case=target(c,item,quote_tag)
                if not case:result='unknown_target'
                else:apply_control=True
            else:
                action='append'
                case=target(c,item,quote_tag)
        elif is_cancel_quote:
            action='cancel'; body=''
            case=target(c,item,quote_tag)
            if not case:result='unknown_target'
            else:apply_control=True
        else:
            action='append'
            body=text
            case=target(c,item,quote_tag)
            if not case:
                # Old/untracked receipt quoted naturally: fall through to new task with quoted text as prerequisite.
                return False
            if 0<len(body)<=4000:
                apply_control=True
            elif not body or not is_appendable(c,root,case):
                return False
    if case and apply_control:
        result='expired' if time.time()-epoch>pending_expiry.TIMEOUT_SECONDS else request(c,root,case,action)
    if natural_quote and result=='inactive':
        # Do not error when naturally quoting an already-finished/expired receipt:
        # fall through to business_queue.ingest_new so the quoted text & historical case serve as new-task context.
        return False
    cid=make_case(c,item,'control',category='control',note=MESSAGES[result])
    target_id=case['id'] if case else None
    c.execute('INSERT INTO owner_task_controls(mid,cid,target,action,body,result,created) VALUES(?,?,?,?,?,?,?)',
              (item['msgid'],cid,target_id,action,body,result,epoch))
    group=item['chattype']=='group'
    c.execute('INSERT INTO request_routes VALUES(?,?,?)',(cid,'control','group' if group else 'private'))
    phase=request_flow.ACK if group else 'first'
    c.execute("INSERT INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
              (cid,phase,time.time(),control_receipt(c,cid)))
    return True


def control_receipt(c,cid):
    row=c.execute('SELECT target,action,result FROM owner_task_controls WHERE cid=?',(cid,)).fetchone()
    tag=('〔'+row['target'][-7:]+'〕') if row['target'] else ''
    msg=MESSAGES[row['result']]
    if row['target'] and row['action']=='append' and row['result']=='awaiting_checkpoint':
        if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='clarifications'").fetchone():
            clar=c.execute(
                "SELECT qid FROM clarifications WHERE case_id=? AND state='open' ORDER BY round_no DESC LIMIT 1",
                (row['target'],),
            ).fetchone()
            if clar:
                msg=f'已登记追加要求；原事项当前正等待你的确认（{clar["qid"]}），请先引用追问消息或发「【答复 {clar["qid"]}】内容」，恢复后将一并执行。'
    return '已收到'+tag+'。'+msg


def mailbox(c,root,cid):
    rows=c.execute("""SELECT t.*,s.item,c.chat,c.sender FROM owner_task_controls t JOIN sources s ON s.mid=t.mid
        AND s.case_id=t.cid JOIN cases c ON c.id=t.target AND s.sender=c.sender
        AND (s.chat=c.chat OR (json_extract(s.item,'$.chattype')='single' AND s.chat=s.sender AND COALESCE(json_extract(s.item,'$.bot_channel'),'feiyu')='feiyu'))
        WHERE t.target=? AND t.result IN ('added_before_start','awaiting_checkpoint','reopened_followup') ORDER BY t.created,t.mid""",(cid,))
    return [{'change_id':r['cid'],'action':r['action'],'text':r['body'],'state':r['result'],
             'dot_acknowledged':bool(r['dot_seen']),'native_acknowledged':bool(r['native_seen'])}
            for r in rows if owner_origin(root,json.loads(r['item']))]


def propose(c,root,cid,item,action,body):
    case=target(c,item,cid)
    if (not owner_origin(root,item) or not case or case['id']!=cid or case['category']!='work'
            or pending_expiry.expired(c,root,cid) or action not in ('append','cancel')
            or not isinstance(body,str) or (action=='append' and not 0<len(body.strip())<=4000) or (action=='cancel' and body)):
        raise ValueError('scoped_owner_task_required')
    if not c.execute(f"SELECT 1 FROM lanes WHERE case_id=? AND (state IN ('submitted','running') OR state IN {pending_expiry.T3_QUEUE_SQL})",(cid,)).fetchone():
        raise ValueError('target_not_active')
    body=body.strip()
    # Repeating a proposal cannot execute it or renew its confirmation window.
    signature=json.dumps([cid,item['chatid'],item['sender'],action,body],ensure_ascii=False)
    pid='p_'+hashlib.sha256(signature.encode()).hexdigest()[:24]
    c.execute('INSERT OR IGNORE INTO task_proposals VALUES(?,?,?,?,?,?,?,NULL)',
              (pid,cid,item['chatid'],item['sender'],action,body,time.time()+600))
    r=c.execute('SELECT expires,confirmed_mid FROM task_proposals WHERE id=?',(pid,)).fetchone()
    state='confirmed' if r['confirmed_mid'] else 'expired' if r['expires']<=time.time() else 'awaiting_original_owner_confirmation'
    return {'proposal':pid,'state':state,'action':action,'text':body,'case_id':cid,'expires':r['expires'],
            'owner_command':'【确认'+('追加' if action=='append' else '取消')+'】 '+pid}


def dot_checkpoint(c,root,cid,change_id):
    change=next((v for v in mailbox(c,root,cid) if v['change_id']==change_id),None)
    if not change:raise ValueError('unknown_owner_change')
    if change['dot_acknowledged']:return {'state':'acknowledged','case_id':cid}
    lane=c.execute("SELECT state FROM lanes WHERE case_id=? AND target='dot'",(cid,)).fetchone()
    if not lane or lane[0]!='running':raise ValueError('original_running_dot_required')
    if change['action']=='cancel':
        c.execute("UPDATE lanes SET state='cancelled',changed=?,detail='signed_owner_stop_checkpoint' WHERE case_id=? AND target='dot' AND state='running'",(time.time(),cid))
    c.execute('UPDATE owner_task_controls SET dot_seen=? WHERE cid=? AND target=?',(time.time(),change_id,cid))
    return {'state':'acknowledged','case_id':cid,'action':change['action']}


def start_native_mailbox(root,work,cid,stop,*,return_worker=False):
    from business_queue import db
    from local_dispatch import _atomic
    import threading
    path=work/'task-control.json'
    def sample(previous):
        with db(root) as c:
            changes=mailbox(c,root,cid)
        encoded=json.dumps({'case_id':cid,'changes':changes},ensure_ascii=False,indent=2)
        if encoded!=previous:_atomic(path,encoded)
        return encoded
    initial=sample(None)
    def watch():
        previous=initial
        while not stop.wait(1):
            try:previous=sample(previous)
            except (OSError,ValueError,sqlite3.Error):pass
    worker=threading.Thread(target=watch,name='owner-task-changes',daemon=True)
    worker.start()
    return worker if return_worker else initial


def native_checkpoint(c,root,cid,work):
    # Called only after verifying the native completion evidence. A request or
    # a mailbox read alone can never mark a running process as stopped.
    from business_queue import load
    raw=load(work/'task-control-ack.json',{})
    if not isinstance(raw,dict) or raw.get('case_id')!=cid or not isinstance(raw.get('changes'),dict):return False
    stopped=False
    for change in mailbox(c,root,cid):
        outcome=raw['changes'].get(change['change_id'])
        if (change['action'],outcome) in (('append','applied'),('cancel','stopped_at_safe_point')):
            c.execute('UPDATE owner_task_controls SET native_seen=COALESCE(native_seen,?) WHERE target=? AND cid=?',(time.time(),cid,change['change_id']))
            stopped=stopped or change['action']=='cancel'
    return stopped
