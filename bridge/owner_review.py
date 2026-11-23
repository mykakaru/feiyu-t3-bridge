"""Bind one owner approval and, when authorized, a reply to the original group."""
import ctypes
from ctypes import wintypes
from contextlib import closing
from hashlib import sha256
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time
import unicodedata
from uuid import uuid4

from command_routing import enabled, private_default
import group_replies
from group_replies import contains_credentials
from identity import resolve


DESKTOP_PREFIX = 'windows-owner-'
REMOTE_PREFIX = 'wecom-owner-'
NOTIFIER = 'FeiyuOwnerReview.exe'
SIGNAL = 'owner-review.signal'
MAX_QUESTION_LENGTH = 4000
TOAST_CONFIRM_WIDTH = 38
TOAST_PREVIEW_WIDTH = 76
AUTHORIZATION = re.compile(r'【授权】[\s\u200b]*(?:〔([0-9a-f]{7})〕|([0-9a-f]{7}|w_[0-9a-f]{32}))?', re.I)
LOCAL_ARTIFACT = re.compile(r'(?i)\b(?:file:|sandbox:|[a-z]:[\\/])|(?:/Users/|/home/|/mnt/[a-z]/|\.codex[\\/])')
HOLD_REPLY = '这条已获授权办理，答复需要授权人确认后再发。'
NO_PUBLIC_ANSWER = '暂未取得可在本群发布的答复。'
DESKTOP_REJECTION = 'desktop_review_rejected'
REJECTION_NOTE = '授权人已静默拒绝。'


def install(c):
    c.execute("""CREATE TABLE IF NOT EXISTS windows_adoptions(
        review_case TEXT PRIMARY KEY REFERENCES cases(id),
        owner_case TEXT NOT NULL UNIQUE REFERENCES cases(id),
        group_chat TEXT NOT NULL, source_hash TEXT NOT NULL, accepted_at REAL NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS owner_group_publications(
        review_case TEXT PRIMARY KEY REFERENCES windows_adoptions(review_case),
        approved_by TEXT NOT NULL,
        authorization_mid TEXT UNIQUE REFERENCES sources(mid))""")


def adoption(c, owner_case):
    return c.execute("SELECT * FROM windows_adoptions WHERE owner_case=?", (owner_case,)).fetchone()


def auto_publication(c, owner_case):
    return c.execute("""SELECT p.* FROM owner_group_publications p JOIN windows_adoptions a USING(review_case)
        WHERE a.owner_case=?""", (owner_case,)).fetchone()


def question_text(item):
    text = re.sub(r'[@＠]大肥鱼(?=$|[\s\u200b，,。！!？?：:【])', '', item['direct'], count=1)
    return re.sub(r'^[\s\u200b]+|[\s\u200b]+$', '', text)


def group_label(chatid):
    return '原群（会话尾号 ' + chatid[-8:] + '）'


def source_hash(row, item):
    payload = json.dumps([row['id'], row['anchor'], row['chat'], row['sender'],
                          item['direct'], len(item['attachments'])], ensure_ascii=False)
    return sha256(payload.encode('utf-8')).hexdigest()


def display_width(character):
    return 2 if unicodedata.east_asian_width(character) in 'WFA' or character in 'MWmw@%' else 1


def notification_text(item):
    question = question_text(item)
    if not question or contains_credentials(question):
        return '', False
    visible = ''.join(' ' if unicodedata.category(character).startswith('C') else character
                      for character in question)
    visible = ' '.join(visible.split())
    width = 0
    for end, character in enumerate(visible):
        width += display_width(character)
        if width > TOAST_PREVIEW_WIDTH:
            return visible[:end].rstrip() + '…', False
    quick = (visible == question and width <= TOAST_CONFIRM_WIDTH and item['msgtype'] == 'text'
             and not item['quotes'] and not item['attachments']
             and not any(unicodedata.category(character).startswith('M') for character in visible))
    return visible, quick


def notification_card(row, item):
    preview, quick = notification_text(item)
    return {'id':row['id'], 'group':group_label(row['chat']), 'preview':preview,
            'source_hash':source_hash(row,item), 'can_quick_confirm':quick}


def review_rows(root, case_id=None):
    from request_flow import member_group

    path = Path(root) / 'business.sqlite3'
    if not path.is_file():
        return
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=2)) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute("""SELECT b.id,b.anchor,b.chat,b.sender,b.at,s.item
            FROM cases b JOIN sources s ON s.case_id=b.id AND s.mid=b.anchor
             AND s.chat=b.chat AND s.sender=b.sender
            WHERE b.category='owner_review' AND (? IS NULL OR b.id=?)
            ORDER BY b.epoch,b.id""",(case_id,case_id))
        for row in rows:
            item = json.loads(row['item'])
            if member_group(root,item) and group_replies.addressed_to_bot(item):
                yield row, item


def pending(root, skip, *, case_id=None):
    with closing(review_rows(root,case_id)) as entries:
        for row,item in entries:
            if row['id'] not in skip:
                return [{**notification_card(row,item), 'at':row['at'], 'question':question_text(item),
                         'attachment_count':len(item['attachments'])}]
    return []


def notification_feed(root):
    return [notification_card(row,item) for row,item in review_rows(root)]


def owner_private_chat(c, root):
    chats = c.execute("""SELECT DISTINCT s.chat FROM sources s
        WHERE s.chat=s.sender AND json_extract(s.item,'$.chattype')='single'
        AND s.mid NOT LIKE ? AND s.mid NOT LIKE ?""", (DESKTOP_PREFIX+'%',REMOTE_PREFIX+'%')).fetchall()
    verified = [row[0] for row in chats if resolve(root, row[0])['is_owner']]
    if len(verified) != 1:
        raise ValueError('unique_verified_owner_private_chat_required')
    return verified[0]


def review_record(c, case_id):
    return c.execute("""SELECT b.*,s.item FROM cases b
        JOIN sources s ON s.case_id=b.id AND s.mid=b.anchor AND s.chat=b.chat AND s.sender=b.sender
        WHERE b.id=? AND b.category IN ('owner_review','owner_adopted')""", (case_id,)).fetchone()


def accept(root, request, *, original=False):
    from business_queue import db

    fields = {'id', 'source_hash'} if original else {'id', 'source_hash', 'question'}
    if not isinstance(request,dict) or set(request) != fields or not all(isinstance(v, str) for v in request.values()):
        raise ValueError('invalid_review_request')
    with db(root, True) as c:
        review = review_record(c, request['id'])
        if not review:
            raise ValueError('pending_review_not_found')
        item = json.loads(review['item'])
        if source_hash(review,item) != request['source_hash']:
            raise ValueError('review_source_changed')
        if original:
            question, quick = notification_text(item)
            if not quick:
                raise ValueError('review_requires_editor')
        else:
            question = request['question']
        result = adopt(c,root,review,item,question)
    if result['state'] == 'accepted':
        try:
            (Path(root) / SIGNAL).touch()
        except OSError:
            logging.exception('Desktop review wake signal failed, durable owner case will still reconcile')
    return result


def rejectable_reviews(c, review_id=None):
    return c.execute("""SELECT b.id,b.anchor,b.chat,b.sender,s.item,a.owner_case
        FROM cases b JOIN sources s ON s.case_id=b.id AND s.mid=b.anchor AND s.chat=b.chat AND s.sender=b.sender
        LEFT JOIN windows_adoptions a ON a.review_case=b.id AND a.group_chat=b.chat
        WHERE (? IS NULL OR b.id=?) AND (b.category='owner_review' OR
            (b.category='owner_adopted' AND a.owner_case IS NOT NULL AND (
              EXISTS(SELECT 1 FROM lanes l WHERE l.case_id=a.owner_case AND l.state IN ('queued','submitted','running')) OR
              EXISTS(SELECT 1 FROM chat_receipts r WHERE r.state IN ('waiting','ready','sending') AND
                ((r.case_id=b.id AND r.phase=?) OR (r.case_id=a.owner_case AND r.phase='final'))))))""",
        (review_id,review_id,group_replies.PHASE))


def panel_rejections(root):
    from business_queue import db

    if not (Path(root)/'business.sqlite3').is_file():
        return {},set()
    with db(root) as c:
        hidden = {row[0] for row in c.execute('SELECT case_id FROM observations WHERE source=?',(DESKTOP_REJECTION,))}
        targets = {}
        for row in rejectable_reviews(c):
            request = {'id':row['id'], 'source_hash':source_hash(row,json.loads(row['item']))}
            targets[row['id']] = request
            if row['owner_case']:
                targets[row['owner_case']] = request
        return targets,hidden


def notify_running_executor(c, owner, now):
    from business_queue import insert_source, make_case, utc

    item = {'msgid':'windows-review-stop-'+uuid4().hex, 'chatid':owner['chat'], 'sender':owner['sender'],
            'received_at':utc(now), 'chattype':'single', 'msgtype':'text', 'direct':REJECTION_NOTE,
            'quotes':[], 'attachments':[], 'delivery_at_import':'desktop_owner_rejection'}
    insert_source(c,item)
    control = make_case(c,item,'control',category='control',note=REJECTION_NOTE)
    c.execute("""INSERT INTO owner_task_controls(mid,cid,target,action,body,result,created)
        VALUES(?,?,?,'cancel','','awaiting_checkpoint',?)""",(item['msgid'],control,owner['id'],now))
    return control


def reject(root, request):
    from business_queue import db
    from request_flow import REVIEW_NOTICES, member_group
    import task_controls

    if (not isinstance(request,dict) or set(request)!={'id','source_hash'}
        or not all(isinstance(v,str) for v in request.values())
        or not re.fullmatch(r'w_[a-f0-9]{32}',request['id'])):
        raise ValueError('invalid_review_request')
    review_id = request['id']
    now = time.time()
    with db(root,True) as c:
        if c.execute('SELECT 1 FROM observations WHERE case_id=? AND source=?',(review_id,DESKTOP_REJECTION)).fetchone():
            return {'state':'already_dismissed'}
        review = rejectable_reviews(c,review_id).fetchone()
        if not review:
            raise ValueError('review_no_longer_active')
        original = json.loads(review['item'])
        if (not member_group(root,original) or not group_replies.addressed_to_bot(original)
            or source_hash(review,original)!=request['source_hash']):
            raise ValueError('review_source_changed')
        owner_id = review['owner_case'] or review_id
        # An already reserved notice cannot be recalled. Accept the decision so
        # it can still stop every subsequent attempt and the final reply.
        if c.execute("""SELECT 1 FROM chat_receipts WHERE case_id IN (?,?) AND state='sending'
                AND phase NOT IN (?,?)""", (review_id, owner_id, *REVIEW_NOTICES)).fetchone():
            raise ValueError('review_delivery_in_progress')
        stop = 'not_started'
        control = None
        if review['owner_case']:
            owner = c.execute('SELECT * FROM cases WHERE id=?',(owner_id,)).fetchone()
            if (not owner or owner['chat']!=owner['sender'] or not resolve(root,owner['sender'])['is_owner']
                or not is_review_source(owner['anchor'])):
                raise ValueError('review_owner_binding_changed')
            stop = task_controls.request(c,root,owner,'cancel')
            if stop=='awaiting_checkpoint':
                control = notify_running_executor(c,owner,now)
        c.execute("UPDATE cases SET category='cancelled',note=?,updated=? WHERE id=?",(REJECTION_NOTE,now,review_id))
        c.execute("""UPDATE chat_receipts SET state='disabled',finished=?,detail=?
            WHERE case_id IN (?,?) AND state IN ('waiting','ready')""",(now,DESKTOP_REJECTION,review_id,owner_id))
        c.execute("UPDATE feedback_alerts SET resolved=? WHERE case_id IN (?,?) AND resolved IS NULL",(now,review_id,owner_id))
        for cid in {review_id,owner_id,control}-{None}:
            c.executemany('INSERT OR IGNORE INTO observations VALUES(?,?,?,?)',
                          [(cid,DESKTOP_REJECTION,now,REJECTION_NOTE),(cid,'owner_requested_no_replay',now,REJECTION_NOTE)])
    return {'state':'dismissed','execution':stop}


def adopt(c, root, review, item, question, *, authorization=None):
    from business_queue import insert_source, make_case, utc
    from request_flow import ACK, REVIEW_AUTHORIZED, REVIEW_PENDING, member_group, review_notice_text

    if not member_group(root, item) or not group_replies.addressed_to_bot(item):
        raise ValueError('review_source_changed')
    question = question.strip()
    if not 0 < len(question) <= MAX_QUESTION_LENGTH:
        raise ValueError('invalid_review_question')
    if contains_credentials(question):
        raise ValueError('credential_content')
    if not enabled(root):
        raise ValueError('bridge_disabled')
    if authorization is not None and (
         authorization['chatid'] != review['chat']
        or not group_replies.owner_mention(root,authorization)
        or not AUTHORIZATION.fullmatch(question_text(authorization))
    ):
        raise ValueError('wrong_authorization_scope')
    existing = c.execute("SELECT owner_case FROM windows_adoptions WHERE review_case=?", (review['id'],)).fetchone()
    if existing:
        return {'state':'already_accepted','case_id':existing['owner_case']}
    if review['category'] != 'owner_review':
        raise ValueError('pending_review_not_found')

    now = time.time()
    owner = owner_private_chat(c, root)
    received = utc(now)
    prefix = REMOTE_PREFIX if authorization else DESKTOP_PREFIX
    origin = 'wecom_group_approval' if authorization else 'desktop_owner_approval'
    label = '企微群内授权' if authorization else 'Windows 接手'
    approved = {'msgid':prefix+uuid4().hex,'chatid':owner,'sender':owner,
                'received_at':received,'chattype':'single','msgtype':'text','direct':question,
                'quotes':[],'attachments':[],'delivery_at_import':origin}
    route = {'dot':'dot','local_agent':'local','dot_then_local':'dot_then_local'}[private_default(root, received)]
    insert_source(c, approved)
    cid = make_case(c, approved, route, note=label+' · '+group_label(review['chat']))
    c.execute("INSERT INTO windows_adoptions VALUES(?,?,?,?,?)",
              (review['id'], cid, review['chat'], source_hash(review,item), now))
    c.execute("UPDATE cases SET category='owner_adopted',updated=? WHERE id=?", (now, review['id']))
    c.execute("""UPDATE chat_receipts SET state='disabled',finished=?,detail='review_approved'
        WHERE case_id=? AND phase IN (?,?) AND state='ready'""", (now,review['id'],REVIEW_PENDING,ACK))
    c.execute("INSERT INTO owner_group_publications VALUES(?,?,?)",
              (review['id'], authorization['sender'] if authorization else owner,
               authorization['msgid'] if authorization else None))
    c.execute("INSERT INTO request_routes VALUES(?, 'quiet', 'private')", (cid,))
    c.executemany("INSERT INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,?,?,?)", [
        (review['id'], REVIEW_AUTHORIZED, 'ready', now, review_notice_text(review['id'], REVIEW_AUTHORIZED)),
        (review['id'], group_replies.PHASE, 'waiting', now, ''),
    ])
    return {'state':'accepted','case_id':cid}


def ingest_authorization(c, root, item, epoch):
    """Consume only the owner-approved, single-request form of the group command."""
    from business_queue import make_case
    from request_flow import ACK, OWNER_AUTHORIZATION

    if not group_replies.is_group(root,item) or not group_replies.addressed_to_bot(item):
        return False
    match = AUTHORIZATION.fullmatch(question_text(item))
    if not match:
        return False
    if not group_replies.owner_mention(root,item):
        return True
    ref = (match[1] or match[2] or '').lower()
    quotes = {question_text({'direct':q}) for q in item['quotes'] if q.strip()}
    choices = []
    if (ref or quotes) and item['msgtype'] in ('text','mixed'):
        rows = c.execute("""SELECT b.*,s.item FROM cases b
            JOIN sources s ON s.case_id=b.id AND s.mid=b.anchor AND s.chat=b.chat AND s.sender=b.sender
            WHERE b.chat=? AND b.category IN ('owner_review','owner_adopted')""", (item['chatid'],))
        for row in rows:
            original = json.loads(row['item'])
            if ref and ref not in (row['id'],row['id'][-7:]):
                continue
            if quotes and question_text(original) not in quotes:
                continue
            choices.append((row,original))
    reply = None
    if len(choices) != 1:
        reply = '请引用本群中的原提问，或在【授权】后填本地通知里的事项编号。'
    else:
        review, original = choices[0]
        try:
            adopt(c,root,review,original,question_text(original),authorization=item)
        except ValueError:
            reply = '这条暂时无法授权办理，请核对原问题和桥的状态。'
    notice = make_case(c,item,OWNER_AUTHORIZATION,category='control',note=reply or '原群授权指令已处理。')
    if reply:
        c.execute("INSERT INTO request_routes VALUES(?,?, 'group')",(notice,OWNER_AUTHORIZATION))
        c.execute("INSERT INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
                   (notice,ACK,epoch,reply))
    return True


def is_desktop_source(mid):
    return isinstance(mid, str) and mid.startswith(DESKTOP_PREFIX)


def is_review_source(mid):
    return isinstance(mid, str) and mid.startswith((DESKTOP_PREFIX,REMOTE_PREFIX))


def native_source(root, mid, chatid):
    from business_queue import db

    if (not is_review_source(mid) or not re.fullmatch(r'(?:windows|wecom)-owner-[a-f0-9]{32}', mid)
            or not resolve(root, chatid)['is_owner']):
        return None
    with db(root) as c:
        row = c.execute("""SELECT s.item FROM windows_adoptions a
            JOIN cases b ON a.owner_case=b.id
            JOIN sources s ON s.case_id=b.id AND s.mid=b.anchor AND s.chat=b.chat AND s.sender=b.sender
            WHERE s.mid=? AND s.chat=? AND s.chat=s.sender""", (mid,chatid)).fetchone()
    if not row:
        return None
    item = json.loads(row['item'])
    return {'msgid':mid,'sender':chatid,'chatid':chatid,'chattype':'single',
            'received_at':item['received_at'],'delivery':'pending','msgtype':'text',
            'source_origin':'windows_owner_approval' if is_desktop_source(mid) else 'wecom_group_approval',
            'parts':[{'type':'text','text':item['direct']}],'attachments':[]}


def local_result_verified(root, owner_case, lane):
    from local_dispatch import _completion_proof, digest

    path = Path(root) / 'local-tasks.sqlite3'
    if not path.is_file():
        return False
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)) as jobs:
        jobs.row_factory = sqlite3.Row
        job = jobs.execute("SELECT * FROM jobs WHERE msgid=? AND kind='business' AND state='completed'",
                           (owner_case['id'],)).fetchone()
    if not job or not owner_case['frozen'] or job['context_hash'] != digest(json.loads(owner_case['frozen'])):
        return False
    proof = _completion_proof(job)
    return bool(proof and proof['result_hash'] == sha256(lane['result'].encode()).hexdigest())


def publication_binding(c, root, review_id):
    """Resolve an active approval for this exact request and original group."""
    if not enabled(root):
        return None
    grant = c.execute("""SELECT p.*,a.owner_case,a.group_chat,a.source_hash
        FROM owner_group_publications p JOIN windows_adoptions a USING(review_case)
        WHERE p.review_case=?""", (review_id,)).fetchone()
    review = review_record(c,review_id)
    if (not grant or not review or review['category'] != 'owner_adopted'
            or grant['group_chat'] != review['chat'] or not resolve(root,grant['approved_by'])['is_owner']):
        return None
    original = json.loads(review['item'])
    from request_flow import member_group
    if (not member_group(root,original) or not group_replies.addressed_to_bot(original)
            or source_hash(review,original) != grant['source_hash']):
        return None
    owner_case = c.execute("SELECT * FROM cases WHERE id=?", (grant['owner_case'],)).fetchone()
    if not owner_case or owner_case['chat'] != owner_case['sender'] or not resolve(root,owner_case['sender'])['is_owner']:
        return None
    if grant['authorization_mid']:
        proof = c.execute("SELECT item FROM sources WHERE mid=? AND chat=? AND sender=?",
                          (grant['authorization_mid'],review['chat'],grant['approved_by'])).fetchone()
        if (not proof or not owner_case['anchor'].startswith(REMOTE_PREFIX)
                or not group_replies.owner_mention(root,json.loads(proof['item']))):
            return None
    elif owner_case['sender'] != grant['approved_by'] or not is_desktop_source(owner_case['anchor']):
        return None
    if c.execute("""SELECT 1 FROM observations WHERE case_id IN (?,?) AND source='owner_requested_no_replay'""",
                 (review_id,owner_case['id'])).fetchone():
        return None
    return original, owner_case


def publication_body(c, root, review_id):
    """Rebuild the exact group payload from the immutable approval and completion."""
    from request_flow import REVIEW_AUTHORIZED

    binding = publication_binding(c, root, review_id)
    if not binding:
        return None
    original, owner_case = binding
    prefix = '〔'+review_id[-7:]+'〕\n'
    new_flow = c.execute('SELECT 1 FROM chat_receipts WHERE case_id=? AND phase=?',
                         (review_id, REVIEW_AUTHORIZED)).fetchone() is not None
    hold = prefix + (NO_PUBLIC_ANSWER if new_flow else HOLD_REPLY)
    if owner_case['category'] in ('cancelled','expired'):
        return prefix + '这条已停止办理。'
    lanes = c.execute("SELECT * FROM lanes WHERE case_id=? AND state<>'handed_off'",(owner_case['id'],)).fetchall()
    if len(lanes) != 1:
        return None
    lane = lanes[0]
    if lane['state'] in ('blocked','cancelled','uncertain'):
        return hold
    if lane['state'] != 'completed':
        return None
    if lane['target']=='dot':
        signed = c.execute("SELECT 1 FROM observations WHERE case_id=? AND source='signed_subscription_read'",
                           (owner_case['id'],)).fetchone()
        if lane['detail'] != 'signed_subscription' or not signed:
            return hold
    elif lane['target']!='local' or not local_result_verified(root,owner_case,lane):
        return hold

    pack = json.loads(owner_case['frozen'] or '{}')
    anchors = [item for item in pack.get('manifest',{}).get('members',[]) if item['msgid']==owner_case['anchor']]
    if len(anchors)!=1 or pack.get('read_only') is not True:
        return hold
    chat = original['chatid']
    publication = {'chatid':chat,'members':[
        {'chattype':'group','chatid':chat,'direct':original['direct'] if new_flow else anchors[0]['direct'],'quotes':[]}]}
    try:
        answer = group_replies.public_text(lane['result'],publication,allow_web_links=new_flow)
        if LOCAL_ARTIFACT.search(answer) or len((prefix+answer).encode('utf-8'))>20480:
            return hold
        return prefix + answer
    except ValueError:
        return hold


def promote_publications(c, root):
    pending = c.execute("""SELECT r.case_id FROM chat_receipts r
        JOIN owner_group_publications p ON p.review_case=r.case_id
        WHERE r.phase=? AND r.state='waiting'""", (group_replies.PHASE,)).fetchall()
    for row in pending:
        body = publication_body(c,root,row['case_id'])
        if body:
            c.execute("""UPDATE chat_receipts SET state='ready',body=?,due=?
                WHERE case_id=? AND phase=? AND state='waiting'""",
                (body,time.time(),row['case_id'],group_replies.PHASE))


def require_notifier_parent(root, executable=NOTIFIER):
    if os.name != 'nt':
        raise ValueError('windows_owner_review_required')
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.QueryFullProcessImageNameW.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR,
                                                  ctypes.POINTER(wintypes.DWORD)]
    kernel.QueryFullProcessImageNameW.restype = wintypes.BOOL
    handle = kernel.OpenProcess(0x1000, False, os.getppid())
    if not handle:
        raise ValueError('windows_owner_review_required')
    try:
        path = ctypes.create_unicode_buffer(32768)
        size = wintypes.DWORD(len(path))
        if (not kernel.QueryFullProcessImageNameW(handle, 0, path, ctypes.byref(size))
                or Path(path.value).resolve() != (Path(root) / executable).resolve()):
            raise ValueError('windows_owner_review_required')
    finally:
        kernel.CloseHandle(handle)


def start_notifier(root):
    installed = Path.home() / 'Wecom Assistant' / 'bridge'
    exe = installed / NOTIFIER
    if os.name != 'nt' or Path(root).resolve() != installed.resolve() or not exe.is_file():
        return
    # The desktop UI needs no tunnel credentials or event-service authorization.
    env = {k:v for k,v in os.environ.items() if not k.upper().startswith(('XIAOAI_', 'MCP_', 'CONTROL_PLANE_'))}
    try:
        subprocess.Popen([str(exe)], cwd=installed, env=env, stdin=subprocess.DEVNULL,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         creationflags=subprocess.CREATE_NO_WINDOW)
    except OSError:
        logging.exception('Owner review notification startup failed')
