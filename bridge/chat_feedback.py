"""Two phase, owner-private WeCom receipts for new authenticated work."""
from contextlib import closing
from hashlib import sha256
import json
import logging
import math
from pathlib import Path
import re
import sqlite3
import threading
import time

from identity import CLI_OWNER, resolve
from business_queue import db, load
from command_routing import enabled
import group_replies
import group_reply_only as replies
import pending_expiry
import request_flow as flow
import private_files
import t3_client

changed = threading.Event()
quiet = re.compile(r'无需回复|不用回复|不要回复|不用回报|不要回报|仅面板|只在面板|do not reply|no reply needed', re.I)
ANSWER_RECOVERY = 'final_recovery'
PROGRESS_DEFAULT_SECONDS = 1800
PROGRESS_STATES = ('launching', 'starting', 'running', *pending_expiry.T3_QUEUE_STATES)
PROGRESS_EVENTS = {
    't3_starting': '正在启动 T3',
    't3_thread_started': 'T3 线程已启动',
    't3_turn_finished': 'T3 已返回本轮结果，桥正在处理',
    'applying_append': '正在同步追加要求',
    'post_completion_append': '正在原线程续办',
    't3_wait_handed_to_reconcile': '后台巡检正在跟踪 T3 线程',
    'escalating_to_neon': '正在切换到升级模型',
}

def install(c):
    c.execute("""CREATE TABLE IF NOT EXISTS chat_receipts(
        case_id TEXT REFERENCES cases(id), phase TEXT, state TEXT, due REAL,
        attempts INTEGER DEFAULT 0, started REAL, finished REAL, detail TEXT DEFAULT '',
        PRIMARY KEY(case_id,phase))""")
    if 'body' not in {r[1] for r in c.execute('PRAGMA table_info(chat_receipts)')}:
        c.execute("ALTER TABLE chat_receipts ADD COLUMN body TEXT NOT NULL DEFAULT ''")
    if 'preflights' not in {r[1] for r in c.execute('PRAGMA table_info(chat_receipts)')}:
        c.execute("ALTER TABLE chat_receipts ADD COLUMN preflights INTEGER NOT NULL DEFAULT 0")
    c.execute("CREATE INDEX IF NOT EXISTS chat_pending ON chat_receipts(state,due)")
    c.execute("""CREATE TABLE IF NOT EXISTS private_feedback_text(
        case_id TEXT PRIMARY KEY REFERENCES cases(id), target_sha256 TEXT NOT NULL,
        source_sha256 TEXT NOT NULL, wire_sha256 TEXT NOT NULL, body TEXT NOT NULL,
        files_ok INTEGER NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS group_feedback_text(
        case_id TEXT REFERENCES cases(id), phase TEXT NOT NULL, target_sha256 TEXT NOT NULL,
        source_sha256 TEXT NOT NULL, wire_sha256 TEXT NOT NULL, body TEXT NOT NULL,
        files_ok INTEGER NOT NULL, PRIMARY KEY(case_id,phase))""")
    replies.install(c)
    flow.install(c)

def enroll(c, root, cid, item, epoch):
    if enabled(root) and flow.enroll(c,root,cid,item,epoch):
        return
    if group_replies.is_group(root,item):
        if not enabled(root):return
        silent=bool(quiet.search(item['direct']))
        replies.enroll(c,root,cid,item,epoch,silent)
        if not silent:
            c.execute("INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due) VALUES(?,?,'waiting',?)",
                      (cid,group_replies.PHASE,epoch))
        return
    settings = load(Path(root)/'owner-chat-feedback.json', {})
    since = settings.get('since_epoch')
    if (type(since) not in (float,int) or epoch < since or not enabled(root)
            or item['chattype'] != 'single' or item['sender'] != item['chatid']
            or not resolve(root,item['sender'])['is_owner'] or quiet.search(item['direct'])):
        return
    c.executemany("INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due) VALUES(?,?,?,?)",
                  [(cid,'first','ready',epoch),(cid,'final','waiting',epoch)])

def managed(c,cid):
    return bool(c.execute("SELECT 1 FROM chat_receipts WHERE case_id=? AND phase NOT GLOB 'progress:*' AND phase NOT IN (?,?,?,?,?,?,?) LIMIT 1",
        (cid,group_replies.PHASE,group_replies.FIRST,replies.INTAKE,replies.RESOLVED,flow.ACK,*flow.REVIEW_NOTICES)).fetchone())

def verified_owner(root,case):
    return enabled(root) and case['chat']==case['sender'] and resolve(root,case['sender'])['is_owner']

def private_delivery_active(root, cid, phase='final'):
    with db(root) as c:
        row = c.execute("""SELECT b.* FROM cases b JOIN chat_receipts r ON r.case_id=b.id
            WHERE b.id=? AND r.phase=? AND r.state='sending' AND b.category NOT IN ('cancelled','expired')
            AND NOT EXISTS (SELECT 1 FROM observations WHERE case_id=b.id AND source='owner_requested_no_replay')""",
                        (cid,phase)).fetchone()
        route = flow.route(c, cid)
        return bool(row and verified_owner(root, row) and
                    (not route or (route['origin_kind'] == 'private' and route['destination'] == 'original')))

def final_text(c,cid):
    lanes=c.execute("SELECT target,state,result,detail FROM lanes WHERE case_id=? AND state<>'handed_off' ORDER BY target",(cid,)).fetchall()
    if not lanes or any(l['state'] not in ('completed','blocked','uncertain','cancelled') for l in lanes):
        return None
    parts=[]
    for lane in lanes:
        if lane['state']=='cancelled':
            body='执行者已按本人要求在安全停点结束本事项。'
        elif lane['state']=='completed' and lane['result']:
            body=lane['result']
        elif lane['state']=='blocked' and lane['result']:
            body='办理受阻。'+lane['result']
        else:
            # Tool output/commands may contain private material. Expose only the recorded status.
            body='本次办理受阻，暂未取得可核验的结果。' if lane['state']=='blocked' else '执行状态待核实，尚未取得完成凭据。'
            if 'exit=' in (lane['detail'] or ''):
                match=re.search(r'exit=(-?\d+); turns=(\d+)',lane['detail'])
                if match:body+=f' 执行进程退出码 {match[1]}，已完成 {match[2]} 轮。'
        prefix=('本机' if lane['target']=='local' else '小爱')+'：' if len(lanes)>1 else ''
        parts.append(prefix+body)
    return '\n\n'.join(parts)

def _session_footer_from_job(job):
    if not job:
        return ''
    keys = set(job.keys()) if hasattr(job, 'keys') else set()
    tid = (job['t3_thread_id'] or '').strip() if 't3_thread_id' in keys else ''
    route = (job['route_kind'] or '').strip() if 'route_kind' in keys else ''
    child_ids = []
    raw_used = job['used_models_json'] if 'used_models_json' in keys else None
    if raw_used:
        try:
            used = json.loads(raw_used)
        except (TypeError, ValueError):
            used = []
        if isinstance(used, list):
            for entry in used:
                if isinstance(entry, dict):
                    cid = str(entry.get('child_thread_id') or '').strip()
                    if cid and cid != tid and cid not in child_ids:
                        child_ids.append(cid)
    lines = []
    if route == 'explicit_passthrough' and child_ids:
        lines.append(f'会话ID：{child_ids[0]}')
        if tid:
            lines.append(f'转发会话ID：{tid}')
    elif tid:
        lines.append(f'会话ID：{tid}')
        if child_ids:
            lines.append('子Agent会话ID：' + '、'.join(child_ids))
    elif child_ids:
        lines.append(f'会话ID：{child_ids[0]}')
    if not lines:
        return ''
    return '\n\n---\n' + '\n'.join(lines)


def _completion_session_footer(c, root, cid):
    return _session_footer_from_job(_progress_job(c, root, cid))


def format_session_footer_for_case(root, cid):
    with db(root) as c:
        return _completion_session_footer(c, root, cid)


def truncate_wecom_markdown(text, *, footer='', limit=20480, notice='\n（节选，完整结果可在大肥鱼面板查看。）'):
    if footer and text.endswith(footer):
        body = text[:-len(footer)]
    else:
        body = text
    full = body + footer
    if len(full.encode('utf-8')) <= limit:
        return full
    tail = notice + footer
    tail_bytes = len(tail.encode('utf-8'))
    budget = max(0, limit - tail_bytes - 100)
    return body.encode('utf-8')[:budget].decode('utf-8', errors='ignore') + tail


def recovery_body(c, root, cid):
    """Prove only the old file-error notice went out and the answer is text."""
    record = c.execute("""SELECT b.*,p.body AS sent_body,p.source_sha256,p.target_sha256,p.wire_sha256,
        a.detail AS accepted_notice FROM cases b
        JOIN request_routes v ON v.case_id=b.id AND v.origin_kind='private' AND v.destination='original'
        JOIN chat_receipts r ON r.case_id=b.id AND r.phase='final' AND r.state='file_incomplete'
        JOIN feedback_attempts a ON a.case_id=b.id AND a.phase=r.phase AND a.attempt=r.attempts
        JOIN private_feedback_text p ON p.case_id=b.id AND p.files_ok=0
        WHERE b.id=? AND b.category='work' AND a.state='file_incomplete'
          AND a.body_sha256=p.source_sha256 AND a.target_sha256=p.target_sha256
          AND NOT EXISTS (SELECT 1 FROM observations WHERE case_id=b.id AND source='owner_requested_no_replay')""",
                       (cid,)).fetchone()
    if not record or not verified_owner(root, record):
        return None
    notice = record['sent_body']
    if (not notice.startswith(f'事项〔{cid[-7:]}〕文件交付未完成。\n') or '\n\n' in notice
            or not notice.endswith('完整办理结果可在大肥鱼面板按事项号查看。')):
        return None
    try:
        receipt = json.loads(record['accepted_notice'])
    except (TypeError, ValueError):
        return None
    if (not isinstance(receipt, dict) or receipt.get('success') is not True or receipt.get('errcode', 0) != 0
            or receipt.get('target_sha256') != record['target_sha256']
            or receipt.get('target_sha256') != sha256(CLI_OWNER.encode()).hexdigest()
            or receipt.get('body_sha256') != record['wire_sha256']
            or receipt.get('body_sha256') != sha256(notice.encode()).hexdigest()):
        return None
    answer = final_text(c, cid)
    footer = _completion_session_footer(c, root, cid)
    raw_plain = f'办理结果〔{cid[-7:]}〕\n{answer}'
    raw_with_footer = raw_plain + footer if footer else raw_plain
    if not answer or record['source_sha256'] not in {
        sha256(raw_plain.encode()).hexdigest(),
        sha256(raw_with_footer.encode()).hexdigest(),
    }:
        return None
    clean_answer = private_files.plain_source_citations(answer)
    body = f'补发办理结果〔{cid[-7:]}〕\n{clean_answer}' + footer
    if clean_answer == answer or private_files.references(clean_answer) or len(body.encode()) > 20480:
        return None
    return body


def queue_answer_recovery(c, root, cid):
    body = recovery_body(c, root, cid)
    return bool(body and c.execute("""INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body)
        VALUES(?,?,'ready',?,?)""", (cid, ANSWER_RECOVERY, time.time(), body)).rowcount)


def _progress_interval(config):
    value = config.get('progress_report_interval_seconds')
    if type(value) is int or (type(value) is float and math.isfinite(value)):
        return value
    return PROGRESS_DEFAULT_SECONDS


def _progress_jobs_available(c, root):
    return pending_expiry.attach_native(c, root) and c.execute(
        "SELECT 1 FROM expiry_native.sqlite_master WHERE type='table' AND name='t3_jobs'"
    ).fetchone() is not None


def _progress_job(c, root, cid):
    if not _progress_jobs_available(c, root):
        return None
    return c.execute('SELECT * FROM expiry_native.t3_jobs WHERE msgid=?', (cid,)).fetchone()


def _progress_channel(c, root, case, job):
    if (not job or job['state'] not in PROGRESS_STATES or case['mode'] != 't3'
            or case['category'] != 'work' or not enabled(root)):
        return None
    cid = case['id']
    planned = flow.route(c, cid)
    destination = planned['destination'] if planned else 'original'
    try:
        context_route = json.loads(job['context_json'])['manifest'].get('return_route')
    except (ValueError, TypeError, KeyError, AttributeError):
        return None
    if context_route is not None and not isinstance(context_route, str):
        return None
    if not planned and context_route is None:
        original = group_replies.original(c, cid, case['chat'])
        if quiet.search(original.get('direct', '')):
            return None
    if {destination, context_route} & {'quiet', 'needs_destination'}:
        return None
    if c.execute("""SELECT 1 FROM observations WHERE case_id=? AND source='owner_requested_no_replay'
        UNION ALL SELECT 1 FROM chat_receipts WHERE case_id=? AND state IN ('ready','sending')
          AND (phase IN ('final',?) OR phase GLOB 'append_final:*') LIMIT 1""",
                 (cid, cid, group_replies.PHASE)).fetchone():
        return None
    cross_group = bool({destination, context_route} & {'cross_group_draft', 'cross_group_direct'})
    if verified_owner(root, case) and (not planned or planned['origin_kind'] == 'private'):
        if cross_group or destination in ('original', 'owner_private'):
            return 'private_bot' if case['bot_channel'] == 'private_bot' else 'feiyu'
    if cross_group:
        return None
    if flow.owner_private(c, root, case):
        return 'feiyu'
    if (planned and planned['origin_kind'] == 'group' and destination in ('original', 'native_original')
            and context_route in (None, 'original', 'native_original')
            and flow.group_ack(c, root, case, None)
            and group_replies.bound_role(c, root, cid, case['chat']) == 'owner'
            and not group_replies.previous_manual(root, cid)):
        return 'group'
    return None


def _progress_body(job, elapsed, channel, config):
    state = job['state']
    prefix = f"进展通报〔{job['msgid'][-7:]}〕"
    minutes = int(elapsed // 60)
    if state in pending_expiry.T3_QUEUE_STATES:
        slot = '重' if state.startswith('heavy_') else '轻'
        timeout = '启动超时继续计时' if config.get('queue_expiry_exempt', True) is False else '排队时间不计入启动超时'
        status = f'当前在本地{slot}队列等待空槽，{timeout}'
    else:
        slot = '重' if job['execution_slot'] in ('regular', 'heavy') else '轻'
        status = f'调度仍显示{slot}槽执行中' if state == 'running' else f'正在启动{slot}槽执行线程'
    text = f'{prefix}：已用时约 {minutes} 分钟。{status}'
    if channel != 'group' and job['requested_model']:
        text += f"（{job['requested_model']}）"
    text += '。'
    if channel != 'group':
        event = PROGRESS_EVENTS.get(job['last_event'])
        if event and state == 'running':
            text += f'最近状态：{event}。'
        link = job['t3_link']
        if link and re.fullmatch(r'(?:https://app\.t3\.codes/threads/|t3-thread://)[\w./:%-]+', link):
            text += f'\n线程：{link}'
        elif job['t3_thread_id']:
            text += f"\n线程编号：{job['t3_thread_id']}"
    return text


def _skip_progress(c, cid, phase, now):
    c.execute("""UPDATE chat_receipts SET state='disabled', finished=?, detail='progress_no_longer_eligible'
        WHERE case_id=? AND phase=? AND state IN ('ready','waiting')""", (now, cid, phase))


def _promote_progress(c, root, now, config):
    from t3_dispatch import _parse_iso_epoch

    interval = _progress_interval(config)
    for case in c.execute("""SELECT b.*,r.phase FROM chat_receipts r JOIN cases b ON b.id=r.case_id
        WHERE r.phase GLOB 'progress:*' AND r.state IN ('ready','waiting')""").fetchall():
        if interval <= 0 or not _progress_channel(c, root, case, _progress_job(c, root, case['id'])):
            _skip_progress(c, case['id'], case['phase'], now)
    if interval <= 0 or not enabled(root) or not _progress_jobs_available(c, root):
        return

    marks = ','.join('?' for _ in PROGRESS_STATES)
    cases = c.execute(f"""SELECT b.*,j.created AS queued_at FROM expiry_native.t3_jobs j JOIN cases b ON b.id=j.msgid
        WHERE j.state IN ({marks}) AND b.mode='t3' AND b.category='work'""", PROGRESS_STATES)
    for case in cases:
        created = _parse_iso_epoch(case['queued_at'])
        if created is None or now < created + interval:
            continue
        cid = case['id']
        previous = c.execute("""SELECT phase,state,finished FROM chat_receipts WHERE case_id=?
            AND phase GLOB 'progress:*' ORDER BY CAST(substr(phase, 10) AS INTEGER) DESC LIMIT 1""",
                             (cid,)).fetchone()
        if previous and (previous['state'] in ('ready', 'waiting', 'sending')
                         or now < (previous['finished'] or created) + interval):
            continue
        job = c.execute('SELECT * FROM expiry_native.t3_jobs WHERE msgid=?', (cid,)).fetchone()
        channel = _progress_channel(c, root, case, job)
        if channel:
            # One outstanding receipt per case. Missed intervals never cause a burst.
            slot = int(previous['phase'].split(':')[1]) + 1 if previous else 1
            c.execute("""INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body)
                VALUES(?,?,'ready',?,?)""", (cid, f'progress:{slot}', now, _progress_body(job, now - created, channel, config)))


def promote(root,visible_group=None):
    import clarifications

    with db(root,True) as c:
        waiting=c.execute("""SELECT r.case_id FROM chat_receipts r JOIN chat_receipts a USING(case_id)
            WHERE r.state='waiting' AND r.phase='final' AND a.phase='first'
            AND a.state IN ('accepted','unknown','rejected','unavailable')""").fetchall()
        for row in waiting:
            if clarifications.has_open(c, row[0]):
                continue
            if final_text(c,row[0]):
                c.execute("UPDATE chat_receipts SET state='ready',due=? WHERE case_id=? AND phase='final' AND state='waiting'",(time.time(),row[0]))
        _promote_progress(c, root, time.time(), t3_client.load_config(root))

def destination(call):
    info=call(['identity','whoami'])
    context=info.get('extra_identity_context','')
    fields=context.split('授权真人用户身份：',1)
    match=re.search(r'^ID[：:]\s*([^\s<>]+)\s*$',fields[1],re.M) if len(fields)==2 else None
    if not match or match[1]!=CLI_OWNER:
        raise ValueError('owner_profile_mismatch')
    return CLI_OWNER


def _ensure_group_feedback_table(c):
    c.execute("""CREATE TABLE IF NOT EXISTS group_feedback_text(
        case_id TEXT REFERENCES cases(id), phase TEXT NOT NULL, target_sha256 TEXT NOT NULL,
        source_sha256 TEXT NOT NULL, wire_sha256 TEXT NOT NULL, body TEXT NOT NULL,
        files_ok INTEGER NOT NULL, PRIMARY KEY(case_id,phase))""")


def group_delivery_active(root, cid, phase, target):
    with db(root) as c:
        row = c.execute("""SELECT b.* FROM cases b JOIN chat_receipts r ON r.case_id=b.id
            WHERE b.id=? AND r.phase=? AND r.state='sending' AND b.category NOT IN ('cancelled','expired')
            AND NOT EXISTS (SELECT 1 FROM observations WHERE case_id=b.id AND source='owner_requested_no_replay')""",
                        (cid, phase)).fetchone()
        if not row or not enabled(root):
            return False
        if phase.startswith('group_relay:'):
            did = phase.split(':', 1)[1]
            drow = c.execute('SELECT state,target_chat_id FROM group_drafts WHERE draft_id=?', (did,)).fetchone()
            return bool(drow and drow['state'] == 'sending' and drow['target_chat_id'] == target)
        route = flow.route(c, cid)
        return bool(
            route and route['origin_kind'] == 'group' and route['destination'] == 'native_original'
            and row['chat'] == target
            and group_replies.bound_role(c, root, cid, row['chat']) == 'owner'
        )


def prepared_group_result(root, cid, phase, target, text, call, attempt):
    """Freeze the group text sent after verified outputs/ links become file cards."""
    source = json.loads(flow.receipt_detail({}, text, target))
    with db(root, True) as c:
        _ensure_group_feedback_table(c)
        frozen = c.execute("SELECT * FROM group_feedback_text WHERE case_id=? AND phase=?", (cid, phase)).fetchone()
        previous = c.execute("""SELECT detail FROM feedback_attempts WHERE case_id=? AND phase=?
            AND attempt<? AND state='throttled' ORDER BY attempt DESC LIMIT 1""", (cid, phase, attempt)).fetchone()
    def authorized():
        return group_delivery_active(root, cid, phase, target)
    if not authorized():
        raise ValueError('group_binding_changed')
    if frozen:
        if (frozen['target_sha256'] != source['target_sha256'] or frozen['source_sha256'] != source['body_sha256']
                or sha256(frozen['body'].encode()).hexdigest() != frozen['wire_sha256']):
            raise ValueError('group_retry_binding_changed')
        return frozen['body'], bool(frozen['files_ok'])
    def legacy_media_call(_args):
        raise private_files.FileIssue("旧投递缺少可核对的附件回执")
    files_ok = True
    text = private_files.plain_source_citations(text)
    remaining = private_files.references(text)
    if remaining:
        text, files_ok = private_files.group_result(
            root, cid, target, text, remaining,
            legacy_media_call if previous else call, authorized,
        )
    text = truncate_wecom_markdown(text)
    if not authorized():
        raise ValueError('group_binding_changed')
    digest = sha256(text.encode()).hexdigest()
    if previous:
        old = json.loads(previous['detail'])
        if old.get('body_sha256') != digest or old.get('target_sha256') != source['target_sha256']:
            raise ValueError('group_retry_binding_changed')
    with db(root, True) as c:
        _ensure_group_feedback_table(c)
        c.execute("INSERT INTO group_feedback_text VALUES(?,?,?,?,?,?,?)",
                  (cid, phase, source['target_sha256'], source['body_sha256'], digest, text, int(files_ok)))
    return text, files_ok


def prepared_private_result(root, cid, target, text, refs, call, attempt):
    """Freeze the text actually sent after local links are converted into file cards."""
    source = json.loads(flow.receipt_detail({}, text, target))
    with db(root) as c:
        frozen = c.execute("SELECT * FROM private_feedback_text WHERE case_id=?", (cid,)).fetchone()
        previous = c.execute("""SELECT detail FROM feedback_attempts WHERE case_id=? AND phase='final'
            AND attempt<? AND state='throttled' ORDER BY attempt DESC LIMIT 1""", (cid, attempt)).fetchone()
    def authorized():
        return private_delivery_active(root, cid)
    if not authorized():
        raise ValueError('owner_binding_changed')
    if frozen:
        if (frozen['target_sha256'] != source['target_sha256'] or frozen['source_sha256'] != source['body_sha256']
                or sha256(frozen['body'].encode()).hexdigest() != frozen['wire_sha256']):
            raise ValueError('private_retry_binding_changed')
        return frozen['body'], bool(frozen['files_ok'])
    def legacy_media_call(_args):
        # Legacy accepted cards can be reconstructed from receipts without uploading.
        raise private_files.FileIssue("旧投递缺少可核对的附件回执")
    files_ok = True
    if refs:
        text = private_files.plain_source_citations(text)
        remaining = private_files.references(text)
        if remaining:
            text, files_ok = private_files.private_result(root, cid, target, text, remaining,
                                                          legacy_media_call if previous else call, authorized)
    with db(root) as c:
        footer = _completion_session_footer(c, root, cid)
    text = truncate_wecom_markdown(text, footer=footer)
    if not authorized():
        raise ValueError('owner_binding_changed')
    digest = sha256(text.encode()).hexdigest()
    if previous:
        old = json.loads(previous['detail'])
        if old.get('body_sha256') != digest or old.get('target_sha256') != source['target_sha256']:
            raise ValueError('private_retry_binding_changed')
    with db(root, True) as c:
        c.execute("INSERT INTO private_feedback_text VALUES(?,?,?,?,?,?)",
                  (cid, source['target_sha256'], source['body_sha256'], digest, text, int(files_ok)))
    return text, files_ok


def _format_colleague_push(payload_json, call):
    import group_relay
    try:
        info = json.loads(payload_json)
    except (TypeError, ValueError):
        return payload_json
    code = info.get('code') or ('F-' + str(info.get('followup_id', ''))[-6:].upper())
    gchat = info.get('group_chat') or ''
    gname = group_relay.group_display_name(gchat, call=call) if gchat else '未知群'
    question = (info.get('question') or '').strip()
    quotes = [q for q in (info.get('quotes') or []) if isinstance(q, str) and q.strip()]
    lines = [
        f'【群友提问待处理 · {code}】',
        f'来源群：{gname}',
        f'问题：{question}',
    ]
    if quotes:
        lines.append('引用：' + ' / '.join(quotes[:2]))
    lines.extend([
        '',
        '快捷处理：',
        f'- 0模型直接回复原群：【直回 {code}】你的答复正文（或引用本条写「直回 你的答复正文」）',
        f'- 交给 Agent 查证并准备回原群：把问题发给我（或新Bot），末尾加「发到 {code}」（先看草稿，回「发」再进群）',
        f'- 忽略本条（过时或已群内答复）：【忽略 {code}】（或引用本条回复「忽略」，2 小时未处理将自动忽略）',
    ])
    return '\n'.join(lines)


def _private_bot_send_files(root, cid, target, text, refs, pb_send_file):
    """Send verified outputs/ files over the dedicated API-mode Private Bot WebSocket."""
    from tempfile import TemporaryDirectory
    import os

    text = private_files.plain_source_citations(text)
    remaining = private_files.references(text)
    if not remaining:
        with db(root) as c:
            footer = _completion_session_footer(c, root, cid)
        return truncate_wecom_markdown(text, footer=footer), True
    parts = []
    pos = 0
    delivered = {}
    errors = []
    work = private_files.workspace(root, cid)
    for start, end, dest in remaining:
        parts.append(text[pos:start])
        try:
            path = private_files.source_path(work, dest)
            if path not in delivered:
                orig_size = path.stat().st_size
                orig_digest = private_files.sha(path)
                key = (
                    cid,
                    sha256(str(target).encode()).hexdigest(),
                    os.path.normcase(str(path.relative_to(work))),
                )
                with db(root, True) as c:
                    private_files.install(c)
                    previous = c.execute(
                        "SELECT sha256,state,name FROM private_file_receipts WHERE case_id=? AND audience=? AND path_key=?",
                        key,
                    ).fetchone()
                    if previous:
                        if previous['sha256'] != orig_digest:
                            raise private_files.FileIssue('同一文件在上次投递后发生了变化')
                        if previous['state'] == 'accepted':
                            delivered[path] = (previous['name'] or path.name, None)
                            parts.append('附件《' + delivered[path][0] + '》')
                            pos = end
                            continue
                        raise private_files.FileIssue('上次投递未确认，请核对文件卡片后再补发')
                with TemporaryDirectory(prefix='pb-file-prep-') as prep_dir:
                    send_path, mtype, prep_note = private_files.prepare_deliverable(
                        path, prep_dir, allow_image_as_file=True
                    )
                    with db(root, True) as c:
                        inserted = c.execute(
                            "INSERT OR IGNORE INTO private_file_receipts VALUES(?,?,?,?,?,?,'sending',?)",
                            (*key, send_path.name, orig_digest, orig_size, time.time()),
                        ).rowcount
                        if not inserted:
                            st_row = c.execute(
                                "SELECT sha256,state,name FROM private_file_receipts WHERE case_id=? AND audience=? AND path_key=?",
                                key,
                            ).fetchone()
                            if st_row and st_row['sha256'] == orig_digest and st_row['state'] == 'accepted':
                                delivered[path] = (st_row['name'] or send_path.name, None)
                                parts.append('附件《' + delivered[path][0] + '》')
                                pos = end
                                continue
                            raise private_files.FileIssue('文件投递已登记，结果尚待确认')
                    resp = None
                    final_state = 'unknown'
                    for attempt in range(3):
                        resp = pb_send_file(root, target, send_path, media_type=mtype)
                        if isinstance(resp, dict) and resp.get('success') is True and resp.get('errcode', 0) == 0:
                            final_state = 'accepted'
                            break
                        code = resp.get('errcode') if isinstance(resp, dict) else None
                        stage = resp.get('stage') if isinstance(resp, dict) else None
                        # Only retry when failure definitely happened before sendMediaMessage (upload/read/auth)
                        # or on explicit rate limiting from sendMediaMessage.
                        can_retry = (
                            (stage != 'send' and code in (-3, -2, -1, 429, 45009, 45011))
                            or (stage == 'send' and code in (429, 45009, 45011))
                        )
                        if not can_retry or attempt == 2:
                            if stage != 'send':
                                final_state = 'pre_send_failed'
                            elif code == -3:
                                final_state = 'unknown'
                            else:
                                final_state = 'rejected'
                            break
                        time.sleep(1.5 * (attempt + 1))
                    with db(root, True) as c:
                        if final_state == 'pre_send_failed':
                            c.execute(
                                "DELETE FROM private_file_receipts WHERE case_id=? AND audience=? AND path_key=? AND state='sending'",
                                key,
                            )
                        else:
                            c.execute(
                                "UPDATE private_file_receipts SET state=?,updated=? WHERE case_id=? AND audience=? AND path_key=?",
                                (final_state, time.time(), *key),
                            )
                    if final_state != 'accepted':
                        code = resp.get('errcode') if isinstance(resp, dict) else None
                        errmsg = resp.get('errmsg', '') if isinstance(resp, dict) else ''
                        raise private_files.FileIssue(
                            private_files.format_wecom_media_error(code, errmsg, prefix='新Bot发送附件失败')
                        )
                    delivered[path] = (send_path.name, prep_note)
            label_name, label_note = delivered[path]
            parts.append('附件《' + label_name + '》' + (f'（{label_note}）' if label_note else ''))
        except Exception as exc:
            errors.append(str(exc) if isinstance(exc, private_files.FileIssue) else f'附件发送失败（{type(exc).__name__}）')
            parts.append('（附件未交付）')
        pos = end
    parts.append(text[pos:])
    result = ''.join(parts)
    with db(root) as c:
        footer = _completion_session_footer(c, root, cid)
    result = truncate_wecom_markdown(result, footer=footer)
    if errors:
        notice = f'事项〔{cid[-7:]}〕部分附件交付未完成：' + '；'.join(dict.fromkeys(errors))
        return notice + '\n\n' + result, False
    return result, True


def _format_group_private_clarify(root, cid, phase, group_chat, text, call):
    import clarifications
    import group_relay

    qid = phase.split(':', 1)[1] if ':' in phase else ''
    tid = None
    with db(root) as c:
        if qid and c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='clarifications'").fetchone():
            crow = c.execute('SELECT thread_id FROM clarifications WHERE qid=?', (qid,)).fetchone()
            if crow and crow['thread_id']:
                tid = crow['thread_id']
        if not tid:
            job = _progress_job(c, root, cid)
            if job and job['t3_thread_id']:
                tid = job['t3_thread_id']
    gname = group_relay.group_display_name(group_chat, call=call) if group_chat else '原群'
    return clarifications.render_group_private_notice(text, case_id=cid, group_name=gname, thread_id=tid)


def deliver_one(root, call, visible_group=None, reply_authorized=None, *, first_only=None):
    import group_relay
    import owner_review
    import private_bot_control
    now=time.time()
    with db(root,True) as c:
        group_relay.install(c)
        phases=(flow.ACK,'first','colleague_push',group_replies.FIRST,replies.INTAKE,*flow.REVIEW_NOTICES)
        slots=','.join('?' for _ in phases)
        # Two independent workers keep a slow final from blocking another intake.
        first_filter='' if first_only is None else f" AND r.phase {'IN' if first_only else 'NOT IN'} ({slots})"
        row=c.execute(f"""SELECT c.*,r.phase,r.attempts,r.preflights,r.body FROM chat_receipts r JOIN cases c ON c.id=r.case_id
          WHERE r.state='ready' AND r.due<=? AND NOT (r.phase IN (?,?) AND EXISTS
          (SELECT 1 FROM chat_receipts i WHERE i.case_id=c.id AND i.phase IN (?,?,?) AND i.state='sending'))
          AND NOT EXISTS (SELECT 1 FROM chat_receipts i WHERE i.case_id=c.id
            AND i.phase GLOB 'progress:*' AND i.state='sending')
          AND NOT (r.phase IN (?,?,?) AND EXISTS (SELECT 1 FROM chat_receipts i WHERE
             i.case_id=c.id AND i.phase IN (?,?,?) AND i.state IN ('ready','sending')))
           {first_filter} ORDER BY r.phase NOT IN ({slots}),r.phase GLOB 'progress:*',r.phase='final',r.due LIMIT 1""",
          (now,group_replies.PHASE,replies.RESOLVED,group_replies.FIRST,replies.INTAKE,group_replies.PHASE,
            group_replies.PHASE,replies.RESOLVED,'final',flow.ACK,'first',flow.REVIEW_AUTHORIZED,
            *(phases if first_only is not None else ()),*phases)).fetchone()
        if not row:return False
        cid,phase=row['id'],row['phase']
        bot_channel=(row['bot_channel'] if 'bot_channel' in row.keys() and row['bot_channel'] else 'feiyu')
        desktop = owner_review.adoption(c, cid)
        expired=pending_expiry.expired(c,root,cid)
        if expired and phase!=flow.EXPIRED:return True
        if phase==flow.EXPIRED and not expired:return False
        label=cid[-7:]
        planned=flow.route(c,cid)
        is_progress=phase.startswith('progress:')
        is_colleague_push=(phase=='colleague_push')
        is_group_relay=phase.startswith('group_relay:')
        is_relay_ack=phase.startswith('relay_ack:')
        is_clarify=phase.startswith(('clarify:','clarify_ack:','clarify_remind:','clarify_park:'))
        is_append_final=phase.startswith('append_final:')
        is_group_private_clarify=bool(is_clarify and planned and planned['origin_kind']=='group')
        group=(phase in (group_replies.PHASE,group_replies.FIRST,replies.INTAKE,replies.RESOLVED,flow.ACK,*flow.REVIEW_NOTICES)
               or (is_append_final and bool(planned and planned['origin_kind']=='group' and planned['destination']!='owner_private')))
        if is_group_private_clarify:
            bot_channel='feiyu'
        allow_group_files = bool(
            not is_colleague_push
            and not is_progress
            and group
            and planned
            and planned['destination'] == 'native_original'
            and (phase == group_replies.PHASE or phase.startswith('append_final:'))
            and group_replies.bound_role(c, root, cid, row['chat']) == 'owner'
        )
        private_override=flow.owner_private(c,root,row)
        if phase==flow.EXPIRED:group=bool(planned and planned['origin_kind']=='group' and not private_override)
        relay_draft=None
        if is_progress:
            config = t3_client.load_config(root)
            channel = _progress_channel(c, root, row, _progress_job(c, root, cid)) if _progress_interval(config) > 0 else None
            if not channel:
                _skip_progress(c, cid, phase, now)
                return True
            group = channel == 'group'
            bot_channel = channel
            error = None
            text = row['body']
        elif is_group_relay:
            did=phase.split(':',1)[1]
            drow=c.execute('SELECT * FROM group_drafts WHERE draft_id=?',(did,)).fetchone()
            relay_draft=dict(drow) if drow else None
            group=True
            error=None
            if not enabled(root) or not relay_draft or relay_draft['state']!='approved_direct':
                error='group_relay_not_approved'
            else:
                try:
                    text=group_relay.validate_group_body(relay_draft['body'],relay_draft['target_chat_id'])
                except ValueError:
                    error='group_relay_safety_blocked'
                    text=''
            if error:
                text=''
        elif is_colleague_push:
            group=False
            bot_channel='feiyu'
            text=row['body']
        elif group:
            natural=replies.tracked(c,cid)
            def publication_error(conn):
                if is_append_final:
                    if not enabled(root) or not group_replies.owner_mention(root,group_replies.original(conn,cid,row['chat'])):
                        return 'group_scope_unavailable'
                    if group_replies.previous_manual(root,cid):
                        return 'prior_group_receipt'
                    frozen_row=conn.execute('SELECT frozen FROM cases WHERE id=?',(cid,)).fetchone()
                    manifest=json.loads(frozen_row[0])['manifest'] if (frozen_row and frozen_row[0]) else {}
                    try:
                        if group_replies.public_body(row['body'],manifest,allow_web_links=True)!=row['body']:
                            return 'group_public_answer_required'
                    except ValueError:
                        return 'group_public_answer_required'
                    return None
                if phase==flow.REVIEW_PENDING and not flow.still_pending(conn,cid):
                    return 'review_decided'
                if phase in (flow.ACK,flow.EXPIRED,flow.REVIEW_PENDING):
                    if not enabled(root) or not flow.group_ack(conn,root,row,visible_group):return 'group_scope_unavailable'
                    return 'prior_group_receipt' if group_replies.previous_manual(root,cid) else None
                if phase==flow.REVIEW_AUTHORIZED:
                    return 'owner_review_retired'
                if phase==group_replies.PHASE and planned and planned['destination']==flow.OWNER_REVIEW:
                    return 'owner_review_retired'
                if flow.member_group(root,group_replies.original(conn,cid,row['chat'])):
                    return 'colleague_llm_disabled'
                if planned and planned['destination']=='native_original':
                    if not enabled(root) or group_replies.previous_manual(root,cid):return 'native_publication_unavailable'
                    return None if flow.approved_native(conn,root,row) else 'native_publication_unavailable'
                if not enabled(root) or not visible_group or not visible_group(row['chat']):
                    return 'group_scope_unavailable'
                if group_replies.previous_manual(root,cid):return 'prior_group_receipt'
                if natural:return replies.send_error(conn,root,row,reply_authorized)
                check=group_replies.approved_received if phase==group_replies.FIRST else group_replies.approved_body
                return None if check(conn,root,row) else 'group_approval_unavailable'
            error=publication_error(c)
            text=group_replies.RECEIVED if phase==group_replies.FIRST else row['body']
            if phase==flow.ACK:text=flow.ack_text(c,row)
            elif phase in flow.REVIEW_NOTICES:text=flow.review_notice_text(cid,phase)
        elif row['body']:
            text=row['body']
        elif phase=='first' and planned:
            text=flow.ack_text(c,row)
        elif phase=='first':
            route={'t3':'已派入 T3 调度办理','local':'本机办理'}.get(row['mode'],'已排队')
            text=f'已收到〔{label}〕，{route}。'
            if row['expected'] and not row['frozen']:text+=f" 正在等候本项的 {row['expected']} 个附件。"
        elif phase==flow.EXPIRED:text=''
        else:
            answer=final_text(c,cid)
            if private_override and answer:
                # Only reviewed group-public material can cross from this group.
                manifest=json.loads(row['frozen'] or '{}').get('manifest',{})
                parts=[]
                for lane in c.execute("SELECT target,state,result FROM lanes WHERE case_id=? AND state<>'handed_off'",(cid,)):
                    try:parts.append(group_replies.public_text(lane['result'],manifest,native=lane['target']=='local'))
                    except ValueError:parts.append('本项已有办理状态，可公开的答复仍待核验。')
                answer='\n'.join(parts)
            if not answer:return False
            # Handle cross-group draft / direct publication cleanly (ONLY for truly completed lanes!)
            session_footer = '' if (private_override or desktop) else _completion_session_footer(c, root, cid)
            lane_completed = bool(c.execute(
                "SELECT 1 FROM lanes WHERE case_id=? AND target='local' AND state='completed'",
                (cid,),
            ).fetchone())
            if lane_completed and planned and planned['destination'] in ('cross_group_draft','cross_group_direct') and row['frozen']:
                tgroup=json.loads(row['frozen']).get('manifest',{}).get('target_group')
                if tgroup and tgroup.get('chat_id'):
                    clean_answer=private_files.plain_source_citations(answer).strip()
                    if planned['destination']=='cross_group_direct':
                        try:
                            safe_body=group_relay.validate_group_body(clean_answer,tgroup['chat_id'])
                            draft=group_relay.create_draft(
                                c,case_id=cid,bot_channel=bot_channel,
                                owner_chat=row['chat'],owner_sender=row['sender'],
                                target_chat_id=tgroup['chat_id'],target_chat_name=tgroup.get('chat_name') or '目标群',
                                followup_id=tgroup.get('followup_id'),body=safe_body,state='approved_direct',now=now,
                            )
                            c.execute(
                                "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
                                (cid,f'group_relay:{draft["draft_id"]}',now,safe_body),
                            )
                            text=f'事项〔{label}〕已查清并通过安全校验，正在由大肥鱼发往【{draft["target_chat_name"]}】（编号 {draft["draft_id"]}）。'+session_footer
                        except ValueError as exc:
                            draft=group_relay.create_draft(
                                c,case_id=cid,bot_channel=bot_channel,
                                owner_chat=row['chat'],owner_sender=row['sender'],
                                target_chat_id=tgroup['chat_id'],target_chat_name=tgroup.get('chat_name') or '目标群',
                                followup_id=tgroup.get('followup_id'),body=clean_answer,state='pending_confirm',now=now,
                            )
                            res_note=(tgroup.get('reason') or '') if tgroup.get('resolution')=='model_selected' else ''
                            text=group_relay.format_draft_preview(draft,safety_warning=str(exc),resolution_note=res_note)+session_footer
                    else:
                        draft=group_relay.create_draft(
                            c,case_id=cid,bot_channel=bot_channel,
                            owner_chat=row['chat'],owner_sender=row['sender'],
                            target_chat_id=tgroup['chat_id'],target_chat_name=tgroup.get('chat_name') or '目标群',
                            followup_id=tgroup.get('followup_id'),body=clean_answer,state='pending_confirm',now=now,
                        )
                        res_note=(tgroup.get('reason') or '') if tgroup.get('resolution')=='model_selected' else ''
                        text=group_relay.format_draft_preview(draft,resolution_note=res_note)+session_footer
                else:
                    text=f'办理结果〔{label}〕\n'+answer+session_footer
            else:
                text=f'办理结果〔{label}〕\n'+answer+session_footer
            if desktop:
                text = 'Windows 接手 · 预期回报' + owner_review.group_label(desktop['group_chat']) + '\n' + text
                text = truncate_wecom_markdown(text, notice='\n（节选，完整结果在肥鱼桥事项面板。）')
            if private_override:
                text = truncate_wecom_markdown(text)
        if phase==flow.EXPIRED:text=f'事项〔{label}〕超过十分钟仍未启动，已过期。'
    if group:
        if error:
            with db(root,True) as c:
                updated=c.execute("UPDATE chat_receipts SET state='disabled',finished=?,detail=? WHERE case_id=? AND phase=? AND state='ready'",(now,error,cid,phase)).rowcount
                if updated and error!='review_decided':flow.alert(c,cid,phase,error)
                if is_group_relay and relay_draft:
                    c.execute("UPDATE group_drafts SET state='rejected',updated_at=?,detail=? WHERE draft_id=?",(now,error,relay_draft['draft_id']))
            return True
    elif not (is_colleague_push or is_progress or is_group_private_clarify) and (not enabled(root) or not (private_override or verified_owner(root,row))):
        with db(root,True) as c:c.execute("UPDATE chat_receipts SET state='disabled',finished=? WHERE case_id=? AND state IN ('ready','waiting')",(now,cid))
        return True
    elif (is_colleague_push or is_group_private_clarify) and not enabled(root):
        with db(root,True) as c:c.execute("UPDATE chat_receipts SET state='disabled',finished=? WHERE case_id=? AND phase=? AND state='ready'",(now,cid,phase))
        return True

    use_private_bot=(not group and not is_colleague_push and bot_channel=='private_bot')
    try:
        if is_group_relay:
            target=group_replies.session(call,relay_draft['target_chat_id'])
        elif use_private_bot:
            if not private_bot_control.status(root).get('authenticated'):
                raise ValueError('private_bot_not_authenticated')
            target=row['chat'] or private_bot_control.owner_ws_userid(root)
        else:
            target=destination(call)
            if group:target=group_replies.session(call,row['chat'])
            if is_colleague_push:
                text=_format_colleague_push(text,call)
            elif is_group_private_clarify:
                text=_format_group_private_clarify(root,cid,phase,row['chat'],text,call)
    except Exception as exc:
        attempts=row['preflights']+1
        with db(root,True) as c:
            reason=str(exc) if isinstance(exc,ValueError) and str(exc) in ('owner_profile_mismatch','group_scope_mismatch','original_group_not_verified','private_bot_not_authenticated') else type(exc).__name__
            updated=c.execute("""UPDATE chat_receipts SET preflights=preflights+1,state=?,due=?,detail=? WHERE case_id=? AND phase=? AND state='ready'
                AND attempts=? AND preflights=?""",
                ('unavailable' if attempts>=5 else 'ready',now+min(60,2**attempts),reason,cid,phase,row['attempts'],row['preflights'])).rowcount
            if updated and attempts>=5:
                flow.alert(c,cid,phase,'preflight_unavailable')
                if is_group_relay and relay_draft:
                    c.execute("UPDATE group_drafts SET state='rejected',updated_at=?,detail=? WHERE draft_id=?",(now,reason,relay_draft['draft_id']))
                    c.execute("INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
                              (cid,f'relay_ack:{relay_draft["draft_id"]}',time.time(),f'未能发送到【{relay_draft["target_chat_name"]}】（大肥鱼未验证到该群会话：{reason}）。'))
        return True
    with db(root,True) as c:
        if pending_expiry.expired(c,root,cid) and phase!=flow.EXPIRED:return True
        if is_progress:
            from t3_dispatch import _parse_iso_epoch

            config = t3_client.load_config(root)
            case = c.execute('SELECT * FROM cases WHERE id=?', (cid,)).fetchone()
            job = _progress_job(c, root, cid)
            created = _parse_iso_epoch(job['created']) if job else None
            if (not case or created is None or _progress_interval(config) <= 0
                    or _progress_channel(c, root, case, job) != channel or case['chat'] != row['chat']):
                _skip_progress(c, cid, phase, time.time())
                return True
            if not row['attempts']:
                text = _progress_body(job, max(0, time.time() - created), channel, config)
            error = None
        elif is_group_relay:
            dcur=c.execute('SELECT state FROM group_drafts WHERE draft_id=?',(relay_draft['draft_id'],)).fetchone()
            error=None if (enabled(root) and dcur and dcur['state']=='approved_direct') else 'group_relay_not_approved'
            allow_group_files = True
        elif group:
            error=publication_error(c)
        elif is_colleague_push:
            error=None if enabled(root) else 'bridge_disabled'
        elif is_group_private_clarify:
            orig_item=group_replies.original(c,cid,row['chat'])
            error=None if (enabled(root) and group_replies.owner_mention(root,orig_item)) else 'owner_binding_changed'
        else:
            error=None if (enabled(root) and (verified_owner(root,row) or flow.owner_private(c,root,row))) else 'owner_binding_changed'
        if phase == ANSWER_RECOVERY and recovery_body(c, root, cid) != text:
            error='recovery_binding_changed'
        if error:
            updated=c.execute("UPDATE chat_receipts SET state='disabled',finished=?,detail=? WHERE case_id=? AND phase=? AND state='ready'",(time.time(),error,cid,phase)).rowcount
            if updated and error!='review_decided':flow.alert(c,cid,phase,error)
            return True
        if c.execute("SELECT 1 FROM observations WHERE case_id=? AND source='owner_requested_no_replay'",(cid,)).fetchone():return False
        binding=json.loads(flow.receipt_detail({},text,target))
        previous=c.execute("SELECT target_sha256,body_sha256 FROM feedback_attempts WHERE case_id=? AND phase=? ORDER BY attempt DESC LIMIT 1",(cid,phase)).fetchone()
        if previous and tuple(previous)!=(binding['target_sha256'],binding['body_sha256']):
            c.execute("UPDATE chat_receipts SET state='disabled',detail='retry_binding_changed' WHERE case_id=? AND phase=? AND state='ready'",(cid,phase))
            flow.alert(c,cid,phase,'retry_binding_changed')
            return True
        started=time.time()
        acquired=c.execute("""UPDATE chat_receipts SET state='sending',attempts=attempts+1,started=? WHERE case_id=? AND phase=?
            AND state='ready' AND due<=? AND attempts=? AND preflights=?""",(started,cid,phase,started,row['attempts'],row['preflights'])).rowcount
        if acquired:
            if is_progress and text != row['body']:
                c.execute('UPDATE chat_receipts SET body=? WHERE case_id=? AND phase=?', (text, cid, phase))
            if phase in ('final', group_replies.PHASE) or phase.startswith('append_final:'):
                # A final send closes outstanding progress before any network I/O.
                c.execute("""UPDATE chat_receipts SET state='disabled', finished=?, detail='progress_no_longer_eligible'
                    WHERE case_id=? AND phase GLOB 'progress:*' AND state IN ('ready','waiting')""", (started, cid))
            c.execute("INSERT INTO feedback_attempts VALUES(?,?,?,?,?,?,'sending','',NULL)",(cid,phase,row['attempts']+1,started,binding['target_sha256'],binding['body_sha256']))
            if is_group_relay and relay_draft:
                c.execute("UPDATE group_drafts SET state='sending',updated_at=? WHERE draft_id=? AND state='approved_direct'",(started,relay_draft['draft_id']))
    if not acquired:return True
    outcome,detail='unknown','unconfirmed_send'
    try:
        files_ok = True
        if phase == 'final' and not group and not private_override and not desktop and not (planned and planned['destination'] in ('cross_group_draft','cross_group_direct')):
            refs = private_files.references(text)
            if refs or len(text.encode()) > 20480:
                if use_private_bot:
                    text, files_ok = _private_bot_send_files(root, cid, target, text, refs, private_bot_control.send_file)
                else:
                    text, files_ok = prepared_private_result(root, cid, target, text, refs, call, row['attempts']+1)
                    if not private_delivery_active(root, cid):
                        raise ValueError('owner_binding_changed')
        elif allow_group_files:
            clean_group = private_files.plain_source_citations(text)
            group_refs = private_files.references(clean_group)
            if group_refs or len(clean_group.encode('utf-8')) > 20480:
                text, files_ok = prepared_group_result(root, cid, phase, target, text, call, row['attempts']+1)
                if is_group_relay:
                    text = group_relay.validate_group_body(text, target)
                if not group_delivery_active(root, cid, phase, target):
                    raise ValueError('group_binding_changed')
        if phase == ANSWER_RECOVERY and not private_delivery_active(root, cid, phase):
            raise ValueError('owner_binding_changed')
        if use_private_bot:
            response=private_bot_control.send_markdown(root,target,text)
        else:
            response=call(['message','aibot','send','--json',json.dumps({'chat_id':target,'msg_type':'markdown','markdown':{'content':text}},ensure_ascii=False)])
        if response.get('success') is True and response.get('errcode',0)==0:
            outcome='accepted' if files_ok else 'file_incomplete'
        elif response.get('success') is False:
            outcome='rejected'
            if response.get('errcode') in (429,45009,45011) and row['attempts']+1<4:outcome='ready'
        detail=flow.receipt_detail(response,text,target)
    except Exception as exc:
        if isinstance(exc,ValueError) and str(exc) in ('owner_binding_changed','private_retry_binding_changed','group_binding_changed','group_retry_binding_changed'):
            detail=str(exc)
        else:
            detail=type(exc).__name__
    with db(root,True) as c:
        c.execute("UPDATE feedback_attempts SET state=?,detail=?,finished=? WHERE case_id=? AND phase=? AND attempt=? AND state='sending'",
                  ('throttled' if outcome=='ready' else outcome,detail,time.time(),cid,phase,row['attempts']+1))
        if outcome=='ready' and phase in flow.REVIEW_NOTICES and publication_error(c):
            outcome='disabled'
        c.execute("UPDATE chat_receipts SET state=?,detail=?,finished=?,due=? WHERE case_id=? AND phase=? AND state='sending'",
                  (outcome,detail,None if outcome=='ready' else time.time(),time.time()+min(60,2**min(6,row['attempts']+1)),cid,phase))
        if outcome in ('unknown','rejected','file_incomplete'):flow.alert(c,cid,phase,'send_'+outcome)
        if outcome=='accepted':
            c.execute('UPDATE feedback_alerts SET resolved=? WHERE case_id=? AND phase=? AND resolved IS NULL',(time.time(),cid,phase))
            if phase == ANSWER_RECOVERY:
                c.execute("UPDATE feedback_alerts SET resolved=? WHERE case_id=? AND phase='final' AND reason='send_file_incomplete' AND resolved IS NULL",
                          (time.time(), cid))
            if is_group_relay and relay_draft:
                now_done=time.time()
                c.execute("UPDATE group_drafts SET state='sent',sent_at=?,updated_at=?,detail='sent' WHERE draft_id=?",(now_done,now_done,relay_draft['draft_id']))
                if relay_draft.get('followup_id'):
                    import conclusion_notes
                    conclusion_notes.resolve_followup_case(c, relay_draft['followup_id'], now=now_done)
                c.execute(
                    "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
                    (cid,f'relay_ack:{relay_draft["draft_id"]}',now_done,f'已由大肥鱼发送到【{relay_draft["target_chat_name"]}】（编号 {relay_draft["draft_id"]}）。'),
                )
        elif is_group_relay and relay_draft and outcome in ('rejected','unknown','disabled','file_incomplete'):
            now_fail=time.time()
            c.execute("UPDATE group_drafts SET state=?,updated_at=?,detail=? WHERE draft_id=?",(outcome,now_fail,detail[:200],relay_draft['draft_id']))
            c.execute(
                "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
                (cid,f'relay_ack:{relay_draft["draft_id"]}',now_fail,f'向【{relay_draft["target_chat_name"]}】发送状态为 {outcome}（编号 {relay_draft["draft_id"]}），请核对群内是否已收到，切勿盲目重发。'),
            )
    if outcome=='file_incomplete' and phase=='final':
        with db(root, True) as c:
            queue_answer_recovery(c, root, cid)
    return True

def serve(stop,root,visible_group=None,reply_authorized=None,call=None):
    from wecom_cli import cli_call
    with db(root,True) as c:
        # A crash after network transmission cannot safely be replayed.
        c.execute("UPDATE feedback_attempts SET state='unknown',detail='interrupted_send',finished=? WHERE state='sending'",(time.time(),))
        for row in c.execute("SELECT case_id,phase FROM chat_receipts WHERE state='sending'").fetchall():
            flow.alert(c,*row,'interrupted_send')
        c.execute("UPDATE chat_receipts SET state='unknown',detail='interrupted_send',finished=? WHERE state='sending'",(time.time(),))
    call=call or cli_call
    def intakes():
        while not stop.is_set():
            try:
                if deliver_one(root,call,visible_group,reply_authorized,first_only=True):continue
            except Exception:logging.exception('First receipt worker')
            stop.wait(.25)
    intake=threading.Thread(target=intakes,name='request-first-receipts',daemon=True)
    intake.start()
    while not stop.is_set():
        changed.clear()
        try:
            promote(root,visible_group)
            if deliver_one(root,call,visible_group,reply_authorized,first_only=False):continue
            with db(root) as c:
                due=c.execute("SELECT MIN(due) FROM chat_receipts WHERE state='ready'").fetchone()[0]
            wait=30 if due is None else max(.1,min(30,due-time.time()))
        except Exception:
            logging.exception('Owner feedback ledger unavailable')
            wait=2
        changed.wait(wait)
    intake.join(timeout=90)
