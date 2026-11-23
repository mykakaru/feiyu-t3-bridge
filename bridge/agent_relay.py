"""Agent-to-Owner WeCom ask registry and zero-LLM reply relay back to originating T3 threads.

Enables any local T3 Agent (including threads outside the Feiyu project) to message the owner
via Claude (`private_bot`) with an embedded Agent identity footer, and routes the owner's
quoted or session-code replies directly back into the originating T3 thread via `t3 thread send`.
If the originating thread is archived or no longer exists, cleanly falls back to the normal
Feiyu new-task pipeline and states the reason in the owner receipt.
"""
from __future__ import annotations

from hashlib import sha256
import json
import logging
import re
import time

from identity import resolve
import t3_client

THREAD_ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9:._-]{3,127}$')
SESSION_CODE_RE = re.compile(r'(?<![A-Za-z0-9_-])(A-[0-9a-f]{4,8})(?![A-Za-z0-9_-])', re.I)

AGENT_FOOTER_SESSION_RE = re.compile(r'来自\s*Agent〔(A-[0-9a-f]{4,8})〕', re.I)
AGENT_FOOTER_TITLE_RE = re.compile(
    r'来自\s*Agent〔A-[0-9a-f]{4,8}〕\s*[·•]\s*([^|\r\n]{1,60}?)\s*(?:\||threadId|$)',
    re.I,
)
AGENT_FOOTER_THREAD_RE = re.compile(
    r'\bthreadId\s*[：:]\s*([A-Za-z0-9][A-Za-z0-9:._-]{3,127})',
    re.I,
)
AGENT_FOOTER_LINE_RE = re.compile(
    r'^\s*(?:——|--|—)\s*来自\s*Agent〔A-[0-9a-f]{4,8}〕.*$',
    re.I | re.M,
)

# Never hijack Feiyu task receipts, clarification cards, group drafts, or colleague pushes.
FEIYU_CARD_QUOTE_RE = re.compile(
    r'^(?:Windows 接手 · [^\n]*\n)?'
    r'(?:(?:(?:追加|补发)?办理结果|已收到(?:确认)?|已收齐|正在收集|需要确认|追问|提醒[：:]\s*事项|事项|已授权|进展通报)〔[0-9a-f]{7}〕'
    r'|【(?:待确认群答复|群友提问待处理)\s*·)',
    re.I,
)

# Reserved bridge commands that must never be swallowed by agent relay.
RESERVED_BRIDGE_CMD_RE = re.compile(
    r'^(?:[@＠]大肥鱼[\s\u2005\u200b]*)?'
    r'【(?:开始收集|收集|合并|合并处理|合批|开始合并|批量开始|合并开始|收尾|结束|取消收集|要求|'
    r'追加(?:任务)?|取消(?:任务)?|确认追加|确认取消|模型|回报|'
    r'发布说明|补充说明|更新说明|删除说明|说明|进度|'
    r'直回|直发群|发群|改发|修改并发送|不发|忽略|跳过|无需处理|不用管|作废|'
    r'答复\s+Q-[0-9a-f]{4,8}|回复\s+Q-[0-9a-f]{4,8}|确认回复)',
    re.I,
)

EXPLICIT_AGENT_CMD_RE = re.compile(
    r'^【(?:回复|答复|转交|传话|发给)(?:[\s：:]+(?:(A-[0-9a-f]{4,8})|(?:threadId[\s：:]*)?([A-Za-z0-9][A-Za-z0-9:._-]{7,127})))?】'
    r'[\s：:]*(?:(A-[0-9a-f]{4,8})|(?:threadId[\s：:]*)?(mcp:[0-9a-f-]{8,64}))?[\s：:]*([\s\S]*)$',
    re.I,
)
LEADING_SESSION_CODE_RE = re.compile(
    r'^(?:(?:回复|答复|转交|传话|发给|给)\s*)?(?:〔)?(A-[0-9a-f]{4,8})(?:〕)?(?:[\s：:,，]+([\s\S]*))?$',
    re.I,
)
LEADING_THREAD_ID_RE = re.compile(
    r'^(?:(?:回复|答复|转交|传话|发给)\s*)?threadId[\s：:]+([A-Za-z0-9][A-Za-z0-9:._-]{3,127})(?:[\s：:,，]+([\s\S]*))?$',
    re.I,
)

WECOM_MARKDOWN_LIMIT = 20480
MAX_FORWARD_RETRIES = 3
ATTACHMENT_SETTLE_GRACE_SECONDS = 30.0
STALE_SENDING_SECONDS = 120.0


def install(c):
    c.execute("""CREATE TABLE IF NOT EXISTS agent_outbound_asks(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        req_id TEXT UNIQUE,
        ws_req_id TEXT,
        msgid TEXT,
        session_code TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        title TEXT NOT NULL DEFAULT '',
        chatid TEXT NOT NULL,
        bot_channel TEXT NOT NULL DEFAULT 'private_bot',
        body TEXT NOT NULL,
        body_sha256 TEXT NOT NULL,
        created_at REAL NOT NULL
    )""")
    c.execute('CREATE INDEX IF NOT EXISTS agent_asks_session ON agent_outbound_asks(session_code, created_at DESC)')
    c.execute('CREATE INDEX IF NOT EXISTS agent_asks_thread ON agent_outbound_asks(thread_id, created_at DESC)')
    c.execute('CREATE INDEX IF NOT EXISTS agent_asks_chat ON agent_outbound_asks(chatid, created_at DESC)')

    c.execute("""CREATE TABLE IF NOT EXISTS agent_relay_forwards(
        mid TEXT PRIMARY KEY,
        case_id TEXT NOT NULL,
        session_code TEXT NOT NULL DEFAULT '',
        thread_id TEXT NOT NULL,
        title TEXT NOT NULL DEFAULT '',
        ask_id INTEGER,
        match_kind TEXT NOT NULL,
        reply_text TEXT NOT NULL,
        quoted_summary TEXT NOT NULL DEFAULT '',
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        due REAL NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        sent_at REAL,
        detail TEXT NOT NULL DEFAULT ''
    )""")
    c.execute('CREATE INDEX IF NOT EXISTS agent_forwards_state ON agent_relay_forwards(state, due)')


def valid_thread_id(thread_id):
    return isinstance(thread_id, str) and bool(THREAD_ID_RE.fullmatch(thread_id.strip()))


def clean_title(title):
    if not isinstance(title, str):
        return ''
    cleaned = ' '.join(title.replace('|', ' ').split()).strip()
    return cleaned[:40]


def derive_session_code(thread_id, *, salt=0):
    raw = str(thread_id).strip() if salt == 0 else f'{str(thread_id).strip()}:{salt}'
    return 'A-' + sha256(raw.encode('utf-8')).hexdigest()[:4].upper()


def allocate_session_code(c, thread_id):
    """Reuse an existing session code for `thread_id`, or allocate a collision-free A-XXXX."""
    install(c)
    tid = str(thread_id).strip()
    existing = c.execute(
        'SELECT session_code FROM agent_outbound_asks WHERE thread_id=? ORDER BY id DESC LIMIT 1',
        (tid,),
    ).fetchone()
    if existing and existing['session_code']:
        return existing['session_code'].upper()
    for salt in range(64):
        code = derive_session_code(tid, salt=salt)
        conflict = c.execute(
            'SELECT thread_id FROM agent_outbound_asks WHERE upper(session_code)=? AND thread_id<>? LIMIT 1',
            (code, tid),
        ).fetchone()
        if not conflict:
            return code
    return 'A-' + sha256(f'{tid}:{time.time_ns()}'.encode('utf-8')).hexdigest()[:6].upper()


def format_footer(session_code, thread_id, title=''):
    code = str(session_code).strip().upper()
    tid = str(thread_id).strip()
    t = clean_title(title)
    if t:
        return f'—— 来自 Agent〔{code}〕· {t} | threadId: {tid}'
    return f'—— 来自 Agent〔{code}〕 threadId: {tid}'


def decorate_message(text, session_code, thread_id, *, title='', limit=WECOM_MARKDOWN_LIMIT):
    """Append the canonical human-friendly Agent identity footer while respecting WeCom byte limit."""
    body = str(text or '').rstrip()
    footer_line = format_footer(session_code, thread_id, title=title)
    last_line = ''
    for line in reversed(body.splitlines()):
        if line.strip():
            last_line = line.strip()
            break
    if session_code.upper() in last_line.upper() and thread_id in last_line and '来自 Agent' in last_line:
        full = body
        footer_block = '\n' + last_line
    else:
        footer_block = '\n\n' + footer_line
        full = body + footer_block

    if len(full.encode('utf-8')) <= limit:
        return full, footer_line

    notice = '\n（消息过长已截断）'
    tail = notice + footer_block
    budget = max(0, limit - len(tail.encode('utf-8')) - 64)
    base = full[:-len(footer_block)] if full.endswith(footer_block) else body
    trimmed = base.encode('utf-8')[:budget].decode('utf-8', errors='ignore').rstrip()
    return trimmed + tail, footer_line


def register_ask(
    root,
    *,
    thread_id,
    body,
    chatid,
    session_code=None,
    title='',
    req_id=None,
    ws_req_id=None,
    msgid=None,
    bot_channel='private_bot',
    now=None,
):
    from business_queue import db

    ts = time.time() if now is None else float(now)
    tid = str(thread_id).strip()
    t = clean_title(title)
    text = str(body or '')
    digest = sha256(text.encode('utf-8')).hexdigest()
    with db(root, True) as c:
        install(c)
        code = (session_code or allocate_session_code(c, tid)).strip().upper()
        cur = c.execute(
            """INSERT OR REPLACE INTO agent_outbound_asks(
                   req_id, ws_req_id, msgid, session_code, thread_id, title,
                   chatid, bot_channel, body, body_sha256, created_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                req_id or None,
                ws_req_id or None,
                msgid or None,
                code,
                tid,
                t,
                str(chatid),
                bot_channel or 'private_bot',
                text,
                digest,
                ts,
            ),
        )
        return {
            'id': cur.lastrowid,
            'session_code': code,
            'thread_id': tid,
            'title': t,
            'req_id': req_id,
        }


def is_agent_ask_quote(quote):
    """Return True if a quoted message carries an Agent-ask identity footer (never Feiyu receipts)."""
    if not isinstance(quote, str):
        return False
    q = quote.strip()
    if not q or FEIYU_CARD_QUOTE_RE.match(q):
        return False
    return bool(AGENT_FOOTER_SESSION_RE.search(q) or AGENT_FOOTER_THREAD_RE.search(q))


def has_explicit_agent_target(text):
    """Return True if owner direct text explicitly addresses an Agent relay target."""
    if not isinstance(text, str):
        return False
    s = text.strip()
    if not s or RESERVED_BRIDGE_CMD_RE.match(s):
        return False
    m_cmd = EXPLICIT_AGENT_CMD_RE.fullmatch(s)
    if m_cmd and (m_cmd.group(1) or m_cmd.group(2) or m_cmd.group(3) or m_cmd.group(4)):
        return True
    if LEADING_SESSION_CODE_RE.fullmatch(s) or LEADING_THREAD_ID_RE.fullmatch(s):
        return True
    return False


def _normalize_for_match(text):
    s = str(text or '')
    s = re.sub(r'[*_`~>#]+', '', s)
    s = re.sub(r'(?m)^\s*[-·•]\s+', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _strip_footer(text):
    s = AGENT_FOOTER_LINE_RE.sub('', str(text or '')).strip()
    return s


def _summarize_quote(quote, max_chars=600):
    clean = _strip_footer(quote)
    if len(clean) <= max_chars:
        return clean
    return clean[:max_chars].rstrip() + '…'


def _lookup_ask_by_thread(c, thread_id, chatid):
    return c.execute(
        """SELECT * FROM agent_outbound_asks
           WHERE thread_id=? AND chatid=?
           ORDER BY created_at DESC, id DESC LIMIT 1""",
        (thread_id, chatid),
    ).fetchone() or c.execute(
        """SELECT * FROM agent_outbound_asks
           WHERE thread_id=?
           ORDER BY created_at DESC, id DESC LIMIT 1""",
        (thread_id,),
    ).fetchone()


def _lookup_ask_by_session(c, session_code, chatid):
    code = str(session_code).strip().upper()
    return c.execute(
        """SELECT * FROM agent_outbound_asks
           WHERE upper(session_code)=? AND chatid=?
           ORDER BY created_at DESC, id DESC LIMIT 1""",
        (code, chatid),
    ).fetchone() or c.execute(
        """SELECT * FROM agent_outbound_asks
           WHERE upper(session_code)=?
           ORDER BY created_at DESC, id DESC LIMIT 1""",
        (code,),
    ).fetchone()


def _match_quote_in_db(c, quote, chatid):
    q = quote.strip()
    if len(q) < 20:
        return None
    q_hash = sha256(q.encode('utf-8')).hexdigest()
    exact = c.execute(
        'SELECT * FROM agent_outbound_asks WHERE chatid=? AND body_sha256=? ORDER BY created_at DESC LIMIT 1',
        (chatid, q_hash),
    ).fetchone()
    if exact:
        return exact
    q_norm = _normalize_for_match(q)
    if len(q_norm) < 20:
        return None
    rows = c.execute(
        'SELECT * FROM agent_outbound_asks WHERE chatid=? ORDER BY created_at DESC, id DESC LIMIT 50',
        (chatid,),
    ).fetchall()
    prefix = q_norm[:120]
    matched = []
    seen_threads = set()
    for r in rows:
        b_norm = _normalize_for_match(r['body'])
        if b_norm == q_norm or (len(prefix) >= 40 and b_norm.startswith(prefix)):
            if r['thread_id'] not in seen_threads:
                seen_threads.add(r['thread_id'])
                matched.append(r)
    return matched[0] if len(matched) == 1 else None


def _owner_private_ok(root, item):
    return (
        item.get('chattype') == 'single'
        and item.get('sender') == item.get('chatid')
        and bool(resolve(root, item.get('sender'))['is_owner'])
    )


def match_reply(c, root, item):
    """Resolve an owner private reply to an Agent-ask target, or return None.

    Returns a dict:
      - {'error': 'empty_reply' | 'unknown_session', 'session_code': ...}
      - {'thread_id', 'session_code', 'title', 'ask_id', 'match_kind', 'reply_text', 'quoted_summary'}
      - or None if this message is not an Agent relay reply.
    """
    if not _owner_private_ok(root, item):
        return None
    if item.get('msgtype') not in ('text', 'voice', 'mixed'):
        return None
    text = (item.get('direct') or '').strip()
    if not text or RESERVED_BRIDGE_CMD_RE.match(text):
        return None

    install(c)
    chatid = item['chatid']
    quotes = [q.strip() for q in (item.get('quotes') or []) if isinstance(q, str) and q.strip()]

    # 1. Quoted Agent-ask message (highest priority, most natural UX)
    for q in quotes:
        if FEIYU_CARD_QUOTE_RE.match(q):
            continue
        m_tid = AGENT_FOOTER_THREAD_RE.search(q)
        m_code = AGENT_FOOTER_SESSION_RE.search(q)
        m_title = AGENT_FOOTER_TITLE_RE.search(q)
        if m_tid or m_code:
            tid = m_tid.group(1).strip() if m_tid else ''
            code = m_code.group(1).upper() if m_code else ''
            title = clean_title(m_title.group(1)) if m_title else ''
            ask_row = None
            if tid:
                ask_row = _lookup_ask_by_thread(c, tid, chatid)
            elif code:
                ask_row = _lookup_ask_by_session(c, code, chatid)
            if not tid and ask_row:
                tid = ask_row['thread_id']
            if not code:
                code = (ask_row['session_code'].upper() if ask_row else derive_session_code(tid)) if tid else ''
            if not title and ask_row:
                title = ask_row['title'] or ''
            if tid and valid_thread_id(tid):
                return {
                    'thread_id': tid,
                    'session_code': code or derive_session_code(tid),
                    'title': title,
                    'ask_id': ask_row['id'] if ask_row else None,
                    'match_kind': 'quote_footer',
                    'reply_text': text,
                    'quoted_summary': _summarize_quote(q),
                }
            if code:
                return {'error': 'unknown_session', 'session_code': code}
        else:
            db_hit = _match_quote_in_db(c, q, chatid)
            if db_hit:
                return {
                    'thread_id': db_hit['thread_id'],
                    'session_code': db_hit['session_code'].upper(),
                    'title': db_hit['title'] or '',
                    'ask_id': db_hit['id'],
                    'match_kind': 'quote_registered_body',
                    'reply_text': text,
                    'quoted_summary': _summarize_quote(q),
                }

    # If quoting a Feiyu card (e.g. 办理结果〔...〕), never hijack unless owner explicitly used 【回复/转交/传话 A-xxxx】.
    quoting_feiyu_card = any(FEIYU_CARD_QUOTE_RE.match(q) for q in quotes)

    # 2. Explicit bracketed command: 【回复/转交/传话 A-XXXX】内容 or 【转交 mcp:...】内容
    m_cmd = EXPLICIT_AGENT_CMD_RE.fullmatch(text)
    if m_cmd:
        code_arg = m_cmd.group(1) or m_cmd.group(3)
        tid_arg = m_cmd.group(2) or m_cmd.group(4)
        body = (m_cmd.group(5) or '').strip()
        if code_arg:
            code = code_arg.upper()
            ask_row = _lookup_ask_by_session(c, code, chatid)
            if not ask_row:
                return {'error': 'unknown_session', 'session_code': code}
            if not body:
                return {'error': 'empty_reply', 'session_code': code}
            return {
                'thread_id': ask_row['thread_id'],
                'session_code': code,
                'title': ask_row['title'] or '',
                'ask_id': ask_row['id'],
                'match_kind': 'explicit_cmd_session',
                'reply_text': body,
                'quoted_summary': _summarize_quote(ask_row['body'], max_chars=360),
            }
        if tid_arg and valid_thread_id(tid_arg):
            tid = tid_arg.strip()
            ask_row = _lookup_ask_by_thread(c, tid, chatid)
            code = ask_row['session_code'].upper() if ask_row else derive_session_code(tid)
            if not body:
                return {'error': 'empty_reply', 'session_code': code}
            return {
                'thread_id': tid,
                'session_code': code,
                'title': ask_row['title'] if ask_row else '',
                'ask_id': ask_row['id'] if ask_row else None,
                'match_kind': 'explicit_cmd_thread',
                'reply_text': body,
                'quoted_summary': _summarize_quote(ask_row['body'], max_chars=360) if ask_row else '',
            }

    if quoting_feiyu_card:
        return None

    # 3. Leading session code: "A-EBB0 同意按A做" / "回复 A-EBB0：同意"
    m_lead_code = LEADING_SESSION_CODE_RE.fullmatch(text)
    if m_lead_code:
        code = m_lead_code.group(1).upper()
        body = (m_lead_code.group(2) or '').strip()
        ask_row = _lookup_ask_by_session(c, code, chatid)
        if ask_row:
            if not body:
                return {'error': 'empty_reply', 'session_code': code}
            return {
                'thread_id': ask_row['thread_id'],
                'session_code': code,
                'title': ask_row['title'] or '',
                'ask_id': ask_row['id'],
                'match_kind': 'leading_session_code',
                'reply_text': body,
                'quoted_summary': _summarize_quote(ask_row['body'], max_chars=360),
            }

    # 4. Leading threadId: "threadId: mcp:xxxx 同意按A做"
    m_lead_tid = LEADING_THREAD_ID_RE.fullmatch(text)
    if m_lead_tid:
        tid = m_lead_tid.group(1).strip()
        body = (m_lead_tid.group(2) or '').strip()
        if valid_thread_id(tid):
            ask_row = _lookup_ask_by_thread(c, tid, chatid)
            code = ask_row['session_code'].upper() if ask_row else derive_session_code(tid)
            if not body:
                return {'error': 'empty_reply', 'session_code': code}
            return {
                'thread_id': tid,
                'session_code': code,
                'title': ask_row['title'] if ask_row else '',
                'ask_id': ask_row['id'] if ask_row else None,
                'match_kind': 'leading_thread_id',
                'reply_text': body,
                'quoted_summary': _summarize_quote(ask_row['body'], max_chars=360) if ask_row else '',
            }

    # 5. Inline registered session code (e.g. "关于 A-EBB0 的问题，选A") — strictly requires DB hit!
    codes = list(dict.fromkeys(m.upper() for m in SESSION_CODE_RE.findall(text)))
    if len(codes) == 1:
        ask_row = _lookup_ask_by_session(c, codes[0], chatid)
        if ask_row:
            return {
                'thread_id': ask_row['thread_id'],
                'session_code': codes[0],
                'title': ask_row['title'] or '',
                'ask_id': ask_row['id'],
                'match_kind': 'inline_registered_session',
                'reply_text': text,
                'quoted_summary': _summarize_quote(ask_row['body'], max_chars=360),
            }

    return None


def _queue_control_receipt(c, cid, text, epoch):
    c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)', (cid, 'control', 'private'))
    c.execute(
        "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
        (cid, 'first', epoch, text),
    )


def intake_reply(c, root, item, epoch):
    """Consume an owner private reply meant for an existing T3 Agent and stage it for forwarding."""
    from business_queue import make_case

    matched = match_reply(c, root, item)
    if not matched:
        return False

    bot_channel = item.get('bot_channel') or 'feiyu'
    if 'error' in matched:
        code = matched.get('session_code') or ''
        if matched['error'] == 'empty_reply':
            msg = f'请在编号后填写要转交原 Agent〔{code}〕的回复内容。'
        else:
            msg = f'未找到 Agent 会话码 {code}，请核对编号或直接引用原 Agent 消息回复。'
        cid = make_case(c, item, 'control', category='control', note=msg, bot_channel=bot_channel)
        _queue_control_receipt(c, cid, msg, epoch)
        return True

    # Idempotency check
    if c.execute('SELECT 1 FROM agent_relay_forwards WHERE mid=?', (item['msgid'],)).fetchone():
        return True

    cid = make_case(
        c, item, 'agent_relay', category='agent_relay',
        note=f'正在转交原 Agent〔{matched["session_code"]}〕（{matched["thread_id"]}）',
        bot_channel=bot_channel,
    )
    c.execute(
        """INSERT INTO agent_relay_forwards(
               mid, case_id, session_code, thread_id, title, ask_id, match_kind,
               reply_text, quoted_summary, state, attempts, due, created_at, updated_at
           ) VALUES(?,?,?,?,?,?,?,?,?,'pending',0,?,?,?)""",
        (
            item['msgid'],
            cid,
            matched['session_code'],
            matched['thread_id'],
            matched.get('title') or '',
            matched.get('ask_id'),
            matched['match_kind'],
            matched['reply_text'],
            matched.get('quoted_summary') or '',
            epoch,
            epoch,
            epoch,
        ),
    )
    return True


def format_forwarded_message(row, source_item=None):
    code = row['session_code'] or derive_session_code(row['thread_id'])
    received_at = (source_item or {}).get('received_at') or ''
    bot_channel = (source_item or {}).get('bot_channel') or 'private_bot'
    ch_label = '克劳德 / private_bot' if bot_channel == 'private_bot' else '大肥鱼 / feiyu'
    header_parts = [f'会话码：{code}']
    if received_at:
        header_parts.append(f'时间：{received_at}')
    header_parts.append(f'渠道：{ch_label}')
    lines = [
        '[来自主人企微私聊回复 · 经克劳德自动转交]',
        '（' + ' · '.join(header_parts) + '）',
        '',
        '【主人回复原文】',
        (row['reply_text'] or '').strip(),
    ]
    quoted = (row['quoted_summary'] or '').strip()
    if quoted:
        lines.extend(['', '【被引用的原问题摘要】', quoted])
    attachments = [
        a for a in ((source_item or {}).get('attachments') or [])
        if isinstance(a, dict) and not a.get('quoted')
    ]
    if attachments:
        lines.extend(['', '【主人随附附件】'])
        for a in attachments:
            name = a.get('display_name') or a.get('type') or '附件'
            lp = a.get('local_path')
            if lp:
                lines.append(f'- {name}: {lp}')
            else:
                lines.append(f'- {name}（状态：{a.get("state") or "unknown"}）')
    return '\n'.join(lines)


def _fallback_to_new_task(root, fwd, reason_text, now):
    """Convert an undeliverable relay case into a standard Feiyu new T3 task with an explanatory note."""
    from business_queue import db, expected, incoming_private_reference, quote_links
    from chat_feedback import enroll

    cid = fwd['case_id']
    code = fwd['session_code'] or derive_session_code(fwd['thread_id'])
    note = f'原 Agent〔{code}〕{reason_text}，已自动转为新任务办理。'
    with db(root, True) as c:
        install(c)
        src = c.execute('SELECT item, epoch FROM sources WHERE mid=?', (fwd['mid'],)).fetchone()
        if not src:
            c.execute(
                "UPDATE agent_relay_forwards SET state='failed', updated_at=?, detail=? WHERE mid=?",
                (now, reason_text, fwd['mid']),
            )
            return False
        item = json.loads(src['item'])
        text = item.get('direct') or ''
        refs = quote_links(c, item)
        count = expected(text)
        if re.search(r'(?:生成|新建|制作|导出|创建)(?:.{0,8})(?:文件|文档|附件)', text) and not re.search(
            r'我.{0,6}(?:会发|发了|已发|会传|上传给你)|(?:用|根据|读取|处理)(?:.{0,6})(?:附件|文件)', text
        ):
            count = 0
        elif not count and not item.get('attachments') and not refs and incoming_private_reference(text):
            count = 1
        c.execute(
            "UPDATE cases SET mode='t3', category='work', expected=?, note=?, related=?, updated=? WHERE id=?",
            (count, note, json.dumps(refs, ensure_ascii=False, sort_keys=True), now, cid),
        )
        c.execute(
            "UPDATE agent_relay_forwards SET state='fallback_new_task', updated_at=?, detail=? WHERE mid=?",
            (now, reason_text, fwd['mid']),
        )
        enroll(c, root, cid, item, src['epoch'])
    return True


def _format_forwarded_receipt(fwd):
    code = fwd['session_code'] or derive_session_code(fwd['thread_id'])
    title = clean_title(fwd['title'] or '')
    if title:
        return f'已转交原 Agent〔{code}〕（{title}）。'
    return f'已转交原 Agent〔{code}〕。'


def next_due(c, now=None):
    if not c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_relay_forwards'").fetchone():
        return None
    row = c.execute(
        "SELECT MIN(due) FROM agent_relay_forwards WHERE state='pending'"
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def process_pending(root, *, now=None):
    """Process pending Agent-relay forwards outside DB write locks.

    Returns {'forwarded': int, 'fallback': int, 'deferred': int}.
    """
    from business_queue import db
    import chat_feedback

    now_ts = time.time() if now is None else float(now)
    summary = {'forwarded': 0, 'fallback': 0, 'deferred': 0}

    with db(root) as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_relay_forwards'").fetchone():
            return summary
        # Recover stale 'checking' rows (read-only phase) if a previous daemon exited mid-check.
        stale_checking = c.execute(
            "SELECT mid FROM agent_relay_forwards WHERE state='checking' AND updated_at<=?",
            (now_ts - STALE_SENDING_SECONDS,),
        ).fetchall()
    if stale_checking:
        with db(root, True) as c:
            c.execute(
                "UPDATE agent_relay_forwards SET state='pending', due=?, updated_at=? WHERE state='checking' AND updated_at<=?",
                (now_ts, now_ts, now_ts - STALE_SENDING_SECONDS),
            )

    with db(root) as c:
        pending = c.execute(
            "SELECT * FROM agent_relay_forwards WHERE state='pending' AND due<=? ORDER BY created_at, mid LIMIT 16",
            (now_ts,),
        ).fetchall()
    if not pending:
        return summary

    for fwd in pending:
        mid = fwd['mid']
        cid = fwd['case_id']
        tid = fwd['thread_id']
        code = fwd['session_code'] or derive_session_code(tid)

        # Wait briefly if mixed message attachments are still downloading
        with db(root) as c:
            src_row = c.execute('SELECT item FROM sources WHERE mid=?', (mid,)).fetchone()
        source_item = json.loads(src_row['item']) if src_row else {}
        settling = any(
            a.get('state') in ('processing', 'pending', 'downloading', 'queued')
            for a in (source_item.get('attachments') or [])
            if isinstance(a, dict) and not a.get('quoted')
        )
        if settling and (now_ts - float(fwd['created_at'])) < ATTACHMENT_SETTLE_GRACE_SECONDS:
            with db(root, True) as c:
                c.execute(
                    "UPDATE agent_relay_forwards SET due=?, updated_at=? WHERE mid=? AND state='pending'",
                    (now_ts + 1.0, now_ts, mid),
                )
            summary['deferred'] += 1
            continue

        # Claim into 'checking' (read-only check phase)
        with db(root, True) as c:
            claimed = c.execute(
                "UPDATE agent_relay_forwards SET state='checking', attempts=attempts+1, updated_at=? WHERE mid=? AND state='pending'",
                (now_ts, mid),
            ).rowcount
        if not claimed:
            continue
        attempt = int(fwd['attempts']) + 1

        # 1. Verify T3 server health first so transient server hiccups never trigger false "not found" fallback.
        srv = t3_client.server_status(root)
        if not srv.get('ok'):
            if attempt >= MAX_FORWARD_RETRIES:
                if _fallback_to_new_task(root, fwd, 'T3 服务暂不可达', now_ts):
                    summary['fallback'] += 1
            else:
                backoff = min(30.0, 5.0 * attempt)
                with db(root, True) as c:
                    c.execute(
                        "UPDATE agent_relay_forwards SET state='pending', due=?, updated_at=?, detail=? WHERE mid=?",
                        (now_ts + backoff, now_ts, f't3_server_unavailable:{srv.get("reason")}', mid),
                    )
                summary['deferred'] += 1
            continue

        # 2. Check target thread existence & archived status
        thread_obj = None
        show_err = None
        try:
            info = t3_client.show_thread(root, tid, messages=1, timeout=15)
            thread_obj = info.get('thread') if isinstance(info, dict) else None
        except Exception as exc:
            show_err = exc

        if show_err is not None or not isinstance(thread_obj, dict):
            # Re-verify server is still up; if server dropped mid-call, defer instead of falling back immediately.
            if not t3_client.server_status(root).get('ok') and attempt < MAX_FORWARD_RETRIES:
                backoff = min(30.0, 5.0 * attempt)
                with db(root, True) as c:
                    c.execute(
                        "UPDATE agent_relay_forwards SET state='pending', due=?, updated_at=?, detail=? WHERE mid=?",
                        (now_ts + backoff, now_ts, 't3_show_transient_error', mid),
                    )
                summary['deferred'] += 1
                continue
            if _fallback_to_new_task(root, fwd, '原线程不存在', now_ts):
                summary['fallback'] += 1
            continue

        if thread_obj.get('archived'):
            if _fallback_to_new_task(root, fwd, '原线程已归档', now_ts):
                summary['fallback'] += 1
            continue

        # 3. Transition to 'sending' right before invoking T3 `thread send` (wait=False -> queues cleanly on busy thread)
        with db(root, True) as c:
            c.execute(
                "UPDATE agent_relay_forwards SET state='sending', updated_at=? WHERE mid=? AND state='checking'",
                (now_ts, mid),
            )

        payload = format_forwarded_message(fwd, source_item=source_item)
        try:
            res = t3_client.send_message(root, tid, payload, wait=False, timeout=30)
        except Exception as exc:
            res = {'exit_code': 1, 'error': f'{type(exc).__name__}:{str(exc)[:180]}'}

        if res.get('exit_code') == 0:
            done_ts = time.time()
            receipt = _format_forwarded_receipt(fwd)
            detail = str(res.get('run_id') or 'sent')
            with db(root, True) as c:
                c.execute(
                    "UPDATE agent_relay_forwards SET state='forwarded', sent_at=?, updated_at=?, detail=? WHERE mid=?",
                    (done_ts, done_ts, detail[:240], mid),
                )
                c.execute(
                    "UPDATE cases SET category='control', note=?, updated=? WHERE id=?",
                    (f'已转交原 Agent〔{code}〕（{tid}）', done_ts, cid),
                )
                _queue_control_receipt(c, cid, receipt, done_ts)
            chat_feedback.changed.set()
            summary['forwarded'] += 1
        else:
            err_text = str(res.get('error') or f't3_send_exit_{res.get("exit_code")}')
            low_err = err_text.lower()
            if 'archived' in low_err:
                if _fallback_to_new_task(root, fwd, '原线程已归档', time.time()):
                    summary['fallback'] += 1
            elif 'not found' in low_err or 'does not exist' in low_err:
                if _fallback_to_new_task(root, fwd, '原线程不存在', time.time()):
                    summary['fallback'] += 1
            elif attempt < MAX_FORWARD_RETRIES:
                backoff = min(30.0, 5.0 * attempt)
                with db(root, True) as c:
                    c.execute(
                        "UPDATE agent_relay_forwards SET state='pending', due=?, updated_at=?, detail=? WHERE mid=?",
                        (time.time() + backoff, time.time(), err_text[:240], mid),
                    )
                summary['deferred'] += 1
            else:
                logging.warning('Agent relay forward failed after retries (%s -> %s): %s', mid, tid, err_text)
                if _fallback_to_new_task(root, fwd, '转交原线程失败', time.time()):
                    summary['fallback'] += 1

    return summary
