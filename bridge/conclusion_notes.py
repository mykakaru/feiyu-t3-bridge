"""Zero-LLM, group-scoped published conclusion notes for colleague follow-ups.

Design invariants (OpenAI Sign-in-with-ChatGPT Terms & Google Antigravity ToS compliance):
- Colleague messages NEVER invoke any personal LLM subscription, T3 thread, or background agent.
- Colleagues can only retrieve static supplementary explanations explicitly authored and
  published by the verified owner for that exact WeCom group.
- Unmatched colleague follow-ups are logged locally for the owner to handle manually.
"""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import re
import time

import group_replies
from identity import resolve

NOTE_ID_RE = re.compile(r'\b(K-[0-9a-f]{4,8})\b', re.I)
OWNER_PUBLISH = re.compile(
    r'^【(?:发布说明|补充说明|归档结论)】(?:\s*(K-[0-9a-f]{4,8}))?[\s：:]*([\s\S]*)$',
    re.I,
)
OWNER_UPDATE = re.compile(r'^【更新说明】\s*(K-[0-9a-f]{4,8})[\s：:]+([\s\S]+)$', re.I)
OWNER_DELETE = re.compile(r'^【(?:删除说明|停用说明)】\s*(K-[0-9a-f]{4,8})\s*$', re.I)
PEER_LOOKUP = re.compile(r'^【说明】(?:\s*(列表|list|K-[0-9a-f]{4,8}))?\s*$', re.I)
FOLLOWUP_EXPIRE_SECONDS = 2 * 3600
CRISIS_RE = re.compile(r'我(?:想|要|准备)(?:去死|自杀|结束自己的生命|伤害自己)')
LOCAL_PATH_RE = re.compile(r'(?i)(?:\bfile:|\bsandbox:|[a-z]:[\\/]|/(?:Users|home|mnt)/|\.codex[\\/])')


def install(c):
    c.execute("""CREATE TABLE IF NOT EXISTS conclusion_notes(
        note_id TEXT PRIMARY KEY,
        chat TEXT NOT NULL,
        title TEXT NOT NULL,
        anchor_text TEXT NOT NULL,
        body TEXT NOT NULL,
        source_case_id TEXT,
        created_by TEXT NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
    )""")
    c.execute('CREATE INDEX IF NOT EXISTS conclusion_notes_chat ON conclusion_notes(chat, active, updated_at)')
    c.execute("""CREATE TABLE IF NOT EXISTS colleague_followups(
        id TEXT PRIMARY KEY,
        chat TEXT NOT NULL,
        sender TEXT NOT NULL,
        source_mid TEXT UNIQUE NOT NULL,
        note_id TEXT,
        question TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open',
        created_at REAL NOT NULL,
        resolved_at REAL
    )""")
    c.execute('CREATE INDEX IF NOT EXISTS colleague_followups_status ON colleague_followups(status, created_at)')


def _strip_mention(text):
    cleaned = re.sub(r'^\s*[@＠]大肥鱼(?=$|[\s\u2005\u200b，,。！!？?：:【])[\s\u2005\u200b]*', '', text or '', count=1)
    return cleaned.strip()


def _normalize(text):
    return ' '.join((text or '').split()).strip().lower()


def _make_note_id(chat, seed):
    digest = sha256(f'{chat}\n{seed}\n{time.time_ns()}'.encode('utf-8')).hexdigest()
    return 'K-' + digest[:4].upper()


def _safe_group_text(text, manifest_chat, allowed_links=()):
    text = (text or '').strip()
    if not text or len(text.encode('utf-8')) > 12000:
        raise ValueError('说明正文长度不合法（需 1-12000 字节）')
    if group_replies.contains_credentials(text):
        raise ValueError('说明正文包含疑似凭据或令牌，已拒绝发布')
    if LOCAL_PATH_RE.search(text):
        raise ValueError('说明正文包含本机私有路径，已拒绝发布')
    fake_manifest = {
        'chatid': manifest_chat,
        'members': [{'chattype': 'group', 'chatid': manifest_chat, 'direct': '\n'.join(allowed_links), 'quotes': []}],
    }
    return group_replies.public_body(text, fake_manifest, allow_web_links=True)


def _queue_group_reply(c, item, cid, text, epoch, route_dest='conclusion_note'):
    c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)', (cid, route_dest, 'group'))
    c.execute(
        "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
        (cid, 'origin_received', epoch, text),
    )


def handle_owner_group_command(c, root, item, epoch):
    """Handle verified owner commands in a group for publishing/updating/deleting static notes."""
    from business_queue import make_case

    if not group_replies.owner_mention(root, item):
        return False
    body = _strip_mention(item.get('direct', ''))
    chat = item['chatid']

    m_del = OWNER_DELETE.fullmatch(body)
    if m_del:
        nid = m_del.group(1).upper()
        updated = c.execute(
            'UPDATE conclusion_notes SET active=0, updated_at=? WHERE note_id=? AND chat=? AND active=1',
            (epoch, nid, chat),
        ).rowcount
        reply = f'已停用本群补充说明〔{nid}〕。' if updated else f'本群未找到有效说明编号〔{nid}〕。'
        cid = make_case(c, item, 'conclusion_note', category='control', note=reply)
        _queue_group_reply(c, item, cid, reply, epoch)
        return True

    m_upd = OWNER_UPDATE.fullmatch(body)
    if m_upd:
        nid = m_upd.group(1).upper()
        new_body_raw = m_upd.group(2).strip()
        existing = c.execute(
            'SELECT * FROM conclusion_notes WHERE note_id=? AND chat=? AND active=1',
            (nid, chat),
        ).fetchone()
        if not existing:
            reply = f'本群未找到可更新的说明编号〔{nid}〕。'
        else:
            try:
                allowed = group_replies.URL.findall(new_body_raw)
                safe_body = _safe_group_text(new_body_raw, chat, allowed)
                c.execute(
                    'UPDATE conclusion_notes SET body=?, updated_at=? WHERE note_id=? AND chat=?',
                    (safe_body, epoch, nid, chat),
                )
                reply = f'已更新本群补充说明〔{nid}〕（{existing["title"]}）。同事查询时将直接返回新版静态说明，不调用模型。'
            except ValueError as exc:
                reply = f'更新失败：{exc}'
        cid = make_case(c, item, 'conclusion_note', category='control', note=reply)
        _queue_group_reply(c, item, cid, reply, epoch)
        return True

    m_pub = OWNER_PUBLISH.fullmatch(body)
    if m_pub:
        custom_id = m_pub.group(1).upper() if m_pub.group(1) else None
        rest = (m_pub.group(2) or '').strip()
        quotes = [q.strip() for q in item.get('quotes', []) if isinstance(q, str) and q.strip()]
        anchor_text = quotes[0] if quotes else ''
        lines = [ln.strip() for ln in rest.splitlines() if ln.strip()]
        if anchor_text:
            title = anchor_text.splitlines()[0][:48]
            note_body = rest
        elif len(lines) >= 2:
            title = lines[0][:48]
            anchor_text = lines[0]
            note_body = '\n'.join(lines[1:])
        else:
            title = rest[:48]
            anchor_text = rest[:120]
            note_body = rest
        if not note_body:
            reply = '请在【发布说明】后填写补充说明正文（可引用原结论消息，或首行写标题、换行写正文）。'
        else:
            try:
                allowed = group_replies.URL.findall(note_body) + group_replies.URL.findall(anchor_text)
                safe_body = _safe_group_text(note_body, chat, allowed)
                nid = custom_id or _make_note_id(chat, title + '\n' + safe_body)
                while c.execute('SELECT 1 FROM conclusion_notes WHERE note_id=?', (nid,)).fetchone() and not custom_id:
                    nid = _make_note_id(chat, nid + str(time.time_ns()))
                c.execute(
                    """INSERT INTO conclusion_notes(note_id,chat,title,anchor_text,body,source_case_id,created_by,created_at,updated_at,active)
                       VALUES(?,?,?,?,?,?,?,?,?,1)
                       ON CONFLICT(note_id) DO UPDATE SET
                         title=excluded.title, anchor_text=excluded.anchor_text, body=excluded.body,
                         updated_at=excluded.updated_at, active=1
                       WHERE conclusion_notes.chat=excluded.chat""",
                    (nid, chat, title, anchor_text, safe_body, None, item['sender'], epoch, epoch),
                )
                reply = (
                    f'已发布本群结论补充说明〔{nid}〕：{title}\n'
                    f'同事引用原结论或发「@大肥鱼 【说明】{nid}」即可查看（纯本地静态查询，不调用模型）。'
                )
            except ValueError as exc:
                reply = f'发布失败：{exc}'
        cid = make_case(c, item, 'conclusion_note', category='control', note=reply)
        _queue_group_reply(c, item, cid, reply, epoch)
        return True

    return False


def _match_quote_note(c, chat, quotes):
    if not quotes:
        return None
    notes = c.execute(
        'SELECT * FROM conclusion_notes WHERE chat=? AND active=1 ORDER BY updated_at DESC LIMIT 100',
        (chat,),
    ).fetchall()
    if not notes:
        return None
    for q in quotes:
        m = NOTE_ID_RE.search(q)
        if m:
            nid = m.group(1).upper()
            hit = next((n for n in notes if n['note_id'].upper() == nid), None)
            if hit:
                return hit
    norm_quotes = [_normalize(q) for q in quotes if _normalize(q)]
    matches = []
    for n in notes:
        na = _normalize(n['anchor_text'])
        nt = _normalize(n['title'])
        if not na and not nt:
            continue
        for nq in norm_quotes:
            if (na and (na == nq or (len(na) >= 8 and na in nq) or (len(nq) >= 8 and nq in na))) or (
                nt and len(nt) >= 6 and nt in nq
            ):
                matches.append(n)
                break
    return matches[0] if len(matches) == 1 else None


def _queue_owner_followup_push(c, item, cid, fid, question, quotes, epoch):
    """Queue a 0-LLM private push to the owner via the primary group bot (大肥鱼)."""
    payload = json.dumps({
        'followup_id': fid,
        'code': 'F-' + fid[-6:].upper(),
        'group_chat': item['chatid'],
        'sender': item['sender'],
        'question': (question or '（引用/附件追问）')[:1500],
        'quotes': [q[:400] for q in (quotes or ())[:2]],
    }, ensure_ascii=False)
    c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)', (cid, 'colleague_push_owner', 'group'))
    c.execute(
        "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
        (cid, 'colleague_push', epoch, payload),
    )


def handle_colleague_group_message(c, root, item, epoch):
    """Strictly zero-LLM handler for non-owner group @mentions.

    Never creates any AI task, never writes to T3, never calls OpenAI or Google models.
    """
    from business_queue import make_case
    import request_flow

    if not group_replies.is_group(root, item) or not group_replies.addressed_to_bot(item):
        return False
    if resolve(root, item.get('sender'))['is_owner']:
        return False

    chat = item['chatid']
    sender = item['sender']
    question = _strip_mention(item.get('direct', ''))
    quotes = [q.strip() for q in item.get('quotes', []) if isinstance(q, str) and q.strip()]
    quiet = request_flow.destination(item, False) == 'quiet'

    if CRISIS_RE.search(question):
        cid = make_case(c, item, 'conclusion_note', category='control', note='安全提醒（零模型静态回复）')
        _queue_group_reply(
            c, item, cid,
            '如果你现在有伤害自己的危险，请立即联系当地急救，找一位能到你身边的可信任的人陪同。',
            epoch,
        )
        return True

    # 1. Explicit 【说明】 or 【说明】K-xxxx lookup
    m_lookup = PEER_LOOKUP.fullmatch(question)
    if m_lookup:
        arg = (m_lookup.group(1) or '').strip()
        if not arg or arg.lower() in ('列表', 'list'):
            rows = c.execute(
                'SELECT note_id, title FROM conclusion_notes WHERE chat=? AND active=1 ORDER BY updated_at DESC LIMIT 20',
                (chat,),
            ).fetchall()
            if not rows:
                reply = '本群目前还没有已发布的结论补充说明。'
            else:
                lines = ['本群已发布的结论补充说明（发送「@大肥鱼 【说明】编号」查看）：']
                for r in rows:
                    lines.append(f'- {r["note_id"]}：{r["title"]}')
                reply = '\n'.join(lines)
        else:
            nid = arg.upper()
            row = c.execute(
                'SELECT * FROM conclusion_notes WHERE note_id=? AND chat=? AND active=1',
                (nid, chat),
            ).fetchone()
            if row:
                reply = f'【结论补充说明 · {row["note_id"]}】{row["title"]}\n\n{row["body"]}'
            else:
                reply = f'本群未找到编号 {nid} 的已发布说明。可发「@大肥鱼 【说明】」查看本群列表。'
        cid = make_case(c, item, 'conclusion_note', category='control', note='群友静态说明查询（零模型）')
        if not quiet:
            _queue_group_reply(c, item, cid, reply, epoch)
        return True

    # 2. Check explicit note ID anywhere in text or quote match against published notes in THIS group
    matched_note = None
    id_hit = NOTE_ID_RE.search(question)
    if id_hit:
        nid = id_hit.group(1).upper()
        matched_note = c.execute(
            'SELECT * FROM conclusion_notes WHERE note_id=? AND chat=? AND active=1',
            (nid, chat),
        ).fetchone()
    if matched_note is None:
        matched_note = _match_quote_note(c, chat, quotes)

    fid = 'f_' + sha256((item['msgid'] + '\n' + chat).encode('utf-8')).hexdigest()[:12]
    if matched_note is not None:
        # Return static published supplementary note. If colleague also typed extra follow-up
        # prose beyond referencing the note, record it AND privately push to owner (still 0 LLM!).
        extra = NOTE_ID_RE.sub('', question).strip(' ，,。！？?：:')
        has_extra = len(extra) >= 6
        if has_extra:
            c.execute(
                """INSERT OR IGNORE INTO colleague_followups(id,chat,sender,source_mid,note_id,question,status,created_at)
                   VALUES(?,?,?,?,?,?,'open',?)""",
                (fid, chat, sender, item['msgid'], matched_note['note_id'], extra[:2000], epoch),
            )
        reply = f'【结论补充说明 · {matched_note["note_id"]}】{matched_note["title"]}\n\n{matched_note["body"]}'
        cid = make_case(c, item, 'conclusion_note', category='control', note=f'命中静态说明 {matched_note["note_id"]}（零模型）')
        if not quiet:
            _queue_group_reply(c, item, cid, reply, epoch)
        if has_extra:
            _queue_owner_followup_push(c, item, cid, fid, extra, quotes, epoch)
        return True

    # 3. Unmatched colleague question: 100% SILENT in the group (zero spam!),
    # record locally AND immediately push privately to the owner (ZERO LLM calls!).
    c.execute(
        """INSERT OR IGNORE INTO colleague_followups(id,chat,sender,source_mid,note_id,question,status,created_at)
           VALUES(?,?,?,?,NULL,?,'open',?)""",
        (fid, chat, sender, item['msgid'], (question or '（引用/附件追问）')[:2000], epoch),
    )
    cid = make_case(c, item, 'colleague_followup', category='colleague_followup', note='群友提问已本地静默登记并私推主人（零模型）。')
    _queue_owner_followup_push(c, item, cid, fid, question, quotes, epoch)
    return True


def list_panel_data(root):
    from business_queue import db

    path = Path(root) / 'business.sqlite3'
    if not path.is_file():
        return {'notes': [], 'followups': []}
    with db(root) as c:
        install(c)
        notes = [
            dict(r) for r in c.execute(
                'SELECT note_id,chat,title,anchor_text,body,source_case_id,created_at,updated_at,active '
                'FROM conclusion_notes WHERE active=1 ORDER BY updated_at DESC LIMIT 100'
            )
        ]
        followups = [
            dict(r) for r in c.execute(
                'SELECT id,chat,sender,note_id,question,status,created_at,resolved_at '
                'FROM colleague_followups ORDER BY status=\'open\' DESC, created_at DESC LIMIT 100'
            )
        ]
    return {'notes': notes, 'followups': followups}


def panel_save_note(root, chat, title, body, *, note_id=None, anchor_text='', source_case_id=None):
    from business_queue import db
    from identity import CLI_OWNER

    chat = (chat or '').strip()
    title = (title or '').strip()[:80]
    body = (body or '').strip()
    if not chat or not title or not body:
        raise ValueError('群会话、标题和说明正文均不能为空')
    allowed = group_replies.URL.findall(body) + group_replies.URL.findall(anchor_text or '')
    safe_body = _safe_group_text(body, chat, allowed)
    now = time.time()
    nid = (note_id or '').strip().upper() or _make_note_id(chat, title + '\n' + safe_body)
    with db(root, True) as c:
        install(c)
        c.execute(
            """INSERT INTO conclusion_notes(note_id,chat,title,anchor_text,body,source_case_id,created_by,created_at,updated_at,active)
               VALUES(?,?,?,?,?,?,?,?,?,1)
               ON CONFLICT(note_id) DO UPDATE SET
                 title=excluded.title, anchor_text=excluded.anchor_text, body=excluded.body,
                 updated_at=excluded.updated_at, active=1
               WHERE conclusion_notes.chat=excluded.chat""",
            (nid, chat, title, (anchor_text or title)[:400], safe_body, source_case_id, CLI_OWNER, now, now),
        )
    return {'note_id': nid, 'chat': chat, 'title': title}


def panel_delete_note(root, note_id):
    from business_queue import db

    nid = (note_id or '').strip().upper()
    with db(root, True) as c:
        install(c)
        n = c.execute('UPDATE conclusion_notes SET active=0, updated_at=? WHERE note_id=?', (time.time(), nid)).rowcount
    return {'deleted': bool(n), 'note_id': nid}


def _close_followup_case(c, source_mid, *, category='control', note='', now=None):
    if not source_mid:
        return 0
    now = time.time() if now is None else now
    return c.execute(
        "UPDATE cases SET category=?, note=?, updated=? WHERE anchor=? AND category='colleague_followup'",
        (category, note, now, source_mid),
    ).rowcount


def ignore_followup(c, followup_id, *, now=None, status='ignored', note='群友提问已由主人手动忽略（零模型）。'):
    now = time.time() if now is None else now
    install(c)
    row = c.execute('SELECT id, source_mid, status FROM colleague_followups WHERE id=?', (followup_id,)).fetchone()
    if not row:
        return False
    updated = c.execute(
        "UPDATE colleague_followups SET status=?, resolved_at=? WHERE id=? AND status='open'",
        (status, now, followup_id),
    ).rowcount
    if updated:
        _close_followup_case(c, row['source_mid'], category='cancelled', note=note, now=now)
    return bool(updated)


def resolve_followup_case(c, followup_id, *, now=None, note='群友提问已通过大肥鱼回复原群。'):
    now = time.time() if now is None else now
    install(c)
    row = c.execute('SELECT id, source_mid FROM colleague_followups WHERE id=?', (followup_id,)).fetchone()
    if not row:
        return False
    c.execute(
        "UPDATE colleague_followups SET status='resolved', resolved_at=? WHERE id=?",
        (now, followup_id),
    )
    _close_followup_case(c, row['source_mid'], category='control', note=note, now=now)
    return True


def resolve_matching_open_followup_in_chat(c, chat, quotes, *, now=None):
    """When owner quotes a colleague message in-group and @大肥鱼, close any matching open followup."""
    if not chat or not quotes:
        return 0
    now = time.time() if now is None else now
    install(c)
    open_rows = c.execute(
        "SELECT id, source_mid, question FROM colleague_followups WHERE chat=? AND status='open' ORDER BY created_at DESC LIMIT 50",
        (chat,),
    ).fetchall()
    if not open_rows:
        return 0
    norm_quotes = [_normalize(_strip_mention(q)) for q in quotes if isinstance(q, str) and _normalize(_strip_mention(q))]
    if not norm_quotes:
        return 0
    matched_ids = []
    for r in open_rows:
        nq = _normalize(r['question'])
        if not nq:
            continue
        if any(nq == q or (len(nq) >= 4 and (nq in q or q in nq)) for q in norm_quotes):
            matched_ids.append((r['id'], r['source_mid']))
    if len(matched_ids) != 1:
        return 0
    fid, source_mid = matched_ids[0]
    updated = c.execute(
        "UPDATE colleague_followups SET status='resolved', resolved_at=? WHERE id=? AND status='open'",
        (now, fid),
    ).rowcount
    if updated:
        _close_followup_case(
            c, source_mid, category='control',
            note='主人已在群内引用该群友消息并 @大肥鱼 转为自主发问。', now=now,
        )
    return updated


def sweep_expired_followups(c, now=None):
    """Silently auto-ignore colleague followups older than 2 hours with zero notifications."""
    now = time.time() if now is None else now
    install(c)
    cutoff = now - FOLLOWUP_EXPIRE_SECONDS
    rows = c.execute(
        "SELECT id, source_mid FROM colleague_followups WHERE status='open' AND created_at<=?",
        (cutoff,),
    ).fetchall()
    count = 0
    for r in rows:
        updated = c.execute(
            "UPDATE colleague_followups SET status='expired', resolved_at=? WHERE id=? AND status='open'",
            (now, r['id']),
        ).rowcount
        if updated:
            count += 1
            _close_followup_case(
                c, r['source_mid'], category='cancelled',
                note='群友提问超过 2 小时未回复，已自动忽略。', now=now,
            )
    return count


def next_followup_deadline(c, now=None):
    install(c)
    row = c.execute(
        "SELECT MIN(created_at)+? FROM colleague_followups WHERE status='open'",
        (FOLLOWUP_EXPIRE_SECONDS,),
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def panel_resolve_followup(root, followup_id):
    from business_queue import db

    fid = (followup_id or '').strip()
    now = time.time()
    with db(root, True) as c:
        install(c)
        row = c.execute('SELECT id, source_mid FROM colleague_followups WHERE id=?', (fid,)).fetchone()
        n = c.execute(
            "UPDATE colleague_followups SET status='resolved', resolved_at=? WHERE id=?",
            (now, fid),
        ).rowcount
        if row and n:
            _close_followup_case(c, row['source_mid'], category='control', note='群友提问已在面板标记处理完成。', now=now)
    return {'resolved': bool(n), 'id': fid}
