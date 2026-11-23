"""Durable multi-round clarification state machine for ambiguous or conflicting owner requests."""
from __future__ import annotations

from hashlib import sha256
import json
import re
import time

import group_replies
from identity import resolve

EXPLICIT_REPLY = re.compile(
    r'^(?:[@＠]大肥鱼[\s\u2005\u200b]*)?'
    r'【(?:答复|回复|确认回复)(?:[\s：:]+(?:(Q-[0-9a-f]{4,8})|(?:〔)?([0-9a-f]{7}|w_[0-9a-f]{32})(?:〕)?))?】'
    r'[\s：:]*(?:(Q-[0-9a-f]{4,8})|(?:〔)?([0-9a-f]{7}|w_[0-9a-f]{32})(?:〕)?)?[\s：:]*([\s\S]+)$',
    re.I,
)
QID_IN_TEXT = re.compile(r'\b(Q-[0-9a-f]{4,8})\b', re.I)
CASE_TAG_IN_QUOTE = re.compile(r'(?:需要确认|追问)〔([0-9a-f]{7})〕')
TASK_RECEIPT_QUOTE_RE = re.compile(
    r'^(?:Windows 接手 · [^\n]*\n)?(?:(?:追加|补发)?办理结果|已收到(?:确认)?|已收齐|正在收集|事项)〔[0-9a-f]{7}〕',
    re.I,
)
SHORT_SELECTION = re.compile(
    r'^(?:选\s*)?(?:[A-Ea-e]|[1-5]|第[一二三四五1-5]个?|方案\s*[A-E1-5]|都可以|都行|按默认|确认|是的|对|不需要|不用|只要第一个|都要|只回(?:我)?私聊|仅(?:回)?私聊|不发群)(?:[，,。\s].{0,32})?$',
    re.I,
)
RESERVED_NEW_COMMAND = re.compile(
    r'^(?:[@＠]大肥鱼[\s\u2005\u200b]*)?【(?:开始收集|收集|合并|合批|收尾|结束|追加|追加任务|取消|取消任务|确认追加|确认取消|模型|回报|发布说明|补充说明|更新说明|删除说明|说明|直回|直发群|发群|改发|不发|忽略|跳过|无需处理)',
    re.I,
)

REMINDER_AFTER_SECONDS = 24 * 3600
PARK_AFTER_SECONDS = 72 * 3600


def install(c):
    c.execute("""CREATE TABLE IF NOT EXISTS clarifications(
        qid TEXT PRIMARY KEY,
        case_id TEXT NOT NULL REFERENCES cases(id),
        round_no INTEGER NOT NULL,
        thread_id TEXT,
        chat TEXT NOT NULL,
        sender TEXT NOT NULL,
        origin TEXT NOT NULL,
        reason TEXT NOT NULL,
        questions_json TEXT NOT NULL,
        prompt_text TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at REAL NOT NULL,
        reminded_at REAL,
        answered_mid TEXT UNIQUE,
        answer_text TEXT,
        answered_at REAL,
        bot_channel TEXT NOT NULL DEFAULT 'feiyu'
    )""")
    if 'bot_channel' not in {r[1] for r in c.execute('PRAGMA table_info(clarifications)')}:
        c.execute("ALTER TABLE clarifications ADD COLUMN bot_channel TEXT NOT NULL DEFAULT 'feiyu'")
    c.execute('CREATE INDEX IF NOT EXISTS clarifications_open ON clarifications(chat, sender, state, created_at)')
    c.execute('CREATE INDEX IF NOT EXISTS clarifications_case ON clarifications(case_id, round_no)')


def _make_qid(case_id, round_no):
    seed = f'{case_id}:{round_no}:{time.time_ns()}'
    return 'Q-' + sha256(seed.encode('utf-8')).hexdigest()[:4].upper()


def _is_group_case(c, case_id, chat=''):
    if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='request_routes'").fetchone():
        r = c.execute('SELECT origin_kind FROM request_routes WHERE case_id=?', (case_id,)).fetchone()
        if r:
            return r['origin_kind'] == 'group'
    return isinstance(chat, str) and chat.startswith('wrk')


def _group_meta_block(case_id, *, group_name=None, thread_id=None, compact=False):
    tag = case_id[-7:]
    glabel = (group_name or '').strip() or '原群'
    agent_label = (thread_id or '').strip() or '待启动（前置规则拦截）'
    if compact:
        return f'来源群：{glabel} · 任务编号：{tag} · Agent ID：{agent_label}'
    return '\n'.join([
        f'来源群：{glabel}',
        f'任务编号：{tag}',
        f'Agent ID：{agent_label}',
    ])


def render_group_private_notice(text, *, case_id, group_name, thread_id=None):
    """Ensure a group-originated clarification/ack/remind/park private message carries group name, task tag, and Agent ID."""
    raw = str(text or '').strip()
    if not raw:
        return raw
    tag = case_id[-7:]
    glabel = (group_name or '').strip() or '原群'
    agent_label = (thread_id or '').strip() or '待启动（前置规则拦截）'
    lines = raw.splitlines()
    if not lines:
        return raw
    is_question = lines[0].startswith(f'需要确认〔{tag}〕')
    if is_question:
        if any(line.startswith('来源群：') for line in lines[1:6]):
            out = []
            for line in lines:
                if line.startswith('来源群：'):
                    out.append(f'来源群：{glabel}')
                elif line.startswith('Agent ID：') and thread_id:
                    out.append(f'Agent ID：{agent_label}')
                else:
                    out.append(line)
            return '\n'.join(out)
        meta = _group_meta_block(case_id, group_name=glabel, thread_id=thread_id, compact=False)
        return '\n'.join([lines[0], meta, *lines[1:]])
    compact_line = _group_meta_block(case_id, group_name=glabel, thread_id=thread_id, compact=True)
    if any(line.startswith('来源群：') for line in lines):
        return '\n'.join(compact_line if line.startswith('来源群：') else line for line in lines)
    return '\n'.join([lines[0], compact_line, *lines[1:]])


def format_question_message(case_id, qid, round_no, reason, questions, summary='', *, is_group=False, group_name=None, thread_id=None):
    tag = case_id[-7:]
    lines = [f'需要确认〔{tag}〕（编号 {qid} · 第 {round_no} 轮追问）']
    if is_group:
        lines.append(_group_meta_block(case_id, group_name=group_name, thread_id=thread_id, compact=False))
    if reason:
        lines.append(f'原因：{reason}')
    if summary and summary != reason:
        lines.append(summary)
    if questions:
        lines.append('')
        for idx, q in enumerate(questions, 1):
            if isinstance(q, str):
                lines.append(f'{idx}. {q}')
                continue
            qtext = str(q.get('question') or q.get('text') or '').strip()
            lines.append(f'{idx}. {qtext}')
            opts = q.get('options') or []
            if isinstance(opts, list) and opts:
                letters = 'ABCDEFGH'
                formatted = []
                for i, opt in enumerate(opts[:8]):
                    label = letters[i] if i < len(letters) else str(i + 1)
                    s = str(opt).strip()
                    if re.match(r'^[A-H1-9][.、:：\s]', s):
                        formatted.append(s)
                    else:
                        formatted.append(f'{label}. {s}')
                lines.append('   ' + '  /  '.join(formatted))
    lines.extend([
        '',
        '回复方式（任选其一）：',
        f'- 引用本条直接回复，或发：【答复 {qid}】你的选择或补充',
        f'- 如暂不办理，发：【取消】{tag}',
    ])
    return '\n'.join(lines)


def open_question(c, case_id, *, thread_id, chat, sender, origin, reason, questions, summary='', now=None, quiet=False, bot_channel=None):
    """Open a durable clarification round, suspend execution, and queue the WeCom question."""
    now = time.time() if now is None else now
    install(c)
    if bot_channel is None:
        ch_row = c.execute('SELECT * FROM cases WHERE id=?', (case_id,)).fetchone()
        bot_channel = (ch_row['bot_channel'] if ch_row and 'bot_channel' in ch_row.keys() and ch_row['bot_channel'] else 'feiyu')
    existing = c.execute(
        "SELECT qid, prompt_text FROM clarifications WHERE case_id=? AND state='open' ORDER BY round_no DESC LIMIT 1",
        (case_id,),
    ).fetchone()
    if existing:
        return existing['qid'], existing['prompt_text']
    prev_round = c.execute('SELECT COALESCE(MAX(round_no), 0) FROM clarifications WHERE case_id=?', (case_id,)).fetchone()[0]
    round_no = int(prev_round) + 1
    qid = _make_qid(case_id, round_no)
    while c.execute('SELECT 1 FROM clarifications WHERE qid=?', (qid,)).fetchone():
        qid = _make_qid(case_id, round_no + 1)
    qlist = list(questions) if isinstance(questions, list) else ([str(questions)] if questions else [])
    is_group = _is_group_case(c, case_id, chat)
    gname = f'原群（尾号 {chat[-8:]}）' if (is_group and isinstance(chat, str) and len(chat) >= 8) else ('原群' if is_group else None)
    body = format_question_message(
        case_id, qid, round_no, reason, qlist, summary,
        is_group=is_group, group_name=gname, thread_id=thread_id,
    )
    c.execute(
        """INSERT INTO clarifications(qid,case_id,round_no,thread_id,chat,sender,origin,reason,questions_json,prompt_text,state,created_at,bot_channel)
           VALUES(?,?,?,?,?,?,?,?,?,?,'open',?,?)""",
        (qid, case_id, round_no, thread_id, chat, sender, origin, str(reason)[:500],
         json.dumps(qlist, ensure_ascii=False), body, now, bot_channel),
    )
    c.execute(
        "UPDATE cases SET category='awaiting_clarification', note=?, updated=? WHERE id=?",
        (f'等待确认（{qid} · 第 {round_no} 轮）：{str(reason)[:200]}', now, case_id),
    )
    c.execute(
        "UPDATE lanes SET state='awaiting_clarification', changed=?, detail=? WHERE case_id=? AND target='local'",
        (now, f'等待主人确认 {qid}', case_id),
    )
    if not quiet:
        c.execute(
            "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
            (case_id, f'clarify:{qid}', now, body),
        )
    return qid, body


def history(c, case_id):
    install(c)
    rows = c.execute(
        'SELECT qid,round_no,origin,reason,questions_json,answer_text,state,created_at,answered_at '
        'FROM clarifications WHERE case_id=? ORDER BY round_no',
        (case_id,),
    ).fetchall()
    out = []
    for r in rows:
        try:
            qs = json.loads(r['questions_json'])
        except ValueError:
            qs = []
        out.append({
            'qid': r['qid'],
            'round_no': r['round_no'],
            'origin': r['origin'],
            'reason': r['reason'],
            'questions': qs,
            'answer': r['answer_text'],
            'state': r['state'],
            'created_at': r['created_at'],
            'answered_at': r['answered_at'],
        })
    return out


def has_open(c, case_id):
    install(c)
    return c.execute("SELECT 1 FROM clarifications WHERE case_id=? AND state='open' LIMIT 1", (case_id,)).fetchone() is not None


def _owner_scope_ok(root, item):
    if not resolve(root, item.get('sender'))['is_owner']:
        return False
    if item.get('chattype') == 'single':
        return item.get('sender') == item.get('chatid')
    if item.get('chattype') == 'group':
        return group_replies.owner_mention(root, item)
    return False


def _open_clarifications_for_intake(c, item, bot_channel):
    chat, sender = item['chatid'], item['sender']
    if (item.get('chattype') == 'single' and chat == sender and bot_channel == 'feiyu'
            and c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='request_routes'").fetchone()):
        return c.execute(
            """SELECT * FROM clarifications
               WHERE sender=? AND bot_channel='feiyu' AND state='open'
                 AND (chat=? OR EXISTS (
                     SELECT 1 FROM request_routes r WHERE r.case_id=clarifications.case_id AND r.origin_kind='group'
                 ))
               ORDER BY created_at DESC""",
            (sender, chat),
        ).fetchall()
    return c.execute(
        "SELECT * FROM clarifications WHERE chat=? AND sender=? AND bot_channel=? AND state='open' ORDER BY created_at DESC",
        (chat, sender, bot_channel),
    ).fetchall()


def _match_open_target(c, item, text, quotes):
    bot_channel = item.get('bot_channel') or 'feiyu'
    open_rows = _open_clarifications_for_intake(c, item, bot_channel)
    if not open_rows:
        return None, None, None

    # 1. Explicit 【答复 ...】 command (supports ID either inside 【答复 Q-xxxx】 or right after 【答复】 Q-xxxx)
    m = EXPLICIT_REPLY.fullmatch(text)
    if m:
        qid_raw = m.group(1) or m.group(3)
        case_raw = m.group(2) or m.group(4)
        answer = (m.group(5) or '').strip()
        if qid_raw:
            hit = next((r for r in open_rows if r['qid'].upper() == qid_raw.upper()), None)
            return hit, answer, ('unknown_qid' if not hit else 'explicit_qid')
        if case_raw:
            cr = case_raw.lower()
            hits = [r for r in open_rows if r['case_id'] == cr or r['case_id'][-7:] == cr]
            return (hits[0] if len(hits) == 1 else None), answer, ('ambiguous_or_unknown_case' if len(hits) != 1 else 'explicit_case')
        if len(open_rows) == 1:
            return open_rows[0], answer, 'explicit_single'
        return None, answer, 'ambiguous_multiple_open'

    import task_controls
    if RESERVED_NEW_COMMAND.match(text) or task_controls.QUOTED_CANCEL.fullmatch(text):
        return None, None, None

    # 2. Quoted bot clarification message
    if quotes:
        for q in quotes:
            mq = QID_IN_TEXT.search(q)
            if mq:
                hit = next((r for r in open_rows if r['qid'].upper() == mq.group(1).upper()), None)
                if hit:
                    return hit, text, 'quote_qid'
            mc = CASE_TAG_IN_QUOTE.search(q)
            if mc:
                tag = mc.group(1).lower()
                hits = [r for r in open_rows if r['case_id'][-7:] == tag]
                if len(hits) == 1:
                    return hits[0], text, 'quote_case'

    # 3. Bare Q-xxxx prefix without brackets (e.g. "Q-3F9A 选A")
    bare_q = re.match(r'^(?:[@＠]大肥鱼[\s\u2005\u200b]*)?(Q-[0-9a-f]{4,8})[\s：:,，]+([\s\S]+)$', text, re.I)
    if bare_q:
        hit = next((r for r in open_rows if r['qid'].upper() == bare_q.group(1).upper()), None)
        if hit:
            return hit, bare_q.group(2).strip(), 'bare_qid'

    # 4. Short unambiguous selection when uniquely 1 open clarification exists (never hijack new tasks or quoted receipts/old QIDs/Agent asks!)
    import agent_relay
    if (not RESERVED_NEW_COMMAND.match(text)
            and not agent_relay.has_explicit_agent_target(text)
            and not any(
                TASK_RECEIPT_QUOTE_RE.match(q.strip())
                or QID_IN_TEXT.search(q)
                or CASE_TAG_IN_QUOTE.search(q)
                or agent_relay.is_agent_ask_quote(q)
                for q in quotes
            )
            and SHORT_SELECTION.fullmatch(text)):
        if len(open_rows) == 1:
            return open_rows[0], text, 'short_single'
        return None, text, 'ambiguous_multiple_open'

    return None, None, None


def intake_answer(c, root, item, epoch):
    """If this verified owner message answers an open clarification, consume & resume it."""
    from business_queue import make_case

    if not _owner_scope_ok(root, item):
        return False
    text = (item.get('direct') or '').strip()
    if not text or item.get('msgtype') not in ('text', 'voice', 'mixed'):
        return False
    install(c)
    quotes = [q.strip() for q in item.get('quotes', []) if isinstance(q, str) and q.strip()]
    row, answer, match_kind = _match_open_target(c, item, text, quotes)

    bot_channel = item.get('bot_channel') or 'feiyu'
    if match_kind in ('unknown_qid', 'ambiguous_or_unknown_case', 'ambiguous_multiple_open'):
        open_rows = _open_clarifications_for_intake(c, item, bot_channel)[:8]
        listing = '、'.join(f'{r["qid"]}（事项〔{r["case_id"][-7:]}〕）' for r in open_rows)
        msg = (
            f'当前有多个待确认事项，请指明编号再回复：{listing}。\n'
            '格式：【答复 Q-XXXX】你的回答，或直接引用对应追问消息回复。'
            if open_rows else '未找到对应的待确认编号，请检查编号是否已答复或已取消。'
        )
        cid = make_case(c, item, 'control', category='control', note=msg, bot_channel=bot_channel)
        c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)', (cid, 'control', 'group' if item['chattype'] == 'group' else 'private'))
        phase = 'origin_received' if item['chattype'] == 'group' else 'first'
        c.execute("INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)", (cid, phase, epoch, msg))
        return True

    if row is None or not answer:
        return False

    # Idempotency check
    if c.execute('SELECT 1 FROM clarifications WHERE answered_mid=?', (item['msgid'],)).fetchone():
        return True

    cid = row['case_id']
    qid = row['qid']
    c.execute('UPDATE sources SET case_id=? WHERE mid=? AND case_id IS NULL', (cid, item['msgid']))
    c.execute(
        "UPDATE clarifications SET state='answered', answered_mid=?, answer_text=?, answered_at=? WHERE qid=? AND state='open'",
        (item['msgid'], answer[:8000], epoch, qid),
    )
    c.execute(
        "UPDATE cases SET category='work', note=?, updated=? WHERE id=?",
        (f'已收到确认（{qid}），继续办理。', epoch, cid),
    )
    # Mark local lane queued (if pre-gate never created a thread) or resume_queued (if thread exists)
    next_lane = 'resume_queued' if row['thread_id'] else 'queued'
    lane_exists = c.execute("SELECT 1 FROM lanes WHERE case_id=? AND target='local'", (cid,)).fetchone()
    if lane_exists:
        c.execute(
            "UPDATE lanes SET state=?, changed=?, detail=? WHERE case_id=? AND target='local'",
            (next_lane, epoch, f'已收到确认 {qid}，恢复执行', cid),
        )
    else:
        c.execute(
            "INSERT OR IGNORE INTO lanes(case_id,target,state,since,changed,detail) VALUES(?,'local',?,?,?,?)",
            (cid, next_lane, epoch, epoch, f'已收到确认 {qid}，准备执行'),
        )
    # Unfreeze pre-gate cases so package() rebuilds with clarification history included
    if not row['thread_id']:
        c.execute('UPDATE cases SET frozen=NULL WHERE id=?', (cid,))
    ack_body = f'已收到确认〔{cid[-7:]}〕（{qid}），继续处理。'
    if _is_group_case(c, cid, row['chat']):
        gname = f'原群（尾号 {row["chat"][-8:]}）' if (isinstance(row['chat'], str) and len(row['chat']) >= 8) else '原群'
        ack_body = render_group_private_notice(ack_body, case_id=cid, group_name=gname, thread_id=row['thread_id'])
    c.execute(
        "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
        (cid, f'clarify_ack:{qid}', epoch, ack_body),
    )
    return True


def sweep_stale(c, now=None):
    """Send one gentle reminder at 24h and park at 72h. Never invent a default answer."""
    now = time.time() if now is None else now
    install(c)
    for row in c.execute("SELECT * FROM clarifications WHERE state='open'").fetchall():
        age = now - row['created_at']
        cid = row['case_id']
        qid = row['qid']
        is_grp = _is_group_case(c, cid, row['chat'])
        gname = f'原群（尾号 {row["chat"][-8:]}）' if (is_grp and isinstance(row['chat'], str) and len(row['chat']) >= 8) else '原群'
        if age >= PARK_AFTER_SECONDS:
            c.execute("UPDATE clarifications SET state='parked' WHERE qid=? AND state='open'", (qid,))
            c.execute("UPDATE cases SET category='cancelled', note=?, updated=? WHERE id=?", (f'追问 {qid} 超过 72 小时未答复，已搁置（未自动猜想执行）。', now, cid))
            c.execute("UPDATE lanes SET state='cancelled', changed=?, detail='追问超时搁置' WHERE case_id=? AND target='local'", (now, cid))
            park_body = f'事项〔{cid[-7:]}〕（{qid}）超过 72 小时未收到确认，已暂停搁置（未擅自执行）。如需继续可另发新消息。'
            if is_grp:
                park_body = render_group_private_notice(park_body, case_id=cid, group_name=gname, thread_id=row['thread_id'])
            c.execute(
                "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
                (cid, f'clarify_park:{qid}', now, park_body),
            )
        elif age >= REMINDER_AFTER_SECONDS and row['reminded_at'] is None:
            c.execute('UPDATE clarifications SET reminded_at=? WHERE qid=? AND reminded_at IS NULL', (now, qid))
            remind_body = f'提醒：事项〔{cid[-7:]}〕仍在等待你的确认（编号 {qid}）。回复「【答复 {qid}】内容」可继续，或发「【取消】{cid[-7:]}」取消。'
            if is_grp:
                remind_body = render_group_private_notice(remind_body, case_id=cid, group_name=gname, thread_id=row['thread_id'])
            c.execute(
                "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
                (cid, f'clarify_remind:{qid}', now, remind_body),
            )
