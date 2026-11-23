"""Zero-LLM group relay, target-group resolution, and owner draft confirmation state machine.

Design invariants:
- Group outbound delivery is ALWAYS executed by the primary group-capable bot (大肥鱼 via wecom-cli)
  after live `message aibot sessions list` verification of `chat_type == 'group'`.
- Colleague messages NEVER enter T3; non-owner group @mentions are pushed privately to the owner
  with 0 LLM calls.
- Owner private commands (【直回】, 【直发群】, 发 / 【改发】 / 不发) are 100% deterministic 0-LLM.
- Every published group payload passes credential and local-path redaction checks and is sent at most once.
"""
from __future__ import annotations

from hashlib import sha256
import json
import re
import time

import conclusion_notes
import group_replies
from identity import resolve

FOLLOWUP_ID_RE = re.compile(r'(?<![A-Za-z0-9_])F-([0-9a-f]{6})(?![A-Za-z0-9_])', re.I)
DRAFT_ID_RE = re.compile(r'(?<![A-Za-z0-9_])(D-[0-9a-f]{4,8})(?![A-Za-z0-9_])', re.I)

DIRECT_REPLY_CMD = re.compile(
    r'^【直回(?:[\s：:]+F-([0-9a-f]{6}))?】[\s：:]*(?:F-([0-9a-f]{6})[\s：:]+)?([\s\S]+)$',
    re.I,
)
DIRECT_REPLY_ID_CMD = re.compile(
    r'^直回[\s：:]+F-([0-9a-f]{6})[\s：:]+([\s\S]+)$',
    re.I,
)
QUOTE_DIRECT_REPLY_RE = re.compile(
    r'^直回[\s：:]+([\s\S]+)$',
    re.I,
)
EMPTY_DIRECT_REPLY_RE = re.compile(
    r'^(?:【)?直回(?:[\s：:]+F-([0-9a-f]{6}))?(?:】)?[。！!\s：:]*$',
    re.I,
)
DIRECT_GROUP_CMD = re.compile(
    r'^【直发群[：:]\s*([^】]{1,60})】[\s：:]*([\s\S]+)$',
    re.I,
)
IGNORE_FOLLOWUP_CMD = re.compile(
    r'^(?:【(?:忽略|跳过|无需处理|不用管|作废)(?:[\s：:]+F-([0-9a-f]{6}))?】|(?:忽略|跳过|无需处理)[\s：:]+F-([0-9a-f]{6}))(?:[\s：:]+F-([0-9a-f]{6}))?[。！!]*$',
    re.I,
)
QUOTE_IGNORE_FOLLOWUP_RE = re.compile(
    r'^(?:【)?(?:忽略|跳过|无需处理|不用管|不用理|已处理|已回|作废)(?:】)?[。！!]*$',
    re.I,
)
TAG_GROUP_TARGET = re.compile(
    r'【(?:发群|回群|回报群|发到群)[：:]\s*([^】]{1,60})】',
    re.I,
)
DIRECT_SEND_OVERRIDE = re.compile(
    r'(?:无需确认直接发|不用确认直接发|直接发不审稿|不用审稿直接发|自动直发(?:原群|群里|目标群)?|直接发过去不用问我)'
)
NEGATED_DIRECT_SEND = re.compile(
    r'(?:不要|别|不用|禁止|切勿|勿)(?:直接发|自动直发|免审直发|不审稿直接发|直接发过去)'
)
ORIG_GROUP_WORDS = re.compile(r'(?:回原群|发原群|发回原群|回到原群|发到原群|回复原群|答复原群|发给原群)')
NATURAL_GROUP_DEST = re.compile(
    r'(?:发到|发往|发回|回复到|回到|同步到|投递到|回答到|发给)\s*[「『【《"\']?\s*'
    r'([^「『【《"\'，,。；;！!\n]{2,40}?)\s*[」』】》"\']?(?:群里|里边|里面|里)?(?:吧|啊|呀)?(?=$|[，,。；;！!\s])'
)
IN_GROUP_ANSWER = re.compile(
    r'在\s*[「『【《"\']?\s*([^「『【《"\'，,。；;！!\n]{2,40}?群)\s*[」』】》"\']?(?:里边|里面|里)?\s*(?:回答|回复|发(?:送)?|公布|说明)'
)
VAGUE_GROUP_FALLBACK = re.compile(
    r'(?<![别不无])(?:发|发到|发往|发回|发给|回复到|回|回到|同步到|投递到|回答到)\s*(那个群|这个群|群里|原群)(?:里边|里面|里)?(?:吧|啊|呀)?(?=$|[，,。；;！!\s])'
)
NEGATED_GROUP_SEND = re.compile(r'(?:别|不要|不用|无需|切勿)(?:发|回|发到|发回|同步到|回复到).{0,6}群')
NON_GROUP_RECIPIENT = re.compile(
    r'(?i)(?:私聊|邮件|邮箱|文档|表格|面板|小爱|本地|电脑|手机|claude|克劳德|大肥鱼|机器人|bot|他们|她们|别人|对方|大家|同事们?)'
)
FULL_LOCAL_PATH_RE = re.compile(
    r'(?i)(?:\bfile:[^\s<>"\'\]\)）。，]+|\bsandbox:[^\s<>"\'\]\)）。，]+|[a-z]:[\\/][^\s<>"\'\]\)）。，]+|/(?:Users|home|mnt)/[^\s<>"\'\]\)）。，]+|\.codex[\\/][^\s<>"\'\]\)）。，]+)'
)
MENTION_RE = re.compile(r'[@＠]([^\s，,。！!？?：:]{2,32})')
TAIL_ID_RE = re.compile(r'尾号\s*([A-Za-z0-9_-]{6,12})')
CHAT_ID_RE = re.compile(r'(wrk[A-Za-z0-9_-]{10,})')
CLARIFY_QUOTE_RE = re.compile(r'(?:\bQ-[0-9a-f]{4,8}\b|(?:需要确认|追问)〔[0-9a-f]{7}〕)', re.I)
TASK_RECEIPT_QUOTE_RE = re.compile(
    r'^(?:Windows 接手 · [^\n]*\n)?(?:(?:追加|补发)?办理结果|已收到(?:确认)?|已收齐|正在收集|事项)〔[0-9a-f]{7}〕',
    re.I,
)
VAGUE_GROUP_REFS = {'群里', '原群', '那个群', '这个群', '群'}
ABSTRACT_GROUP_NAMES = {'某群', '某某群', '指定群', '特定群', '相应群', '对应群'}
PRIVATE_OR_SELF_NAMES = {
    '我', '本人', '私聊', '本人私聊', '我私聊', '我的私聊', '原私聊', '本私聊', '这里', '原会话', '原路',
    'claude', '克劳德', '大肥鱼', '他们', '她们', '别人', '对方', '同事', '同事们',
}
VAGUE_GROUP_NAMES = VAGUE_GROUP_REFS | ABSTRACT_GROUP_NAMES | PRIVATE_OR_SELF_NAMES

CONFIRM_SEND_RE = re.compile(
    r'^(?:【)?(?:发|发送|确认发|确认发送|同意发送|发吧)(?:[\s：:]+(D-[0-9a-f]{4,8}))?(?:】)?(?:\s+(D-[0-9a-f]{4,8}))?[。！!]*$',
    re.I,
)
QUOTE_CONFIRM_RE = re.compile(r'^(?:发|发送|确认|可以|ok|发吧|好的发|行发)[。！!]*$', re.I)
EDIT_SEND_RE = re.compile(
    r'^【(?:改发|修改并发送)(?:[\s：:]+(D-[0-9a-f]{4,8}))?】[\s：:]*(?:(D-[0-9a-f]{4,8})[\s：:]+)?([\s\S]+)$',
    re.I,
)
CANCEL_DRAFT_RE = re.compile(
    r'^(?:【)?(?:不发|取消发|取消发送|作废草稿)(?:[\s：:]+(D-[0-9a-f]{4,8}))?(?:】)?(?:\s+(D-[0-9a-f]{4,8}))?[。！!]*$',
    re.I,
)
QUOTE_CANCEL_RE = re.compile(r'^(?:不发|取消|算了|别发|不要发)[。！!]*$', re.I)


def install(c):
    c.execute("""CREATE TABLE IF NOT EXISTS group_drafts(
        draft_id TEXT PRIMARY KEY,
        case_id TEXT NOT NULL,
        bot_channel TEXT NOT NULL DEFAULT 'feiyu',
        owner_chat TEXT NOT NULL,
        owner_sender TEXT NOT NULL,
        target_chat_id TEXT NOT NULL,
        target_chat_name TEXT NOT NULL,
        followup_id TEXT,
        body TEXT NOT NULL,
        body_sha256 TEXT NOT NULL,
        state TEXT NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        sent_at REAL,
        detail TEXT DEFAULT ''
    )""")
    c.execute('CREATE INDEX IF NOT EXISTS group_drafts_owner ON group_drafts(owner_chat, bot_channel, state, created_at)')


def _make_draft_id(seed, prefix='D'):
    return f'{prefix}-' + sha256(f'{seed}:{time.time_ns()}'.encode('utf-8')).hexdigest()[:4].upper()


def list_groups(call=None):
    """Fetch live group sessions visible to the primary group bot (大肥鱼)."""
    if call is None:
        from wecom_cli import cli_call
        call = cli_call
    try:
        data = call(['message', 'aibot', 'sessions', 'list'])
    except Exception:
        return []
    rows = data.get('sessions', []) if isinstance(data, dict) else []
    out = []
    for s in rows:
        if isinstance(s, dict) and s.get('chat_type') == 'group' and isinstance(s.get('chat_id'), str) and s['chat_id']:
            out.append({
                'chat_id': s['chat_id'],
                'chat_name': (s.get('chat_name') or '').strip(),
                'last_msg_time': s.get('last_msg_time') or '',
            })
    return out


def group_display_name(chat_id, call=None, sessions=None):
    groups = sessions if sessions is not None else list_groups(call)
    for g in groups:
        if g['chat_id'] == chat_id:
            return g['chat_name'] or f'未命名群（尾号 {chat_id[-8:]}）'
    return f'原群（尾号 {chat_id[-8:]}）'


def match_group_name(query, groups):
    """Return (matched_group_or_None, candidates_list). Strict exact-first matching."""
    q = (query or '').strip().strip('「」『』【】《》"\' ')
    if not q:
        return None, groups
    # 1. Exact chat_id match
    by_id = [g for g in groups if g['chat_id'] == q]
    if len(by_id) == 1:
        return by_id[0], by_id
    # 2. Exact chat_name match (case-insensitive)
    qlow = q.lower()
    exact = [g for g in groups if g['chat_name'] and g['chat_name'].lower() == qlow]
    if len(exact) == 1:
        return exact[0], exact
    if len(exact) > 1:
        return None, exact
    # 3. Exact match after stripping trailing '群' on query if name doesn't end with '群'
    if qlow.endswith('群') and len(qlow) >= 3:
        qstem = qlow[:-1]
        stem_exact = [g for g in groups if g['chat_name'] and g['chat_name'].lower() == qstem]
        if len(stem_exact) == 1:
            return stem_exact[0], stem_exact
    # 4. Unique substring match (only if >= 2 chars and strictly 1 hit)
    if len(qlow) >= 2:
        sub = [g for g in groups if g['chat_name'] and (qlow in g['chat_name'].lower() or (qlow.endswith('群') and len(qlow) >= 3 and qlow[:-1] in g['chat_name'].lower()))]
        if len(sub) == 1:
            return sub[0], sub
        if len(sub) > 1:
            return None, sub
    return None, groups


def validate_group_body(text, target_chat_id):
    text = (text or '').strip()
    if not text:
        raise ValueError('发群正文不能为空')
    if len(text.encode('utf-8')) > 20480:
        raise ValueError('发群正文超过企微 20480 字节上限，请精简后再发')
    if group_replies.contains_credentials(text):
        raise ValueError('正文包含疑似密码/密钥/访问令牌，已拦截发送')
    if conclusion_notes.LOCAL_PATH_RE.search(text):
        raise ValueError('正文包含本机私有路径，已拦截发送（请先去除本机路径再发）')
    return text


def _sanitize_clue_text(text, max_len=90):
    raw = (text or '').strip()
    if not raw:
        return ''
    raw = re.sub(r'^\s*[@＠]大肥鱼(?=$|[\s\u2005\u200b，,。！!？?：:【])[\s\u2005\u200b]*', '', raw, count=1).strip()
    if not raw or group_replies.contains_credentials(raw):
        return ''
    raw = FULL_LOCAL_PATH_RE.sub('[路径]', raw)
    raw = conclusion_notes.LOCAL_PATH_RE.sub('[路径]', raw)
    raw = ' '.join(raw.split())
    return raw[:max_len]


def _extract_mentions(text):
    out = []
    for m in MENTION_RE.finditer(text or ''):
        tok = m.group(1).strip(' ，,。！!？?：:[]【】')
        while tok.endswith(')') and tok.count(')') > tok.count('('):
            tok = tok[:-1]
        while tok.endswith('）') and tok.count('）') > tok.count('（'):
            tok = tok[:-1]
        tok = tok.lstrip('(（')
        if not tok or tok == '大肥鱼':
            continue
        label = '@' + tok
        if label not in out:
            out.append(label)
    return out


def collect_group_clues(c, groups, *, max_groups=None, max_snippets=4):
    """Enrich live group sessions with compact, redacted local recent speaker/topic clues."""
    enriched = []
    has_followups = bool(
        c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='colleague_followups'").fetchone()
    )
    has_sources = bool(
        c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sources'").fetchone()
    )
    target_list = (groups or []) if max_groups is None else (groups or [])[:max_groups]
    for g in target_list:
        cid = g['chat_id']
        cname = (g.get('chat_name') or '').strip()
        display = cname or f'未命名群（尾号 {cid[-8:]}）'
        speakers = []
        mentions = []
        snippets = []
        followups = []

        if has_sources:
            rows = c.execute(
                """SELECT sender, epoch, item FROM sources
                   WHERE chat=? AND json_extract(item, '$.chattype')='group'
                   ORDER BY epoch DESC LIMIT 12""",
                (cid,),
            ).fetchall()
            for r in rows:
                snd = (r['sender'] or '').strip()
                if snd and snd not in speakers and len(speakers) < 6:
                    speakers.append(snd)
                try:
                    item = json.loads(r['item'])
                except (TypeError, ValueError):
                    continue
                direct = item.get('direct') or ''
                quotes = [q for q in (item.get('quotes') or []) if isinstance(q, str)]
                # Sanitize & credential-filter BEFORE extracting any mentions or snippets!
                for blob in (direct, *quotes):
                    if not blob or group_replies.contains_credentials(blob):
                        continue
                    safe_blob = FULL_LOCAL_PATH_RE.sub('[路径]', blob)
                    safe_blob = conclusion_notes.LOCAL_PATH_RE.sub('[路径]', safe_blob)
                    for mn in _extract_mentions(safe_blob):
                        if mn not in mentions and len(mentions) < 8:
                            mentions.append(mn)
                cleaned = _sanitize_clue_text(direct, max_len=90)
                if cleaned and len(snippets) < max_snippets:
                    ts = (item.get('received_at') or '')[5:16].replace('T', ' ')
                    prefix = f'[{ts} {snd}] ' if ts and snd else (f'[{snd}] ' if snd else '')
                    entry = prefix + cleaned
                    if entry not in snippets:
                        snippets.append(entry)

        if has_followups:
            frows = c.execute(
                """SELECT id, sender, question, status FROM colleague_followups
                   WHERE chat=? AND status='open' ORDER BY created_at DESC LIMIT 4""",
                (cid,),
            ).fetchall()
            for fr in frows:
                qclean = _sanitize_clue_text(fr['question'] or '', max_len=80)
                followups.append({
                    'code': 'F-' + str(fr['id'])[-6:].upper(),
                    'sender': fr['sender'] or '',
                    'status': fr['status'] or 'open',
                    'question': qclean,
                })
                if fr['sender'] and fr['sender'] not in speakers and len(speakers) < 6:
                    speakers.append(fr['sender'])

        enriched.append({
            'chat_id': cid,
            'chat_name': cname,
            'display_name': display,
            'last_msg_time': g.get('last_msg_time') or '',
            'recent_speakers': speakers,
            'mentioned_names': mentions,
            'recent_snippets': snippets,
            'open_followups': followups,
        })
    return enriched


def _is_subsequence(needle, haystack):
    if not needle or not haystack or len(needle) > len(haystack):
        return False
    it = iter(haystack)
    return all(ch in it for ch in needle)


def _clue_tokens(text):
    raw = (text or '').lower()
    raw = re.sub(r'(?:那个群|这个群|群里|原群|所在群|的群|里的|里边|里面|发到|发往|发回|回复到|发给)', ' ', raw)
    tokens = []
    for tok in re.findall(r'[a-z0-9_]{2,}', raw):
        if tok not in tokens:
            tokens.append(tok)
    for seg in re.findall(r'[\u4e00-\u9fff]{2,}', raw):
        if seg not in tokens:
            tokens.append(seg)
        for i in range(len(seg) - 1):
            bg = seg[i:i + 2]
            if bg not in tokens:
                tokens.append(bg)
    return tokens


def rank_group_candidates(query, enriched_groups, *, context_text='', limit=3):
    """Heuristic ranking helper for ordering manifest groups & fail-closed <=3 fallback options.

    Never used to auto-bind a group without model/owner verification.
    """
    if not enriched_groups:
        return []
    q = (query or '').strip().strip('「」『』【】《》"\' ').lower()
    qstem = re.sub(r'(?:那个群|这个群|所在群|群里|的群|群)$', '', q).strip()
    q_tokens = _clue_tokens(q)
    ctx_tokens = _clue_tokens(context_text)[:16]

    scored = []
    for idx, g in enumerate(enriched_groups):
        name_low = (g.get('chat_name') or '').lower()
        mentions_blob = ' '.join(g.get('mentioned_names') or []).lower()
        snippets_blob = ' '.join(g.get('recent_snippets') or []).lower()
        follow_blob = ' '.join(
            f"{f.get('code', '')} {f.get('sender', '')} {f.get('question', '')}"
            for f in (g.get('open_followups') or [])
        ).lower()
        speakers_blob = ' '.join(g.get('recent_speakers') or []).lower()
        clue_blob = f'{mentions_blob} {snippets_blob} {follow_blob} {speakers_blob}'

        score = 0.0
        if qstem and len(qstem) >= 2 and name_low:
            if qstem == name_low:
                score += 100.0
            elif qstem in name_low or name_low in qstem:
                score += 60.0
            elif _is_subsequence(qstem, name_low):
                score += 45.0
        for tok in q_tokens:
            if name_low and tok in name_low:
                score += 18.0 if len(tok) >= 3 else 10.0
            if mentions_blob and tok in mentions_blob:
                score += 22.0
            if snippets_blob and tok in snippets_blob:
                score += 12.0 if len(tok) >= 3 else 6.0
            if follow_blob and tok in follow_blob:
                score += 14.0
            if speakers_blob and tok in speakers_blob:
                score += 16.0
        for tok in ctx_tokens:
            if name_low and tok in name_low:
                score += 3.0
            if clue_blob and tok in clue_blob:
                score += 2.0
        # Mild recency tie-breaker preserving original sessions-list order
        score += max(0.0, 0.5 - idx * 0.02)
        scored.append((score, idx, g))

    scored.sort(key=lambda item: (-item[0], item[1]))
    if limit is None:
        return [item[2] for item in scored]
    return [item[2] for item in scored[:max(1, limit)]]


def format_group_options(enriched_groups, *, limit=3):
    """Format up to `limit` (default 3) concise group choices plus private-only fallback, with bindings."""
    opts, bindings = format_group_options_with_bindings(enriched_groups, limit=limit)
    return opts


def format_group_options_with_bindings(enriched_groups, *, limit=3):
    """Return (options_list, bindings_list) mapping each option position to a verified chat_id or cancel_group."""
    opts = []
    bindings = []
    for g in (enriched_groups or [])[:max(1, limit)]:
        label = g.get('display_name') or g.get('chat_name') or f'未命名群（尾号 {g["chat_id"][-8:]}）'
        clue = ''
        if not g.get('chat_name'):
            if g.get('mentioned_names'):
                clue = '近期提及 ' + '/'.join(g['mentioned_names'][:2])
            elif g.get('recent_snippets'):
                clue = g['recent_snippets'][0][:36]
        elif g.get('open_followups'):
            clue = f"待跟进 {g['open_followups'][0]['code']}"
        text = f'{label}（{clue}）' if clue else label
        opts.append(text)
        bindings.append({
            'label': text,
            'chat_id': g['chat_id'],
            'chat_name': g.get('chat_name') or f'未命名群（尾号 {g["chat_id"][-8:]}）',
            'cancel_group': False,
        })
    priv = '只回我私聊，不发群'
    opts.append(priv)
    bindings.append({'label': priv, 'chat_id': None, 'chat_name': '', 'cancel_group': True})
    return opts, bindings


def _match_option_to_live_group(option_text, live_groups):
    """Resolve one option label (from a model or bridge ASK) to a live group dict, or None."""
    text = str(option_text or '').strip()
    if not text:
        return None
    m_cid = CHAT_ID_RE.search(text)
    if m_cid:
        cid = m_cid.group(1)
        hits = [g for g in live_groups if g['chat_id'] == cid]
        if len(hits) == 1:
            return hits[0]
    m_tail = TAIL_ID_RE.search(text)
    if m_tail:
        tail = m_tail.group(1)
        hits = [g for g in live_groups if g['chat_id'].endswith(tail)]
        if len(hits) == 1:
            return hits[0]
    # Strip leading letter prefix ("A. ") and trailing parenthetical clue ("（...）")
    clean = re.sub(r'^[A-H1-9][.、:：\s]+', '', text).strip()
    head = re.split(r'[（(·|]', clean, maxsplit=1)[0].strip()
    for probe in (head, clean):
        if not probe or probe.startswith('未命名群'):
            continue
        hit, _ = match_group_name(probe, live_groups)
        if hit:
            return hit
    return None


def _is_private_option(text):
    return bool(re.search(r'(?:只回(?:我)?私聊|仅(?:回)?私聊|不发群|私聊给我)', str(text or '')))


def normalize_target_group_questions(questions, live_groups, enriched_groups=None, *, query='', context_text=''):
    """Enforce <=3 candidate groups + private option & attach authoritative option_bindings for target_group questions."""
    if not isinstance(questions, list):
        return questions
    out = []
    for q in questions:
        if not isinstance(q, dict):
            out.append(q)
            continue
        qid = str(q.get('id') or '').lower()
        qtext = str(q.get('question') or q.get('text') or '')
        opts = q.get('options') if isinstance(q.get('options'), list) else []
        looks_group_q = (
            qid in ('target_group', 'group', 'target_chat')
            or ('群' in qtext and opts)
            or any(_match_option_to_live_group(o, live_groups) is not None for o in opts)
        )
        if not looks_group_q:
            out.append(q)
            continue
        new_opts = []
        bindings = []
        seen_ids = set()
        for raw_opt in opts:
            s = str(raw_opt).strip()
            if not s or _is_private_option(s):
                continue
            hit = _match_option_to_live_group(s, live_groups)
            if hit:
                if hit['chat_id'] in seen_ids:
                    continue
                seen_ids.add(hit['chat_id'])
                if len(new_opts) < 3:
                    new_opts.append(s)
                    bindings.append({
                        'label': s,
                        'chat_id': hit['chat_id'],
                        'chat_name': hit['chat_name'] or f'未命名群（尾号 {hit["chat_id"][-8:]}）',
                        'cancel_group': False,
                    })
        if not new_opts and enriched_groups:
            ranked = rank_group_candidates(query, enriched_groups, context_text=context_text, limit=3)
            new_opts, bindings = format_group_options_with_bindings(ranked, limit=3)
        else:
            priv = '只回我私聊，不发群'
            new_opts.append(priv)
            bindings.append({'label': priv, 'chat_id': None, 'chat_name': '', 'cancel_group': True})
        out.append({
            **q,
            'id': q.get('id') or 'target_group',
            'options': new_opts,
            'option_bindings': bindings,
        })
    return out


def resolve_owner_clarification_choice(clar_history, live_groups, *, mode='draft'):
    """Authoritatively resolve the owner's latest answered target_group clarification choice.

    Returns:
      - {'cancel_group': True, 'reason': ...} when owner explicitly selected private-only;
      - Verified group dict {'chat_id', 'chat_name', ..., 'resolution': 'owner_clarified'} when owner
        unambiguously selected one of the bound options (or named a unique live group);
      - None when no answered target_group clarification unambiguously dictates the choice.
    """
    for h in reversed(clar_history or ()):
        if h.get('state') != 'answered':
            continue
        ans = str(h.get('answer') or '').strip()
        if not ans:
            continue
        questions = h.get('questions') or []
        t_q = None
        for q in questions:
            if isinstance(q, dict) and (q.get('option_bindings') or str(q.get('id') or '').lower() in ('target_group', 'group', 'target_chat') or '群' in str(q.get('question') or '')):
                t_q = q
                break
        if t_q is None and h.get('origin') not in ('agent_target_group_verify', 'pregate_group_target'):
            continue

        # 1. Direct private-only phrase from owner ALWAYS wins!
        if _is_private_option(ans):
            return {'cancel_group': True, 'reason': '主人追问答复明确选择仅回私聊'}

        bindings = t_q.get('option_bindings') if isinstance(t_q, dict) else None
        opts = (t_q.get('options') or []) if isinstance(t_q, dict) else []

        # 2. Letter / number / ordinal selection ("A" / "选B" / "2" / "第一个")
        m_sel = re.fullmatch(r'(?:选\s*|方案\s*)?([A-Ha-h]|[1-8]|第([一二三四五1-5])个?)(?:[\s，,。].*)?', ans)
        if m_sel:
            idx = None
            if m_sel.group(2):
                ord_map = {'一': 0, '二': 1, '三': 2, '四': 3, '五': 4, '1': 0, '2': 1, '3': 2, '4': 3, '5': 4}
                idx = ord_map.get(m_sel.group(2))
            else:
                tok = m_sel.group(1).upper()
                idx = (ord(tok) - ord('A')) if tok.isalpha() else (int(tok) - 1)
            if idx is not None:
                if isinstance(bindings, list) and 0 <= idx < len(bindings):
                    b = bindings[idx]
                    if b.get('cancel_group'):
                        return {'cancel_group': True, 'reason': '主人追问选定「只回我私聊，不发群」'}
                    cid = b.get('chat_id')
                    if cid:
                        hit = next((g for g in live_groups if g['chat_id'] == cid), None)
                        if hit:
                            return {
                                'chat_id': hit['chat_id'],
                                'chat_name': hit['chat_name'] or f'未命名群（尾号 {hit["chat_id"][-8:]}）',
                                'followup_id': None,
                                'mode': 'direct' if mode == 'direct' else 'draft',
                                'resolution': 'owner_clarified',
                                'reason': f'主人追问确认选定第 {idx + 1} 项',
                            }
                elif 0 <= idx < len(opts):
                    chosen_text = str(opts[idx])
                    if _is_private_option(chosen_text):
                        return {'cancel_group': True, 'reason': '主人追问选定「只回我私聊，不发群」'}
                    hit = _match_option_to_live_group(chosen_text, live_groups)
                    if hit:
                        return {
                            'chat_id': hit['chat_id'],
                            'chat_name': hit['chat_name'] or f'未命名群（尾号 {hit["chat_id"][-8:]}）',
                            'followup_id': None,
                            'mode': 'direct' if mode == 'direct' else 'draft',
                            'resolution': 'owner_clarified',
                            'reason': f'主人追问确认选定第 {idx + 1} 项',
                        }

        # 3. Direct group name / chat_id / tail ID typed by owner
        hit = _match_option_to_live_group(ans, live_groups)
        if hit:
            return {
                'chat_id': hit['chat_id'],
                'chat_name': hit['chat_name'] or f'未命名群（尾号 {hit["chat_id"][-8:]}）',
                'followup_id': None,
                'mode': 'direct' if mode == 'direct' else 'draft',
                'resolution': 'owner_clarified',
                'reason': '主人追问答复明确指定目标群',
            }
        # Only inspect the most recent target_group clarification round
        break
    return None


def validate_model_target_group(selection, call=None, *, groups=None, mode='draft'):
    """Strictly verify model-selected target_group against live 大肥鱼 group sessions.

    Returns:
      - {'cancel_group': True, 'reason': ...} if model/owner chose private-only;
      - Verified group dict {'chat_id', 'chat_name', 'followup_id', 'mode', 'resolution', 'reason'}
        ONLY when chat_id (or unique chat_name) exists in live `chat_type == 'group'` sessions;
      - None if missing, invalid, ambiguous, or unverified (caller MUST fail closed).
    """
    if not isinstance(selection, dict):
        return None
    reason = str(selection.get('reason') or '').strip()[:200]
    if selection.get('cancel_group') is True:
        return {'cancel_group': True, 'reason': reason or '选择仅回私聊'}

    live = groups if groups is not None else list_groups(call)
    if not live:
        return None

    chat_id = str(selection.get('chat_id') or '').strip()
    chat_name = str(selection.get('chat_name') or '').strip()
    hit = None
    if chat_id:
        matches = [g for g in live if g['chat_id'] == chat_id]
        if len(matches) != 1:
            return None
        hit = matches[0]
    elif chat_name:
        hit, _ = match_group_name(chat_name, live)
        if not hit:
            return None
    else:
        return None

    return {
        'chat_id': hit['chat_id'],
        'chat_name': hit['chat_name'] or f'未命名群（尾号 {hit["chat_id"][-8:]}）',
        'followup_id': str(selection.get('followup_id') or '').strip() or None,
        'mode': 'direct' if mode == 'direct' else 'draft',
        'resolution': 'model_selected',
        'reason': reason,
    }


def parse_group_target(text, quotes=()):
    """Extract cross-group target specification from an owner's private message, or None."""
    raw = (text or '').strip()
    if not raw:
        return None
    mode = 'direct' if (DIRECT_SEND_OVERRIDE.search(raw) and not NEGATED_DIRECT_SEND.search(raw)) else 'draft'

    # 1. Explicit 【发群：...】 / 【回群：...】 tag
    m_tag = TAG_GROUP_TARGET.search(raw)
    if m_tag:
        val = m_tag.group(1).strip()
        mf = FOLLOWUP_ID_RE.search(val)
        if mf:
            return {'kind': 'followup', 'value': 'F-' + mf.group(1).upper(), 'mode': mode}
        return {'kind': 'name', 'value': val, 'mode': mode}

    # 2. Explicit F-xxxxxx reference in text
    m_fid = FOLLOWUP_ID_RE.search(raw)
    if m_fid and re.search(r'发|回|答|群|处理|查', raw):
        return {'kind': 'followup', 'value': 'F-' + m_fid.group(1).upper(), 'mode': mode}

    # 3. Quoting a push card 【群友提问待处理 · F-xxxxxx】 + 原群 intent
    quoted_fids = []
    for q in (quotes or ()):
        if isinstance(q, str):
            for hit in FOLLOWUP_ID_RE.findall(q):
                quoted_fids.append('F-' + hit.upper())
    if quoted_fids and (ORIG_GROUP_WORDS.search(raw) or re.search(r'(?:发|回|答).{0,4}群', raw)):
        return {'kind': 'followup', 'value': quoted_fids[0], 'mode': mode}

    if NEGATED_GROUP_SEND.search(raw):
        return None

    # 4. Natural group name ("发到AI测试群" / "发到男主适配" / "发到peter的那个群" / "在交互系统优化群群里回答")
    for pattern in (IN_GROUP_ANSWER, NATURAL_GROUP_DEST):
        for m in pattern.finditer(raw):
            cand = m.group(1).strip(' ，,。；;！!：:「」『』【】《》"\'')
            cand_low = cand.lower()
            if not cand or cand_low in PRIVATE_OR_SELF_NAMES or cand in ABSTRACT_GROUP_NAMES:
                continue
            if cand in VAGUE_GROUP_REFS:
                return {'kind': 'vague', 'value': cand, 'mode': mode}
            # Require either explicit "群" cue or not clearly a private/bot/person/file noun
            if NON_GROUP_RECIPIENT.search(cand):
                continue
            mf = FOLLOWUP_ID_RE.search(cand)
            if mf:
                return {'kind': 'followup', 'value': 'F-' + mf.group(1).upper(), 'mode': mode}
            # Avoid matching arbitrary sentences unless it mentions 群 or came from 发到/回复到/回到 + short name
            if '群' in cand or '群' in raw[max(0, m.start() - 4):min(len(raw), m.end() + 4)] or (
                pattern is NATURAL_GROUP_DEST and len(cand) <= 18 and not re.search(r'[的了呢吗吧呀]', cand)
            ):
                return {'kind': 'name', 'value': cand, 'mode': mode}

    # 5. Vague group destination ("发群里" / "发到那个群" / "回到群里") -> let model inspect recent clues & narrow <= 3
    m_vague = VAGUE_GROUP_FALLBACK.search(raw)
    if m_vague:
        return {'kind': 'vague', 'value': m_vague.group(1), 'mode': mode}

    return None


def resolve_followup(c, code):
    m = FOLLOWUP_ID_RE.search(code or '')
    if not m:
        return None
    suffix = m.group(1).lower()
    rows = c.execute(
        'SELECT * FROM colleague_followups WHERE lower(substr(id, -6))=? ORDER BY created_at DESC LIMIT 2',
        (suffix,),
    ).fetchall()
    return rows[0] if len(rows) == 1 else None


def resolve_target_for_case(c, root, anchor, clar_history=(), call=None):
    """Resolve owner's cross-group target: fast-path deterministic hit, or hand live clues to T3 model."""
    install(c)
    text = anchor.get('direct') or ''
    quotes = anchor.get('quotes') or []
    spec = parse_group_target(text, quotes)

    # Check if a clarification answer just chose a specific group name/ID (legacy pregate compatibility)
    chosen_answer = ''
    for h in reversed(clar_history or ()):
        if h.get('origin') == 'pregate_group_target' and h.get('state') == 'answered' and h.get('answer'):
            chosen_answer = str(h['answer']).strip()
            break

    groups = list_groups(call)
    followup_id = None
    mode = spec['mode'] if spec else 'draft'

    if chosen_answer:
        prev_q = next((h for h in reversed(clar_history) if h.get('origin') == 'pregate_group_target'), None)
        opts = (prev_q.get('questions') or [{}])[0].get('options') if prev_q else []
        m_letter = re.fullmatch(r'(?:选\s*)?([A-Ha-h]|[1-8])(?:[\s，,。].*)?', chosen_answer)
        query = chosen_answer
        if m_letter and opts:
            tok = m_letter.group(1).upper()
            idx = (ord(tok) - ord('A')) if tok.isalpha() else (int(tok) - 1)
            if 0 <= idx < len(opts):
                query = str(opts[idx])
        if '只回我私聊' in query or '不发群' in query:
            return {'cancel_group': True}
        m_cid = re.search(r'(wrk[A-Za-z0-9_-]{10,})', query)
        if m_cid:
            query = m_cid.group(1)
        hit, _ = match_group_name(query, groups)
        if hit:
            return {
                'chat_id': hit['chat_id'],
                'chat_name': hit['chat_name'] or f'未命名群（尾号 {hit["chat_id"][-8:]}）',
                'followup_id': None,
                'mode': mode,
                'resolution': 'deterministic',
            }

    if not spec:
        return None

    # Fast path 1: verified F-xxxxxx followup whose group is live in 大肥鱼 sessions list
    if spec['kind'] == 'followup':
        f_row = resolve_followup(c, spec['value'])
        if f_row:
            followup_id = f_row['id']
            chat_id = f_row['chat']
            hit = next((g for g in groups if g['chat_id'] == chat_id), None)
            if hit:
                return {
                    'chat_id': hit['chat_id'],
                    'chat_name': hit['chat_name'] or f'未命名群（尾号 {hit["chat_id"][-8:]}）',
                    'followup_id': followup_id,
                    'mode': mode,
                    'resolution': 'deterministic',
                }
        reason = (
            f'原提问群（编号 {spec["value"]}）当前未出现在大肥鱼 sessions list 中'
            if f_row else f'未找到对应的群友提问编号 {spec["value"]}'
        )
        enriched = collect_group_clues(c, groups)
        ordered = rank_group_candidates(spec['value'], enriched, context_text=text, limit=None)
        return {
            'pending_model_selection': {
                'status': 'pending_model_selection',
                'kind': 'followup',
                'query': spec['value'],
                'mode': mode,
                'reason': reason,
                'deterministic_candidates': [],
                'suggested_top3': [g['chat_id'] for g in ordered[:3]],
                'available_groups': ordered[:24],
            }
        }

    # Fast path 2: exact chat_id / exact chat_name / unique contiguous substring
    if spec['kind'] == 'name':
        hit, cands = match_group_name(spec['value'], groups)
        if hit:
            return {
                'chat_id': hit['chat_id'],
                'chat_name': hit['chat_name'] or f'未命名群（尾号 {hit["chat_id"][-8:]}）',
                'followup_id': None,
                'mode': mode,
                'resolution': 'deterministic',
            }
        enriched = collect_group_clues(c, groups)
        ordered = rank_group_candidates(spec['value'], enriched, context_text=text, limit=None)
        det_ids = [g['chat_id'] for g in cands[:6]] if (cands and len(cands) < len(groups)) else []
        reason = (
            f'群名「{spec["value"]}」存在多个字面匹配，需由模型结合语义与近期群线索判断或缩窄至 ≤3 候选'
            if det_ids else
            f'群名「{spec["value"]}」无精确/连续子串命中，需由模型结合跳字缩写、成员/发言线索判断或缩窄至 ≤3 候选'
        )
        return {
            'pending_model_selection': {
                'status': 'pending_model_selection',
                'kind': 'name',
                'query': spec['value'],
                'mode': mode,
                'reason': reason,
                'deterministic_candidates': det_ids,
                'suggested_top3': [g['chat_id'] for g in ordered[:3]],
                'available_groups': ordered[:24],
            }
        }

    # Vague group reference ("发到那个群" / "发群里") -> hand live group clues to model
    enriched = collect_group_clues(c, groups)
    ordered = rank_group_candidates(spec['value'], enriched, context_text=text, limit=None)
    return {
        'pending_model_selection': {
            'status': 'pending_model_selection',
            'kind': 'vague',
            'query': spec['value'],
            'mode': mode,
            'reason': f'目标群表述「{spec["value"]}」未写明群名，需由模型结合上下文与近期群线索判断或缩窄至 ≤3 候选',
            'deterministic_candidates': [],
            'suggested_top3': [g['chat_id'] for g in ordered[:3]],
            'available_groups': ordered[:24],
        }
    }


def create_draft(c, *, case_id, bot_channel, owner_chat, owner_sender, target_chat_id, target_chat_name, body, followup_id=None, state='pending_confirm', now=None):
    install(c)
    now = time.time() if now is None else now
    existing = c.execute(
        'SELECT * FROM group_drafts WHERE case_id=? ORDER BY created_at DESC LIMIT 1',
        (case_id,),
    ).fetchone()
    if existing:
        return dict(existing)
    did = _make_draft_id(f'{case_id}:{target_chat_id}')
    while c.execute('SELECT 1 FROM group_drafts WHERE draft_id=?', (did,)).fetchone():
        did = _make_draft_id(f'{case_id}:{did}:{time.time_ns()}')
    digest = sha256(body.encode('utf-8')).hexdigest()
    c.execute(
        """INSERT INTO group_drafts(
               draft_id, case_id, bot_channel, owner_chat, owner_sender,
               target_chat_id, target_chat_name, followup_id, body, body_sha256,
               state, created_at, updated_at
           ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (did, case_id, bot_channel or 'feiyu', owner_chat, owner_sender,
         target_chat_id, target_chat_name, followup_id, body, digest, state, now, now),
    )
    return dict(c.execute('SELECT * FROM group_drafts WHERE draft_id=?', (did,)).fetchone())


def format_draft_preview(draft, *, safety_warning='', resolution_note=''):
    lines = [
        f'【待确认群答复 · {draft["draft_id"]}】',
        f'目标群：{draft["target_chat_name"]}',
    ]
    if resolution_note:
        lines.append(f'目标群判定依据：{resolution_note}')
    if safety_warning:
        lines.append(f'安全拦截提示：{safety_warning}（已自动转为私聊草稿，未发群）')
    lines.extend([
        '━━━━━━━━━━━━━━',
        draft['body'],
        '━━━━━━━━━━━━━━',
        f'确认发送：回复「发」或「发 {draft["draft_id"]}」（大肥鱼 0 模型秒发 1 条到目标群）',
        f'修改后发：回复「【改发 {draft["draft_id"]}】你的修改版正文」',
        f'取消发送：回复「不发」或「不发 {draft["draft_id"]}」',
    ])
    return '\n'.join(lines)


def _queue_owner_notice(c, item, cid, text, epoch, bot_channel):
    c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)', (cid, 'control', 'private'))
    c.execute(
        "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
        (cid, 'first', epoch, text),
    )


def handle_owner_private_command(c, root, item, epoch, *, bot_channel='feiyu', call=None):
    """Consume 0-LLM owner private group-relay / draft-confirmation commands before T3 intake."""
    from business_queue import make_case

    if (item.get('chattype') != 'single'
            or item.get('sender') != item.get('chatid')
            or not resolve(root, item.get('sender'))['is_owner']):
        return False
    text = (item.get('direct') or '').strip()
    if not text or item.get('msgtype') not in ('text', 'voice', 'mixed'):
        return False
    install(c)
    quotes = [q.strip() for q in item.get('quotes', []) if isinstance(q, str) and q.strip()]
    followup_quoted_fids = []
    for q in quotes:
        if TASK_RECEIPT_QUOTE_RE.match(q) or CLARIFY_QUOTE_RE.search(q):
            continue
        for mq in FOLLOWUP_ID_RE.findall(q):
            followup_quoted_fids.append(mq)

    # 1. 【直回 F-xxxxxx】正文 / 直回 F-xxxxxx 正文 / 引用秒推卡片 + 【直回】正文 or 直回 正文
    m_dr = DIRECT_REPLY_CMD.fullmatch(text)
    m_dr_id = None if m_dr else DIRECT_REPLY_ID_CMD.fullmatch(text)
    m_dr_quote = None if (m_dr or m_dr_id or not followup_quoted_fids) else QUOTE_DIRECT_REPLY_RE.fullmatch(text)
    m_dr_empty = None if (m_dr or m_dr_id or m_dr_quote) else EMPTY_DIRECT_REPLY_RE.fullmatch(text)
    if m_dr_empty and (text.startswith('【') or m_dr_empty.group(1) or followup_quoted_fids):
        reply = '请把要发送到群里的答复正文写在「直回」后面，例如：「直回 你的答复正文」或「【直回】你的答复正文」。'
        cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
        _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
        return True
    if m_dr or m_dr_id or m_dr_quote:
        if m_dr:
            fhex = m_dr.group(1) or m_dr.group(2)
            body_raw = (m_dr.group(3) or '').strip()
        elif m_dr_id:
            fhex = m_dr_id.group(1)
            body_raw = (m_dr_id.group(2) or '').strip()
        else:
            fhex = None
            body_raw = (m_dr_quote.group(1) or '').strip()
        if not fhex and followup_quoted_fids:
            fhex = followup_quoted_fids[0]
        if not fhex:
            reply = '请提供提问编号，例如「【直回 F-A1B2C3】你的答复正文」，或直接引用秒推消息后发「直回 你的答复正文」。'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        if not body_raw:
            reply = '请把要发送到群里的答复正文写在「直回」后面，例如：「直回 你的答复正文」或「【直回】你的答复正文」。'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        f_row = resolve_followup(c, 'F-' + fhex)
        if not f_row:
            reply = f'未找到群友提问编号 F-{fhex.upper()}，请核对编号。'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        try:
            safe_body = validate_group_body(body_raw, f_row['chat'])
        except ValueError as exc:
            reply = f'未能发群：{exc}'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        groups = list_groups(call)
        gname = group_display_name(f_row['chat'], sessions=groups)
        cid = make_case(c, item, 'group_relay', category='control', note=f'0模型直回 {gname}', bot_channel=bot_channel)
        draft = create_draft(
            c, case_id=cid, bot_channel=bot_channel,
            owner_chat=item['chatid'], owner_sender=item['sender'],
            target_chat_id=f_row['chat'], target_chat_name=gname,
            followup_id=f_row['id'], body=safe_body, state='approved_direct', now=epoch,
        )
        c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)', (cid, 'group_relay', 'private'))
        c.execute(
            "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
            (cid, f'group_relay:{draft["draft_id"]}', epoch, safe_body),
        )
        return True

    # 2. 【直发群：群名】正文  (0-LLM direct send to named group)
    m_dg = DIRECT_GROUP_CMD.fullmatch(text)
    if m_dg:
        gquery = m_dg.group(1).strip()
        body_raw = m_dg.group(2).strip()
        groups = list_groups(call)
        hit, cands = match_group_name(gquery, groups)
        if not hit:
            names = '、'.join((g['chat_name'] or f"未命名群({g['chat_id'][-8:]})") for g in (cands or groups)[:8])
            reply = f'未能唯一匹配目标群「{gquery}」，当前大肥鱼可见群：{names or "暂无"}。请写准确群名后重试。'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        try:
            safe_body = validate_group_body(body_raw, hit['chat_id'])
        except ValueError as exc:
            reply = f'未能发群：{exc}'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        gname = hit['chat_name'] or f'未命名群（尾号 {hit["chat_id"][-8:]}）'
        cid = make_case(c, item, 'group_relay', category='control', note=f'0模型直发 {gname}', bot_channel=bot_channel)
        draft = create_draft(
            c, case_id=cid, bot_channel=bot_channel,
            owner_chat=item['chatid'], owner_sender=item['sender'],
            target_chat_id=hit['chat_id'], target_chat_name=gname,
            followup_id=None, body=safe_body, state='approved_direct', now=epoch,
        )
        c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)', (cid, 'group_relay', 'private'))
        c.execute(
            "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
            (cid, f'group_relay:{draft["draft_id"]}', epoch, safe_body),
        )
        return True

    # 2.5 【忽略 F-xxxxxx】 or quoting push card + 忽略 (0-LLM manual ignore)
    quoted_fids = list(followup_quoted_fids)
    m_ign = IGNORE_FOLLOWUP_CMD.fullmatch(text)
    is_quote_ign = bool(quoted_fids and QUOTE_IGNORE_FOLLOWUP_RE.fullmatch(text))
    if m_ign or is_quote_ign:
        fhex = (m_ign.group(1) or m_ign.group(2) or m_ign.group(3)) if m_ign else None
        if not fhex and quoted_fids:
            fhex = quoted_fids[0]
        if not fhex:
            reply = '请提供提问编号，例如「【忽略 F-A1B2C3】」，或直接引用秒推消息后回复「忽略」。'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        f_row = resolve_followup(c, 'F-' + fhex)
        if not f_row:
            reply = f'未找到群友提问编号 F-{fhex.upper()}，请核对编号。'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        fcode = 'F-' + f_row['id'][-6:].upper()
        st = f_row['status'] or 'open'
        if st == 'open':
            conclusion_notes.ignore_followup(c, f_row['id'], now=epoch)
            reply = f'已忽略群友提问 {fcode}，后续不再提醒或监控。'
        elif st == 'ignored':
            reply = f'群友提问 {fcode} 之前已忽略，无需重复操作。'
        elif st == 'expired':
            reply = f'群友提问 {fcode} 已超过 2 小时自动忽略，无需再处理。'
        else:
            reply = f'群友提问 {fcode} 之前已处理完成，无需再忽略。'
        cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
        _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
        return True

    # 3. Draft confirmation / edit / cancel (ONLY active when pending_confirm drafts exist!)
    open_drafts = c.execute(
        """SELECT * FROM group_drafts
           WHERE owner_chat=? AND owner_sender=? AND bot_channel=? AND state='pending_confirm'
           ORDER BY created_at DESC""",
        (item['chatid'], item['sender'], bot_channel),
    ).fetchall()
    if not open_drafts:
        return False

    quoted_did = None
    for q in quotes:
        mq = DRAFT_ID_RE.search(q)
        if mq:
            quoted_did = mq.group(1).upper()
            break

    # If the owner is quoting a clarification question (Q-xxxx / 需要确认〔...〕), a task/batch receipt,
    # or an Agent-initiated ask without a draft ID, yield immediately so bare "发"/"不发"/"取消发送" never hijacks an unrelated draft!
    import agent_relay
    if not quoted_did and not DRAFT_ID_RE.search(text) and (
        agent_relay.has_explicit_agent_target(text)
        or any(
            CLARIFY_QUOTE_RE.search(q)
            or TASK_RECEIPT_QUOTE_RE.match(q.strip())
            or agent_relay.is_agent_ask_quote(q)
            for q in quotes
        )
    ):
        return False

    def _pick_draft(explicit_did):
        wanted = (explicit_did or quoted_did or '').upper()
        if wanted:
            hit = next((d for d in open_drafts if d['draft_id'].upper() == wanted), None)
            return hit, ('not_found' if not hit else 'ok')
        if len(open_drafts) == 1:
            return open_drafts[0], 'ok'
        return None, 'multiple'

    # 3a. Edit & Send: 【改发 D-xxxx】新正文
    m_edit = EDIT_SEND_RE.fullmatch(text)
    if m_edit:
        did_arg = m_edit.group(1) or m_edit.group(2)
        new_body_raw = (m_edit.group(3) or '').strip()
        draft, err = _pick_draft(did_arg)
        if not draft:
            listing = '、'.join(f'{d["draft_id"]}（{d["target_chat_name"]}）' for d in open_drafts[:6])
            reply = f'当前有多个待发草稿，请指定编号：{listing}。格式：【改发 D-XXXX】修改后正文' if err == 'multiple' else '未找到对应的待发草稿编号。'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        try:
            safe_body = validate_group_body(new_body_raw, draft['target_chat_id'])
        except ValueError as exc:
            reply = f'修改稿未能发群：{exc}'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        c.execute(
            "UPDATE group_drafts SET body=?, body_sha256=?, state='approved_direct', updated_at=? WHERE draft_id=? AND state='pending_confirm'",
            (safe_body, sha256(safe_body.encode('utf-8')).hexdigest(), epoch, draft['draft_id']),
        )
        cid = make_case(c, item, 'group_relay', category='control', note=f'改发草稿 {draft["draft_id"]}', bot_channel=bot_channel)
        c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)', (cid, 'group_relay', 'private'))
        c.execute(
            "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
            (cid, f'group_relay:{draft["draft_id"]}', epoch, safe_body),
        )
        return True

    # 3b. Confirm send: 发 / 发 D-xxxx / quote + 发
    m_conf = CONFIRM_SEND_RE.fullmatch(text)
    is_quote_conf = bool(quoted_did and QUOTE_CONFIRM_RE.fullmatch(text))
    if m_conf or is_quote_conf:
        did_arg = (m_conf.group(1) or m_conf.group(2)) if m_conf else None
        draft, err = _pick_draft(did_arg)
        if not draft:
            listing = '、'.join(f'{d["draft_id"]}（{d["target_chat_name"]}）' for d in open_drafts[:6])
            reply = f'当前有多个待发草稿，请指定编号发送：{listing}（例如回复「发 {open_drafts[0]["draft_id"]}」）。' if err == 'multiple' else '未找到对应的待发草稿编号。'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        try:
            safe_body = validate_group_body(draft['body'], draft['target_chat_id'])
        except ValueError as exc:
            reply = f'草稿 {draft["draft_id"]} 未通过发群安全校验：{exc}'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        updated = c.execute(
            "UPDATE group_drafts SET state='approved_direct', updated_at=? WHERE draft_id=? AND state='pending_confirm'",
            (epoch, draft['draft_id']),
        ).rowcount
        if updated:
            cid = make_case(c, item, 'group_relay', category='control', note=f'确认发送草稿 {draft["draft_id"]}', bot_channel=bot_channel)
            c.execute('INSERT OR IGNORE INTO request_routes VALUES(?,?,?)', (cid, 'group_relay', 'private'))
            c.execute(
                "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
                (cid, f'group_relay:{draft["draft_id"]}', epoch, safe_body),
            )
        return True

    # 3c. Cancel draft: 不发 / 取消发送 / 不发 D-xxxx
    m_can = CANCEL_DRAFT_RE.fullmatch(text)
    is_quote_can = bool(quoted_did and QUOTE_CANCEL_RE.fullmatch(text))
    if m_can or is_quote_can:
        did_arg = (m_can.group(1) or m_can.group(2)) if m_can else None
        draft, err = _pick_draft(did_arg)
        if not draft:
            listing = '、'.join(f'{d["draft_id"]}（{d["target_chat_name"]}）' for d in open_drafts[:6])
            reply = f'当前有多个待发草稿，请指定要取消的编号：{listing}。' if err == 'multiple' else '未找到对应的待发草稿编号。'
            cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
            _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
            return True
        c.execute(
            "UPDATE group_drafts SET state='cancelled', updated_at=?, detail='owner_cancelled' WHERE draft_id=? AND state='pending_confirm'",
            (epoch, draft['draft_id']),
        )
        reply = f'已取消群答复草稿 {draft["draft_id"]}，未向【{draft["target_chat_name"]}】发送任何消息。'
        cid = make_case(c, item, 'control', category='control', note=reply, bot_channel=bot_channel)
        _queue_owner_notice(c, item, cid, reply, epoch, bot_channel)
        return True

    return False
