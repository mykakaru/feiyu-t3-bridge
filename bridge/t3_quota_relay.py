"""Quota-triggered T3 continuations. SQLite owns launch reservations across processes."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import time
import uuid

import ambiguity_gate
import private_bot_control
import t3_client
import t3_dispatch as dispatch

GEMINI = 'antigravity_gemini'
SHARED = 'antigravity_shared'
CODEX = 'codex_openai'

_COMMON = r'\b(?:too many requests|(?:http(?:/\d(?:\.\d)?)?|status(?:[ _]code)?)\s*[:=]?\s*429)\b'
_AG = re.compile(_COMMON + r'|\b(?:(?:individual )?quota (?:reached|exhausted)|resource[ _]exhausted|rate limit exceeded)\b', re.I)
_OAI = re.compile(_COMMON + r"|\b(?:usage_limit_reached|you['’]ve hit your usage limit|usage limit (?:has been )?reached|insufficient_quota|rate_limit_exceeded|quota exceeded)\b", re.I)
_DURATION = r'\d+\s*(?:hours?|hrs?|h|minutes?|mins?|m|seconds?|secs?|s)'
_RESET = re.compile(r'\b(?:resets?\s+in|try\s+again\s+in|retry[ _-]after)\s*[:=]?\s*((?:' + _DURATION + r'\s*)+)', re.I)
_FACTORS = {'h': 3600, 'm': 60, 's': 1}


def pool_for_model(provider, model):
    p, m = str(provider).lower(), str(model).lower()
    if p == 'codex':
        return CODEX
    if p == 'antigravity':
        if m.startswith('gemini-'):
            return GEMINI
        if m.startswith(('claude-', 'gpt-oss-')):
            return SHARED
    return 'other'


def parse_reset_seconds(text):
    match = _RESET.search(str(text))
    if not match:
        return 1200
    seconds = sum(int(n) * _FACTORS[u.lower()] for n, u in
                  re.findall(r'(\d+)\s*([hms])', match[1], re.I))
    return max(60, min(seconds, 21600))


def _error_fields(value, reference_time):
    if isinstance(value, list):
        return '\n'.join(_error_fields(item, reference_time) for item in value)
    if isinstance(value, dict):
        if value.get('class') in ('tool_error', 'tool_failure', 'user_cancelled', 'interrupted'):
            return ''
        fields = ['message', 'code', 'type', 'error', 'failure', 'detail', 'reason']
        lines = [_error_fields(value[k], reference_time) for k in fields if k in value]
        for key in ('retry_after', 'retry_after_seconds', 'resets_in_seconds'):
            seconds = value.get(key)
            if isinstance(seconds, int) and seconds >= 0:
                lines.append(f'retry after {seconds}s')
                break
        else:
            at = value.get('resets_at')
            if isinstance(at, (int, float)) and 0 < at < 10**11:
                lines.append(f'retry after {max(0, int(at-reference_time))}s')
        if any(str(value.get(k)) == '429' for k in ('code', 'status', 'statusCode', 'status_code')):
            lines.append('HTTP 429')
        return '\n'.join(lines)
    return 'HTTP 429' if str(value).strip() == '429' else str(value or '')


def classify_quota_failure(status, provider, model, error_text='', assistant_text='', error_items=None, *, failure_time=None):
    pool = pool_for_model(provider, model)
    result = dict(is_quota=False, pool=pool, reason='no_provider_quota_signal',
                  matched_text='', reset_seconds=1200, reset_parsed=False)
    if str(status).lower() not in ('failed', 'error') or pool == 'other':
        return result
    pattern = _OAI if pool == CODEX else _AG
    # Assistant prose is untrusted. Only terminal provider-shaped lines qualify.
    errors = _error_fields([error_text, error_items], time.time() if failure_time is None else failure_time)
    tail = str(assistant_text or '')[-800:].strip().splitlines()
    banners = [re.sub(r'^\s*(?:⚠\ufe0f?\s*)?(?:error\s*:\s*)?', '', line, flags=re.I) for line in tail[-3:]]
    for candidate in [errors, *(line for line in banners if pattern.match(line))]:
        match = pattern.search(candidate)
        if match:
            combined = candidate + '\n' + '\n'.join(tail)
            return dict(is_quota=True, pool=pool, reason=match[0], matched_text=candidate.strip()[-800:],
                        reset_seconds=parse_reset_seconds(combined), reset_parsed=bool(_RESET.search(combined)))
    return result


_SCHEMA = """
CREATE TABLE IF NOT EXISTS quota_pool_health(
 pool TEXT PRIMARY KEY, exhausted_until REAL NOT NULL, last_error TEXT, source_thread_id TEXT, updated TEXT);
CREATE TABLE IF NOT EXISTS quota_relay_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS t3_quota_relays(
 id INTEGER PRIMARY KEY, source_thread_id TEXT NOT NULL, source_run_id TEXT NOT NULL,
 root_thread_id TEXT NOT NULL, hop_count INTEGER NOT NULL DEFAULT 1, scope TEXT NOT NULL, project_id TEXT,
 source_provider TEXT, source_model TEXT, source_pool TEXT, target_provider TEXT,target_model TEXT,
 target_pool TEXT,target_options_json TEXT,target_thread_id TEXT,status TEXT NOT NULL,reason TEXT,
 checkpoint_path TEXT,notified INTEGER NOT NULL DEFAULT 0,created TEXT NOT NULL,updated TEXT NOT NULL,
 UNIQUE(source_thread_id,source_run_id));
CREATE INDEX IF NOT EXISTS quota_relay_target ON t3_quota_relays(target_thread_id);
"""


def _db(root):
    conn = sqlite3.connect(Path(root) / 'local-tasks.sqlite3', timeout=3, factory=dispatch.ClosingConnection)
    conn.row_factory = sqlite3.Row
    try:
        tables = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN "
                              "('quota_pool_health','t3_quota_relays','quota_relay_meta')").fetchone()[0]
        if tables != 3 or not conn.execute("SELECT 1 FROM quota_relay_meta WHERE key='watcher_enabled_at'").fetchone():
            conn.executescript('BEGIN IMMEDIATE;\n' + _SCHEMA)
            conn.execute("INSERT OR IGNORE INTO quota_relay_meta VALUES('watcher_enabled_at',?)", (dispatch.stamp(),))
            conn.commit()
    except Exception:
        conn.close()
        raise
    return conn


def _cooldowns(conn, now):
    return dict(conn.execute('SELECT pool,exhausted_until FROM quota_pool_health WHERE exhausted_until>?', (now,)))


def is_pool_exhausted(root, pool, *, now=None):
    with _db(root) as conn:
        return pool in _cooldowns(conn, time.time() if now is None else now)


def _choose(pool, lightweight, catalog, cooldowns):
    neon = ('antigravity', 'gemini-4-neon', 'xhigh')
    gpt = ('codex', 'gpt-6.2-sol', 'medium') if lightweight and pool == GEMINI else ('codex', 'gpt-6.2-astra', 'high')
    candidates = {SHARED: [neon, gpt], GEMINI: [gpt], CODEX: [neon]}.get(pool, [])
    for provider, model, effort in candidates:
        target_pool = pool_for_model(provider, model)
        if target_pool in cooldowns or not catalog['providers'].get(provider, {}).get('enabled'):
            continue
        for entry in catalog['models']:
            if (entry['provider'], entry['id']) != (provider, model):
                continue
            key = entry.get('option_id')
            options = {key: effort} if key and effort in entry.get('efforts', []) else {}
            role = ('cross_pool_executor' if provider == 'codex' else
                    'first_responder_commander' if pool == SHARED else 'cross_pool_commander')
            return dict(ok=True, provider=provider, model=model, pool=target_pool, options=options, role=role)
    exhausted = candidates and all(pool_for_model(p, m) in cooldowns for p, m, _ in candidates)
    return dict(ok=False, reason='all_pools_exhausted' if exhausted else 'catalog_unavailable', cooldowns=cooldowns)


def choose_relay_target(root, source_pool, *, lightweight=False, now=None):
    with _db(root) as conn:
        cooldowns = _cooldowns(conn, time.time() if now is None else now)
    return _choose(source_pool, lightweight, t3_client.live_catalog(root), cooldowns)


def _daily_count(conn, now):
    day = datetime.fromtimestamp(now, timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return conn.execute("SELECT count(*) FROM t3_quota_relays WHERE created>=? "
                        "AND status IN ('launching','relayed','launch_failed')", (day.isoformat(),)).fetchone()[0]


def _telemetry(root):
    try:
        with _db(root) as conn:
            now = time.time()
            conn.execute('BEGIN IMMEDIATE')
            watermark = conn.execute("SELECT value FROM quota_relay_meta WHERE key='watcher_enabled_at'").fetchone()[0]
            data = dict(updated=dispatch.stamp(), watcher_enabled_at=watermark, cooldowns=_cooldowns(conn, now), daily_count=_daily_count(conn, now),
                        recent=[dict(r) for r in conn.execute('SELECT * FROM t3_quota_relays ORDER BY id DESC LIMIT 20')])
            dispatch._atomic(Path(root) / 'quota-relay-status.json', json.dumps(data, ensure_ascii=False, indent=2))
    except (OSError, sqlite3.Error) as exc:
        dispatch.log(root, '', 'quota_telemetry_error', str(exc))


def _owned(conn, tid):
    if conn.execute("SELECT 1 FROM t3_quota_relays WHERE scope='feiyu' "
                    "AND (target_thread_id=? OR source_thread_id=?)", (tid, tid)).fetchone():
        return True
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='t3_jobs'").fetchone():
        return False
    return bool(conn.execute('SELECT 1 FROM t3_jobs WHERE t3_thread_id=? OR t3_triage_thread_id=? LIMIT 1',
                             (tid, tid)).fetchone())


def _known(conn, tid, rid):
    return conn.execute('SELECT 1 FROM t3_quota_relays WHERE source_thread_id=? AND source_run_id=?', (tid, rid)).fetchone()


def _reserve(root, source, signal, scope, lightweight, skip):
    cfg = t3_client.load_config(root)
    catalog = t3_client.live_catalog(root)
    tid, rid = source['threadId'], source['runId']
    with _db(root) as conn:
        conn.execute('BEGIN IMMEDIATE')
        if _known(conn, tid, rid) or (scope == 'manual' and _owned(conn, tid)):
            return None
        now = time.time()
        stamp = dispatch.stamp()
        end = min(source.get('failure_time') or now, now) + signal['reset_seconds']
        conn.execute('INSERT INTO quota_pool_health VALUES(?,?,?,?,?) ON CONFLICT(pool) DO UPDATE SET '
                     'exhausted_until=max(excluded.exhausted_until,exhausted_until),last_error=excluded.last_error,'
                     'source_thread_id=excluded.source_thread_id,updated=excluded.updated',
                     (signal['pool'], end, signal['reason'], tid, stamp))
        parent = conn.execute('SELECT hop_count,root_thread_id FROM t3_quota_relays WHERE target_thread_id=?',
                              (tid,)).fetchone()
        hop, origin = (parent[0]+1, parent[1]) if parent else (1, tid)
        target = _choose(signal['pool'], lightweight, catalog, _cooldowns(conn, now))
        status, reason = 'launching', signal['reason']
        if skip:
            status, reason = 'skipped', skip
        elif hop > int(cfg.get('quota_relay_max_hops', 2)):
            status, reason = 'skipped', 'max_hops'
        elif not target['ok']:
            reason = target['reason']
            status = 'exhausted_all' if reason == 'all_pools_exhausted' else 'skipped'
        elif _daily_count(conn, now) >= int(cfg.get('quota_relay_daily_cap', 24)):
            status, reason = 'skipped', 'daily_cap'
        key = conn.execute('INSERT INTO t3_quota_relays(source_thread_id,source_run_id,root_thread_id,hop_count,'
                           'scope,project_id,source_provider,source_model,source_pool,target_provider,target_model,'
                           'target_pool,target_options_json,status,reason,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                           (tid, rid, origin, hop, scope, source['project_id'], source['provider'], source['model'],
                            signal['pool'], target.get('provider'), target.get('model'), target.get('pool'),
                            json.dumps(target.get('options', {})), status, reason, stamp, stamp)).lastrowid
        return key, hop, status, target


def _excerpt(text, byte_limit):
    raw = str(text or '').encode('utf-8', errors='replace')
    return raw[:byte_limit].decode('utf-8', errors='ignore') + ('…' if len(raw) > byte_limit else '')


def build_checkpoint_prompt(root, source, runtime, messages, target, hop, *, work=None, context=None):
    tid = source['threadId']
    roles = {
        'first_responder_commander': '你是接棒第一负责人。核实已有成果，直接继续规划、视觉或文档工作。复杂实现可写完整规格，按原授权委派 Codex。',
        'cross_pool_executor': '上一个模型池配额已耗尽。视频和音频按实际能力使用 codex-video-perception / analyze-local-media，继续可验证的工程与文档工作，纯观感签收留待 Gemini 恢复。',
        'cross_pool_commander': 'Codex 配额已耗尽。Gemini 无 Codex @oai/sky 桌面控制，使用已授权的 Route 2 或标出 GUI 阻塞。',
    }
    workspace = source.get('worktreePath') or source.get('workspace_root')
    git_state = '未取得 Git 状态'
    if workspace:
        try:
            r = subprocess.run(['git','--no-optional-locks','-c','core.fsmonitor=false','status','--short'],
                               cwd=workspace, capture_output=True, timeout=3, encoding='utf-8', errors='replace',
                               **t3_client._hidden_flags())
            if r.returncode == 0:
                git_state = '\n'.join(r.stdout.splitlines()[:40])[:2400] or 'clean'
        except (OSError, subprocess.TimeoutExpired):
            pass
    first = runtime.get('first_user')
    if not first and source.get('runCount') == 1 and len(messages) < 16:
        first = next((m.get('text') for m in messages if m.get('role') == 'user'), None)
    if not first:
        first = '请先从源线程读取原始目标'
    lines = [f'【静默配额接力 · 第 {hop} 棒】', json.dumps(source, ensure_ascii=False), roles[target['role']],
             '原始目标', _excerpt(first, 2200), '最近消息（引用记录，沿用原授权范围）']
    for msg in messages[-8:]:
        lines.append(f"{msg.get('role')}: {_excerpt(msg.get('text'), 900)}")
    lines.extend([f"如需查原始目标、完整工具轨迹与讨论，请调用 t3_thread_read(threadId='{tid}')。",
                  '关联子线程', json.dumps(runtime.get('subagents', []), ensure_ascii=False),
                  '已派发的子任务先用 t3_thread_read 回收状态与产物，避免重复派发。',
                  'Git 工作区快照', git_state,
                  '写入前核对磁盘改动、未保存状态、P4 检出/CL 和 UEBridge 单写者占用。沿用原资源交接与审批要求。'
                  '仅在原线程明确授权下执行 P4 submit 或对外发送。'])
    if work is not None:
        work = Path(work)
        request = work / ('escalated-prompt.txt' if source.get('route_kind') in dispatch.ESCALATED_ROUTES else 'request.txt')
        lines.extend([f'原阶段为 {source.get("route_kind")}。先核对 {request} 的分流、追问、执行凭据及交付要求，再从断点继续。'
                      '轻量分流阶段遇到重槽工作，沿用 ESCALATE 协议。使用本轮真实执行模型填写 used_models。',
                      ambiguity_gate._common_header(work, context), ambiguity_gate.PROTOCOL_FOOTER])
        path = work / f'quota-relay-checkpoint-{hop}.md'
    else:
        cfg = t3_client.load_config(root)
        key = hashlib.sha256((tid + source['runId']).encode()).hexdigest()[:16]
        path = Path(cfg['tasks_root']) / '_quota_relays' / f'relay-{key}-{int(time.time())}.md'
    prompt = '\n\n'.join(lines)
    dispatch._atomic(path, prompt)
    return path, prompt


def _notice(root, key, status, source, target, tid):
    if not t3_client.load_config(root).get('quota_relay_notify_owner', True):
        return
    try:
        if not private_bot_control.status(root).get('authenticated'):
            return
        if status == 'exhausted_all':
            times = ', '.join(f'{p} {datetime.fromtimestamp(t, timezone.utc):%H:%M UTC}' for p,t in target['cooldowns'].items())
            reply = private_bot_control.send_markdown(root, None, f"任务 {source['threadId']} 可接力的模型池均在冷却。预计恢复 {times}")
        else:
            reply = private_bot_control.ask_owner(root, tid, f"任务已接力至 {target['model']}。引用本条可继续指挥。", title='静默接力·'+source['title'][:24])
        if reply.get('success'):
            with _db(root) as conn:
                conn.execute('UPDATE t3_quota_relays SET notified=1 WHERE id=?', (key,))
    except Exception as exc:
        dispatch.log(root, '', 'quota_notice_error', type(exc).__name__)


def _bind_job(conn, job, row_id, source_tid, launch, target, detail):
    tid = launch['thread_id']
    link = (launch.get('raw') or {}).get('link') or ''
    changed = conn.execute(
        "UPDATE t3_jobs SET t3_thread_id=?,t3_link=?,requested_provider=?,requested_model=?,"
        "requested_effort=?,requested_options_json=?,t3_archive_after=NULL,t3_archived_at=NULL,"
        "last_event='t3_quota_relayed',detail=?,updated=? WHERE msgid=? AND t3_thread_id=? "
        "AND state='running' AND lease IS ?",
        (tid, link, target['provider'], target['model'], next(iter(target['options'].values()), None),
         json.dumps(target['options']), detail, dispatch.stamp(), job['msgid'], source_tid, job['lease']))
    if changed.rowcount:
        conn.execute('INSERT INTO log(at,msgid,kind,detail) VALUES(?,?,?,?)',
                     (dispatch.stamp(), job['msgid'], 't3_quota_relayed', f'{source_tid} -> {tid}'))
        return True
    conn.execute("UPDATE t3_quota_relays SET reason='job_ownership_changed_after_launch' WHERE id=?", (row_id,))
    return False


def _existing_feiyu_launch(root, job, tid, rid):
    with _db(root) as conn:
        row = conn.execute("SELECT * FROM t3_quota_relays WHERE source_thread_id=? AND source_run_id=? AND scope='feiyu'",
                           (tid, rid)).fetchone()
    if not row or row['status'] not in ('launching','launch_failed','relayed'):
        return None
    if row['status'] == 'relayed':
        return {'already_handled': True}
    age = time.time() - dispatch._parse_iso_epoch(row['updated'])
    if row['status'] == 'launching' and age < t3_client.launch_timeout(root) + 60:
        return {'already_handled': True}
    # A lost acknowledgement may still have created the exact preallocated thread.
    new_tid = row['target_thread_id']
    if new_tid:
        try:
            thread = t3_client.show_thread(root, new_tid, messages=1, timeout=6)['thread']
        except (OSError, RuntimeError, KeyError, subprocess.TimeoutExpired):
            thread = {}
        if (thread.get('threadId'), thread.get('projectId'), thread.get('providerInstanceId'), thread.get('model')) == (
                new_tid, row['project_id'], row['target_provider'], row['target_model']):
            launch = dict(thread_id=new_tid, run_id=thread.get('latestRunId'), raw=thread)
            target = dict(provider=row['target_provider'], model=row['target_model'], options=json.loads(row['target_options_json']))
            with _db(root) as conn:
                if _bind_job(conn, job, row['id'], tid, launch, target, 'recovered_launch_ack'):
                    conn.execute("UPDATE t3_quota_relays SET status='relayed',updated=? WHERE id=?", (dispatch.stamp(), row['id']))
                    return launch
    if row['status'] == 'launching':
        with _db(root) as conn:
            conn.execute("UPDATE t3_quota_relays SET status='launch_failed',updated=?,reason='launch_ack_unknown' "
                         "WHERE id=? AND status='launching'", (dispatch.stamp(), row['id']))
    return None


def _relay(root, project, info, res, *, job=None, context=None, work=None, cutoff=None):
    thread = info.get('thread') or {}
    tid = res.get('thread_id')
    rid = res.get('run_id') or thread.get('latestRunId')
    if not (tid and rid) or thread.get('latestRunId') not in (None, rid):
        return None
    rt = t3_client.inspect_thread_runtime(root, tid, run_id=rid)
    completed = dispatch._parse_iso_epoch(rt.get('completed_at'))
    if cutoff is not None and completed is not None and completed < cutoff:
        return None
    metadata = {**rt.get('thread', {}), **thread}
    if metadata.get('status') not in (None, 'failed', 'error') or rt.get('user_stopped'):
        return None
    selection = metadata.get('modelSelection') or {}
    provider = rt.get('run_provider') or selection.get('instanceId') or metadata.get('providerInstanceId') or (job and job['requested_provider'])
    model = rt.get('run_model') or selection.get('model') or metadata.get('model') or (job and job['requested_model'])
    if (metadata.get('archived') or metadata.get('archivedAt') or metadata.get('deletedAt') or metadata.get('parentThreadId')
            or metadata.get('delegatedFrom') or metadata.get('latestRunStopRequested')
            or (metadata.get('lineage') or {}).get('parentThreadId') or metadata.get('activeRunId')
            or metadata.get('projectId') not in (None, project['project_id'])):
        return None
    if rt.get('run_status') and rt['run_status'] not in ('failed','error'):
        return None
    status = res.get('status') or rt.get('run_status')
    last_reply = next((m.get('text') for m in reversed(info.get('messages') or [])
                       if m.get('role') == 'assistant' and m.get('runId') == rid), '')
    signal = classify_quota_failure(status, provider, model, [res.get('error'), metadata.get('lastError')],
                                    rt.get('assistant_text') or last_reply or res.get('reply'), rt.get('error_items'),
                                    failure_time=completed)
    if not signal['is_quota']:
        return None
    source = dict(threadId=tid, runId=rid, provider=provider, model=model, project_id=project['project_id'],
                  project_title=project.get('title'), workspace_root=project.get('workspace_root'),
                  worktreePath=metadata.get('worktreePath'), branch=metadata.get('branch'),
                  title=metadata.get('title') or '', quota_reason=signal['reason'], runCount=metadata.get('runCount'),
                  failure_time=completed)
    if job:
        source['route_kind'] = job['route_kind']
    skip = ''
    exact_modes = metadata.get('runtimeMode') and metadata.get('interactionMode')
    if not exact_modes and (not job or source['worktreePath'] or metadata.get('runtimeMode') or metadata.get('interactionMode')):
        skip = 'source_modes_missing'
    scope = 'feiyu' if job else 'manual'
    light = bool(job and job['route_kind'] == 'triage_hydrogen' and not context.get('explicit_model'))
    reservation = _reserve(root, source, signal, scope, light, skip)
    if not reservation:
        return _existing_feiyu_launch(root, job, tid, rid) if job else None
    key, hop, state, target = reservation
    try:
        if state == 'exhausted_all':
            _notice(root, key, state, source, target, None)
        if state != 'launching':
            return None
        path, prompt = build_checkpoint_prompt(root, source, rt, info.get('messages') or [], target, hop,
                                               work=work, context=context)
        planned_tid = str(uuid.uuid4()) if exact_modes else None
        with _db(root) as conn:
            conn.execute('UPDATE t3_quota_relays SET checkpoint_path=?,target_thread_id=? WHERE id=?', (str(path), planned_tid, key))
        # Never replay an ambiguous CLI acknowledgement. The durable launch reservation consumes its cap.
        fresh = t3_client.show_thread(root, tid, messages=1)['thread']
        if (fresh.get('latestRunId') != rid or fresh.get('status') not in ('failed','error')
                or fresh.get('activeRunId') or fresh.get('archived') or fresh.get('archivedAt') or fresh.get('deletedAt')
                or fresh.get('latestRunStopRequested') or any(fresh.get(k) != thread.get(k)
                    for k in ('worktreePath','branch','runtimeMode','interactionMode','parentThreadId'))):
            with _db(root) as conn:
                conn.execute("UPDATE t3_quota_relays SET status='skipped',reason='source_changed' WHERE id=?", (key,))
            return None
        if job:
            with _db(root) as conn:
                current = conn.execute("SELECT 1 FROM t3_jobs WHERE msgid=? AND state='running' "
                                       "AND t3_thread_id=? AND lease IS ?", (job['msgid'], tid, job['lease'])).fetchone()
            cancelled = [change['change_id'] for change in dispatch._sync_control_mailbox(root, job['msgid'])
                         if change['action'] == 'cancel']
            if not current or cancelled:
                with _db(root) as conn:
                    conn.execute("UPDATE t3_quota_relays SET status='skipped',reason='job_changed' WHERE id=?", (key,))
                if current and cancelled:
                    dispatch._ack_controls(root, job['msgid'], cancelled, action='cancel')
                    dispatch.update_state(root, job['msgid'], 'cancelled', detail='本人已取消任务',
                                          t3_archive_after=dispatch._archive_deadline(root))
                    return {'already_handled': True}
                return None
            for name in ('t3-response.json', 'execution-proof.json'):
                prior = Path(work) / name
                if prior.is_file():
                    prior.rename(prior.with_name(f'quota-source-{key}-{name}'))
        if exact_modes:
            launch = t3_client.launch_in_source_workspace(root, project['project_id'], {**metadata,'workspace_root':project.get('workspace_root')}, target,
                                                         '接力·'+source['title'], prompt, thread_id=planned_tid)
        else:
            launch = t3_client.start_thread(root, project['project_id'], target['provider'], target['model'],
                                            '接力·'+source['title'], prompt, wait=False, options=target['options'])
        new_tid = launch.get('thread_id')
        if launch.get('exit_code') != 0 or not new_tid:
            raise RuntimeError(str(launch.get('error') or 'missing_launch_ack')[:240])
        if job and not (launch.get('raw') or {}).get('link'):
            launch = {**launch, 'raw': {**(launch.get('raw') or {}), 'link': dispatch._fetch_link(root, new_tid)}}
        with _db(root) as conn:
            conn.execute("UPDATE t3_quota_relays SET status='relayed',target_thread_id=?,updated=? WHERE id=?",
                         (new_tid, dispatch.stamp(), key))
            if job and not _bind_job(conn, job, key, tid, launch, target, signal['reason']):
                return {'already_handled': True}
        if not job:
            _notice(root, key, 'relayed', source, target, new_tid)
        return launch
    except Exception as exc:
        with _db(root) as conn:
            conn.execute("UPDATE t3_quota_relays SET status='launch_failed',reason=?,updated=? WHERE id=? AND status='launching'",
                         (str(exc)[:300], dispatch.stamp(), key))
        dispatch.log(root, job['msgid'] if job else '', 'quota_relay_error', str(exc))
        return _existing_feiyu_launch(root, job, tid, rid) if job else None
    finally:
        _telemetry(root)


def try_relay_feiyu_job(root, job, context, work, res):
    if not t3_client.load_config(root).get('quota_relay_enabled') or str(res.get('status') or '').lower() not in ('failed','error',''):
        return None
    tid = res.get('thread_id') or job['t3_thread_id']
    if not tid:
        return None
    info = res.get('raw') or {}
    if not isinstance(info, dict) or not info.get('thread'):
        info = t3_client.show_thread(root, tid, messages=16)
    project_id = dispatch._project_id(root, job)
    project = next((p for p in t3_client.list_active_projects(root) if p['project_id'] == project_id), None)
    if not project:
        return None
    return _relay(root, project, info, {**res, 'thread_id': tid, 'status': res.get('status') or (info.get('thread') or {}).get('status')},
                  job=job, context=context, work=work)


def sweep_all_t3_threads(root):
    cfg = t3_client.load_config(root)
    if not cfg.get('quota_relay_enabled'):
        return
    with _db(root) as conn:
        enabled = conn.execute("SELECT value FROM quota_relay_meta WHERE key='watcher_enabled_at'").fetchone()[0]
    cutoff = max(dispatch._parse_iso_epoch(enabled), time.time()-int(cfg.get('quota_relay_recency_seconds', 1800)))
    for project in t3_client.list_active_projects(root):
        try:
            threads = t3_client.list_threads(root, project['project_id'], timeout=6)
        except Exception as exc:
            dispatch.log(root, '', 'quota_project_read_error', str(exc))
            continue
        for th in threads:
            tid, rid = th.get('threadId') or th.get('id'), th.get('latestRunId')
            if (not tid or th.get('status') not in ('failed','error') or th.get('parentThreadId')
                or th.get('archived') or th.get('archivedAt') or th.get('deletedAt') or (dispatch._parse_iso_epoch(th.get('updatedAt')) or 0) < cutoff):
                continue
            try:
                with _db(root) as conn:
                    if _owned(conn, tid) or _known(conn, tid, rid):
                        continue
                info = t3_client.show_thread(root, tid, messages=16)
                actual = info['thread']
                rid = actual.get('latestRunId')
                if not rid or actual.get('status') not in ('failed', 'error'):
                    continue
                current = next((m.get('text') for m in reversed(info.get('messages') or [])
                                if m.get('role') == 'assistant' and m.get('runId') == rid), '')
                _relay(root, project, info, dict(thread_id=tid, run_id=rid, status=actual['status'], reply=current), cutoff=cutoff)
            except Exception as exc:
                dispatch.log(root, '', 'quota_thread_read_error', f'{tid} {exc}')
    _telemetry(root)
