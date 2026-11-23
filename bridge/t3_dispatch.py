"""T3 Code task dispatcher for Feiyu Bridge.

Replaces legacy direct `codex.exe exec` execution with official T3 Code threads:
- Default owner messages start on lightweight `antigravity / gemini-4-hydrogen (high)`.
- Ambiguity / contradiction / missing-info triggers durable `awaiting_clarification`
  and resumes the exact same T3 thread after the owner replies in WeCom.
- Complex tasks escalate cleanly from Hydrogen High to `antigravity / gemini-4-neon (xhigh)`
  (or higher/multi-agent collaboration via T3 `delegate_task`).
- Explicit owner model requests route directly (when options match defaults) or via
  a zero-work verified `delegate_task` pass-through (guaranteeing exact non-default
  effort/serviceTier such as `gpt-6.2-astra max` or `gpt-6.2-sol max ultrafast`).
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import ctypes
from datetime import datetime, timezone
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import subprocess
import sys
import threading
import time

import ambiguity_gate
import clarifications
from command_routing import enabled
from identity import resolve
import pending_expiry
import settings
import t3_client
import t3_quota_relay as quota_relay
from wecom_cli import cli_call

ROOT = Path(__file__).resolve().parent
dispatch_wake = threading.Event()
TERMINAL = ('completed', 'failed', 'blocked', 'uncertain', 'cancelled')
ACTIVE_SQL = "('launching','starting','running')"
HEAVY_SLOTS = ('regular', 'heavy')
LIGHT_SLOTS = ('light', 'office')
ESCALATED_ROUTES = ('escalated_neon', 'escalated_light')


def slot_limits(cfg):
    regular = cfg.get('regular_slots')
    light = cfg.get('light_slots')
    if light is None:
        light = cfg.get('light_burst_slots')
    return (max(0, int(4 if regular is None else regular)), max(0, int(8 if light is None else light)))


def resume_queue_state(job):
    if job['execution_slot'] in HEAVY_SLOTS or job['route_kind'] == 'escalated_neon':
        return 'heavy_resume_queued'
    return 'resume_queued'


def stamp():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


def db(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(root / 'local-tasks.sqlite3', timeout=15, factory=ClosingConnection)
    conn.row_factory = sqlite3.Row
    conn.executescript('''
      PRAGMA journal_mode=WAL;
      PRAGMA synchronous=FULL;
      CREATE TABLE IF NOT EXISTS t3_jobs(
        msgid TEXT PRIMARY KEY,
        row_json TEXT NOT NULL,
        state TEXT NOT NULL,
        created TEXT NOT NULL,
        updated TEXT NOT NULL,
        pid INTEGER,
        worker_identity TEXT,
        lease TEXT,
        execution_slot TEXT NOT NULL DEFAULT 'light',
        office_class TEXT,
        context_json TEXT NOT NULL,
        context_hash TEXT NOT NULL,
        workspace TEXT,
        t3_project_id TEXT,
        t3_thread_id TEXT,
        t3_triage_thread_id TEXT,
        t3_link TEXT,
        route_kind TEXT,
        requested_provider TEXT,
        requested_model TEXT,
        requested_effort TEXT,
        requested_options_json TEXT,
        used_models_json TEXT,
        clarification_qid TEXT,
        turns INTEGER NOT NULL DEFAULT 0,
        last_event TEXT,
        applied_control_seq INTEGER NOT NULL DEFAULT 0,
        detail TEXT,
        result_hash TEXT,
        t3_archived_at TEXT,
        t3_triage_archived_at TEXT,
        t3_archive_after REAL,
        escalate_info_json TEXT
      );
      CREATE INDEX IF NOT EXISTS t3_jobs_state ON t3_jobs(state, created);
      CREATE TABLE IF NOT EXISTS log(id INTEGER PRIMARY KEY, at TEXT, msgid TEXT, kind TEXT, detail TEXT);
    ''')
    migrations = {'t3_archived_at': 'TEXT', 't3_triage_archived_at': 'TEXT',
                  't3_archive_after': 'REAL', 'escalate_info_json': 'TEXT'}
    if not migrations.keys() <= {r[1] for r in conn.execute('PRAGMA table_info(t3_jobs)')}:
        conn.execute('BEGIN IMMEDIATE')
        cols = {r[1] for r in conn.execute('PRAGMA table_info(t3_jobs)')}
        for column, sql_type in migrations.items():
            if column not in cols:
                conn.execute(f'ALTER TABLE t3_jobs ADD COLUMN {column} {sql_type}')
    conn.commit()
    return conn


def log(root, msgid, kind, detail=''):
    with db(root) as conn:
        conn.execute('INSERT INTO log(at,msgid,kind,detail) VALUES(?,?,?,?)',
                     (stamp(), msgid, kind, str(detail)[:500]))


def _atomic(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + secrets.token_hex(6) + '.tmp')
    try:
        with temporary.open('x', encoding='utf-8', newline='\n') as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        delays = (0.0, 0.02, 0.05, 0.1, 0.2, 0.35)
        for idx, pause in enumerate(delays):
            if pause:
                time.sleep(pause)
            try:
                temporary.replace(path)
                return
            except OSError as exc:
                locked = getattr(exc, 'winerror', None) in (5, 32, 33) or getattr(exc, 'errno', None) in (errno.EACCES, errno.EBUSY)
                if not locked or idx == len(delays) - 1:
                    raise
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _process_identity(pid):
    if not pid:
        return None
    if os.name == 'nt':
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.GetProcessTimes.argtypes = [
            wintypes.HANDLE, ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME),
            ctypes.POINTER(wintypes.FILETIME), ctypes.POINTER(wintypes.FILETIME),
        ]
        kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel.OpenProcess(0x1000, False, int(pid))
        if not handle:
            return None
        try:
            created, ended, kernel_time, user = (wintypes.FILETIME() for _ in range(4))
            if not kernel.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(ended), ctypes.byref(kernel_time), ctypes.byref(user)):
                return None
            code = wintypes.DWORD()
            if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)) or code.value != 259:
                return None
            return str(pid) + ':' + str((created.dwHighDateTime << 32) | created.dwLowDateTime)
        finally:
            kernel.CloseHandle(handle)
    try:
        fields = Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()
        return str(pid) + ':' + fields[19]
    except (OSError, IndexError):
        return None


def _hidden(detached=False):
    if os.name != 'nt':
        return {}
    flags = subprocess.CREATE_NO_WINDOW
    if detached:
        flags |= subprocess.DETACHED_PROCESS | subprocess.CREATE_BREAKAWAY_FROM_JOB
    return {'creationflags': flags}


class T3Signal:
    def __init__(self, root):
        self.handle = None
        if os.name == 'nt':
            from ctypes import wintypes
            self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
            self.kernel.CreateEventW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.BOOL, wintypes.LPCWSTR]
            self.kernel.CreateEventW.restype = wintypes.HANDLE
            self.kernel.SetEvent.argtypes = [wintypes.HANDLE]
            self.kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            name = 'Local\\FeiyuT3Dispatcher' + hashlib.sha256(str(Path(root).resolve()).lower().encode()).hexdigest()[:24]
            self.handle = self.kernel.CreateEventW(None, False, False, name)
            if not self.handle:
                raise ctypes.WinError(ctypes.get_last_error())

    def set(self):
        if self.handle:
            self.kernel.SetEvent(self.handle)
        dispatch_wake.set()

    def wait(self, seconds):
        if self.handle:
            result = self.kernel.WaitForSingleObject(self.handle, int(seconds * 1000))
            if result not in (0, 258):
                raise RuntimeError('t3_signal_wait_failed')
            return result == 0
        changed = dispatch_wake.wait(seconds)
        dispatch_wake.clear()
        return changed

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None


def wake(root):
    signal = T3Signal(root)
    try:
        signal.set()
    finally:
        signal.close()


def update_state(root, msgid, value, **fields):
    allowed = {
        'pid', 'worker_identity', 'lease', 'workspace', 't3_project_id', 't3_thread_id',
        't3_triage_thread_id', 't3_link', 'route_kind', 'requested_provider', 'requested_model',
        'requested_effort', 'requested_options_json', 'used_models_json', 'clarification_qid',
        'turns', 'last_event', 'applied_control_seq', 'detail', 'result_hash',
        't3_archived_at', 't3_triage_archived_at', 't3_archive_after',
    }
    if not set(fields) <= allowed:
        raise ValueError('unsupported_t3_state_fields')
    with db(root) as conn:
        cols = ''.join(',' + k + '=?' for k in fields)
        conn.execute('UPDATE t3_jobs SET state=?,updated=?' + cols + ' WHERE msgid=?',
                     [value, stamp(), *fields.values(), msgid])


def _direct_text(row):
    return '\n'.join(
        part['text'] for part in row.get('parts', [])
        if part.get('type') in ('text', 'voice_transcript')
        and not part.get('quoted') and isinstance(part.get('text'), str)
    ).strip()


def _eligible(root, row, context):
    if not isinstance(context, dict) or not enabled(root):
        return False
    case_id = row.get('msgid')
    manifest = context.get('manifest')
    if not isinstance(case_id, str) or not case_id or not isinstance(manifest, dict):
        return False
    if context.get('case_id') != case_id or manifest.get('case_id') != case_id:
        return False
    if manifest.get('sender') != row.get('sender') or manifest.get('chatid') != row.get('chatid'):
        return False
    # Strictly owner-only! Colleague messages can NEVER enter T3 dispatch.
    if not resolve(root, row.get('sender'))['is_owner'] or not _direct_text(row):
        return False
    if row.get('chattype') == 'single':
        if row.get('sender') != row.get('chatid'):
            return False
    elif row.get('chattype') == 'group':
        if manifest.get('audience') != row.get('chatid'):
            return False
    else:
        return False
    return True


def enqueue_business(root, row):
    """Durably enqueue or re-queue (after clarification) one verified owner T3 task."""
    from business_queue import local_context

    context = local_context(root, row.get('msgid'))
    if not _eligible(root, row, context):
        return False
    encoded, checksum = canonical(context), digest(context)
    with db(root):
        pass
    prefs = settings.read(root)
    explicit = context.get('explicit_model')
    if explicit:
        req_provider = explicit['provider']
        req_model = explicit['model']
        req_effort = explicit.get('effort')
        req_options = explicit.get('options') or {}
        route_kind = 'explicit_direct' if explicit.get('matches_defaults') else 'explicit_passthrough'
    else:
        entry = prefs['entry_model']
        req_provider = entry['provider']
        req_model = entry['model']
        req_effort = entry.get('effort')
        req_options = {}
        route_kind = 'triage_hydrogen'

    queued = False
    with pending_expiry.native_reservation(root) as conn:
        if pending_expiry.expired(conn, root, row['msgid']):
            return False
        lane = conn.execute(
            "SELECT l.state FROM lanes l JOIN cases c ON c.id=l.case_id "
            "WHERE c.id=? AND l.target='local' AND c.category='work' AND l.state NOT IN ('cancelled','expired')",
            (row['msgid'],),
        ).fetchone()
        if not lane:
            return False
        existing = conn.execute('SELECT state, context_hash, execution_slot, route_kind FROM expiry_native.t3_jobs WHERE msgid=?', (row['msgid'],)).fetchone()
        if existing:
            if existing['state'] == 'awaiting_clarification' and lane['state'] == 'resume_queued':
                state = resume_queue_state(existing)
                slot = 'regular' if state == 'heavy_resume_queued' else 'light'
                conn.execute(
                    "UPDATE expiry_native.t3_jobs SET state=?, execution_slot=?, lease=NULL, pid=NULL, worker_identity=NULL, "
                    "t3_archive_after=NULL, updated=?, context_json=?, context_hash=? WHERE msgid=? AND state='awaiting_clarification'",
                    (state, slot, stamp(), encoded, checksum, row['msgid']),
                )
                queued = True
            elif existing['state'] in pending_expiry.T3_QUEUE_STATES and existing['context_hash'] != checksum:
                conn.execute(
                    'UPDATE expiry_native.t3_jobs SET context_json=?, context_hash=?, updated=? WHERE msgid=?',
                    (encoded, checksum, stamp(), row['msgid']),
                )
        else:
            cfg = t3_client.load_config(root)
            conn.execute(
                '''INSERT INTO expiry_native.t3_jobs(
                    msgid,row_json,state,execution_slot,created,updated,context_json,context_hash,t3_project_id,
                    route_kind,requested_provider,requested_model,requested_effort,requested_options_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (
                    row['msgid'], canonical(row), 'queued', 'light', stamp(), stamp(),
                    encoded, checksum, cfg.get('t3_project_id') or '75aaaab5-7541-4157-8725-77e0cc4e6905',
                    route_kind, req_provider, req_model, req_effort, json.dumps(req_options, ensure_ascii=False),
                ),
            )
            queued = True
    if queued:
        wake(root)
    return True


def _tasks_root(root):
    cfg = t3_client.load_config(root)
    raw = cfg.get('tasks_root')
    if not raw:
        try:
            raw = json.loads((Path(root) / 'command-routing.json').read_text(encoding='utf-8-sig')).get('tasks_root')
        except (OSError, ValueError):
            raw = None
    return Path(raw or (Path.home() / 'Documents' / 'Codex' / 'WecomTasks')).resolve()


def _project_id(root, job):
    cfg = t3_client.load_config(root)
    return job['t3_project_id'] or cfg.get('t3_project_id') or '75aaaab5-7541-4157-8725-77e0cc4e6905'


def _title_for(context, prefix='肥鱼'):
    manifest = context['manifest']
    anchor = next((m for m in manifest.get('members', []) if m['msgid'] == manifest.get('anchor_msgid')), {})
    raw = (manifest.get('batch', {}).get('confirmed_request') or anchor.get('direct') or context['case_id']).strip()
    clean = re.sub(r'https?://\S+', '', raw)
    clean = re.sub(r'^(?:\s*[@＠]大肥鱼|\s*【[^】]{1,24}】)+', '', clean).strip()
    clean = ' '.join(clean.split())[:42]
    tag = context['case_id'][-7:]
    return f'{prefix}〔{tag}〕· {clean}' if clean else f'{prefix}〔{tag}〕'


def _completion_proof(job):
    if not job['workspace']:
        return None
    work = Path(job['workspace'])
    try:
        receipt = json.loads((work / 'completion.json').read_text(encoding='utf-8'))
        text = (work / 'result.txt').read_text(encoding='utf-8').strip()
        if (receipt.get('case_id') != job['msgid']
                or receipt.get('result_hash') != hashlib.sha256(text.encode('utf-8')).hexdigest()
                or not text
                or not receipt.get('t3_thread_id')):
            return None
        if job['lease'] and receipt.get('lease') and receipt.get('lease') != job['lease']:
            return None
        if job['t3_thread_id'] and receipt['t3_thread_id'] != job['t3_thread_id']:
            return None
        if job['route_kind'] in ESCALATED_ROUTES and receipt['t3_thread_id'] == job['t3_triage_thread_id']:
            return None
        return receipt
    except (OSError, ValueError, TypeError):
        return None


def _sync_control_mailbox(root, cid):
    from business_queue import db as bdb
    import task_controls

    with bdb(root) as c:
        return task_controls.mailbox(c, root, cid)


def _ack_controls(root, cid, change_ids, action='append'):
    from business_queue import db as bdb

    if not change_ids:
        return
    now = time.time()
    with bdb(root, True) as c:
        for ch_id in change_ids:
            c.execute(
                'UPDATE owner_task_controls SET native_seen=COALESCE(native_seen,?) WHERE target=? AND cid=?',
                (now, cid, ch_id),
            )
        if action == 'cancel':
            c.execute("UPDATE lanes SET state='cancelled', changed=?, detail='运行中收到本人取消指令，已安全停机' WHERE case_id=? AND target='local'", (now, cid))


def _fetch_link(root, thread_id):
    if not thread_id:
        return ''
    try:
        info = t3_client.show_thread(root, thread_id, messages=1)
        return (info.get('thread') or {}).get('link') or ''
    except Exception:
        return ''


def _record_result_file(work, text):
    _atomic(work / 'result.txt', text.strip() + '\n')


def auto_archive_config(root):
    cfg = t3_client.load_config(root)
    enabled_flag = bool(cfg.get('auto_archive_enabled', True))
    try:
        ttl = int(cfg.get('auto_archive_after_seconds', 600))
    except (TypeError, ValueError):
        ttl = 600
    ttl = max(60, ttl)
    archive_triage = bool(cfg.get('archive_triage_on_escalate', True))
    return {
        'enabled': enabled_flag,
        'after_seconds': ttl,
        'archive_triage_on_escalate': archive_triage,
    }


def _archive_deadline(root, now=None):
    cfg = auto_archive_config(root)
    if not cfg['enabled']:
        return None
    return (time.time() if now is None else float(now)) + cfg['after_seconds']


def _parse_iso_epoch(raw):
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _try_archive_triage(root, msgid, triage_thread_id):
    if not triage_thread_id:
        return False
    try:
        res = t3_client.archive_thread(root, triage_thread_id)
    except Exception as exc:
        log(root, msgid, 't3_triage_archive_deferred', f'{triage_thread_id} {type(exc).__name__}')
        return False
    if res.get('ok'):
        with db(root) as conn:
            conn.execute(
                'UPDATE t3_jobs SET t3_triage_archived_at=?, updated=? WHERE msgid=? AND t3_triage_thread_id=?',
                (stamp(), stamp(), msgid, triage_thread_id),
            )
        log(root, msgid, 't3_triage_archived', triage_thread_id)
        return True
    log(root, msgid, 't3_triage_archive_deferred', f'{triage_thread_id} {str(res.get("error") or "")[:180]}')
    return False


def _queue_post_append_receipt(root, msgid, context, change_ids, final_text):
    if not change_ids or not final_text:
        return
    manifest = (context or {}).get('manifest') or {}
    if manifest.get('return_route') == 'quiet':
        return
    from business_queue import db as bdb
    import chat_feedback
    import group_replies
    import private_files

    phase = 'append_final:' + str(change_ids[-1])
    if manifest.get('group_reply') and manifest.get('return_route') != 'owner_private':
        raw = final_text.strip()
        try:
            body = group_replies.public_text(raw, manifest, native=True, allow_web_links=True)
        except ValueError:
            clean = private_files.plain_source_citations(raw).strip()
            try:
                body = group_replies.public_body(clean, manifest, allow_web_links=True)
            except ValueError:
                return
    else:
        footer = chat_feedback.format_session_footer_for_case(root, msgid)
        body = chat_feedback.truncate_wecom_markdown(
            f'追加办理结果〔{msgid[-7:]}〕\n{final_text.strip()}' + footer,
            footer=footer,
        )
    with bdb(root, True) as c:
        c.execute(
            "INSERT OR IGNORE INTO chat_receipts(case_id,phase,state,due,body) VALUES(?,?,'ready',?,?)",
            (msgid, phase, time.time(), body),
        )


def _refresh_manifest_clarifications(root, msgid, context, work, *, call=None):
    """Refresh clarification_history and (if empty while pending) live available_groups in manifest.json."""
    from business_queue import db as bdb
    import group_relay

    changed = False
    with bdb(root) as c:
        hist = clarifications.history(c, msgid)
        if context['manifest'].get('clarification_history') != hist:
            context['manifest']['clarification_history'] = hist
            changed = True
        gtr = context['manifest'].get('group_target_resolution') or {}
        if gtr.get('status') == 'pending_model_selection' and not (gtr.get('available_groups') or []):
            live_groups = group_relay.list_groups(call or cli_call)
            if live_groups:
                enriched = group_relay.collect_group_clues(c, live_groups)
                anchor = next((m for m in context['manifest'].get('members', []) if m['msgid'] == context['manifest'].get('anchor_msgid')), {})
                ordered = group_relay.rank_group_candidates(
                    gtr.get('query') or '', enriched, context_text=anchor.get('direct') or '', limit=None,
                )
                gtr = {**gtr, 'suggested_top3': [g['chat_id'] for g in ordered[:3]], 'available_groups': ordered[:24]}
                context['manifest']['group_target_resolution'] = gtr
                changed = True
    if changed and work and Path(work).is_dir():
        _atomic(Path(work) / 'manifest.json', json.dumps(context['manifest'], ensure_ascii=False, indent=2))


def _persist_bound_context(root, msgid, context, work, *, cancel_group=False):
    """Atomically sync mutated cross-group target binding across business.sqlite3 and t3_jobs in ONE transaction."""
    from business_queue import dumped
    import pending_expiry

    encoded, checksum = canonical(context), digest(context)
    now = time.time()
    ts = stamp()
    with pending_expiry.native_reservation(root) as conn:
        if cancel_group:
            conn.execute("UPDATE request_routes SET destination='original' WHERE case_id=?", (msgid,))
        conn.execute('UPDATE cases SET frozen=?, updated=? WHERE id=?', (dumped(context), now, msgid))
        conn.execute(
            'UPDATE expiry_native.t3_jobs SET context_json=?, context_hash=?, updated=? WHERE msgid=?',
            (encoded, checksum, ts, msgid),
        )
    if work and Path(work).is_dir():
        _atomic(Path(work) / 'manifest.json', json.dumps(context['manifest'], ensure_ascii=False, indent=2))
    return checksum


def _bind_model_target_group(root, msgid, job, context, work, parsed, *, call=None):
    """Validate and bind target_group before COMPLETED.

    Security invariants:
    1. An explicit answered owner clarification choice (e.g. "只回我私聊" or option A/B/C bound to a live chat_id)
       is enforced AUTHORITATIVELY by the bridge, overriding any conflicting LLM target_group output.
    2. Otherwise, model-selected target_group MUST match a live verified group in 大肥鱼 sessions list.
    3. Any missing, unverified, or hallucinated target_group FAILS CLOSED into a <=3 candidate ASK.
    """
    import group_relay
    from business_queue import db as bdb

    manifest = context['manifest']
    rroute = manifest.get('return_route')
    if rroute not in ('cross_group_draft', 'cross_group_direct'):
        return 'not_applicable', None, job['context_hash']
    existing = manifest.get('target_group') or {}
    if existing.get('chat_id'):
        return 'already_bound', existing, job['context_hash']

    call = call or cli_call
    mode = 'direct' if rroute == 'cross_group_direct' else 'draft'
    live_groups = group_relay.list_groups(call)
    res_meta = dict(manifest.get('group_target_resolution') or {})

    # 1. Authoritative owner clarification choice ALWAYS overrides LLM output!
    with bdb(root) as c:
        clar_hist = clarifications.history(c, msgid)
    owner_choice = group_relay.resolve_owner_clarification_choice(clar_hist, live_groups, mode=mode)
    validated = owner_choice if owner_choice is not None else group_relay.validate_model_target_group(
        parsed.get('target_group'),
        groups=live_groups,
        mode=mode,
    )

    if validated and validated.get('cancel_group'):
        manifest['return_route'] = 'original'
        res_meta['status'] = 'cancelled_to_private'
        res_meta['resolution_reason'] = validated.get('reason') or ''
        manifest['group_target_resolution'] = res_meta
        new_hash = _persist_bound_context(root, msgid, context, work, cancel_group=True)
        log(root, msgid, 't3_target_group_cancelled_to_private', validated.get('reason') or '')
        return 'bound_private', validated, new_hash

    if validated and validated.get('chat_id'):
        # Verify optional followup_id belongs to this exact group before storing
        fid = validated.get('followup_id')
        if fid:
            with bdb(root) as c:
                frow = c.execute(
                    'SELECT id FROM colleague_followups WHERE id=? AND chat=?',
                    (fid, validated['chat_id']),
                ).fetchone()
            if not frow:
                validated['followup_id'] = None
        manifest['target_group'] = validated
        res_meta['status'] = 'bound'
        manifest['group_target_resolution'] = res_meta
        new_hash = _persist_bound_context(root, msgid, context, work, cancel_group=False)
        log(root, msgid, 't3_target_group_bound', f'{validated["chat_id"]} ({validated["chat_name"]})')
        return 'bound_group', validated, new_hash

    # Fail closed: model omitted target_group or returned an unverified/stale chat_id -> force <=3 candidate ASK!
    with bdb(root) as c:
        enriched = group_relay.collect_group_clues(c, live_groups)
    query = res_meta.get('query') or ''
    anchor = next((m for m in manifest.get('members', []) if m['msgid'] == manifest.get('anchor_msgid')), {})
    ranked = group_relay.rank_group_candidates(query, enriched, context_text=anchor.get('direct') or '', limit=3)
    opts, bindings = group_relay.format_group_options_with_bindings(ranked, limit=3)
    fallback_ask = {
        'reason': '模型未返回经大肥鱼实名核验的有效目标群，已拦截发送并请你确认',
        'summary': f'目标群「{query or "未指定"}」尚未核验锁定。为防发错群，请从以下候选群中选择，或改为只回私聊：',
        'questions': [{
            'id': 'target_group',
            'question': '请选择本次要发送的目标群（或改为仅回你私聊）：',
            'options': opts,
            'option_bindings': bindings,
        }],
    }
    log(root, msgid, 't3_target_group_unverified_ask', f'raw={str(parsed.get("target_group"))[:120]}')
    return 'needs_ask', fallback_ask, job['context_hash']


def _default_launch(root, job, context, provider, model, *, lightweight):
    options = {}
    if (not context.get('explicit_model') and t3_client.load_config(root).get('quota_relay_enabled')
            and quota_relay.pool_for_model(provider, model) == quota_relay.GEMINI
            and quota_relay.is_pool_exhausted(root, quota_relay.GEMINI)):
        target = quota_relay.choose_relay_target(root, quota_relay.GEMINI, lightweight=lightweight)
        if not target['ok']:
            raise RuntimeError('quota_default_launch_blocked: ' + target['reason'])
        provider, model, options = target['provider'], target['model'], target['options']
        update_state(root, job['msgid'], 'running', requested_provider=provider, requested_model=model,
                     requested_options_json=canonical(options), requested_effort=next(iter(options.values()), None))
        log(root, job['msgid'], 't3_proactive_pool_fallback', f'{provider}/{model}')
    return provider, model, ({'options': options} if options else {})


def _start_escalated(root, job, context, work, *, stop_mailbox=None):
    msgid = job['msgid']
    esc_info = json.loads(job['escalate_info_json'])
    _refresh_manifest_clarifications(root, msgid, context, work)
    strict = settings.read(root).get('clarification_mode') == 'strict'
    prompt = ambiguity_gate.build_escalated_prompt(work, context, esc_info, strict=strict)
    _atomic(work / 'escalated-prompt.txt', prompt)
    (work / 't3-response.json').unlink(missing_ok=True)
    provider, model, extra = _default_launch(root, job, context, job['requested_provider'], job['requested_model'], lightweight=False)
    with t3_client.thread_ready_hook(lambda tid, _rid=None, lk='': _bind_live_thread(root, msgid, tid, lk)):
        res = t3_client.start_thread(
            root, _project_id(root, job), provider, model,
            _title_for(context, prefix='肥鱼高阶'), prompt, wait=True, **extra,
        )
    with db(root) as conn:
        fresh_job = conn.execute('SELECT * FROM t3_jobs WHERE msgid=?', (msgid,)).fetchone()
    return _handle_outcome(
        root, msgid, job['lease'], fresh_job, context, work, res,
        allow_escalate=False, post_append_cids=esc_info.get('post_append_cids'), stop_mailbox=stop_mailbox,
    )


def _park_for_clarification(root, msgid, thread_id, link, manifest, parsed, used_models, *, origin):
    from business_queue import initialize
    initialize(root)
    reason = parsed.get('reason') or '指令存在歧义或信息不足'
    with pending_expiry.native_reservation(root) as conn:
        qid, _ = clarifications.open_question(
            conn, msgid, thread_id=thread_id, chat=manifest['chatid'], sender=manifest['sender'],
            origin=origin, reason=reason, questions=parsed.get('questions') or [],
            summary=parsed.get('summary') or '', quiet=manifest.get('return_route') == 'quiet',
            bot_channel=manifest.get('bot_channel'),
        )
        conn.execute(
            "UPDATE expiry_native.t3_jobs SET state='awaiting_clarification', updated=?, t3_thread_id=?, t3_link=?, "
            "clarification_qid=?, t3_archive_after=NULL, used_models_json=?, last_event='awaiting_clarification', "
            "lease=NULL, pid=NULL, worker_identity=NULL, detail=? WHERE msgid=?",
            (stamp(), thread_id, link, qid, canonical(used_models), f'已触发追问 {qid}：{reason[:140]}', msgid),
        )
        conn.execute('INSERT INTO expiry_native.log(at,msgid,kind,detail) VALUES(?,?,?,?)',
                     (stamp(), msgid, 'clarification_asked', f'{qid} {reason[:180]}'))
    return True


def _handle_outcome(root, msgid, lease, job, context, work, res, *, allow_escalate=True, explicit_passthrough=None, post_append_cids=None, stop_mailbox=None, defer_relay_wait=False):
    """Process one T3 turn result: ASK, ESCALATE, BLOCK, or COMPLETED (with append-hold check)."""
    from business_queue import db as bdb

    thread_id = res.get('thread_id') or job['t3_thread_id']
    link = _fetch_link(root, thread_id) or job['t3_link'] or ''
    failed = res['exit_code'] not in (0, 2) or str(res.get('status') or '').lower() in (
        'failed', 'error', 'interrupted', 'cancelled')
    if failed:
        with db(root) as conn:
            bound = conn.execute("UPDATE t3_jobs SET t3_thread_id=?,t3_link=?,state='running',updated=? "
                                 "WHERE msgid=? AND lease IS ? AND state IN ('running','starting','launching') "
                                 "AND (t3_thread_id IS NULL OR t3_thread_id=?)",
                                 (thread_id, link, stamp(), msgid, job['lease'], thread_id)).rowcount
        if not bound:
            return False
    elif thread_id:
        update_state(root, msgid, 'running', t3_thread_id=thread_id, t3_link=link, last_event='t3_turn_finished')

    changes = _sync_control_mailbox(root, msgid)
    cancels = [ch for ch in changes if ch['action'] == 'cancel']
    if cancels:
        if stop_mailbox:
            stop_mailbox()
        _ack_controls(root, msgid, [ch['change_id'] for ch in cancels], action='cancel')
        update_state(root, msgid, 'cancelled', t3_thread_id=thread_id, t3_link=link,
                     t3_archive_after=_archive_deadline(root), detail='本人已取消任务')
        log(root, msgid, 't3_cancelled', 'owner_cancel_applied')
        return True

    if failed:
        if t3_client.load_config(root).get('quota_relay_enabled'):
            try:
                launched = quota_relay.try_relay_feiyu_job(root, job, context, work, res)
            except (OSError, RuntimeError, ValueError, sqlite3.Error, subprocess.TimeoutExpired) as exc:
                log(root, msgid, 'quota_relay_error', str(exc))
                launched = None
            if launched:
                if defer_relay_wait or launched.get('already_handled'):
                    return True
                with db(root) as conn:
                    bound = conn.execute('SELECT * FROM t3_jobs WHERE msgid=?', (msgid,)).fetchone()
                outcome = t3_client.wait_for_thread(root, launched['thread_id'], run_id=launched.get('run_id'))
                return _handle_outcome(root, msgid, lease, bound, context, work, outcome, allow_escalate=allow_escalate,
                                       explicit_passthrough=explicit_passthrough, post_append_cids=post_append_cids,
                                       stop_mailbox=stop_mailbox)
        err = str(res.get('error') or res.get('reply') or f't3_exit_{res["exit_code"]}')
        with db(root) as conn:
            changed = conn.execute("UPDATE t3_jobs SET state='failed',updated=?,t3_archive_after=?,detail=? "
                                   "WHERE msgid=? AND lease IS ? AND t3_thread_id IS ? AND state='running'",
                                   (stamp(), _archive_deadline(root), err[:240], msgid, job['lease'], thread_id)).rowcount
        if changed:
            log(root, msgid, 't3_failed', err[:240])
        return False

    parsed = ambiguity_gate.parse_t3_response(res.get('reply', ''), work, waiting_for_answer=bool(res.get('waiting_for_answer')))
    action = parsed['action']
    used_models = parsed.get('used_models') or []
    proof_file = work / 'execution-proof.json'
    if proof_file.is_file():
        try:
            proof = json.loads(proof_file.read_text(encoding='utf-8-sig'))
            if isinstance(proof, dict) and proof.get('executedModel'):
                used_models = [{
                    'provider': proof.get('providerInstanceId') or job['requested_provider'],
                    'model': proof.get('executedModel'),
                    'options': proof.get('options'),
                    'child_thread_id': proof.get('childThreadId'),
                    'role': 'explicit_specialist',
                }]
        except (OSError, ValueError):
            pass

    if action == 'ASK':
        import group_relay
        manifest = context['manifest']
        gtr = manifest.get('group_target_resolution') or {}
        if (manifest.get('return_route') in ('cross_group_draft', 'cross_group_direct')
                and gtr.get('status') == 'pending_model_selection'
                and not (manifest.get('target_group') or {}).get('chat_id')):
            live_groups = group_relay.list_groups(cli_call)
            with bdb(root) as c:
                enriched = group_relay.collect_group_clues(c, live_groups)
            anchor = next((m for m in manifest.get('members', []) if m['msgid'] == manifest.get('anchor_msgid')), {})
            parsed['questions'] = group_relay.normalize_target_group_questions(
                parsed.get('questions') or [], live_groups, enriched,
                query=gtr.get('query') or '', context_text=anchor.get('direct') or '',
            )
        if stop_mailbox:
            stop_mailbox()
        return _park_for_clarification(
            root, msgid, thread_id, link, manifest, parsed, used_models, origin='agent_gate',
        )

    if action == 'ESCALATE':
        if not allow_escalate:
            update_state(
                root, msgid, 'failed',
                t3_thread_id=thread_id, t3_link=link,
                t3_archive_after=_archive_deadline(root),
                used_models_json=json.dumps(used_models, ensure_ascii=False),
                last_event='escalate_disallowed',
                detail='unexpected_escalate_disallowed',
            )
            log(root, msgid, 't3_escalate_disallowed', f'thread={thread_id}')
            return False
        esc_info = dict(parsed['escalate'], workload_tier=parsed['workload_tier'])
        if post_append_cids:
            esc_info['post_append_cids'] = post_append_cids
        heavy = esc_info['workload_tier'] != 'light' or job['execution_slot'] in HEAVY_SLOTS
        esc_cfg = settings.read(root)['escalation_model']
        esc_provider, esc_model, esc_effort = esc_cfg['provider'], esc_cfg['model'], esc_cfg.get('effort')
        update_state(
            root, msgid, 'running',
            t3_triage_thread_id=thread_id,
            t3_triage_archived_at=None,
        )
        if auto_archive_config(root)['archive_triage_on_escalate'] and thread_id:
            _try_archive_triage(root, msgid, thread_id)
        if heavy and stop_mailbox:
            stop_mailbox()
        detail = f'升级至 {esc_model} ({esc_effort})：{parsed["reason"][:120]}'
        with pending_expiry.native_reservation(root) as conn:
            lease_fields = ', lease=NULL, pid=NULL, worker_identity=NULL' if heavy else ''
            changed = conn.execute(
                'UPDATE expiry_native.t3_jobs SET state=?, execution_slot=?, route_kind=?, '
                'requested_provider=?, requested_model=?, requested_effort=?, requested_options_json=NULL, '
                'escalate_info_json=?, t3_thread_id=NULL, t3_link=NULL, clarification_qid=NULL, '
                't3_archive_after=NULL, updated=?, last_event=?, detail=?' + lease_fields +
                " WHERE msgid=? AND state='running' AND lease IS ?",
                ('heavy_queued' if heavy else 'running', 'regular' if heavy else 'light',
                 'escalated_neon' if heavy else 'escalated_light', esc_provider, esc_model, esc_effort,
                 canonical(esc_info), stamp(), 'heavy_queued' if heavy else 'escalating_to_neon',
                 detail, msgid, job['lease']),
            ).rowcount
            if not changed:
                return False
            conn.execute('INSERT INTO expiry_native.log(at,msgid,kind,detail) VALUES(?,?,?,?)',
                         (stamp(), msgid, 't3_escalated', f'from={thread_id} to={esc_provider}/{esc_model}({esc_effort})'))
            if heavy:
                conn.execute(
                    "UPDATE lanes SET state='heavy_queued', detail=?, changed=? WHERE case_id=? AND target='local' "
                    "AND state NOT IN ('cancelled','expired')",
                    (detail, time.time(), msgid),
                )
            else:
                fresh_job = conn.execute('SELECT * FROM expiry_native.t3_jobs WHERE msgid=?', (msgid,)).fetchone()
        if heavy:
            if stop_mailbox is None:
                # Recovery callers have no worker finally block to signal the queue.
                wake(root)
            return True
        return _start_escalated(root, fresh_job, context, work, stop_mailbox=stop_mailbox)

    if action == 'BLOCK':
        reason = parsed.get('summary') or parsed.get('reason') or '任务受阻，无法继续执行。'
        _record_result_file(work, reason)
        update_state(
            root, msgid, 'blocked',
            t3_thread_id=thread_id, t3_link=link,
            t3_archive_after=_archive_deadline(root),
            used_models_json=json.dumps(used_models, ensure_ascii=False),
            last_event='blocked',
            detail=reason[:240],
        )
        log(root, msgid, 't3_blocked', reason[:240])
        return False

    # COMPLETED: check for unapplied in-flight 【追加】 before publishing final result!
    unapplied_appends = [ch for ch in changes if ch['action'] == 'append' and not ch.get('native_acknowledged')]
    if unapplied_appends and thread_id:
        for _ in range(3):
            app_prompt = ambiguity_gate.build_append_followup_prompt(work, context, unapplied_appends, explicit_passthrough=explicit_passthrough)
            update_state(root, msgid, 'running', t3_archive_after=None, last_event='applying_append', detail=f'正在向 T3 线程同步 {len(unapplied_appends)} 条【追加】')
            log(root, msgid, 't3_append_sent', ','.join(ch['change_id'] for ch in unapplied_appends))
            try:
                (work / 't3-response.json').unlink(missing_ok=True)
            except OSError:
                pass
            res2 = t3_client.send_message(root, thread_id, app_prompt, wait=True)
            applied_ids = [ch['change_id'] for ch in unapplied_appends]
            _ack_controls(root, msgid, applied_ids, action='append')
            with db(root) as conn:
                fresh_job = conn.execute('SELECT * FROM t3_jobs WHERE msgid=?', (msgid,)).fetchone()
            merged_cids = [*(post_append_cids or []), *applied_ids] if post_append_cids else None
            return _handle_outcome(root, msgid, lease, fresh_job, context, work, res2, allow_escalate=False,
                                   explicit_passthrough=explicit_passthrough, post_append_cids=merged_cids, stop_mailbox=stop_mailbox)

    final_text = (parsed.get('final_answer') or '').strip()
    if not final_text and (work / 'result.txt').is_file():
        final_text = (work / 'result.txt').read_text(encoding='utf-8-sig').strip()
    if not final_text:
        update_state(
            root, msgid, 'failed',
            t3_thread_id=thread_id, t3_link=link,
            t3_archive_after=_archive_deadline(root),
            detail='empty_t3_final_answer',
        )
        return False

    # Verify & bind model-selected cross-group target BEFORE recording durable completion!
    bind_status, bind_payload, current_context_hash = _bind_model_target_group(
        root, msgid, job, context, work, parsed,
    )
    if bind_status == 'needs_ask':
        if stop_mailbox:
            stop_mailbox()
        return _park_for_clarification(
            root, msgid, thread_id, link, context['manifest'], bind_payload, used_models,
            origin='agent_target_group_verify',
        )

    _record_result_file(work, final_text)
    result_hash = hashlib.sha256(final_text.encode('utf-8')).hexdigest()
    if not used_models:
        used_models = [{
            'provider': job['requested_provider'],
            'model': job['requested_model'],
            'effort': job['requested_effort'],
            'role': job['route_kind'],
        }]
    receipt = {
        'case_id': msgid,
        'lease': lease,
        'context_hash': current_context_hash,
        't3_thread_id': thread_id,
        't3_triage_thread_id': job['t3_triage_thread_id'],
        't3_link': link,
        'route_kind': job['route_kind'],
        'used_models': used_models,
        'result_hash': result_hash,
        'completed_at': stamp(),
    }
    if bind_status in ('bound_group', 'bound_private', 'already_bound') and bind_payload:
        receipt['target_group'] = bind_payload
    _atomic(work / 'completion.json', json.dumps(receipt, ensure_ascii=False, indent=2))
    if stop_mailbox:
        stop_mailbox()
    archive_after = _archive_deadline(root)
    update_state(
        root, msgid, 'completed',
        t3_thread_id=thread_id, t3_link=link,
        clarification_qid=None,
        t3_archived_at=None,
        t3_archive_after=archive_after,
        used_models_json=json.dumps(used_models, ensure_ascii=False),
        turns=(job['turns'] or 0) + 1,
        result_hash=result_hash,
        last_event='completed',
        detail='t3_durable_result_verified',
    )
    if post_append_cids:
        _queue_post_append_receipt(root, msgid, context, post_append_cids, final_text)
    log(root, msgid, 't3_completed', f'thread={thread_id} route={job["route_kind"]}')
    return True


def run_job(root, msgid, lease=None):
    """Execute or resume one reserved T3 job."""
    root = Path(root)
    with db(root) as conn:
        job = conn.execute('SELECT * FROM t3_jobs WHERE msgid=?', (msgid,)).fetchone()
        if not job or job['state'] != 'launching' or not lease or not secrets.compare_digest(job['lease'] or '', lease):
            return False
        claimed = conn.execute(
            "UPDATE t3_jobs SET state='starting', updated=?, pid=?, worker_identity=? WHERE msgid=? AND state='launching' AND lease=?",
            (stamp(), os.getpid(), _process_identity(os.getpid()), msgid, lease),
        ).rowcount
    if not claimed:
        return False

    context = json.loads(job['context_json'])
    row = json.loads(job['row_json'])
    mailbox_stop = threading.Event()
    mailbox_worker = None

    def stop_mailbox():
        mailbox_stop.set()
        if mailbox_worker is not None:
            mailbox_worker.join()

    work = None
    turn_started_ts = time.time()
    try:
        from business_queue import initialize as init_business_db
        init_business_db(root)
        if digest(context) != job['context_hash'] or not _eligible(root, row, context):
            raise ValueError('frozen_authority_or_owner_binding_invalid')

        work = _tasks_root(root) / ('business-' + hashlib.sha256(msgid.encode('utf-8')).hexdigest()[:24])
        work.mkdir(parents=True, exist_ok=True)
        (work / 'outputs').mkdir(exist_ok=True)
        _atomic(work / 'manifest.json', json.dumps(context['manifest'], ensure_ascii=False, indent=2))
        _atomic(work / 'source.json', json.dumps(row, ensure_ascii=False, indent=2))

        import task_controls
        mailbox_worker = task_controls.start_native_mailbox(root, work, msgid, mailbox_stop, return_worker=True)

        if job['route_kind'] == 'escalated_neon' and not job['t3_thread_id']:
            cancels = [ch['change_id'] for ch in _sync_control_mailbox(root, msgid) if ch['action'] == 'cancel']
            if cancels:
                _ack_controls(root, msgid, cancels, action='cancel')
                update_state(root, msgid, 'cancelled', detail='本人已取消任务')
                return True
            update_state(root, msgid, 'running', workspace=str(work), last_event='t3_starting')
            return _start_escalated(root, job, context, work, stop_mailbox=stop_mailbox)

        from private_doc_read import read_snapshot
        readback = read_snapshot(root, row, context, cli_call)
        if readback:
            _atomic(work / 'wecom-readback.json', json.dumps(readback, ensure_ascii=False, indent=2))

        update_state(root, msgid, 'running', workspace=str(work), last_event='t3_starting')

        prefs = settings.read(root)
        strict = prefs.get('clarification_mode', 'strict') == 'strict'
        explicit = context.get('explicit_model')
        explicit_passthrough = explicit if (explicit and not explicit.get('matches_defaults')) else None

        # Clear any stale t3-response.json from an earlier turn before invoking T3 so parse_t3_response never reads stale data.
        try:
            (work / 't3-response.json').unlink(missing_ok=True)
        except OSError:
            pass

        # Case 1a: Resume existing T3 thread after clarification answer
        if job['t3_thread_id'] and job['clarification_qid']:
            from business_queue import db as bdb
            with bdb(root) as c:
                crow = c.execute(
                    'SELECT qid, answer_text FROM clarifications WHERE qid=?',
                    (job['clarification_qid'],),
                ).fetchone()
            answer_text = crow['answer_text'] if crow else ''
            _refresh_manifest_clarifications(root, msgid, context, work)
            allow_escalate = job['route_kind'] == 'triage_hydrogen'
            prompt = ambiguity_gate.build_clarification_resume_prompt(
                work, context, job['clarification_qid'], answer_text,
                explicit_passthrough=explicit_passthrough, allow_escalate=allow_escalate,
            )
            _atomic(work / f'resume-{job["clarification_qid"]}.txt', prompt)
            res = t3_client.send_message(root, job['t3_thread_id'], prompt, wait=True)
            return _handle_outcome(
                root, msgid, lease, job, context, work, res,
                allow_escalate=allow_escalate,
                explicit_passthrough=explicit_passthrough,
                stop_mailbox=stop_mailbox,
            )

        # Case 1b: Resume existing T3 thread after post-completion 【追加】 within 10-min grace window
        if job['t3_thread_id']:
            changes = _sync_control_mailbox(root, msgid)
            unapplied = [ch for ch in changes if ch['action'] == 'append' and not ch.get('native_acknowledged')]
            if unapplied:
                prompt = ambiguity_gate.build_append_followup_prompt(
                    work, context, unapplied, explicit_passthrough=explicit_passthrough,
                )
                applied_ids = [ch['change_id'] for ch in unapplied]
                _atomic(work / f'append-{applied_ids[-1]}.txt', prompt)
                update_state(root, msgid, 'running', t3_archive_after=None, last_event='post_completion_append', detail=f'正在向原 T3 线程发送 {len(unapplied)} 条【追加】')
                log(root, msgid, 't3_post_append_sent', ','.join(applied_ids))
                res = t3_client.send_message(root, job['t3_thread_id'], prompt, wait=True)
                _ack_controls(root, msgid, applied_ids, action='append')
                return _handle_outcome(
                    root, msgid, lease, job, context, work, res,
                    allow_escalate=(job['route_kind'] == 'triage_hydrogen'),
                    explicit_passthrough=explicit_passthrough,
                    post_append_cids=applied_ids,
                    stop_mailbox=stop_mailbox,
                )

        # Case 2: Brand-new T3 thread launch
        if explicit and explicit.get('matches_defaults'):
            provider, model = explicit['provider'], explicit['model']
            prompt = ambiguity_gate.build_explicit_direct_prompt(work, context, explicit, strict=strict)
            title = _title_for(context, prefix=f'肥鱼 {model}')
            allow_escalate = False
        elif explicit:
            entry = prefs['entry_model']
            provider, model = entry['provider'], entry['model']
            prompt = ambiguity_gate.build_explicit_passthrough_prompt(work, context, explicit, strict=strict)
            title = _title_for(context, prefix=f'肥鱼直达 {explicit["model"]}')
            allow_escalate = False
        else:
            entry = prefs['entry_model']
            provider, model = entry['provider'], entry['model']
            prompt = ambiguity_gate.build_triage_prompt(work, context, prefs['escalation_model'], strict=strict)
            title = _title_for(context, prefix='肥鱼')
            allow_escalate = True

        _atomic(work / 'request.txt', prompt)
        provider, model, extra = _default_launch(root, job, context, provider, model, lightweight=True)
        with t3_client.thread_ready_hook(lambda tid, _rid=None, lk='': _bind_live_thread(root, msgid, tid, lk)):
            res = t3_client.start_thread(root, _project_id(root, job), provider, model, title, prompt, wait=True, **extra)
        with db(root) as conn:
            job = conn.execute('SELECT * FROM t3_jobs WHERE msgid=?', (msgid,)).fetchone()
        return _handle_outcome(
            root, msgid, lease, job, context, work, res,
            allow_escalate=allow_escalate,
            explicit_passthrough=explicit_passthrough,
            stop_mailbox=stop_mailbox,
        )
    except subprocess.TimeoutExpired as exc:
        if _recover_after_timeout(root, msgid, lease, context, work, exc, turn_started_ts, stop_mailbox=stop_mailbox):
            return True
        update_state(root, msgid, 'failed', detail=type(exc).__name__ + ':' + str(exc)[:200])
        log(root, msgid, 't3_worker_error', type(exc).__name__ + ':' + str(exc)[:200])
        return False
    except Exception as exc:
        update_state(root, msgid, 'failed', detail=type(exc).__name__ + ':' + str(exc)[:200])
        log(root, msgid, 't3_worker_error', type(exc).__name__ + ':' + str(exc)[:200])
        return False
    finally:
        stop_mailbox()
        wake(root)


def _bind_live_thread(root, msgid, thread_id, link=''):
    if not thread_id:
        return
    fields = {'t3_thread_id': thread_id, 'last_event': 't3_thread_started'}
    if link:
        fields['t3_link'] = link
    update_state(root, msgid, 'running', **fields)


def _recover_after_timeout(root, msgid, lease, context, work, exc, turn_started_ts, *, stop_mailbox=None):
    """Prevent false failed->blocked when a long T3 run actually finished or is still actively running."""
    with db(root) as conn:
        fresh_job = conn.execute('SELECT * FROM t3_jobs WHERE msgid=?', (msgid,)).fetchone()
    if not fresh_job:
        return False

    # 1. Inspect the live T3 thread first (works immediately now that t3_thread_id is persisted on launch).
    live = _recover_from_live_t3(root, fresh_job, stop_mailbox=stop_mailbox)
    if live == 'recovered':
        log(root, msgid, 't3_timeout_recovered_live', f'thread={fresh_job["t3_thread_id"] or ""}')
        return True
    if live == 'still_running':
        update_state(
            root, msgid, 'running',
            worker_identity=None,
            last_event='t3_wait_handed_to_reconcile',
            detail='单轮等待超限但 T3 线程仍在执行，已交由后台巡检持续追踪直至完成',
        )
        log(root, msgid, 't3_wait_timeout_handed_to_reconcile', f'thread={fresh_job["t3_thread_id"] or ""}')
        return True

    # 2. Fallback: recover from TimeoutExpired stdout JSON or a freshly written t3-response.json from this turn.
    allow_escalate = fresh_job['route_kind'] == 'triage_hydrogen'
    recovered_res = t3_client._recover_timeout_stdout(exc, fallback_thread_id=fresh_job['t3_thread_id'])
    if recovered_res is None and work and (work / 't3-response.json').is_file():
        resp_path = work / 't3-response.json'
        try:
            st = resp_path.stat()
            if st.st_mtime >= (turn_started_ts - 1.0):
                loaded = json.loads(resp_path.read_text(encoding='utf-8-sig'))
                act = str(loaded.get('action') or '').upper() if isinstance(loaded, dict) else ''
                if act in ('COMPLETED', 'ASK', 'BLOCK') or (act == 'ESCALATE' and allow_escalate):
                    tid = fresh_job['t3_thread_id'] or ''
                    recovered_res = {
                        'exit_code': 0,
                        'thread_id': tid,
                        'run_id': None,
                        'status': 'completed',
                        'reply': '',
                        'error': None,
                        'waiting_for_answer': False,
                        'raw': loaded,
                    }
        except (OSError, ValueError):
            recovered_res = None

    if recovered_res is not None:
        log(root, msgid, 't3_timeout_recovered_artifact', f'thread={recovered_res.get("thread_id") or ""}')
        return _handle_outcome(
            root, msgid, lease, fresh_job, context, work, recovered_res,
            allow_escalate=allow_escalate,
            explicit_passthrough=None,
            stop_mailbox=stop_mailbox,
        )
    return False


def _recover_from_live_t3(root, job, *, stop_mailbox=None):
    """Inspect a live T3 thread when the local Python wait-wrapper exited unexpectedly.

    Never replays or sends a new prompt. Returns:
    - 'still_running' if T3 server is still actively executing this thread;
    - 'recovered' if an already-completed or needs-input turn belonging to this job was harvested via _handle_outcome;
    - None if no safe proof exists (caller falls back to 'uncertain').
    """
    tid = job['t3_thread_id']
    if not tid or not t3_client.server_status(root)['ok']:
        return None
    # Never confuse an already-archived Hydrogen triage thread with an escalated Neon thread!
    if job['route_kind'] in ESCALATED_ROUTES and tid == job['t3_triage_thread_id']:
        return None
    try:
        info = t3_client.show_thread(root, tid, messages=8)
    except Exception:
        return None
    thread = info.get('thread') if isinstance(info, dict) else None
    if not isinstance(thread, dict):
        return None
    status = str(thread.get('status') or '').lower()
    pending_reqs = int(thread.get('pendingRequestCount') or 0)
    work = Path(job['workspace']) if job['workspace'] else None
    if pending_reqs > 0 and status not in ('completed', 'failed', 'cancelled', 'interrupted', 'error'):
        if not work or not work.is_dir():
            return None
        try:
            context = json.loads(job['context_json'])
        except (TypeError, ValueError):
            return None
        reply = t3_client._extract_latest_reply(info)
        res = {
            'exit_code': 2,
            'thread_id': tid,
            'run_id': thread.get('activeRunId') or thread.get('latestRunId'),
            'status': 'waiting_for_answer',
            'reply': reply,
            'error': None,
            'waiting_for_answer': True,
            'raw': info,
        }
        log(root, job['msgid'], 't3_recovered_waiting_for_answer', f'thread={tid}')
        _handle_outcome(
            root, job['msgid'], job['lease'] or '', job, context, work, res,
            allow_escalate=(job['route_kind'] == 'triage_hydrogen'),
            explicit_passthrough=None,
            stop_mailbox=stop_mailbox,
        )
        return 'recovered'
    if (
        status in ('running', 'starting', 'preparing', 'queued', 'waiting')
        or thread.get('activeRunId')
        or thread.get('pendingBackgroundTasks')
        or thread.get('blockedOnThreadWait')
    ):
        return 'still_running'
    failed = status in ('failed', 'error')
    if (status == 'idle' and job['last_event'] == 't3_quota_relayed' and not thread.get('latestRunId')
            and time.time() - (_parse_iso_epoch(job['updated']) or 0) <= t3_client.launch_timeout(root)):
        return 'still_running'
    if status != 'completed' and not failed:
        return None
    latest_run = thread.get('latestRunId')
    messages = info.get('messages') if isinstance(info.get('messages'), list) else []
    run_msgs = [m for m in messages if isinstance(m, dict) and (not latest_run or m.get('runId') == latest_run)]
    assistant = next((m for m in reversed(run_msgs) if m.get('role') == 'assistant' and isinstance(m.get('text'), str) and m.get('text').strip()), None)
    if not assistant and not failed:
        return None
    user_msg = next((m for m in reversed(run_msgs) if m.get('role') == 'user' and isinstance(m.get('text'), str)), None)
    user_text = user_msg['text'] if user_msg else ''
    job_updated_ts = _parse_iso_epoch(job['updated']) or 0.0
    resp_fresh = False
    if work and (work / 't3-response.json').is_file():
        try:
            resp_fresh = (work / 't3-response.json').stat().st_mtime >= (job_updated_ts - 2.0)
        except OSError:
            resp_fresh = False

    # Causality proof: ensure latestRunId is genuinely THIS job's turn, never a prior round.
    qid = job['clarification_qid']
    if qid:
        if qid not in user_text and not resp_fresh:
            return None
    elif (job['turns'] or 0) > 0:
        if not resp_fresh and '追加' not in user_text:
            return None

    if not work or not work.is_dir():
        return None
    try:
        context = json.loads(job['context_json'])
    except (TypeError, ValueError):
        return None
    res = {
        'exit_code': 1 if failed else 0,
        'thread_id': tid,
        'run_id': latest_run,
        'status': status,
        'reply': assistant['text'] if assistant else '',
        'error': thread.get('lastError') if failed else None,
        'waiting_for_answer': False,
        'raw': info,
    }
    log(root, job['msgid'], 't3_recovered_from_live_thread', f'thread={tid} run={latest_run or ""}')
    _handle_outcome(
        root,
        job['msgid'],
        job['lease'] or '',
        job,
        context,
        work,
        res,
        allow_escalate=(job['route_kind'] == 'triage_hydrogen'),
        explicit_passthrough=None,
        stop_mailbox=stop_mailbox,
        defer_relay_wait=stop_mailbox is None,
    )
    return 'recovered'


def reconcile(root):
    """Crash recovery audit for active T3 workers. Never auto-replays uncertain work."""
    with db(root) as conn:
        # Legacy initial jobs used the regular schema default. Old resume jobs kept their prior slot.
        conn.execute("UPDATE t3_jobs SET execution_slot='light' WHERE state='queued' AND execution_slot<>'light'")
        conn.execute("""UPDATE t3_jobs SET state='heavy_resume_queued', execution_slot='regular'
                        WHERE state='resume_queued' AND (execution_slot IN ('regular','heavy') OR route_kind='escalated_neon')""")
        conn.execute(f"""UPDATE t3_jobs SET lease=NULL, pid=NULL, worker_identity=NULL
                         WHERE state IN {pending_expiry.T3_QUEUE_SQL}
                         AND (lease IS NOT NULL OR pid IS NOT NULL OR worker_identity IS NOT NULL)""")
        active = conn.execute(
            f"SELECT * FROM t3_jobs WHERE state IN {ACTIVE_SQL}"
        ).fetchall()
    for job in active:
        if job['worker_identity'] and _process_identity(job['pid']) == job['worker_identity']:
            continue
        proof = _completion_proof(job)
        if proof:
            update_state(
                root, job['msgid'], 'completed',
                t3_thread_id=proof['t3_thread_id'],
                t3_link=proof.get('t3_link') or job['t3_link'],
                t3_archive_after=_archive_deadline(root),
                result_hash=proof['result_hash'],
                detail='recovered_from_t3_durable_receipt',
            )
            wake(root)
            continue
        live = _recover_from_live_t3(root, job)
        if live in ('still_running', 'recovered'):
            if live == 'recovered':
                wake(root)
            continue
        update_state(
            root, job['msgid'], 'uncertain',
            worker_identity=None,
            detail='t3_worker_absent_no_auto_replay',
        )
        wake(root)


def sweep_auto_archive(root, *, now=None):
    """Auto-archive escalated triage threads and idle completed Feiyu threads past their TTL.

    Strictly scoped to Feiyu Bridge threads inside the dedicated `t3_project_id` (肥鱼任务).
    Never touches pinned threads, active/running/waiting threads, threads awaiting clarification,
    or threads interacted with inside T3 Code within the last `auto_archive_after_seconds` (default 600s).
    """
    root = Path(root)
    summary = {'triage_archived': [], 'archived': [], 'postponed': []}
    acfg = auto_archive_config(root)
    if not acfg['enabled'] and not acfg['archive_triage_on_escalate']:
        return summary
    if not t3_client.server_status(root)['ok']:
        return summary
    now_ts = time.time() if now is None else float(now)
    ttl = acfg['after_seconds']
    active_statuses = {'running', 'starting', 'preparing', 'queued', 'waiting'}

    # Phase 1: Immediately archive redundant completed Hydrogen triage threads from escalated jobs.
    if acfg['archive_triage_on_escalate']:
        with db(root) as conn:
            triage_rows = conn.execute(
                """SELECT msgid, t3_triage_thread_id FROM t3_jobs
                   WHERE t3_triage_thread_id IS NOT NULL
                     AND t3_triage_archived_at IS NULL
                     AND COALESCE(t3_thread_id, '') <> t3_triage_thread_id"""
            ).fetchall()
        for row in triage_rows:
            tid = row['t3_triage_thread_id']
            if _try_archive_triage(root, row['msgid'], tid):
                summary['triage_archived'].append(tid)

    if not acfg['enabled']:
        return summary

    # Phase 2: Archive expired terminal t3_jobs threads after verifying live T3 idleness.
    with db(root) as conn:
        due_rows = conn.execute(
            """SELECT msgid, t3_thread_id, t3_archive_after FROM t3_jobs
               WHERE state IN ('completed','blocked','cancelled','failed')
                 AND t3_thread_id IS NOT NULL
                 AND t3_archived_at IS NULL
                 AND t3_archive_after IS NOT NULL
                 AND t3_archive_after <= ?""",
            (now_ts,),
        ).fetchall()
    for row in due_rows:
        tid = row['t3_thread_id']
        msgid = row['msgid']
        live_thread = None
        try:
            info = t3_client.show_thread(root, tid, messages=1)
            live_thread = info.get('thread') if isinstance(info, dict) else None
        except Exception:
            live_thread = None
        if isinstance(live_thread, dict):
            if live_thread.get('archived'):
                with db(root) as conn:
                    conn.execute('UPDATE t3_jobs SET t3_archived_at=?, updated=? WHERE msgid=?', (stamp(), stamp(), msgid))
                summary['archived'].append(tid)
                continue
            live_status = str(live_thread.get('status') or '').lower()
            if live_thread.get('pinned') or live_status in active_statuses or live_thread.get('pendingBackgroundTasks'):
                with db(root) as conn:
                    conn.execute('UPDATE t3_jobs SET t3_archive_after=?, updated=? WHERE msgid=?', (now_ts + ttl, stamp(), msgid))
                summary['postponed'].append(tid)
                continue
            updated_ts = _parse_iso_epoch(live_thread.get('updatedAt'))
            if updated_ts is not None and (now_ts - updated_ts) < ttl:
                with db(root) as conn:
                    conn.execute('UPDATE t3_jobs SET t3_archive_after=?, updated=? WHERE msgid=?', (updated_ts + ttl, stamp(), msgid))
                summary['postponed'].append(tid)
                continue
        try:
            res = t3_client.archive_thread(root, tid)
        except Exception as exc:
            log(root, msgid, 't3_archive_error', f'{tid} {type(exc).__name__}')
            continue
        if res.get('ok'):
            with db(root) as conn:
                conn.execute('UPDATE t3_jobs SET t3_archived_at=?, updated=? WHERE msgid=?', (stamp(), stamp(), msgid))
            log(root, msgid, 't3_auto_archived', tid)
            summary['archived'].append(tid)
        elif res.get('busy'):
            with db(root) as conn:
                conn.execute('UPDATE t3_jobs SET t3_archive_after=?, updated=? WHERE msgid=?', (now_ts + min(120, ttl), stamp(), msgid))
            summary['postponed'].append(tid)

    # Phase 3: Clean up idle historical/orphan completed threads strictly inside the dedicated Feiyu project.
    cfg = t3_client.load_config(root)
    project_id = cfg.get('t3_project_id')
    if not project_id:
        return summary
    try:
        listed = t3_client.list_threads(root, project_id)
    except Exception:
        return summary
    with db(root) as conn:
        all_jobs = conn.execute(
            'SELECT msgid, state, t3_thread_id, t3_triage_thread_id, t3_archive_after FROM t3_jobs'
        ).fetchall()
    protected = set()
    for j in all_jobs:
        if j['state'] not in TERMINAL or (j['t3_archive_after'] is not None and float(j['t3_archive_after']) > now_ts):
            if j['t3_thread_id']:
                protected.add(j['t3_thread_id'])
        if j['state'] not in TERMINAL and j['t3_triage_thread_id'] and j['t3_triage_thread_id'] == j['t3_thread_id']:
            protected.add(j['t3_triage_thread_id'])

    already_done = set(summary['archived']) | set(summary['triage_archived']) | set(summary['postponed'])
    for th in listed:
        tid = th.get('threadId') or th.get('id')
        if not tid or tid in protected or tid in already_done:
            continue
        # Hard guard: NEVER touch threads outside the dedicated Feiyu project!
        if th.get('projectId') and th.get('projectId') != project_id:
            continue
        if th.get('parentThreadId') or th.get('pinned') or th.get('archived'):
            continue
        if th.get('pendingBackgroundTasks'):
            continue
        status = str(th.get('status') or '').lower()
        if status not in ('completed', 'idle', 'failed', 'cancelled'):
            continue
        updated_ts = _parse_iso_epoch(th.get('updatedAt') or th.get('createdAt'))
        if updated_ts is None or (now_ts - updated_ts) < ttl:
            continue
        try:
            res = t3_client.archive_thread(root, tid)
        except Exception:
            continue
        if res.get('ok'):
            with db(root) as conn:
                conn.execute('UPDATE t3_jobs SET t3_archived_at=?, updated=? WHERE t3_thread_id=? AND t3_archived_at IS NULL', (stamp(), stamp(), tid))
                conn.execute('UPDATE t3_jobs SET t3_triage_archived_at=?, updated=? WHERE t3_triage_thread_id=? AND t3_triage_archived_at IS NULL', (stamp(), stamp(), tid))
            log(root, '', 't3_orphan_archived', tid)
            summary['archived'].append(tid)
    return summary


def tick(root):
    if not enabled(root):
        return False
    if not t3_client.server_status(root)['ok']:
        return False
    max_regular, max_light = slot_limits(t3_client.load_config(root))
    with db(root):
        pass
    row = None
    with pending_expiry.native_reservation(root) as conn:
        counts = dict(conn.execute(f"SELECT execution_slot, COUNT(*) FROM expiry_native.t3_jobs WHERE state IN {ACTIVE_SQL} GROUP BY execution_slot"))
        pools = (
            ('regular', HEAVY_SLOTS, max_regular, 'heavy_resume_queued', 'heavy_queued'),
            ('light', LIGHT_SLOTS, max_light, 'resume_queued', 'queued'),
        )
        for slot, aliases, limit, resume, queued in pools:
            if sum(counts.get(alias, 0) for alias in aliases) >= limit:
                continue
            candidates = conn.execute(
                "SELECT j.msgid, j.state FROM expiry_native.t3_jobs j JOIN cases c ON c.id=j.msgid "
                "JOIN lanes l ON l.case_id=c.id AND l.target='local' WHERE j.state IN (?,?) "
                "AND c.category='work' AND l.state IN ('queued','submitted','resume_queued','heavy_queued','heavy_resume_queued') "
                "AND NOT EXISTS (SELECT 1 FROM observations o WHERE o.case_id=j.msgid AND o.source=?) "
                "ORDER BY (j.state=?) DESC, j.created, j.rowid",
                (resume, queued, pending_expiry.AUDIT, resume),
            )
            row = next((r for r in candidates if not pending_expiry.expired(conn, root, r['msgid'])), None)
            candidates.close()
            if row:
                break
        if not row:
            return False
        lease = secrets.token_hex(32)
        conn.execute(
            "UPDATE expiry_native.t3_jobs SET state='launching', updated=?, lease=?, execution_slot=?, pid=NULL, worker_identity=NULL WHERE msgid=? AND state=?",
            (stamp(), lease, slot, row['msgid'], row['state']),
        )
    try:
        with (Path(root) / 'local-launch.log').open('ab') as output:
            cmd = [sys.executable, str(Path(__file__).resolve()), 'job', '--root', str(root), '--msgid', row['msgid'], '--lease', lease]
            try:
                proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=output, stderr=output, **_hidden(detached=True))
            except PermissionError:
                flags = subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS
                proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=output, stderr=output, creationflags=flags)
        with db(root) as conn:
            conn.execute("UPDATE t3_jobs SET pid=?, worker_identity=? WHERE msgid=? AND state='launching' AND lease=?",
                         (proc.pid, _process_identity(proc.pid), row['msgid'], lease))
        log(root, row['msgid'], 't3_worker_started', 'pid=' + str(proc.pid))
    except Exception as exc:
        update_state(root, row['msgid'], 'uncertain', detail='launch_' + type(exc).__name__)
    return True


def serve(stop, root, on_change=None):
    root = Path(root)
    from win_runtime import SingleInstance
    with SingleInstance('T3BusinessDispatcher', root) as lock:
        if not lock.acquired:
            return
        signal = T3Signal(root)
        quota_worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix='t3-quota-audit')
        quota_future = None

        def queue_quota_audit():
            nonlocal quota_future
            if quota_future is not None:
                if not quota_future.done():
                    return
                try:
                    quota_future.result()
                except Exception as exc:
                    log(root, '', 't3_quota_sweep_error', str(exc))
            quota_future = quota_worker.submit(quota_relay.sweep_all_t3_threads, root)

        def heartbeat():
            t3_st = t3_client.server_status(root)
            cfg = t3_client.load_config(root)
            regular, light = slot_limits(cfg)
            _atomic(root / 'local-dispatch-status.json', canonical({
                'pid': os.getpid(),
                'engine': 't3',
                'state': 'running',
                'updated_at': stamp(),
                't3_server_ok': t3_st['ok'],
                't3_server_reason': t3_st['reason'],
                't3_server_port': t3_st['port'],
                't3_project_id': cfg.get('t3_project_id'),
                'regular_slots': regular,
                'light_slots': light,
                'light_burst_slots': light,
                'native_slots': regular,
                'private_office_burst_slots': light,
            }))

        try:
            queue_quota_audit()
            reconcile(root)
            try:
                sweep_auto_archive(root)
            except Exception as exc:
                log(root, '', 't3_archive_sweep_error', type(exc).__name__)
            next_audit = time.monotonic() + 30
            while not stop.is_set():
                try:
                    if time.monotonic() >= next_audit:
                        reconcile(root)
                        queue_quota_audit()
                        try:
                            sweep_auto_archive(root)
                        except Exception as exc:
                            log(root, '', 't3_archive_sweep_error', type(exc).__name__)
                        next_audit = time.monotonic() + 30
                    for _ in range(sum(slot_limits(t3_client.load_config(root)))):
                        if stop.is_set() or not tick(root):
                            break
                    try:
                        heartbeat()
                    except OSError:
                        pass
                except Exception as exc:
                    log(root, '', 't3_dispatcher_error', type(exc).__name__)
                while not stop.is_set():
                    if signal.wait(1):
                        if on_change:
                            on_change()
                        break
                    if time.monotonic() >= next_audit:
                        break
                    try:
                        heartbeat()
                    except OSError:
                        pass
        finally:
            quota_worker.shutdown(wait=True)
            signal.close()


def snapshot(root):
    with db(root) as conn:
        t3_rows = [dict(r) for r in conn.execute(
            '''SELECT msgid,state,execution_slot,created,updated,pid,t3_thread_id,t3_triage_thread_id,t3_link,workspace,
                      route_kind,requested_provider,requested_model,requested_effort,used_models_json,
                      clarification_qid,turns,last_event,detail,result_hash,
                      t3_archived_at,t3_triage_archived_at,t3_archive_after
               FROM t3_jobs ORDER BY created DESC LIMIT 80'''
        ).fetchall()]
        legacy_rows = []
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone():
            legacy_rows = [dict(r) for r in conn.execute(
                '''SELECT msgid,state,created,updated,thread_id,workspace,detail FROM jobs ORDER BY created DESC LIMIT 30'''
            ).fetchall()]
        logs = [dict(r) for r in conn.execute('SELECT at,msgid,kind,detail FROM log ORDER BY id DESC LIMIT 150').fetchall()]

    jobs = []
    for r in t3_rows:
        try:
            used = json.loads(r.pop('used_models_json') or '[]')
        except ValueError:
            used = []
        jobs.append({
            **r,
            'thread_id': r['t3_thread_id'] or '',
            'engine': 't3',
            'used_models': used,
            'result_path': str(Path(r['workspace']) / 'result.txt') if r['workspace'] else None,
        })
    for r in legacy_rows:
        jobs.append({
            **r,
            't3_thread_id': r.get('thread_id') or '',
            't3_link': '',
            'engine': 'legacy_codex',
            'route_kind': 'legacy',
            'requested_model': '',
            'requested_effort': '',
            'used_models': [],
            'result_path': str(Path(r['workspace']) / 'result.txt') if r.get('workspace') else None,
        })
    regular, light = slot_limits(t3_client.load_config(root))
    return {'jobs': jobs[:100], 'log': logs, 'regular_slots': regular, 'light_slots': light,
            'light_burst_slots': light}


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['job', 'status'])
    parser.add_argument('--root', default=str(ROOT))
    parser.add_argument('--msgid')
    parser.add_argument('--lease')
    args = parser.parse_args()
    if args.action == 'job':
        sys.exit(0 if run_job(Path(args.root), args.msgid, args.lease) else 1)
    print(json.dumps(snapshot(Path(args.root)), ensure_ascii=False))
