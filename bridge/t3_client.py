"""Official T3 Code CLI adapter and live catalog reader for Feiyu Bridge."""
from __future__ import annotations

import base64
import contextlib
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import re
import secrets
import socket
import sqlite3
import struct
import subprocess
import threading
import time

DEFAULT_T3_HOME = Path.home() / '.t3'
DEFAULT_T3_CLI = DEFAULT_T3_HOME / 'bin' / 't3.cmd'
DEFAULT_TURN_TIMEOUT = 10800
DEFAULT_LAUNCH_TIMEOUT = 90
_ACTIVE_THREAD_STATUSES = {'running', 'starting', 'preparing', 'queued'}
_TERMINAL_ERROR_STATUSES = {'failed', 'cancelled', 'interrupted', 'error'}
_THREAD_LOCAL = threading.local()


def turn_timeout(root=None, override=None):
    if override is not None:
        return max(1.0, float(override))
    cfg = load_config(root) if root else {}
    raw = cfg.get('t3_turn_timeout_seconds')
    try:
        val = float(raw) if raw is not None else float(DEFAULT_TURN_TIMEOUT)
    except (TypeError, ValueError):
        val = float(DEFAULT_TURN_TIMEOUT)
    return max(60.0, val)


def launch_timeout(root=None, override=None):
    if override is not None:
        return max(1.0, float(override))
    cfg = load_config(root) if root else {}
    raw = cfg.get('t3_launch_timeout_seconds')
    try:
        val = float(raw) if raw is not None else float(DEFAULT_LAUNCH_TIMEOUT)
    except (TypeError, ValueError):
        val = float(DEFAULT_LAUNCH_TIMEOUT)
    return max(15.0, val)


@contextlib.contextmanager
def thread_ready_hook(callback):
    """Register a thread-local callback invoked as soon as a T3 thread ID is known."""
    prev = getattr(_THREAD_LOCAL, 'on_thread_ready', None)
    _THREAD_LOCAL.on_thread_ready = callback
    try:
        yield
    finally:
        _THREAD_LOCAL.on_thread_ready = prev


def _notify_thread_ready(thread_id, run_id=None, link=''):
    cb = getattr(_THREAD_LOCAL, 'on_thread_ready', None)
    if cb and thread_id:
        try:
            cb(thread_id, run_id, link)
        except Exception:
            pass


def t3_home(root=None):
    cfg = load_config(root) if root else {}
    raw = cfg.get('t3_home') or os.environ.get('T3CODE_HOME')
    return Path(raw) if raw else DEFAULT_T3_HOME


def t3_cli_path(root=None):
    cfg = load_config(root) if root else {}
    raw = cfg.get('t3_cli') or os.environ.get('T3CODE_CLI_PATH')
    return Path(raw) if raw else (t3_home(root) / 'bin' / 't3.cmd')


def load_config(root):
    if not root:
        return {}
    path = Path(root) / 't3-bridge-config.json'
    try:
        data = json.loads(path.read_text(encoding='utf-8-sig'))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _projection(root):
    path = (t3_home(root) / 'userdata' / 'statev2.sqlite').resolve()
    conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)
    conn.row_factory = sqlite3.Row
    return conn


def list_active_projects(root):
    try:
        with contextlib.closing(_projection(root)) as conn:
            return [dict(row) for row in conn.execute(
                'SELECT project_id,title,workspace_root FROM projection_projects '
                'WHERE deleted_at IS NULL AND archived_at IS NULL ORDER BY project_id')]
    except (OSError, sqlite3.Error):
        cfg = load_config(root)
        return [{'project_id': cfg['t3_project_id'], 'title': cfg.get('t3_project_title', ''),
                 'workspace_root': cfg.get('tasks_root', '')}] if cfg.get('t3_project_id') else []


def inspect_thread_runtime(root, thread_id, *, run_id=None):
    snapshot = dict(error_items=[], subagents=[], run_model=None, run_provider=None,
                    run_status=None, assistant_text='', first_user='', thread={})
    try:
        with contextlib.closing(_projection(root)) as conn:
            conn.execute('BEGIN')
            query = 'SELECT * FROM orchestration_v2_projection_runs WHERE thread_id=?'
            args = [thread_id]
            if run_id:
                query += ' AND run_id=?'
                args.append(run_id)
            row = conn.execute(query + ' ORDER BY ordinal DESC LIMIT 1', args).fetchone()
            if row:
                payload = json.loads(row['payload_json'])
                selection = payload.get('modelSelection') or {}
                run_id = row['run_id']
                snapshot.update(run_id=run_id, run_model=selection.get('model') or payload.get('model'),
                                run_provider=row['provider_instance_id'] or selection.get('instanceId'),
                                run_status=row['status'], completed_at=row['completed_at'])
                items = conn.execute(
                    "SELECT payload_json FROM orchestration_v2_projection_turn_items "
                    "WHERE thread_id=? AND run_id=? AND type='error' ORDER BY ordinal DESC LIMIT 16",
                    (thread_id, run_id))
                snapshot['error_items'] = [json.loads(i[0]).get('failure') or {} for i in items]
                snapshot['user_stopped'] = bool(conn.execute(
                    "SELECT 1 FROM orchestration_v2_projection_turn_items WHERE thread_id=? AND run_id=? "
                    "AND type='run_interrupt_request' LIMIT 1", (thread_id, run_id)).fetchone())
                last = conn.execute(
                    "SELECT substr(json_extract(payload_json,'$.text'),-800) FROM orchestration_v2_projection_turn_items "
                    "WHERE thread_id=? AND run_id=? AND type='assistant_message' ORDER BY ordinal DESC LIMIT 1",
                    (thread_id, run_id)).fetchone()
                snapshot['assistant_text'] = (last[0] or '') if last else ''
            for child in conn.execute('SELECT * FROM orchestration_v2_projection_subagents WHERE thread_id=?',
                                      (thread_id,)):
                snapshot['subagents'].append(dict(child_thread_id=child['child_thread_id'],
                    provider=child['provider_instance_id'], status=child['status'],
                    completion_delivery_state=child['completion_delivery_state'],
                    title=json.loads(child['payload_json']).get('title')))
            th = conn.execute('SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=?',
                              (thread_id,)).fetchone()
            if th:
                snapshot['thread'] = json.loads(th[0])
            first = conn.execute(
                "SELECT substr(json_extract(payload_json,'$.text'),1,2200) FROM orchestration_v2_projection_messages "
                "WHERE thread_id=? AND role='user' ORDER BY created_at LIMIT 1", (thread_id,)).fetchone()
            snapshot['first_user'] = (first[0] or '') if first else ''
    except (OSError, ValueError, sqlite3.Error):
        pass
    return snapshot


def _pid_alive(pid):
    if not isinstance(pid, int) or pid <= 0:
        return False
    if os.name != 'nt':
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.OpenProcess(0x1000, False, int(pid))
    if not handle:
        return False
    try:
        code = wintypes.DWORD()
        return bool(kernel.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259
    finally:
        kernel.CloseHandle(handle)


def server_status(root=None):
    home = t3_home(root)
    cli = t3_cli_path(root)
    runtime_path = home / 'userdata' / 'server-runtime.json'
    result = {
        'ok': False,
        'cli_exists': cli.is_file(),
        'pid': None,
        'port': None,
        'version': None,
        'reason': 'server_runtime_missing',
    }
    if not cli.is_file():
        result['reason'] = 't3_cli_missing'
        return result
    try:
        rt = json.loads(runtime_path.read_text(encoding='utf-8-sig'))
    except (OSError, ValueError):
        return result
    pid = rt.get('pid')
    port = rt.get('port')
    host = rt.get('host') or '127.0.0.1'
    result['pid'] = pid
    result['port'] = port
    result['version'] = rt.get('serverVersion')
    if not _pid_alive(pid):
        result['reason'] = 'server_pid_dead'
        return result
    if not isinstance(port, int) or port <= 0:
        result['reason'] = 'server_port_invalid'
        return result
    try:
        with socket.create_connection((host, port), timeout=1.5):
            result['ok'] = True
            result['reason'] = 'ready'
    except OSError:
        result['reason'] = 'server_port_unreachable'
    return result


def live_catalog(root=None):
    """Read live enabled provider/model catalogs cached by T3 Code."""
    home = t3_home(root)
    cache_dir = home / 'caches'
    providers = {}
    models = []
    seen = set()
    preferred_order = ('antigravity', 'codex')
    cache_files = []
    if cache_dir.is_dir():
        for name in preferred_order:
            p = cache_dir / f'{name}.json'
            if p.is_file():
                cache_files.append((name, p))
        for p in sorted(cache_dir.glob('*.json')):
            if p.stem not in preferred_order:
                cache_files.append((p.stem, p))
    for pid, path in cache_files:
        try:
            data = json.loads(path.read_text(encoding='utf-8-sig'))
        except (OSError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        enabled = bool(data.get('enabled') and data.get('installed') and data.get('status') == 'ready')
        providers[pid] = {
            'id': pid,
            'display_name': data.get('displayName') or pid,
            'enabled': enabled,
            'auth_type': (data.get('auth') or {}).get('type'),
            'auth_label': (data.get('auth') or {}).get('label'),
        }
        if not enabled:
            continue
        for row in data.get('models', []):
            if not isinstance(row, dict):
                continue
            slug = row.get('slug')
            if not isinstance(slug, str) or not slug:
                continue
            key = (pid, slug.lower())
            if key in seen:
                continue
            seen.add(key)
            descriptors = (row.get('capabilities') or {}).get('optionDescriptors') or []
            option_id = None
            efforts = []
            default_effort = None
            service_tiers = []
            default_service_tier = None
            for desc in descriptors:
                if not isinstance(desc, dict):
                    continue
                did = desc.get('id')
                opts = [o.get('id') for o in desc.get('options', []) if isinstance(o, dict) and isinstance(o.get('id'), str)]
                cur = desc.get('currentValue')
                if not cur:
                    cur = next((o.get('id') for o in desc.get('options', []) if isinstance(o, dict) and o.get('isDefault')), None)
                if did in ('thought_level', 'reasoningEffort', 'effort'):
                    option_id = did
                    efforts = list(dict.fromkeys(opts))
                    default_effort = cur if cur in efforts else (efforts[0] if efforts else None)
                elif did == 'serviceTier':
                    service_tiers = list(dict.fromkeys(opts))
                    default_service_tier = cur if cur in service_tiers else (service_tiers[0] if service_tiers else None)
            models.append({
                'provider': pid,
                'id': slug,
                'label': row.get('name') or slug,
                'option_id': option_id,
                'efforts': efforts,
                'default': default_effort,
                'service_tiers': service_tiers,
                'default_service_tier': default_service_tier,
                'is_default_model': bool(row.get('isDefault')),
            })
    return {'providers': providers, 'models': models}


def _hidden_flags():
    if os.name != 'nt':
        return {}
    return {'creationflags': subprocess.CREATE_NO_WINDOW}


def _clean_env(root=None):
    env = {
        k: v for k, v in os.environ.items()
        if not k.upper().startswith(('XIAOAI_', 'CONTROL_PLANE_', 'MCP_EXTRA_HEADERS', 'MCP_DISCOVERY_EXTRA_HEADERS'))
    }
    env['PYTHONIOENCODING'] = 'utf-8'
    env['T3CODE_HOME'] = str(t3_home(root))
    return env


def _parse_json_output(raw):
    text = (raw or '').strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        for line in reversed(text.splitlines()):
            line = line.strip()
            if line.startswith('{') and line.endswith('}'):
                try:
                    return json.loads(line)
                except ValueError:
                    continue
    return None


def _script_or_asar_exists(script_str):
    sp = Path(script_str)
    if sp.is_file():
        return True
    low = script_str.lower()
    idx = low.find('.asar')
    if idx > 0:
        return Path(script_str[:idx + 5]).is_file()
    return False


def _resolve_cli_command(cli, args, env):
    """Bypass cmd.exe on Windows when t3.cmd is the standard Electron-as-Node launcher.

    Avoids Windows cmd.exe orphan-pipe hangs if a subprocess ever times out.
    """
    cli_path = Path(cli)
    if os.name == 'nt' and cli_path.suffix.lower() in ('.cmd', '.bat'):
        try:
            text = cli_path.read_text(encoding='utf-8-sig', errors='ignore')
        except OSError:
            text = ''
        m = re.search(r'^"([^"\r\n]+\.exe)"\s+"([^"\r\n]+bin\.mjs)"\s+%\*\s*$', text, re.M | re.I)
        if m:
            exe_str, script_str = m.group(1), m.group(2)
            if Path(exe_str).is_file() and _script_or_asar_exists(script_str):
                direct_env = dict(env)
                direct_env['ELECTRON_RUN_AS_NODE'] = '1'
                direct_env['T3CODE_DESKTOP_CLI_LAUNCHER'] = '1'
                direct_env['T3CODE_CLI_PATH'] = str(cli_path)
                return [exe_str, script_str, *args], direct_env
    return [str(cli_path), *args], env


def _kill_process_tree(proc):
    pid = getattr(proc, 'pid', None)
    if os.name == 'nt' and pid:
        try:
            subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(pid)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
                **_hidden_flags(),
            )
            return
        except Exception:
            pass
    try:
        proc.kill()
    except OSError:
        pass


def _run_cli(root, args, *, input_text=None, timeout=60, runner=subprocess.run):
    cli = t3_cli_path(root)
    if not cli.is_file():
        raise RuntimeError('t3_cli_missing')
    env = _clean_env(root)
    if runner is not subprocess.run:
        cmd = [str(cli), *args]
        kwargs = {
            'capture_output': True,
            'text': True,
            'encoding': 'utf-8',
            'errors': 'replace',
            'timeout': timeout,
            'env': env,
            **_hidden_flags(),
        }
        if input_text is not None:
            kwargs['input'] = input_text
        return runner(cmd, **kwargs)

    cmd, run_env = _resolve_cli_command(cli, args, env)
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE if input_text is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
        env=run_env,
        **_hidden_flags(),
    )
    try:
        stdout, stderr = proc.communicate(input=input_text, timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_process_tree(proc)
        try:
            stdout, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            for stream in (proc.stdout, proc.stderr):
                try:
                    if stream:
                        stream.close()
                except OSError:
                    pass
            stdout, stderr = '', ''
        raise subprocess.TimeoutExpired(cmd, timeout, output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(cmd, proc.returncode, stdout, stderr)


def _extract_latest_reply(info):
    thread = info.get('thread') if isinstance(info, dict) else None
    latest_run = thread.get('latestRunId') if isinstance(thread, dict) else None
    messages = info.get('messages') if isinstance(info, dict) and isinstance(info.get('messages'), list) else []
    run_msgs = [m for m in messages if isinstance(m, dict) and (not latest_run or m.get('runId') == latest_run)]
    assistant = next(
        (m for m in reversed(run_msgs) if m.get('role') == 'assistant' and isinstance(m.get('text'), str) and m.get('text').strip()),
        None,
    )
    if assistant is None:
        assistant = next(
            (m for m in reversed(messages) if isinstance(m, dict) and m.get('role') == 'assistant' and isinstance(m.get('text'), str) and m.get('text').strip()),
            None,
        )
    return assistant['text'] if assistant else ''


def wait_for_thread(
    root,
    thread_id,
    *,
    run_id=None,
    prev_run_id=None,
    prev_run_count=None,
    timeout=None,
    poll_interval=None,
    settle_seconds=4.0,
    show_fn=None,
    sleep_fn=time.sleep,
    monotonic_fn=time.monotonic,
):
    """Poll `show_thread` until a T3 thread turn settles, needs input, or ends.

    Handles delegated subagents (`pendingBackgroundTasks`), thread waits (`blockedOnThreadWait`),
    native user requests (`pendingRequestCount > 0`), and brief post-send / post-wake transitions
    without holding a fragile long-lived CLI pipe open.
    """
    effective_timeout = turn_timeout(root, timeout)
    start_ts = monotonic_fn()
    deadline = start_ts + effective_timeout
    fetch = show_fn or show_thread
    consecutive_errors = 0
    first_error_ts = None
    settled_sig = None
    settled_since = None
    poll_idx = 0

    def _inspect_once(final_check=False):
        nonlocal settled_sig, settled_since
        info = fetch(root, thread_id, messages=8)
        thread = info.get('thread') if isinstance(info, dict) else None
        if not isinstance(thread, dict):
            return None
        now_m = monotonic_fn()
        elapsed = now_m - start_ts
        status = str(thread.get('status') or '').lower()
        latest_run = thread.get('latestRunId')
        active_run = thread.get('activeRunId')
        pending_reqs = int(thread.get('pendingRequestCount') or 0)
        pending_bg = thread.get('pendingBackgroundTasks') or []
        blocked_wait = bool(thread.get('blockedOnThreadWait'))
        run_count = int(thread.get('runCount') or 0)
        updated_at = str(thread.get('updatedAt') or '')

        # 1. Guard against reading a stale completed run right after `thread send` (wait=False).
        if not final_check and prev_run_id is not None and elapsed < 60.0:
            same_old_run = (latest_run == prev_run_id) and (prev_run_count is None or run_count <= prev_run_count)
            if same_old_run and status in ('completed', 'idle') and not active_run and not pending_bg and pending_reqs == 0:
                if run_id is None or latest_run != run_id:
                    settled_sig = None
                    settled_since = None
                    return None

        # 2. Native user question / approval request (may occur while activeRunId is still set!).
        if pending_reqs > 0 and status not in ('completed', *_TERMINAL_ERROR_STATUSES):
            reply = _extract_latest_reply(info)
            return {
                'exit_code': 2,
                'thread_id': thread_id,
                'run_id': active_run or latest_run or run_id,
                'status': 'waiting_for_answer',
                'reply': reply,
                'error': None,
                'waiting_for_answer': True,
                'raw': info,
            }

        # 3. Still actively running or waiting on delegated subagents / background tasks / another thread.
        if (
            status in _ACTIVE_THREAD_STATUSES
            or status == 'waiting'
            or active_run
            or pending_bg
            or blocked_wait
        ):
            settled_sig = None
            settled_since = None
            return None

        # 4. Brand-new thread briefly idle before run 1 registers.
        if not final_check and status == 'idle' and run_count == 0 and elapsed < 30.0:
            settled_sig = None
            settled_since = None
            return None

        # 5. Terminal error / cancellation.
        if status in _TERMINAL_ERROR_STATUSES:
            reply = next((m['text'] for m in reversed(info.get('messages') or [])
                          if latest_run and m.get('runId') == latest_run and m.get('role') == 'assistant'
                          and isinstance(m.get('text'), str)), '')
            err = thread.get('lastError') or f't3_thread_{status}'
            return {
                'exit_code': 1,
                'thread_id': thread_id,
                'run_id': latest_run or run_id,
                'status': status,
                'reply': reply,
                'error': str(err)[:500],
                'waiting_for_answer': False,
                'raw': info,
            }

        # 6. Completed / idle after execution.
        if status in ('completed', 'idle'):
            reply = _extract_latest_reply(info)
            has_protocol = '<<<FEIYU_JSON>>>' in reply
            if has_protocol or final_check or settle_seconds <= 0:
                return {
                    'exit_code': 0,
                    'thread_id': thread_id,
                    'run_id': latest_run or run_id,
                    'status': 'completed',
                    'reply': reply,
                    'error': None,
                    'waiting_for_answer': False,
                    'raw': info,
                }
            sig = (latest_run, updated_at, len(reply))
            if sig == settled_sig and settled_since is not None and (now_m - settled_since) >= float(settle_seconds):
                return {
                    'exit_code': 0,
                    'thread_id': thread_id,
                    'run_id': latest_run or run_id,
                    'status': 'completed',
                    'reply': reply,
                    'error': None,
                    'waiting_for_answer': False,
                    'raw': info,
                }
            if sig != settled_sig:
                settled_sig = sig
                settled_since = now_m
            return None
        return None

    while True:
        try:
            outcome = _inspect_once(final_check=False)
            consecutive_errors = 0
            first_error_ts = None
            if outcome is not None:
                return outcome
        except Exception:
            consecutive_errors += 1
            now_m = monotonic_fn()
            if first_error_ts is None:
                first_error_ts = now_m
            # Tolerate transient T3 server hiccups up to 300s (or remaining deadline)
            if (now_m - first_error_ts) >= 300.0 and consecutive_errors >= 10:
                raise

        now_m = monotonic_fn()
        if now_m >= deadline:
            try:
                final_outcome = _inspect_once(final_check=True)
                if final_outcome is not None:
                    return final_outcome
            except Exception:
                pass
            raise subprocess.TimeoutExpired(['t3', 'thread', 'wait', str(thread_id)], effective_timeout)

        if poll_interval is not None:
            delay = float(poll_interval)
        else:
            delay = 1.5 if poll_idx < 4 else (2.5 if poll_idx < 12 else 4.0)
            if consecutive_errors > 0:
                delay = min(15.0, 2.0 * (2 ** min(4, consecutive_errors - 1)))
        poll_idx += 1
        sleep_fn(min(delay, max(0.05, deadline - now_m)))


def _result_from_proc(proc, fallback_thread_id=None):
    data = _parse_json_output(proc.stdout) or {}
    thread_id = data.get('threadId') or (data.get('thread') or {}).get('threadId') or fallback_thread_id
    return {
        'exit_code': proc.returncode,
        'thread_id': thread_id,
        'run_id': data.get('runId'),
        'status': data.get('status') or ('waiting_for_answer' if proc.returncode == 2 else 'completed' if proc.returncode == 0 else 'failed'),
        'reply': data.get('reply') or '',
        'error': data.get('error') or (proc.stderr.strip()[:500] if proc.returncode not in (0, 2) and not data.get('error') else None),
        'waiting_for_answer': proc.returncode == 2,
        'raw': data,
    }


def _recover_timeout_stdout(exc, fallback_thread_id=None):
    raw_out = exc.output
    if isinstance(raw_out, bytes):
        raw_out = raw_out.decode('utf-8', errors='replace')
    data = _parse_json_output(raw_out) if isinstance(raw_out, str) else None
    if not isinstance(data, dict):
        return None
    status = str(data.get('status') or '').lower()
    reply = data.get('reply') or ''
    if status == 'completed' and reply:
        tid = data.get('threadId') or (data.get('thread') or {}).get('threadId') or fallback_thread_id
        return {
            'exit_code': 0,
            'thread_id': tid,
            'run_id': data.get('runId'),
            'status': 'completed',
            'reply': reply,
            'error': None,
            'waiting_for_answer': False,
            'raw': data,
        }
    return None


def start_thread(
    root,
    project_id,
    provider,
    model,
    title,
    prompt,
    *,
    wait=True,
    timeout=None,
    runner=subprocess.run,
    options=None,
):
    args = [
        'thread', 'start',
        '--provider', str(provider),
        '--model', str(model),
        '--title', str(title)[:120],
        '--json',
    ]
    if provider and model and options:
        for key, value in (options.items() if isinstance(options, dict) else options):
            if key and str(key).strip() and value is not None and str(value).strip():
                rendered = str(value).lower() if isinstance(value, bool) else str(value).strip()
                args.extend(['--option', f'{str(key).strip()}={rendered}'])
    if wait and runner is subprocess.run:
        launch_args = [*args, str(project_id), '-']
        proc = _run_cli(root, launch_args, input_text=prompt, timeout=launch_timeout(root), runner=runner)
        launched = _result_from_proc(proc)
        tid = launched.get('thread_id')
        if proc.returncode != 0 or not tid:
            return launched
        link = (launched.get('raw') or {}).get('link') or ''
        if not link:
            try:
                info = show_thread(root, tid, messages=1, timeout=15)
                link = (info.get('thread') or {}).get('link') or ''
            except Exception:
                link = ''
        _notify_thread_ready(tid, launched.get('run_id'), link)
        return wait_for_thread(
            root,
            tid,
            run_id=launched.get('run_id'),
            timeout=turn_timeout(root, timeout),
        )

    if wait:
        args.append('--wait')
    args.extend([str(project_id), '-'])
    effective_timeout = turn_timeout(root, timeout) if wait else launch_timeout(root, timeout)
    try:
        proc = _run_cli(root, args, input_text=prompt, timeout=effective_timeout, runner=runner)
    except subprocess.TimeoutExpired as exc:
        recovered = _recover_timeout_stdout(exc)
        if recovered is not None:
            return recovered
        raise
    res = _result_from_proc(proc)
    if res.get('thread_id'):
        _notify_thread_ready(res['thread_id'], res.get('run_id'), (res.get('raw') or {}).get('link') or '')
    return res


def send_message(
    root,
    thread_id,
    message,
    *,
    wait=True,
    timeout=None,
    runner=subprocess.run,
):
    args = ['thread', 'send', '--json']
    if wait and runner is subprocess.run:
        prev_run_id = None
        prev_run_count = None
        try:
            snap = show_thread(root, thread_id, messages=1, timeout=15)
            th = snap.get('thread') if isinstance(snap, dict) else None
            if isinstance(th, dict):
                prev_run_id = th.get('latestRunId')
                prev_run_count = int(th.get('runCount') or 0)
        except Exception:
            pass
        send_args = [*args, str(thread_id), '-']
        proc = _run_cli(root, send_args, input_text=message, timeout=launch_timeout(root), runner=runner)
        sent = _result_from_proc(proc, fallback_thread_id=thread_id)
        if proc.returncode != 0:
            return sent
        tid = sent.get('thread_id') or thread_id
        _notify_thread_ready(tid, sent.get('run_id'), (sent.get('raw') or {}).get('link') or '')
        return wait_for_thread(
            root,
            tid,
            run_id=sent.get('run_id'),
            prev_run_id=prev_run_id,
            prev_run_count=prev_run_count,
            timeout=turn_timeout(root, timeout),
        )

    if wait:
        args.append('--wait')
    args.extend([str(thread_id), '-'])
    effective_timeout = turn_timeout(root, timeout) if wait else launch_timeout(root, timeout)
    try:
        proc = _run_cli(root, args, input_text=message, timeout=effective_timeout, runner=runner)
    except subprocess.TimeoutExpired as exc:
        recovered = _recover_timeout_stdout(exc, fallback_thread_id=thread_id)
        if recovered is not None:
            return recovered
        raise
    return _result_from_proc(proc, fallback_thread_id=thread_id)


def show_thread(root, thread_id, *, messages=6, timeout=30, runner=subprocess.run):
    args = ['thread', 'show', str(thread_id), '--messages', str(messages), '--json']
    proc = _run_cli(root, args, timeout=timeout, runner=runner)
    data = _parse_json_output(proc.stdout)
    if proc.returncode != 0 or not isinstance(data, dict):
        raise RuntimeError('t3_thread_show_failed_' + str(proc.returncode))
    return data


def list_threads(root, project_id=None, *, timeout=30, runner=subprocess.run):
    """List non-archived threads in a T3 project via the official CLI."""
    args = ['thread', 'list', '--json']
    if project_id:
        args.extend(['--project', str(project_id)])
    proc = _run_cli(root, args, timeout=timeout, runner=runner)
    data = _parse_json_output(proc.stdout)
    if proc.returncode != 0 or data is None:
        raise RuntimeError('t3_thread_list_failed_' + str(proc.returncode))
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict) and isinstance(data.get('threads'), list):
        return [item for item in data['threads'] if isinstance(item, dict)]
    return []


def _runtime_endpoint(root=None):
    runtime_path = t3_home(root) / 'userdata' / 'server-runtime.json'
    rt = json.loads(runtime_path.read_text(encoding='utf-8-sig'))
    host = rt.get('host') or '127.0.0.1'
    port = int(rt['port'])
    return host, port


def _issue_scoped_session(root, scopes, *, ttl='2m', label='feiyu-bridge-lifecycle', timeout=20, runner=subprocess.run):
    cli = t3_cli_path(root)
    if not cli.is_file():
        raise RuntimeError('t3_cli_missing')
    cmd = [str(cli), 'auth', 'session', 'issue']
    for scope in scopes:
        cmd.extend(['--scope', str(scope)])
    cmd.extend(['--ttl', str(ttl), '--label', str(label), '--json'])
    proc = runner(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
        timeout=timeout,
        env=_clean_env(root),
        **_hidden_flags(),
    )
    data = _parse_json_output(proc.stdout)
    if proc.returncode != 0 or not isinstance(data, dict) or not data.get('token'):
        err = (proc.stderr or proc.stdout or '').strip()[:300]
        raise RuntimeError(f't3_auth_session_issue_failed_{proc.returncode}:{err}')
    return data['token'], data.get('sessionId')


def _revoke_scoped_session(root, session_id, *, timeout=15, runner=subprocess.run):
    if not session_id:
        return
    cli = t3_cli_path(root)
    if not cli.is_file():
        return
    try:
        runner(
            [str(cli), 'auth', 'session', 'revoke', str(session_id)],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=timeout,
            env=_clean_env(root),
            **_hidden_flags(),
        )
    except Exception:
        pass


def _ws_rpc_call(root, method, payload, *, scopes=('orchestration:operate',), timeout=20, runner=subprocess.run):
    """Execute one authenticated T3 WebSocket RPC call using an official short-lived scoped session."""
    host, port = _runtime_endpoint(root)
    token, session_id = _issue_scoped_session(root, scopes, timeout=timeout, runner=runner)
    try:
        key = base64.b64encode(os.urandom(16)).decode('ascii')
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            req = (
                f'GET /ws?orchestrationProtocol=2 HTTP/1.1\r\n'
                f'Host: {host}:{port}\r\n'
                f'Upgrade: websocket\r\n'
                f'Connection: Upgrade\r\n'
                f'Sec-WebSocket-Key: {key}\r\n'
                f'Sec-WebSocket-Version: 13\r\n'
                f'Authorization: Bearer {token}\r\n\r\n'
            )
            sock.sendall(req.encode('ascii'))
            buf = b''
            while b'\r\n\r\n' not in buf:
                chunk = sock.recv(4096)
                if not chunk:
                    raise RuntimeError('t3_ws_handshake_eof')
                buf += chunk
            hdr, rest = buf.split(b'\r\n\r\n', 1)
            status_line = hdr.split(b'\r\n', 1)[0].decode('latin1', errors='replace')
            if ' 101 ' not in status_line:
                raise RuntimeError('t3_ws_handshake_failed:' + status_line[:160])

            def send_frame(opcode, data_bytes):
                mask = os.urandom(4)
                out = bytearray([0x80 | (opcode & 0x0F)])
                n = len(data_bytes)
                if n < 126:
                    out.append(0x80 | n)
                elif n < 65536:
                    out.append(0x80 | 126)
                    out.extend(struct.pack('!H', n))
                else:
                    out.append(0x80 | 127)
                    out.extend(struct.pack('!Q', n))
                out.extend(mask)
                out.extend(bytes(b ^ mask[i & 3] for i, b in enumerate(data_bytes)))
                sock.sendall(out)

            def recv_exact(n):
                nonlocal rest
                while len(rest) < n:
                    chunk = sock.recv(65536)
                    if not chunk:
                        raise EOFError('t3_ws_closed')
                    rest += chunk
                out, rest = rest[:n], rest[n:]
                return out

            def recv_message():
                chunks = []
                while True:
                    b1, b2 = recv_exact(2)
                    fin = bool(b1 & 0x80)
                    op = b1 & 0x0F
                    masked = bool(b2 & 0x80)
                    ln = b2 & 0x7F
                    if ln == 126:
                        ln = struct.unpack('!H', recv_exact(2))[0]
                    elif ln == 127:
                        ln = struct.unpack('!Q', recv_exact(8))[0]
                    mask = recv_exact(4) if masked else None
                    data = recv_exact(ln)
                    if mask:
                        data = bytes(b ^ mask[i & 3] for i, b in enumerate(data))
                    if op == 9:  # Ping -> Pong
                        send_frame(10, data)
                        continue
                    if op == 10:  # Pong
                        continue
                    if op == 8:  # Close
                        raise RuntimeError('t3_ws_closed_by_server')
                    chunks.append(data)
                    if fin:
                        return b''.join(chunks).decode('utf-8', errors='replace')

            envelope = {
                '_tag': 'Request',
                'id': '1',
                'tag': str(method),
                'payload': payload,
                'headers': [],
            }
            send_frame(1, json.dumps(envelope, ensure_ascii=False).encode('utf-8'))
            for _ in range(16):
                raw_msg = recv_message()
                parsed = json.loads(raw_msg)
                if isinstance(parsed, dict) and parsed.get('_tag') == 'Exit' and str(parsed.get('requestId')) == '1':
                    return parsed.get('exit') or {}
            raise RuntimeError('t3_ws_rpc_no_exit')
    finally:
        _revoke_scoped_session(root, session_id, runner=runner)


def _extract_rpc_error(exit_obj):
    if not isinstance(exit_obj, dict):
        return 'unknown_rpc_error'
    text = json.dumps(exit_obj.get('cause', exit_obj), ensure_ascii=False)
    return text[:500]


def launch_in_source_workspace(root, project_id, source, target, title, prompt, *, thread_id):
    """The public launch RPC also accepts an existing worktree and the source's exact modes."""
    path = source.get('worktreePath')
    strategy = {'type': 'root'}
    workspace = path or source.get('workspace_root')
    if source.get('branch') and workspace:
        current = subprocess.run(['git','symbolic-ref','--short','HEAD'], cwd=workspace, timeout=3,
                                 capture_output=True, text=True, encoding='utf-8', errors='replace', **_hidden_flags())
        if current.returncode or current.stdout.strip() != source['branch']:
            raise RuntimeError('source_branch_changed')
    if path:
        if not Path(path).is_dir():
            raise ValueError('source_worktree_missing')
        strategy = {'type': 'existing_worktree', 'worktreePath': path}
    payload = dict(commandId='feiyu:relay:'+thread_id, threadId=thread_id, projectId=project_id, title=title[:120],
                   runtimeMode=source['runtimeMode'], interactionMode=source['interactionMode'],
                   workspaceStrategy=strategy,
                   modelSelection=dict(instanceId=target['provider'], model=target['model'],
                                       options=[dict(id=k, value=v) for k,v in target['options'].items()]),
                   initialMessage=dict(text=prompt, attachments=[]))
    result = _ws_rpc_call(root, 'orchestration.launchThread', payload, timeout=launch_timeout(root))
    if result.get('_tag') != 'Success':
        raise RuntimeError(_extract_rpc_error(result))
    value = result['value']
    if value['threadId'] != thread_id:
        raise RuntimeError('unexpected_relay_thread_id')
    runs = (value.get('projection') or {}).get('runs') or []
    return dict(exit_code=0, thread_id=value['threadId'], run_id=runs[-1]['id'] if runs else None, raw={})


def archive_thread(root, thread_id, *, timeout=20, runner=subprocess.run):
    """Archive one T3 thread out of the main sidebar into Settings -> Archived (zero LLM calls)."""
    if not thread_id:
        return {'ok': False, 'error': 'missing_thread_id', 'busy': False, 'already_archived': False}
    cmd = {
        'type': 'thread.archive',
        'commandId': f'feiyu:archive:{secrets.token_hex(12)}',
        'threadId': str(thread_id),
    }
    exit_obj = _ws_rpc_call(root, 'orchestration.dispatchCommand', cmd, scopes=('orchestration:operate',), timeout=timeout, runner=runner)
    if isinstance(exit_obj, dict) and exit_obj.get('_tag') == 'Success':
        return {'ok': True, 'already_archived': False, 'busy': False, 'value': exit_obj.get('value')}
    err = _extract_rpc_error(exit_obj)
    low = err.lower()
    if 'already archived' in low or 'not found' in low:
        return {'ok': True, 'already_archived': True, 'busy': False, 'error': err}
    if 'running delegated tasks' in low or 'active' in low:
        return {'ok': False, 'already_archived': False, 'busy': True, 'error': err}
    return {'ok': False, 'already_archived': False, 'busy': False, 'error': err}


def settle_thread(root, thread_id, *, timeout=20, runner=subprocess.run):
    """Mark one T3 thread settled via official WebSocket RPC."""
    if not thread_id:
        return {'ok': False, 'error': 'missing_thread_id'}
    cmd = {
        'type': 'thread.settle',
        'commandId': f'feiyu:settle:{secrets.token_hex(12)}',
        'threadId': str(thread_id),
    }
    exit_obj = _ws_rpc_call(root, 'orchestration.dispatchCommand', cmd, scopes=('orchestration:operate',), timeout=timeout, runner=runner)
    if isinstance(exit_obj, dict) and exit_obj.get('_tag') == 'Success':
        return {'ok': True, 'value': exit_obj.get('value')}
    return {'ok': False, 'error': _extract_rpc_error(exit_obj)}
