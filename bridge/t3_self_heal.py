"""Automatic T3 self-healing escalation for Feiyu Bridge.

When local restarts fail repeatedly or any critical bridge component remains
unhealthy/stalled past the escalation threshold, this module directly invokes
the official T3 CLI (`t3_client.start_thread(..., wait=False)`) — completely
bypassing `bridge_daemon.py` — so a T3 agent can diagnose and fix the root cause
even when both WeCom bots and the bridge daemon are down.
"""
from __future__ import annotations

from datetime import datetime, timezone
import errno
import json
import os
from pathlib import Path
import secrets
import time

import t3_client
from win_runtime import read_json_retry

STATE_FILE = 'self-heal-state.json'
GLOBAL_COOLDOWN_SECONDS = 600          # At most 1 self-heal thread per 10 min globally
SIGNATURE_COOLDOWN_SECONDS = 1800      # At most 1 self-heal thread per 30 min per fault signature
ACTIVE_THREAD_GRACE_SECONDS = 1200     # Treat an active self-heal thread as live for up to 20 min
MAX_DAILY_TRIGGERS = 12                # Hard daily circuit breaker
RECOVERY_NOTIFY_MIN_OUTAGE = 120       # Notify owner via private_bot only for outages >= 2 min


def _stamp():
    return datetime.now(timezone.utc).isoformat()


def _today_utc():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def _read_json(path):
    return read_json_retry(path)


def _atomic_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.' + secrets.token_hex(6) + '.tmp')
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        for pause in (0.0, 0.02, 0.06, 0.15):
            if pause:
                time.sleep(pause)
            try:
                tmp.replace(path)
                return
            except OSError as exc:
                locked = getattr(exc, 'winerror', None) in (5, 32, 33) or getattr(exc, 'errno', None) in (errno.EACCES, errno.EBUSY)
                if not locked or pause == 0.15:
                    raise
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _tail_text(path, max_lines=30, max_chars=2400):
    p = Path(path)
    if not p.is_file():
        return '(missing)'
    try:
        size = p.stat().st_size
        if size == 0:
            return '(empty 0B)'
        with p.open('rb') as f:
            if size > 16384:
                f.seek(size - 16384)
            raw = f.read().decode('utf-8', errors='replace')
        lines = raw.splitlines()[-max_lines:]
        text = '\n'.join(lines)
        return text[-max_chars:]
    except OSError as exc:
        return f'(read error: {type(exc).__name__})'


def _recent_modified_files(root, limit=12):
    root = Path(root)
    rows = []
    try:
        for p in root.iterdir():
            if not p.is_file() or p.suffix.lower() not in ('.py', '.cjs', '.cs', '.json'):
                continue
            if p.name.endswith('.tmp') or p.name in ('status.json', 'private-bot-status.json', 'watchdog-state.json', 'tray_ui_state.json'):
                continue
            try:
                st = p.stat()
                rows.append((st.st_mtime, p.name, st.st_size))
            except OSError:
                continue
    except OSError:
        return []
    rows.sort(reverse=True)
    return [
        {
            'name': name,
            'size': size,
            'mtime': datetime.fromtimestamp(mtime, timezone.utc).isoformat(),
        }
        for mtime, name, size in rows[:limit]
    ]


def _select_heal_target(root):
    cfg = t3_client.load_config(root)
    project_id = cfg.get('t3_project_id')
    esc = cfg.get('escalation_model') or {}
    provider = esc.get('provider') or 'antigravity'
    model = esc.get('model') or 'gemini-4-neon'
    return project_id, provider, model


def _is_thread_still_running(root, thread_id):
    if not thread_id:
        return False
    try:
        info = t3_client.show_thread(root, thread_id, messages=1, timeout=15)
        st = str(info.get('status') or (info.get('thread') or {}).get('status') or '').lower()
        return st in ('running', 'starting', 'preparing', 'queued', 'waiting')
    except Exception:
        return False


def build_diagnostic_prompt(root, component, reason, details, components_snapshot):
    root = Path(root).resolve()
    status_files = {}
    for name in (
        'status.json',
        'private-bot-status.json',
        'bridge-daemon-status.json',
        'local-dispatch-status.json',
        'watchdog-state.json',
        'tray_ui_state.json',
    ):
        status_files[name] = _read_json(root / name)

    logs = {}
    for name in (
        'bridge-daemon.log',
        'watchdog.log',
        'watchdog-launch.log',
        'worker.stderr.log',
        'private-bot-worker.log',
        'tray-lifecycle.log',
    ):
        logs[name] = _tail_text(root / name, max_lines=25, max_chars=1800)

    recent_files = _recent_modified_files(root, limit=10)
    lines = [
        f'# 【肥鱼桥自动告警】{component} 异常（{reason}），请立即自动排查并恢复',
        '',
        f'- 触发时间 (UTC)：`{_stamp()}`',
        f'- 桥接目录：`{root}`',
        f'- 故障组件：`{component}`',
        f'- 故障原因：`{reason}`',
        f'- 故障明细：`{json.dumps(details or {}, ensure_ascii=False)}`',
        '',
        '## 一、各组件实时健康快照',
        '```json',
        json.dumps(components_snapshot or {}, ensure_ascii=False, indent=2),
        '```',
        '',
        '## 二、状态文件快照',
        '```json',
        json.dumps(status_files, ensure_ascii=False, indent=2)[:4000],
        '```',
        '',
        '## 三、近期改动过的代码/配置文件',
        '```json',
        json.dumps(recent_files, ensure_ascii=False, indent=2),
        '```',
        '',
        '## 四、关键日志尾部',
    ]
    for name, content in logs.items():
        lines.append(f'### {name}')
        lines.append('```text')
        lines.append(content)
        lines.append('```')

    lines.extend([
        '',
        '## 五、你的自愈执行要求',
        '1. **立即定位根因**：检查进程列表、语法/导入/数据库锁/凭据/端口/最近修改的文件与异常堆栈，明确为什么自动重启仍未恢复。',
        '2. **最小代价修复**：若是代码回归、锁文件残留、卡死进程或配置异常，直接修复并跑通桥接单元测试（`test_watchdog.py`、`test_self_heal.py`、`test_t3_bridge.py`）。',
        '3. **安全脱离重启（极其重要）**：',
        '   - 绝不要在受限 Job Object 内直接裸启动后台进程后立刻退出导致被系统强杀；',
        '   - 优先通过 `python bridge_daemon.py start` / `python bridge_control.py start` / `python private_bot_control.py start` / `python watchdog.py start`（它们已内置 `win_runtime.spawn_detached_or_delegate` 委托托盘或 `FeiyuBridge-HealthGuard` 计划任务脱离启动），或直接运行 `schtasks /Run /TN FeiyuBridge-HealthGuard`。',
        '4. **端到端验证**：等待 5 秒后另起一条命令复查所有组件（`wecom_receiver.cjs`、`private_bot_receiver.cjs`、`bridge_daemon.py`、`watchdog.py`、`WecomBridge.exe`）PID 仍存活、心跳持续刷新、`queue/` 中滞留消息恢复消费。',
        '5. **汇报主人**：恢复后若克劳德（`private_bot`）已在线，可通过 `python private_bot_control.py send --text "..."` 给主人发一条简短清晰的恢复汇报（说明原因与已修复动作）；若需要人工介入（如企微扫码/凭据过期/被别处顶号），明确告知阻塞点。',
    ])
    return '\n'.join(lines)


def maybe_trigger(
    root,
    component,
    reason,
    *,
    details=None,
    components_snapshot=None,
    now=None,
    start_thread_fn=None,
    server_status_fn=None,
    is_thread_running_fn=None,
):
    """Evaluate guardrails and, if allowed, launch one non-blocking T3 self-heal thread.

    Returns dict: {'triggered': bool, 'status': str, 'thread_id': str|None, ...}
    """
    root = Path(root).resolve()
    now = time.time() if now is None else float(now)
    state_path = root / STATE_FILE
    state = _read_json(state_path)
    today = _today_utc()
    if state.get('daily_date') != today:
        state['daily_date'] = today
        state['daily_count'] = 0

    sig = f'{component}:{reason}'

    # 1. Hard daily cap
    if int(state.get('daily_count', 0)) >= MAX_DAILY_TRIGGERS:
        return {'triggered': False, 'status': 'daily_cap_reached', 'signature': sig}

    # 2. Global cooldown
    last_any = float(state.get('last_triggered_at') or 0)
    if last_any and (now - last_any) < GLOBAL_COOLDOWN_SECONDS:
        return {
            'triggered': False,
            'status': 'global_cooldown',
            'retry_in': int(GLOBAL_COOLDOWN_SECONDS - (now - last_any)),
            'signature': sig,
        }

    # 3. Per-signature cooldown
    sig_map = state.get('signature_cooldowns')
    if not isinstance(sig_map, dict):
        sig_map = {}
    last_sig = float(sig_map.get(sig) or 0)
    if last_sig and (now - last_sig) < SIGNATURE_COOLDOWN_SECONDS:
        return {
            'triggered': False,
            'status': 'signature_cooldown',
            'retry_in': int(SIGNATURE_COOLDOWN_SECONDS - (now - last_sig)),
            'signature': sig,
        }

    # 4. Concurrent active self-heal thread guard
    active_tid = state.get('active_thread_id')
    active_at = float(state.get('active_started_at') or 0)
    if active_tid and active_at and (now - active_at) < ACTIVE_THREAD_GRACE_SECONDS:
        check_fn = is_thread_running_fn or _is_thread_still_running
        if check_fn(root, active_tid):
            return {
                'triggered': False,
                'status': 'active_heal_thread_running',
                'thread_id': active_tid,
                'signature': sig,
            }

    # 5. Check T3 server readiness & project target
    status_fn = server_status_fn or t3_client.server_status
    t3_st = status_fn(root)
    if not t3_st.get('ok'):
        return {
            'triggered': False,
            'status': 't3_server_unavailable',
            't3_reason': t3_st.get('reason'),
            'signature': sig,
        }

    project_id, provider, model = _select_heal_target(root)
    if not project_id:
        return {'triggered': False, 'status': 't3_project_id_missing', 'signature': sig}

    title = f'【肥鱼桥自动告警】{component} {reason}'
    prompt = build_diagnostic_prompt(root, component, reason, details, components_snapshot)

    launcher = start_thread_fn or t3_client.start_thread
    try:
        res = launcher(
            root,
            project_id,
            provider,
            model,
            title,
            prompt,
            wait=False,
            timeout=60,
        )
    except Exception as exc:
        state['last_error'] = {'at': _stamp(), 'signature': sig, 'error': f'{type(exc).__name__}:{str(exc)[:200]}'}
        try:
            _atomic_json(state_path, state)
        except OSError:
            pass
        return {'triggered': False, 'status': 'launch_exception', 'error': str(exc)[:200], 'signature': sig}

    tid = res.get('thread_id')
    ok = (res.get('exit_code') == 0) or bool(tid)
    if not ok:
        state['last_error'] = {'at': _stamp(), 'signature': sig, 'error': str(res.get('error') or 'launch_failed')[:200]}
        try:
            _atomic_json(state_path, state)
        except OSError:
            pass
        return {'triggered': False, 'status': 'launch_failed', 'error': res.get('error'), 'signature': sig}

    state['last_triggered_at'] = now
    state['last_triggered_iso'] = _stamp()
    state['active_thread_id'] = tid
    state['active_started_at'] = now
    state['active_signature'] = sig
    state['daily_count'] = int(state.get('daily_count', 0)) + 1
    sig_map[sig] = now
    # Prune old signature entries older than 24h
    state['signature_cooldowns'] = {k: v for k, v in sig_map.items() if now - float(v) < 86400}
    history = state.get('history') if isinstance(state.get('history'), list) else []
    history.append({
        'at': _stamp(),
        'component': component,
        'reason': reason,
        'thread_id': tid,
        'provider': provider,
        'model': model,
    })
    state['history'] = history[-30:]
    try:
        _atomic_json(state_path, state)
    except OSError:
        pass
    return {
        'triggered': True,
        'status': 'triggered',
        'thread_id': tid,
        'signature': sig,
        'provider': provider,
        'model': model,
    }


def notify_recovery(root, component, outage_seconds, *, via_t3_heal=False):
    """Send a single concise recovery notice via private_bot after a notable outage."""
    if outage_seconds < RECOVERY_NOTIFY_MIN_OUTAGE:
        return False
    try:
        import private_bot_control
        st = private_bot_control.status(root)
        if not st.get('authenticated'):
            return False
        mins = max(1, int(round(outage_seconds / 60.0)))
        via = '（已联动 T3 诊断线程）' if via_t3_heal else '（已自动重启拉回）'
        text = f'【肥鱼桥自愈通知】组件 `{component}` 曾中断约 {mins} 分钟，现已恢复正常连接{via}。'
        res = private_bot_control.send_markdown(root, None, text, timeout=15.0)
        return bool(res.get('success'))
    except Exception:
        return False
