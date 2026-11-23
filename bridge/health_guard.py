"""External Scheduled-Task health guard for Feiyu T3 Bridge.

Invoked periodically (every 3 minutes) by Windows Task Scheduler (`FeiyuBridge-HealthGuard`)
or on-demand via `schtasks /Run /TN FeiyuBridge-HealthGuard` when an agent running inside
a restricted Windows Job Object requests a clean restart.

Because Task Scheduler runs in an independent user session outside any T3/Codex/Antigravity
Job Object, every service spawned here survives indefinitely.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import t3_self_heal
from win_runtime import (
    RECOVERY_REQUEST_FILE,
    SingleInstance,
    atomic_write_json,
    pid_alive,
    read_json_retry,
    terminate_matching_processes,
)


def _stamp():
    return datetime.now(timezone.utc).isoformat()


def _read_json(path):
    return read_json_retry(path)


def _confirm_unhealthy(check_fn, *, probes=3, interval=0.4):
    """Confirm a component stays unhealthy across consecutive samples.

    Prevents single-tick false positives from concurrent Windows status-file
    replacement or momentary heartbeat jitter.
    """
    count = max(1, int(probes))
    for idx in range(count):
        if check_fn():
            return False
        if idx + 1 < count and interval > 0:
            time.sleep(interval)
    return True


def _component_diagnostic(root, comp):
    if comp == 'bridge_daemon':
        st = _read_json(root / 'bridge-daemon-status.json')
        ds = _read_json(root / 'local-dispatch-status.json')
        return {
            'daemon_state': st.get('state'),
            'daemon_pid': st.get('pid'),
            'daemon_pid_alive': pid_alive(st.get('pid')),
            'daemon_age_s': round(_age(st), 3) if _age(st) != float('inf') else None,
            'dispatch_age_s': round(_age(ds), 3) if _age(ds) != float('inf') else None,
        }
    if comp == 'feiyu_receiver':
        st = _read_json(root / 'status.json')
    elif comp == 'private_bot':
        st = _read_json(root / 'private-bot-status.json')
    else:
        st = _read_json(root / 'watchdog-state.json')
    return {
        'state': st.get('state'),
        'pid': st.get('pid'),
        'pid_alive': pid_alive(st.get('pid')),
        'age_s': round(_age(st), 3) if _age(st) != float('inf') else None,
        'stopped_at': st.get('stopped_at'),
    }


def _age(data, key='updated_at'):
    try:
        return time.time() - datetime.fromisoformat(str(data[key]).replace('Z', '+00:00')).timestamp()
    except (KeyError, ValueError, TypeError):
        return float('inf')


def _panel_python(root):
    rt = _read_json(root / 'panel-runtime.json')
    py = rt.get('python')
    if py and Path(py).is_file():
        return str(py)
    exe = Path(sys.executable)
    if exe.name.lower() == 'pythonw.exe':
        sibling = exe.with_name('python.exe')
        if sibling.is_file():
            return str(sibling)
    return str(exe)


def _run_start(py, root, script, *, timeout=60):
    try:
        env = dict(os.environ)
        env['FEIYU_INDEPENDENT_HOST'] = '1'
        proc = subprocess.run(
            [py, str(root / script), 'start'],
            cwd=root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
            timeout=timeout,
            check=False,
        )
        return {'script': script, 'exit': proc.returncode, 'stdout': (proc.stdout or '').strip()[:200]}
    except Exception as exc:
        return {'script': script, 'exit': -1, 'error': f'{type(exc).__name__}:{str(exc)[:160]}'}


def _ensure_tray(root):
    tray_st = _read_json(root / 'tray_ui_state.json')
    tray_ok = (
        tray_st.get('state') == 'running'
        and -1.0 <= _age(tray_st) < 30.0
        and pid_alive(tray_st.get('pid')) is not False
    )
    if tray_ok:
        return {'service': 'WecomBridge.exe', 'action': 'healthy', 'pid': tray_st.get('pid')}
    exe = root / 'WecomBridge.exe'
    if not exe.is_file():
        return {'service': 'WecomBridge.exe', 'action': 'missing_binary'}
    try:
        flags = subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS
        if os.name == 'nt':
            flags |= subprocess.CREATE_BREAKAWAY_FROM_JOB
        try:
            subprocess.Popen(
                [str(exe), '--tray', '--login'],
                cwd=root,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=flags,
            )
        except PermissionError:
            subprocess.Popen(
                [str(exe), '--tray', '--login'],
                cwd=root,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,
            )
        return {'service': 'WecomBridge.exe', 'action': 'started'}
    except OSError as exc:
        return {'service': 'WecomBridge.exe', 'action': 'start_failed', 'error': str(exc)[:160]}


def _is_daemon_healthy(root):
    st = _read_json(root / 'bridge-daemon-status.json')
    ds = _read_json(root / 'local-dispatch-status.json')
    if st.get('state') != 'running' or not (-1.0 <= _age(st) < 25.0):
        return False
    if pid_alive(st.get('pid')) is False:
        return False
    if not (-1.0 <= _age(ds) < 30.0):
        return False
    return True


def _is_receiver_healthy(root, status_file, stop_file):
    if (root / stop_file).exists():
        return True
    st = _read_json(root / status_file)
    if st.get('error_type') == 'WSAuthFailureError' or 'replaced' in str(st.get('reason', '')).lower():
        return True  # Handled by self-heal escalation, not blind restart loop
    return (
        st.get('state') == 'authenticated'
        and -1.0 <= _age(st) < 20.0
        and pid_alive(st.get('pid')) is not False
    )


def _is_watchdog_healthy(root):
    if (root / 'watchdog.stop').exists():
        return True
    st = _read_json(root / 'watchdog-state.json')
    return (
        not st.get('stopped_at')
        and -1.0 <= _age(st) < 30.0
        and pid_alive(st.get('pid')) is not False
    )


def run_once(root=ROOT, *, force_services=None):
    root = Path(root).resolve()
    with SingleInstance('HealthGuardRun', root) as lock:
        if not lock.acquired:
            return {'skipped': True, 'reason': 'already_running'}
        py = _panel_python(root)
        req_path = root / RECOVERY_REQUEST_FILE
        req = _read_json(req_path) if req_path.is_file() else {}
        if req_path.is_file():
            try:
                req_path.unlink(missing_ok=True)
            except OSError:
                pass
        forced = set(force_services or []) | set(req.get('services') or [])
        actions = []
        attempted = set()

        # 1. Ensure tray host is alive
        actions.append(_ensure_tray(root))

        def _recover_feiyu():
            attempted.add('feiyu_receiver')
            st = _read_json(root / 'status.json')
            if _age(st) >= 45.0 and pid_alive(st.get('pid')) is True:
                terminate_matching_processes(root, ['wecom_receiver.cjs', 'bridge_control.py'])
            actions.append(_run_start(py, root, 'bridge_control.py', timeout=75))

        def _recover_private_bot():
            attempted.add('private_bot')
            st = _read_json(root / 'private-bot-status.json')
            if _age(st) >= 45.0 and pid_alive(st.get('pid')) is True:
                terminate_matching_processes(root, ['private_bot_receiver.cjs', 'private_bot_control.py'])
            actions.append(_run_start(py, root, 'private_bot_control.py', timeout=45))

        def _recover_daemon():
            attempted.add('bridge_daemon')
            st = _read_json(root / 'bridge-daemon-status.json')
            if _age(st) >= 45.0 and pid_alive(st.get('pid')) is True:
                terminate_matching_processes(root, ['bridge_daemon.py'])
            actions.append(_run_start(py, root, 'bridge_daemon.py', timeout=30))

        def _recover_watchdog():
            attempted.add('watchdog')
            st = _read_json(root / 'watchdog-state.json')
            if _age(st) >= 45.0 and pid_alive(st.get('pid')) is True:
                terminate_matching_processes(root, ['watchdog.py'])
            actions.append(_run_start(py, root, 'watchdog.py', timeout=25))

        # 2. Ensure Feiyu receiver
        if not (root / 'stop.request').exists() and (
            'feiyu_receiver' in forced
            or _confirm_unhealthy(lambda: _is_receiver_healthy(root, 'status.json', 'stop.request'), probes=2, interval=0.25)
        ):
            _recover_feiyu()

        # 3. Ensure Private Bot (Claude) receiver
        if (root / 'private-bot-credentials.dpapi').is_file() and not (root / 'private-bot.stop').exists():
            if (
                'private_bot' in forced
                or _confirm_unhealthy(lambda: _is_receiver_healthy(root, 'private-bot-status.json', 'private-bot.stop'), probes=2, interval=0.25)
            ):
                _recover_private_bot()

        # 4. Ensure bridge_daemon
        if not (root / 'bridge-daemon.stop').exists() and (
            'bridge_daemon' in forced
            or _confirm_unhealthy(lambda: _is_daemon_healthy(root), probes=2, interval=0.25)
        ):
            _recover_daemon()

        # 5. Ensure watchdog
        if not (root / 'watchdog.stop').exists() and (
            'watchdog' in forced
            or _confirm_unhealthy(lambda: _is_watchdog_healthy(root), probes=2, interval=0.25)
        ):
            _recover_watchdog()

        # 6. Post-check: confirm across consecutive probes before declaring failure,
        # and never claim `health_guard_recovery_failed` for a component we never attempted to recover.
        time.sleep(2.0)
        checks = []
        if not (root / 'stop.request').exists():
            checks.append(('feiyu_receiver', lambda: _is_receiver_healthy(root, 'status.json', 'stop.request'), _recover_feiyu))
        if (root / 'private-bot-credentials.dpapi').is_file() and not (root / 'private-bot.stop').exists():
            checks.append(('private_bot', lambda: _is_receiver_healthy(root, 'private-bot-status.json', 'private-bot.stop'), _recover_private_bot))
        if not (root / 'bridge-daemon.stop').exists():
            checks.append(('bridge_daemon', lambda: _is_daemon_healthy(root), _recover_daemon))
        if not (root / 'watchdog.stop').exists():
            checks.append(('watchdog', lambda: _is_watchdog_healthy(root), _recover_watchdog))

        still_bad = []
        bad_diagnostics = {}
        for comp_name, check_fn, recover_fn in checks:
            if not _confirm_unhealthy(check_fn, probes=3, interval=0.4):
                continue
            if comp_name not in attempted:
                recover_fn()
                time.sleep(2.0)
                if not _confirm_unhealthy(check_fn, probes=2, interval=0.4):
                    continue
            still_bad.append(comp_name)
            bad_diagnostics[comp_name] = _component_diagnostic(root, comp_name)

        heal_res = None
        if still_bad:
            heal_res = t3_self_heal.maybe_trigger(
                root,
                '+'.join(still_bad),
                'health_guard_recovery_failed',
                details={
                    'failed_components': still_bad,
                    'actions': actions,
                    'diagnostics': bad_diagnostics,
                },
                components_snapshot={'still_bad': still_bad, 'diagnostics': bad_diagnostics},
            )

        report = {
            'at': _stamp(),
            'pid': os.getpid(),
            'forced': sorted(forced),
            'actions': actions,
            'still_bad': still_bad,
            'diagnostics': bad_diagnostics,
            'self_heal': heal_res,
        }
        try:
            atomic_write_json(root / 'health-guard-state.json', report)
        except OSError:
            pass
        return report


def main():
    if sys.stdout is not None:
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass
    p = argparse.ArgumentParser()
    p.add_argument('--root', default=str(ROOT))
    p.add_argument('--force', nargs='*', default=[])
    args = p.parse_args()
    res = run_once(Path(args.root), force_services=args.force)
    print(json.dumps(res, ensure_ascii=False))
    return 0 if not res.get('still_bad') else 1


if __name__ == '__main__':
    raise SystemExit(main())
