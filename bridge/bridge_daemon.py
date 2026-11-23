"""Pure-local T3 Feiyu Bridge daemon.

Replaces the legacy Dot MCP Event Server + tunnel-client architecture:
- Watches `queue/` and `media-state/` via Win32 DirectorySignal (plus bounded periodic sweep).
- Runs `business_queue.reconcile`, `t3_dispatch.serve`, and `chat_feedback.serve`.
- Supervises the receiver-only `watchdog.py`.
- Zero external tunnel, zero Dot HTTP server, zero FeiyuOwnerReview popups.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import errno
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import secrets
import subprocess
import sys
import threading
import time

import business_queue
import chat_feedback
import clarifications
import pending_expiry
import t3_client
import t3_dispatch
from win_runtime import DirectorySignal, SingleInstance, read_json_retry, spawn_detached_or_delegate

ROOT = Path(__file__).resolve().parent
STATUS_FILE = 'bridge-daemon-status.json'
STOP_FILE = 'bridge-daemon.stop'
RELOAD_FILE = 'daemon-reload.request'


def stamp():
    return datetime.now(timezone.utc).isoformat()


def _is_transient_lock(exc):
    return getattr(exc, 'winerror', None) in (5, 32, 33) or getattr(exc, 'errno', None) in (errno.EACCES, errno.EBUSY)


def _atomic(path, payload):
    path = Path(path)
    tmp = path.with_name(path.name + '.' + secrets.token_hex(6) + '.tmp')
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        delays = (0.0, 0.02, 0.05, 0.1, 0.2, 0.35)
        for idx, pause in enumerate(delays):
            if pause:
                time.sleep(pause)
            try:
                tmp.replace(path)
                return
            except OSError as exc:
                if not _is_transient_lock(exc) or idx == len(delays) - 1:
                    raise
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _read_json(path):
    return read_json_retry(path)


def is_running(root=ROOT):
    root = Path(root)
    with SingleInstance('BridgeDaemon', root) as probe:
        return not probe.acquired


def _configure_logging(root=ROOT):
    for stream in (sys.stdout, sys.stderr):
        if stream is not None:
            try:
                stream.reconfigure(encoding='utf-8', line_buffering=True)
            except Exception:
                pass
    handlers = [logging.StreamHandler()]
    try:
        fh = RotatingFileHandler(
            Path(root) / 'bridge-daemon.log',
            maxBytes=2 * 1024 * 1024,
            backupCount=3,
            encoding='utf-8',
        )
        handlers.append(fh)
    except OSError:
        pass
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s',
        handlers=handlers,
        force=True,
    )


def _startup_verify_and_reconnect(root):
    """Post-start verification & reconnect check after launch or force-restart.

    Ensures:
    1. All previously running T3 workers/threads are reconciled (never dropped silently).
    2. Sibling transports (Feiyu receiver, Private Bot/Claude, Watchdog) are verified live
       and immediately reconnected if any dropped during a restart window.
    """
    root = Path(root).resolve()
    host_env = dict(os.environ)
    host_env['FEIYU_INDEPENDENT_HOST'] = '1'
    report = {
        'at': stamp(),
        't3_reconciled': False,
        'reconnected': [],
        'checks': {},
    }

    # 1. Reconcile active T3 jobs/threads immediately
    try:
        t3_dispatch.reconcile(root)
        report['t3_reconciled'] = True
    except Exception as exc:
        report['t3_reconcile_error'] = type(exc).__name__
        logging.exception('Startup T3 reconcile failed')

    # 2. Verify Feiyu receiver & reconnect if down
    if not (root / 'stop.request').exists():
        try:
            import bridge_control
            rx_st = _read_json(root / 'status.json')
            rx_ok = bridge_control.live(rx_st) and rx_st.get('state') == 'authenticated'
            report['checks']['feiyu_receiver'] = 'ok' if rx_ok else 'reconnecting'
            if not rx_ok:
                logging.warning('Startup check: feiyu_receiver not authenticated; triggering reconnect')
                subprocess.run(
                    [sys.executable, str(root / 'bridge_control.py'), 'start'],
                    cwd=root, env=host_env, stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                    timeout=45, check=False,
                )
                rx_after = _read_json(root / 'status.json')
                rx_ok_after = bridge_control.live(rx_after) and rx_after.get('state') == 'authenticated'
                report['checks']['feiyu_receiver'] = 'reconnected' if rx_ok_after else 'still_unhealthy'
                if rx_ok_after:
                    report['reconnected'].append('feiyu_receiver')
        except Exception as exc:
            report['checks']['feiyu_receiver'] = f'error:{type(exc).__name__}'

    # 3. Verify Private Bot (Claude) & reconnect if down
    try:
        import private_bot_control
        if private_bot_control.is_configured(root) and not (root / private_bot_control.STOP_FILE).exists():
            pb_ok = bool(private_bot_control.status(root).get('authenticated'))
            report['checks']['private_bot'] = 'ok' if pb_ok else 'reconnecting'
            if not pb_ok:
                logging.warning('Startup check: private_bot not authenticated; triggering reconnect')
                subprocess.run(
                    [sys.executable, str(root / 'private_bot_control.py'), 'start', '--root', str(root)],
                    cwd=root, env=host_env, stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                    timeout=35, check=False,
                )
                pb_ok_after = bool(private_bot_control.status(root).get('authenticated'))
                report['checks']['private_bot'] = 'reconnected' if pb_ok_after else 'still_unhealthy'
                if pb_ok_after:
                    report['reconnected'].append('private_bot')
    except Exception as exc:
        report['checks']['private_bot'] = f'error:{type(exc).__name__}'

    # 4. Verify Watchdog & reconnect if missing
    if not (root / 'watchdog.stop').exists():
        try:
            with SingleInstance('Watchdog', root) as wlock:
                missing = wlock.acquired
            report['checks']['watchdog'] = 'reconnecting' if missing else 'ok'
            if missing:
                logging.warning('Startup check: watchdog missing; starting watchdog')
                subprocess.run(
                    [sys.executable, str(root / 'watchdog.py'), 'start'],
                    cwd=root, env=host_env, stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                    timeout=20, check=False,
                )
                with SingleInstance('Watchdog', root) as wlock2:
                    ok_after = not wlock2.acquired
                report['checks']['watchdog'] = 'reconnected' if ok_after else 'still_unhealthy'
                if ok_after:
                    report['reconnected'].append('watchdog')
        except Exception as exc:
            report['checks']['watchdog'] = f'error:{type(exc).__name__}'

    logging.info('Startup verify complete: %s', json.dumps(report, ensure_ascii=False))
    return report


def run(root=ROOT):
    root = Path(root).resolve()
    _configure_logging(root)
    logging.info('BridgeDaemon starting (pid=%s, root=%s)', os.getpid(), root)
    with SingleInstance('BridgeDaemon', root) as lock:
        if not lock.acquired:
            return 0
        (root / STOP_FILE).unlink(missing_ok=True)
        (root / 'queue').mkdir(exist_ok=True)
        (root / 'media-state').mkdir(exist_ok=True)
        stopped = threading.Event()
        wake_reconcile = threading.Event()
        startup_check = {}

        def request_reconcile():
            wake_reconcile.set()

        queue_watch = DirectorySignal(root / 'queue', request_reconcile)
        media_watch = DirectorySignal(root / 'media-state', request_reconcile)

        def write_status(state='running', error=None):
            # Telemetry writes must never crash a worker thread on transient Windows file locks.
            try:
                t3_st = t3_client.server_status(root)
            except Exception as exc:
                t3_st = {'ok': False, 'reason': 'status_check_error:' + type(exc).__name__}
            pb_ok = False
            try:
                import private_bot_control
                pb_ok = bool(private_bot_control.status(root).get('authenticated'))
            except Exception:
                pb_ok = False
            try:
                payload = {
                    'pid': os.getpid(),
                    'engine': 't3',
                    'state': state,
                    'updated_at': stamp(),
                    't3_server_ok': t3_st['ok'],
                    't3_server_reason': t3_st['reason'],
                    'private_bot_ok': pb_ok,
                    'error': error,
                }
                if startup_check:
                    payload['startup_check'] = startup_check
                _atomic(root / STATUS_FILE, payload)
                return True
            except Exception:
                logging.warning('Failed to write %s (state=%s)', STATUS_FILE, state, exc_info=True)
                return False

        def reconcile_loop():
            while not stopped.is_set():
                try:
                    wake_reconcile.clear()
                    err = None
                    try:
                        business_queue.reconcile(root)
                        with business_queue.db(root, True) as c:
                            clarifications.sweep_stale(c)
                        chat_feedback.changed.set()
                    except Exception as exc:
                        err = type(exc).__name__
                        logging.exception('Business reconciliation error')
                    write_status('running', err)

                    wait = 3.0
                    try:
                        import agent_relay
                        import conclusion_notes
                        with business_queue.db(root) as c:
                            batch_due = c.execute(
                                "SELECT MIN(deadline_epoch) FROM message_batches WHERE deadline_epoch IS NOT NULL AND state IN ('collecting','sealing')"
                            ).fetchone()[0]
                            exp_due = pending_expiry.next_deadline(c, time.time(), root=root)
                            followup_due = conclusion_notes.next_followup_deadline(c, time.time())
                            relay_due = agent_relay.next_due(c, time.time())
                        for dl in (batch_due, exp_due, followup_due, relay_due):
                            if dl is not None:
                                wait = max(0.1, min(wait, dl - time.time()))
                    except Exception:
                        pass
                    wake_reconcile.wait(wait)
                except Exception:
                    logging.exception('Unexpected reconcile_loop error')
                    stopped.wait(1.0)

        def maintenance_loop():
            next_watch_check = 0.0
            while not stopped.wait(1.0):
                try:
                    if (root / STOP_FILE).exists():
                        stopped.set()
                        return
                    if _read_json(root / RELOAD_FILE).get('pid') == os.getpid():
                        stopped.set()
                        return
                    if time.monotonic() >= next_watch_check and not (root / 'watchdog.stop').exists():
                        next_watch_check = time.monotonic() + 8.0
                        host_env = dict(os.environ)
                        host_env['FEIYU_INDEPENDENT_HOST'] = '1'
                        with SingleInstance('Watchdog', root) as wlock:
                            missing = wlock.acquired
                        if missing:
                            try:
                                subprocess.run(
                                    [sys.executable, str(root / 'watchdog.py'), 'start'],
                                    cwd=root, env=host_env, stdin=subprocess.DEVNULL,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                                    timeout=15, check=False,
                                )
                            except OSError:
                                pass
                        try:
                            import bridge_control
                            if not (root / 'stop.request').exists():
                                rx_st = _read_json(root / 'status.json')
                                if not bridge_control.live(rx_st):
                                    subprocess.run(
                                        [sys.executable, str(root / 'bridge_control.py'), 'start'],
                                        cwd=root, env=host_env, stdin=subprocess.DEVNULL,
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                                        timeout=45, check=False,
                                    )
                        except Exception:
                            pass
                        try:
                            import private_bot_control
                            if private_bot_control.is_configured(root) and not (root / private_bot_control.STOP_FILE).exists():
                                if not private_bot_control.status(root).get('running'):
                                    subprocess.run(
                                        [sys.executable, str(root / 'private_bot_control.py'), 'start', '--root', str(root)],
                                        cwd=root, env=host_env, stdin=subprocess.DEVNULL,
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                        creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                                        timeout=25, check=False,
                                    )
                        except Exception:
                            pass
                except Exception:
                    logging.exception('Unexpected maintenance_loop error')

        worker_specs = [
            ('feiyu-reconcile', reconcile_loop, ()),
            ('feiyu-t3-dispatch', t3_dispatch.serve, (stopped, root, request_reconcile)),
            ('feiyu-chat-feedback', chat_feedback.serve, (stopped, root, None, None)),
            ('feiyu-maintenance', maintenance_loop, ()),
        ]
        # Initial synchronous reconcile + post-start sibling verification before entering background loops
        try:
            business_queue.reconcile(root)
        except Exception:
            logging.exception('Initial reconcile failed')
        try:
            startup_check.update(_startup_verify_and_reconnect(root))
        except Exception:
            logging.exception('Startup verify failed')
        write_status('running')
        chat_feedback.changed.set()
        t3_dispatch.wake(root)
        workers = {}
        last_restart = {}
        for name, target, args in worker_specs:
            t = threading.Thread(target=target, args=args, name=name, daemon=True)
            t.start()
            workers[name] = t
        try:
            while not stopped.wait(1.0):
                now_mono = time.monotonic()
                for name, target, args in worker_specs:
                    t = workers[name]
                    if not t.is_alive() and not stopped.is_set():
                        if now_mono - last_restart.get(name, 0.0) >= 2.0:
                            last_restart[name] = now_mono
                            logging.error('Worker thread %s exited unexpectedly; restarting', name)
                            nt = threading.Thread(target=target, args=args, name=name, daemon=True)
                            nt.start()
                            workers[name] = nt
        except KeyboardInterrupt:
            stopped.set()
        finally:
            stopped.set()
            wake_reconcile.set()
            chat_feedback.changed.set()
            t3_dispatch.wake(root)
            queue_watch.close()
            media_watch.close()
            for w in workers.values():
                w.join(timeout=10)
            write_status('stopped')
    return 0


def _spawn_detached(args, *, cwd, logf, root=ROOT):
    env = dict(os.environ)
    env['PYTHONUNBUFFERED'] = '1'
    env['PYTHONIOENCODING'] = 'utf-8'
    return spawn_detached_or_delegate(
        args,
        cwd=cwd,
        root=root,
        service_name='bridge_daemon',
        stdout=logf,
        stderr=logf,
        env=env,
    )


def start(root=ROOT):
    root = Path(root).resolve()
    if is_running(root):
        return {'running': True, 'already_running': True, **_read_json(root / STATUS_FILE)}
    (root / STOP_FILE).unlink(missing_ok=True)
    with (root / 'bridge-daemon.log').open('ab') as logf:
        mode, via = _spawn_detached(
            [sys.executable, str(Path(__file__).resolve()), 'run', '--root', str(root)],
            cwd=root, logf=logf, root=root,
        )
    if mode == 'blocked':
        return {'running': False, 'error': via, **_read_json(root / STATUS_FILE)}
    for _ in range(40):
        time.sleep(0.25)
        if is_running(root):
            st = _read_json(root / STATUS_FILE)
            if st.get('state') == 'running':
                out = {'running': True, **st}
                if mode == 'delegated':
                    out['delegated_via'] = via
                return out
    out = {'running': is_running(root), **_read_json(root / STATUS_FILE)}
    if mode == 'delegated':
        out['delegated_via'] = via
    return out


def stop_daemon(root=ROOT):
    root = Path(root).resolve()
    (root / STOP_FILE).touch()
    for _ in range(30):
        if not is_running(root):
            break
        time.sleep(0.3)
    (root / STOP_FILE).unlink(missing_ok=True)
    return {'stopped': not is_running(root), **_read_json(root / STATUS_FILE)}


def main():
    if sys.stdout is not None:
        sys.stdout.reconfigure(encoding='utf-8')
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['start', 'stop', 'status', 'run'])
    p.add_argument('--root', default=str(ROOT))
    args = p.parse_args()
    root = Path(args.root)
    if args.action == 'run':
        return run(root)
    if args.action == 'start':
        res = start(root)
        print(json.dumps(res, ensure_ascii=False))
        return 0 if res.get('running') else 1
    if args.action == 'stop':
        res = stop_daemon(root)
        print(json.dumps(res, ensure_ascii=False))
        return 0 if res.get('stopped') else 1
    st = _read_json(root / STATUS_FILE)
    print(json.dumps({'running': is_running(root), **st}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
