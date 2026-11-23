"""Independent user-session full-stack bridge watchdog.

Monitors and self-heals all critical Feiyu T3 Bridge components:
1. `feiyu_receiver` (`status.json` / `wecom_receiver.cjs`)
2. `private_bot` (`private-bot-status.json` / `private_bot_receiver.cjs`, when configured)
3. `bridge_daemon` (`bridge-daemon-status.json` + `local-dispatch-status.json`)
4. End-to-end stall detection (`queue/` unconsumed messages > 60s or `chat_receipts` stuck `ready` > 60s)

When a process is dead, stale/hung, disconnected too long, or stalled, watchdog verifies
process identity, force-terminates hung instances if needed, and restarts them.
When local restarts fail repeatedly (>=2 attempts) or an outage persists (>=180s; >=60s for blocked),
watchdog escalates directly to T3 Code via `t3_self_heal.maybe_trigger` (bypassing `bridge_daemon`).

Never reads message content or executes inbound instructions.
"""
import argparse, ctypes, errno, json, os, sqlite3, subprocess, sys, tempfile, threading, time, uuid
from ctypes import wintypes
from datetime import datetime, timezone
from pathlib import Path
from bridge_control import ROOT
import t3_self_heal
from win_runtime import (
    DirectorySignal,
    SingleInstance,
    pid_alive as _rt_pid_alive,
    read_json_retry,
    spawn_detached_or_delegate,
    terminate_matching_processes,
)

# Force-kill / escalation thresholds (seconds)
STALE_KILL_SECONDS = 45
DISCONNECTED_KILL_SECONDS = 60
QUEUE_STALL_SECONDS = 60
QUEUE_STALL_MAX_AGE_SECONDS = 900  # Ignore ancient files (>15 min) that belong to pending_expiry
RECEIPT_STALL_SECONDS = 60
T3_HEAL_MIN_ATTEMPTS = 2
T3_HEAL_MIN_OUTAGE_SECONDS = 180
T3_HEAL_BLOCKED_SECONDS = 60


def stamp(): return datetime.now(timezone.utc).isoformat()


def read_json(p):
    return read_json_retry(p)


def atomic(p, data):
    p = Path(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.' + p.name + '-', suffix='.tmp', dir=p.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(data, stream, ensure_ascii=False, indent=2)
        for pause in (0, .02, .06, .16):
            if pause: time.sleep(pause)
            try:
                os.replace(tmp, p)
                return
            except OSError as exc:
                locked = getattr(exc, 'winerror', None) in (5, 32, 33) or exc.errno in (errno.EACCES, errno.EBUSY)
                if not locked or pause == .16: raise
    finally:
        tmp.unlink(missing_ok=True)


def heartbeat(path, saved):
    # Heartbeats are replaceable. Preserve pending incident records until their archive write succeeds.
    try:
        atomic(path, saved)
    except OSError as exc:
        saved['heartbeat_write_failures'] = saved.get('heartbeat_write_failures', 0) + 1
        saved['last_heartbeat_write_error'] = {'at': stamp(), 'type': type(exc).__name__}
        return False
    return True


def age(s, now):
    try: return now - datetime.fromisoformat(s['updated_at'].replace('Z', '+00:00')).timestamp()
    except (KeyError, ValueError, TypeError): return float('inf')


def classify(s, now, stop=False):
    if stop or s.get('reason') == 'local stop request': return 'paused'
    if s.get('error_type') == 'WSAuthFailureError' or 'replaced' in str(s.get('reason', '')).lower(): return 'blocked'
    # Same-host timestamp serialization can round forward by a fraction of a microsecond.
    # Only tolerate 1 ms; genuinely future-dated or stale records remain unhealthy.
    if not -0.001 <= age(s, now) < 15 or s.get('state') == 'stopped': return 'dead_or_stale'
    return 'healthy' if s.get('state') == 'authenticated' else 'disconnected'


def classify_daemon(daemon_state, dispatch_state, now, *, stop=False, stall_info=None):
    if stop:
        return 'paused'
    if daemon_state.get('state') != 'running' or not (-1.0 <= age(daemon_state, now) < 25):
        return 'dead_or_stale'
    if not (-1.0 <= age(dispatch_state, now) < 30):
        return 'dead_or_stale'
    if stall_info and stall_info.get('stalled'):
        return 'stalled'
    return 'healthy'


def detect_end_to_end_stall(root, now=None):
    """Detect silent end-to-end processing stalls without reading message contents.

    Checks two metadata-only conditions:
    1. A queue/*.json file written 60s..900s ago has not been recorded in business.sqlite3 `intake_files`.
    2. A chat_receipts row has been in state='ready' with attempts=0 for > 60s past `due`.
    """
    root = Path(root)
    now = time.time() if now is None else float(now)
    db_path = root / 'business.sqlite3'
    queue_dir = root / 'queue'
    stalled_queue = []
    stalled_receipts = 0

    if not db_path.is_file():
        return {'stalled': False, 'stalled_queue': [], 'stalled_receipts': 0}

    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=3)
    except sqlite3.Error:
        try:
            conn = sqlite3.connect(db_path, timeout=3)
        except sqlite3.Error:
            return {'stalled': False, 'stalled_queue': [], 'stalled_receipts': 0}

    try:
        known_files = set()
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='intake_files'").fetchone():
            known_files = {r[0] for r in conn.execute('SELECT name FROM intake_files')}

        if queue_dir.is_dir():
            for p in queue_dir.glob('*.json'):
                try:
                    mtime = p.stat().st_mtime
                except OSError:
                    continue
                delta = now - mtime
                if QUEUE_STALL_SECONDS <= delta <= QUEUE_STALL_MAX_AGE_SECONDS:
                    if p.name not in known_files:
                        stalled_queue.append({'file': p.name, 'age_seconds': int(delta)})

        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='chat_receipts'").fetchone():
            row = conn.execute(
                "SELECT COUNT(*) FROM chat_receipts WHERE state='ready' AND attempts=0 AND due <= ? AND due >= ?",
                (now - RECEIPT_STALL_SECONDS, now - QUEUE_STALL_MAX_AGE_SECONDS),
            ).fetchone()
            stalled_receipts = int(row[0] if row else 0)
    except sqlite3.Error:
        return {'stalled': False, 'stalled_queue': [], 'stalled_receipts': 0}
    finally:
        conn.close()

    stalled = bool(stalled_queue or stalled_receipts > 0)
    reason = None
    if stalled_queue:
        reason = f'queue_unconsumed:{len(stalled_queue)}'
    elif stalled_receipts > 0:
        reason = f'receipt_stuck:{stalled_receipts}'
    return {
        'stalled': stalled,
        'reason': reason,
        'stalled_queue': stalled_queue[:5],
        'stalled_receipts': stalled_receipts,
    }


def sample_state(root):
    # Sample wall clock after the read, not before a concurrent heartbeat update.
    state = read_json(Path(root) / 'status.json')
    return state, time.time()


def pid_alive(pid):
    return _rt_pid_alive(pid)


def transition(saved, health, now):
    """Pure persisted episode state machine: one down and recovery per episode."""
    notices = []
    if health == 'healthy':
        saved['bad_since'] = None
        if saved.get('incident'):
            notices.append(('recovered', saved['incident'])); saved['incident'] = None
        saved['attempts'] = 0; saved['retry_at'] = 0
    elif health != 'paused':
        if saved.get('bad_since') is None: saved['bad_since'] = now
        if not saved.get('incident') and now - saved['bad_since'] >= 20:
            saved['incident'] = uuid.uuid4().hex; notices.append(('down', saved['incident']))
    else:
        saved['bad_since'] = None
    return notices


def publish_notices(root, saved, *, transport=False):
    """Drain both current and legacy pending transitions exclusively into the local archive."""
    pending = []
    for entry in saved.get('pending_notices', []):
        kind, incident = entry[:2]
        archive = Path(root) / 'watchdog-incidents'
        if kind in ('probe_failed', 'probe_changed', 'probe_recovered'):
            local = datetime.fromisoformat(entry[4]).astimezone()
            filename = local.strftime('%H%M%S-%f') + '-' + incident + '-' + kind + '.json'
            path = archive / 'probes' / local.strftime('%Y-%m-%d') / filename
        else:
            path = archive / (incident + '-' + kind + '.json')
        previous = read_json(path)
        if previous.get('incident') == incident and previous.get('kind') == kind:
            # The archive is immutable, including any historical delivery annotation.
            continue
        reason = entry[2] if len(entry) > 2 else 'transport_unavailable'
        is_test = entry[3] if len(entry) > 3 else (Path(root) / 'watchdog-test.flag').exists()
        at = entry[4] if len(entry) > 4 else stamp()
        record = {'incident': incident, 'kind': kind, 'at': at, 'test': is_test,
                  'reason' if transport else 'health': reason, 'notice': 'local_archive_only'}
        if len(entry) > 5 and isinstance(entry[5], dict):
            record['observation'] = entry[5]
        try:
            atomic(path, record)
        except OSError as exc:
            saved['last_archive_error'] = {'at': stamp(), 'type': type(exc).__name__}
            pending.append([kind, incident, reason, is_test, at, *entry[5:]])
    saved['pending_notices'] = pending


def _should_force_kill(health, bad_duration):
    if health == 'stalled':
        return bad_duration >= 5.0
    if health == 'dead_or_stale':
        return bad_duration >= STALE_KILL_SECONDS
    if health == 'disconnected':
        return bad_duration >= DISCONNECTED_KILL_SECONDS
    return False


def _should_escalate_t3(health, attempts, bad_duration):
    if health in ('healthy', 'paused'):
        return False
    if health == 'blocked':
        return bad_duration >= T3_HEAL_BLOCKED_SECONDS
    return attempts >= T3_HEAL_MIN_ATTEMPTS or bad_duration >= T3_HEAL_MIN_OUTAGE_SECONDS


def _restart_component(root, comp_name, *, force_kill=False, log_path=None):
    """Restart a single supervised component, optionally force-killing hung processes first."""
    root = Path(root)
    killed = []
    if comp_name == 'feiyu_receiver':
        if force_kill:
            killed = terminate_matching_processes(root, ['wecom_receiver.cjs', 'bridge_control.py'])
        cmd = [sys.executable, str(root / 'bridge_control.py'), 'start']
    elif comp_name == 'private_bot':
        if force_kill:
            killed = terminate_matching_processes(root, ['private_bot_receiver.cjs', 'private_bot_control.py'])
        cmd = [sys.executable, str(root / 'private_bot_control.py'), 'start', '--root', str(root)]
    elif comp_name == 'bridge_daemon':
        if force_kill:
            killed = terminate_matching_processes(root, ['bridge_daemon.py'])
        cmd = [sys.executable, str(root / 'bridge_daemon.py'), 'start', '--root', str(root)]
    else:
        return None, killed

    lp = log_path or (root / 'watchdog-launch.log')
    env = dict(os.environ)
    env['FEIYU_INDEPENDENT_HOST'] = '1'
    with lp.open('ab') as log:
        line = f'[{stamp()}] restart {comp_name} force_kill={force_kill} killed={killed}\n'
        log.write(line.encode('utf-8'))
        log.flush()
        proc = subprocess.Popen(
            cmd,
            cwd=root,
            env=env,
            stdout=log,
            stderr=log,
            stdin=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
        )
    return proc, killed


def _record_component_archive(root, comp_name, kind, incident, health, extra=None):
    archive = Path(root) / 'watchdog-incidents'
    path = archive / f'{incident}-{comp_name}-{kind}.json'
    record = {
        'incident': incident,
        'component': comp_name,
        'kind': kind,
        'health': health,
        'at': stamp(),
        'test': (Path(root) / 'watchdog-test.flag').exists(),
    }
    if extra:
        record['detail'] = extra
    try:
        atomic(path, record)
    except OSError:
        pass


def _async_notify_recovery(root, comp_name, outage_seconds, via_t3_heal):
    def _worker():
        try:
            t3_self_heal.notify_recovery(root, comp_name, outage_seconds, via_t3_heal=via_t3_heal)
        except Exception:
            pass
    threading.Thread(target=_worker, name=f'notify-{comp_name}', daemon=True).start()


def _async_t3_heal(root, comp_name, reason, details, snapshot, on_done):
    def _worker():
        try:
            res = t3_self_heal.maybe_trigger(
                root,
                comp_name,
                reason,
                details=details,
                components_snapshot=snapshot,
            )
            on_done(res)
        except Exception as exc:
            on_done({'triggered': False, 'status': f'exception:{type(exc).__name__}'})
    threading.Thread(target=_worker, name=f't3heal-{comp_name}', daemon=True).start()


def _post_restart_verify(root, restarted_comp, comps, snapshot, now, *, log_path=None):
    """Cross-verify restarted component AND all sibling components right after a restart.

    If a restart (especially a force-kill) coincided with or caused any sibling component
    to disconnect, immediately clear that sibling's backoff (`retry_at = 0`) so it reconnects
    without waiting for a backoff window.
    """
    sibling_issues = []
    for name, info in snapshot.items():
        if name == restarted_comp:
            continue
        if name == 'private_bot' and not info.get('configured', True):
            continue
        h = info.get('health')
        if h not in ('healthy', 'paused'):
            sibling_issues.append({'component': name, 'health': h, 'pid': info.get('pid')})
            cs = comps.get(name)
            if isinstance(cs, dict) and cs.get('retry_at', 0) > now:
                cs['retry_at'] = 0  # Immediate reconnect pass on next tick

    target_info = snapshot.get(restarted_comp) or {}
    verified_ok = target_info.get('health') == 'healthy'
    result = {
        'at': stamp(),
        'restarted': restarted_comp,
        'target_health': target_info.get('health'),
        'target_pid': target_info.get('pid'),
        'verified_ok': verified_ok,
        'sibling_issues': sibling_issues,
    }
    try:
        lp = log_path or (Path(root) / 'watchdog-launch.log')
        with lp.open('ab') as log:
            line = (
                f'[{stamp()}] post_restart_verify target={restarted_comp} '
                f'ok={verified_ok} health={target_info.get("health")} '
                f'sibling_issues={json.dumps(sibling_issues, ensure_ascii=False)}\n'
            )
            log.write(line.encode('utf-8'))
    except OSError:
        pass
    return result


def _supervise_component(
    root,
    comp_name,
    comp_state,
    health,
    pid,
    now,
    restarts,
    all_snapshot,
    *,
    extra_detail=None,
    mutex_name=None,
    all_comps=None,
    on_post_verify=None,
):
    """Generic per-component supervisor: tracks outage, restarts/force-kills,
    runs post-restart cross-component verification, and escalates to T3."""
    comp_state.setdefault('attempts', 0)
    comp_state.setdefault('retry_at', 0)
    comp_state.setdefault('bad_since', None)
    comp_state.setdefault('t3_heal_triggered', False)
    comp_state['health'] = health
    comp_state['pid'] = pid
    if extra_detail:
        comp_state['detail'] = extra_detail
    else:
        comp_state.pop('detail', None)

    # Reap finished restart subprocess and immediately cross-verify all components
    proc = restarts.get(comp_name)
    if proc is not None and proc.poll() is not None:
        exit_code = proc.returncode
        restarts[comp_name] = None
        proc = None
        comp_state['last_restart_exit'] = exit_code
        verify_res = _post_restart_verify(
            root, comp_name, all_comps if isinstance(all_comps, dict) else {}, all_snapshot, now,
        )
        comp_state['last_post_restart_verify'] = verify_res
        if on_post_verify:
            on_post_verify(verify_res)

    if health == 'healthy':
        if comp_state.get('bad_since') is not None:
            outage = max(0.0, now - float(comp_state['bad_since']))
            if comp_state.get('incident'):
                _record_component_archive(
                    root, comp_name, 'recovered', comp_state['incident'], 'healthy',
                    {'outage_seconds': int(outage), 'attempts': comp_state.get('attempts', 0)},
                )
            if outage >= t3_self_heal.RECOVERY_NOTIFY_MIN_OUTAGE and not (Path(root) / 'watchdog-test.flag').exists():
                _async_notify_recovery(root, comp_name, outage, bool(comp_state.get('t3_heal_triggered')))
        comp_state['bad_since'] = None
        comp_state['incident'] = None
        comp_state['attempts'] = 0
        comp_state['retry_at'] = 0
        comp_state['t3_heal_triggered'] = False
        comp_state.pop('recovery_blocker', None)
        comp_state.pop('last_killed_pids', None)
        return

    if health == 'paused':
        comp_state['bad_since'] = None
        comp_state['attempts'] = 0
        comp_state['retry_at'] = 0
        comp_state.pop('recovery_blocker', None)
        return

    if comp_state.get('bad_since') is None:
        comp_state['bad_since'] = now
    bad_duration = max(0.0, now - float(comp_state['bad_since']))

    if not comp_state.get('incident') and bad_duration >= 20.0:
        comp_state['incident'] = uuid.uuid4().hex
        _record_component_archive(root, comp_name, 'down', comp_state['incident'], health, extra_detail)

    # Attempt local restart or force-kill + restart (never blind-restart `blocked`)
    if health in ('dead_or_stale', 'disconnected', 'stalled') and now >= comp_state.get('retry_at', 0) and proc is None:
        alive = pid_alive(pid)
        force_kill = False
        can_start = False

        if alive is False:
            # Dead process: check mutex if applicable
            if mutex_name:
                with SingleInstance(mutex_name, root) as guard:
                    free = guard.acquired
                if free:
                    can_start = True
                elif _should_force_kill(health, bad_duration):
                    # Worker wrapper may be hung holding the mutex; force kill & restart
                    force_kill = True
                    can_start = True
            else:
                can_start = True
        else:
            # Process appears alive or inaccessible: wait for force-kill threshold, then kill & restart
            if _should_force_kill(health, bad_duration):
                force_kill = True
                can_start = True
            else:
                comp_state['recovery_blocker'] = f'waiting_before_force_kill:{int(bad_duration)}s'

        if can_start:
            comp_state.pop('recovery_blocker', None)
            comp_state['attempts'] = int(comp_state.get('attempts', 0)) + 1
            backoff = min(300, 10 * (2 ** min(comp_state['attempts'] - 1, 5)))
            # Faster first retry for dead processes
            if comp_state['attempts'] == 1 and not force_kill:
                backoff = 8
            comp_state['retry_at'] = now + backoff
            comp_state['last_restart_at'] = stamp()
            try:
                started_proc, killed = _restart_component(root, comp_name, force_kill=force_kill)
                restarts[comp_name] = started_proc
                if killed:
                    comp_state['last_killed_pids'] = killed
            except OSError as exc:
                comp_state['recovery_blocker'] = f'spawn_failed:{type(exc).__name__}'

    # Escalate to T3 self-healing when local recovery isn't enough
    if _should_escalate_t3(health, int(comp_state.get('attempts', 0)), bad_duration):
        if not comp_state.get('_heal_inflight'):
            comp_state['_heal_inflight'] = True
            details = {
                'health': health,
                'pid': pid,
                'attempts': comp_state.get('attempts', 0),
                'bad_duration_seconds': int(bad_duration),
                'extra': extra_detail,
            }

            def _done(res):
                comp_state['_heal_inflight'] = False
                comp_state['last_t3_heal'] = {**res, 'at': stamp()}
                if res.get('triggered'):
                    comp_state['t3_heal_triggered'] = True

            _async_t3_heal(root, comp_name, health, details, dict(all_snapshot), _done)


def run(root=ROOT):
    root = Path(root).resolve()
    with SingleInstance('Watchdog', root) as lock:
        if not lock.acquired: return 0
        wake = threading.Event()
        watch = DirectorySignal(root, wake.set)
        path = root / 'watchdog-state.json'
        saved = read_json(path)
        saved.setdefault('attempts', 0)
        saved.setdefault('retry_at', 0)
        comps = saved.setdefault('components', {})
        if not isinstance(comps, dict):
            comps = {}
            saved['components'] = comps
        # Clear any stale in-flight markers left from a previous watchdog process
        for cstate in comps.values():
            if isinstance(cstate, dict):
                cstate.pop('_heal_inflight', None)

        next_heartbeat = time.monotonic()
        next_stall_check = 0.0
        stall_info = {'stalled': False, 'stalled_queue': [], 'stalled_receipts': 0}
        restarts = {'feiyu_receiver': None, 'private_bot': None, 'bridge_daemon': None}
        saved.pop('stopped_at', None)
        try:
            while not (root / 'watchdog.stop').exists():
                # 1. Sample all component states
                s, now = sample_state(root)
                feiyu_health = classify(s, now, (root / 'stop.request').exists())

                # Preserve legacy top-level incident/archive behavior for Feiyu receiver
                notices = transition(saved, feiyu_health, now)
                if notices:
                    saved.setdefault('pending_notices', []).extend(
                        [[k, i, feiyu_health, (root / 'watchdog-test.flag').exists(), stamp()] for k, i in notices]
                    )
                    atomic(path, saved)
                if saved.get('pending_notices'):
                    publish_notices(root, saved)
                    atomic(path, saved)

                # Sample private_bot (Claude)
                pb_configured = (root / 'private-bot-credentials.dpapi').is_file()
                pb_state = read_json(root / 'private-bot-status.json') if pb_configured else {}
                pb_health = (
                    classify(pb_state, now, (root / 'private-bot.stop').exists())
                    if pb_configured else 'paused'
                )

                # Sample bridge_daemon + end-to-end stall (every 5s)
                if time.monotonic() >= next_stall_check:
                    next_stall_check = time.monotonic() + 5.0
                    if not (root / 'bridge-daemon.stop').exists():
                        stall_info = detect_end_to_end_stall(root, now)
                    else:
                        stall_info = {'stalled': False, 'stalled_queue': [], 'stalled_receipts': 0}

                daemon_state = read_json(root / 'bridge-daemon-status.json')
                dispatch_state = read_json(root / 'local-dispatch-status.json')
                daemon_health = classify_daemon(
                    daemon_state,
                    dispatch_state,
                    now,
                    stop=(root / 'bridge-daemon.stop').exists(),
                    stall_info=stall_info,
                )

                snapshot = {
                    'feiyu_receiver': {'health': feiyu_health, 'pid': s.get('pid'), 'state': s.get('state')},
                    'private_bot': {'health': pb_health, 'pid': pb_state.get('pid'), 'state': pb_state.get('state'), 'configured': pb_configured},
                    'bridge_daemon': {'health': daemon_health, 'pid': daemon_state.get('pid'), 'state': daemon_state.get('state'), 'stall': stall_info},
                }

                # 2. Supervise each component
                def _record_verify(vres):
                    saved['last_post_restart_check'] = vres

                feiyu_cs = comps.setdefault('feiyu_receiver', {})
                _supervise_component(
                    root, 'feiyu_receiver', feiyu_cs, feiyu_health, s.get('pid'), now, restarts, snapshot,
                    mutex_name='Receiverworker', all_comps=comps, on_post_verify=_record_verify,
                )
                # Mirror legacy top-level fields for panel compatibility
                saved['attempts'] = feiyu_cs.get('attempts', 0)
                saved['retry_at'] = feiyu_cs.get('retry_at', 0)
                if feiyu_cs.get('recovery_blocker'):
                    saved['recovery_blocker'] = feiyu_cs['recovery_blocker']
                else:
                    saved.pop('recovery_blocker', None)

                if pb_configured:
                    pb_cs = comps.setdefault('private_bot', {})
                    _supervise_component(
                        root, 'private_bot', pb_cs, pb_health, pb_state.get('pid'), now, restarts, snapshot,
                        mutex_name='PrivateBotworker', all_comps=comps, on_post_verify=_record_verify,
                    )

                daemon_cs = comps.setdefault('bridge_daemon', {})
                _supervise_component(
                    root, 'bridge_daemon', daemon_cs, daemon_health, daemon_state.get('pid'), now, restarts, snapshot,
                    extra_detail=stall_info if stall_info.get('stalled') else None,
                    mutex_name='BridgeDaemon', all_comps=comps, on_post_verify=_record_verify,
                )

                # 3. Persist heartbeat
                if time.monotonic() >= next_heartbeat:
                    overall = 'healthy'
                    active_healths = [feiyu_health, daemon_health] + ([pb_health] if pb_configured else [])
                    if any(h not in ('healthy', 'paused') for h in active_healths):
                        overall = next(h for h in active_healths if h not in ('healthy', 'paused'))
                    clean_comps = {
                        k: {ck: cv for ck, cv in v.items() if not ck.startswith('_')}
                        for k, v in comps.items() if isinstance(v, dict)
                    }
                    saved.update(
                        pid=os.getpid(),
                        updated_at=stamp(),
                        health=feiyu_health,
                        overall_health=overall,
                        components=clean_comps,
                    )
                    heartbeat(path, saved)
                    next_heartbeat += 1
                    if next_heartbeat <= time.monotonic():
                        next_heartbeat = time.monotonic() + 1

                wake.wait(max(0, next_heartbeat - time.monotonic()))
                wake.clear()
                # Coalesce repeated file-change notifications without delaying the next pulse.
                time.sleep(min(.05, max(0, next_heartbeat - time.monotonic())))
        finally:
            watch.close()
            saved.update(stopped_at=stamp())
            heartbeat(path, saved)
    return 0


def start(root=ROOT):
    root = Path(root).resolve()
    with SingleInstance('Watchdog', root) as lock:
        if not lock.acquired:
            return {'running': True, 'already_running': True, **read_json(root / 'watchdog-state.json')}
    (root / 'watchdog.stop').unlink(missing_ok=True)
    cmd = [sys.executable, str(Path(__file__).resolve()), 'run']
    with (root / 'watchdog.log').open('ab') as log:
        mode, via = spawn_detached_or_delegate(
            cmd, cwd=root, root=root, service_name='watchdog', stdout=log, stderr=log,
        )
    if mode == 'blocked':
        return {'running': False, 'error': via}
    for _ in range(25):
        time.sleep(0.25)
        with SingleInstance('Watchdog', root) as probe:
            running = not probe.acquired
        if running:
            st = read_json(root / 'watchdog-state.json')
            out = {'running': True, **st}
            if mode == 'delegated':
                out['delegated_via'] = via
            return out
    st = read_json(root / 'watchdog-state.json')
    out = {'running': False, **st}
    if mode == 'delegated':
        out['delegated_via'] = via
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['run', 'start', 'status', 'stop'])
    a = p.parse_args()
    if a.action == 'run': return run()
    if a.action == 'status': print(json.dumps(read_json(ROOT / 'watchdog-state.json'), ensure_ascii=False)); return 0
    if a.action == 'stop': (ROOT / 'watchdog.stop').touch(); return 0
    res = start(ROOT)
    return 0 if res.get('running') else 1


if __name__ == '__main__':
    raise SystemExit(main())
