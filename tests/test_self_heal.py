"""Unit tests for Feiyu Bridge high-availability watchdog, stall detection,
job-escaped delegation, and T3 self-healing escalation."""
from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_BRIDGE_DIR = str(_Path(__file__).resolve().parent.parent / 'bridge')
if _BRIDGE_DIR not in _sys.path:
    _sys.path.insert(0, _BRIDGE_DIR)

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

import t3_self_heal
import watchdog
import win_runtime


def _iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


class SelfHealAndWatchdogTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.root = Path(self.td.name)
        (self.root / 'queue').mkdir()
        (self.root / 't3-bridge-config.json').write_text(json.dumps({
            't3_project_id': 'proj-feiyu-test',
            'escalation_model': {'provider': 'antigravity', 'model': 'gemini-4-neon', 'effort': 'xhigh'},
        }), encoding='utf-8')

    def tearDown(self):
        self.td.cleanup()

    def _init_business_db(self, intake_names=(), receipts=()):
        conn = sqlite3.connect(self.root / 'business.sqlite3')
        conn.execute('CREATE TABLE intake_files(name TEXT PRIMARY KEY, signature TEXT NOT NULL)')
        conn.execute('CREATE TABLE chat_receipts(case_id TEXT, phase TEXT, state TEXT, due REAL, attempts INTEGER DEFAULT 0)')
        for name in intake_names:
            conn.execute('INSERT INTO intake_files(name, signature) VALUES(?, ?)', (name, 'sig'))
        for r in receipts:
            conn.execute('INSERT INTO chat_receipts(case_id, phase, state, due, attempts) VALUES(?, ?, ?, ?, ?)', r)
        conn.commit()
        conn.close()

    def test_01_classify_daemon_states(self):
        now = 1795260000.0
        ok_d = {'state': 'running', 'updated_at': _iso(now - 3)}
        ok_disp = {'state': 'running', 'updated_at': _iso(now - 4)}
        self.assertEqual(watchdog.classify_daemon(ok_d, ok_disp, now), 'healthy')
        self.assertEqual(watchdog.classify_daemon(ok_d, ok_disp, now, stop=True), 'paused')
        self.assertEqual(watchdog.classify_daemon({'state': 'running', 'updated_at': _iso(now - 30)}, ok_disp, now), 'dead_or_stale')
        self.assertEqual(watchdog.classify_daemon(ok_d, {'state': 'running', 'updated_at': _iso(now - 40)}, now), 'dead_or_stale')
        self.assertEqual(watchdog.classify_daemon(ok_d, ok_disp, now, stall_info={'stalled': True}), 'stalled')

    def test_02_end_to_end_stall_detection(self):
        now = time.time()
        self._init_business_db(intake_names=['consumed.json'])

        # 1. Consumed file is never stalled
        p_ok = self.root / 'queue' / 'consumed.json'
        p_ok.write_text('{}', encoding='utf-8')
        os.utime(p_ok, (now - 120, now - 120))
        res = watchdog.detect_end_to_end_stall(self.root, now)
        self.assertFalse(res['stalled'])

        # 2. Brand-new file (< 60s) is not stalled yet
        p_new = self.root / 'queue' / 'fresh.json'
        p_new.write_text('{}', encoding='utf-8')
        os.utime(p_new, (now - 15, now - 15))
        res = watchdog.detect_end_to_end_stall(self.root, now)
        self.assertFalse(res['stalled'])

        # 3. Ancient file (> 15min) is ignored (handled by pending_expiry)
        p_old = self.root / 'queue' / 'ancient.json'
        p_old.write_text('{}', encoding='utf-8')
        os.utime(p_old, (now - 1200, now - 1200))
        res = watchdog.detect_end_to_end_stall(self.root, now)
        self.assertFalse(res['stalled'])

        # 4. Unconsumed file aged 90s triggers stall
        p_stuck = self.root / 'queue' / 'stuck.json'
        p_stuck.write_text('{}', encoding='utf-8')
        os.utime(p_stuck, (now - 90, now - 90))
        res = watchdog.detect_end_to_end_stall(self.root, now)
        self.assertTrue(res['stalled'])
        self.assertEqual(res['reason'], 'queue_unconsumed:1')

        # 5. Receipt stuck in 'ready' for > 60s triggers stall even when queue is clean
        p_stuck.unlink()
        self._init_business_db_add_receipt('w_case1', 'first', 'ready', now - 90, 0)
        res = watchdog.detect_end_to_end_stall(self.root, now)
        self.assertTrue(res['stalled'])
        self.assertEqual(res['reason'], 'receipt_stuck:1')

    def _init_business_db_add_receipt(self, cid, phase, state, due, attempts):
        conn = sqlite3.connect(self.root / 'business.sqlite3')
        conn.execute('INSERT INTO chat_receipts(case_id, phase, state, due, attempts) VALUES(?, ?, ?, ?, ?)',
                     (cid, phase, state, due, attempts))
        conn.commit()
        conn.close()

    def test_03_supervise_force_kills_hung_process_and_escalates_to_t3(self):
        comp_state = {}
        restarts = {'bridge_daemon': None}
        snap = {'bridge_daemon': {'health': 'dead_or_stale'}}
        t0 = 1795260000.0
        fake_proc = MagicMock()
        fake_proc.poll.return_value = 0

        restart_calls = []
        heal_calls = []

        def fake_restart(root, comp, *, force_kill=False, log_path=None):
            restart_calls.append((comp, force_kill))
            return fake_proc, ([9999] if force_kill else [])

        def fake_heal(root, comp, reason, details, snapshot, on_done):
            heal_calls.append((comp, reason, details))
            on_done({'triggered': True, 'thread_id': 'heal-thread-1'})

        with patch.object(watchdog, 'pid_alive', return_value=True), \
             patch.object(watchdog, '_restart_component', side_effect=fake_restart), \
             patch.object(watchdog, '_async_t3_heal', side_effect=fake_heal):
            # At t0+10s: live PID stale, waits before force kill
            watchdog._supervise_component(self.root, 'bridge_daemon', comp_state, 'dead_or_stale', 9999, t0, restarts, snap)
            watchdog._supervise_component(self.root, 'bridge_daemon', comp_state, 'dead_or_stale', 9999, t0 + 10, restarts, snap)
            self.assertEqual(restart_calls, [])
            self.assertIn('waiting_before_force_kill', comp_state.get('recovery_blocker', ''))

            # At t0+50s (> 45s): force kills and restarts attempt #1
            watchdog._supervise_component(self.root, 'bridge_daemon', comp_state, 'dead_or_stale', 9999, t0 + 50, restarts, snap)
            self.assertEqual(restart_calls, [('bridge_daemon', True)])
            self.assertEqual(comp_state['attempts'], 1)
            self.assertEqual(heal_calls, [])

            # At t0+80s (after retry_at): force kills and restarts attempt #2 -> escalates to T3
            watchdog._supervise_component(self.root, 'bridge_daemon', comp_state, 'dead_or_stale', 9999, t0 + 80, restarts, snap)
            self.assertEqual(len(restart_calls), 2)
            self.assertEqual(comp_state['attempts'], 2)
            self.assertEqual(len(heal_calls), 1)
            self.assertEqual(heal_calls[0][0], 'bridge_daemon')
            self.assertTrue(comp_state['t3_heal_triggered'])

    def test_04_t3_self_heal_guardrails(self):
        now = 1795260000.0
        launched = []

        def fake_start(root, project_id, provider, model, title, prompt, *, wait=False, timeout=60):
            launched.append({'project_id': project_id, 'provider': provider, 'model': model, 'title': title, 'wait': wait})
            return {'exit_code': 0, 'thread_id': f'tid-{len(launched)}'}

        ok_server = lambda root: {'ok': True, 'reason': 'ready'}
        down_server = lambda root: {'ok': False, 'reason': 'server_pid_dead'}

        # 1. Server down -> skipped cleanly
        r0 = t3_self_heal.maybe_trigger(self.root, 'bridge_daemon', 'dead_or_stale', now=now, start_thread_fn=fake_start, server_status_fn=down_server)
        self.assertFalse(r0['triggered'])
        self.assertEqual(r0['status'], 't3_server_unavailable')

        # 2. First trigger succeeds non-blocking (wait=False)
        r1 = t3_self_heal.maybe_trigger(self.root, 'bridge_daemon', 'dead_or_stale', now=now, start_thread_fn=fake_start, server_status_fn=ok_server)
        self.assertTrue(r1['triggered'])
        self.assertEqual(r1['thread_id'], 'tid-1')
        self.assertFalse(launched[0]['wait'])

        # 3. Within global cooldown (600s) -> blocked even for another component
        r2 = t3_self_heal.maybe_trigger(self.root, 'private_bot', 'disconnected', now=now + 120, start_thread_fn=fake_start, server_status_fn=ok_server)
        self.assertFalse(r2['triggered'])
        self.assertEqual(r2['status'], 'global_cooldown')

        # 4. After global cooldown (700s), same signature still in 1800s signature cooldown
        r3 = t3_self_heal.maybe_trigger(self.root, 'bridge_daemon', 'dead_or_stale', now=now + 700, start_thread_fn=fake_start, server_status_fn=ok_server)
        self.assertFalse(r3['triggered'])
        self.assertEqual(r3['status'], 'signature_cooldown')

        # 5. Different signature after global cooldown, but previous heal thread still running -> blocked
        r4 = t3_self_heal.maybe_trigger(
            self.root, 'private_bot', 'disconnected', now=now + 700,
            start_thread_fn=fake_start, server_status_fn=ok_server,
            is_thread_running_fn=lambda root, tid: True,
        )
        self.assertFalse(r4['triggered'])
        self.assertEqual(r4['status'], 'active_heal_thread_running')

        # 6. Once previous heal thread completed -> different signature allowed
        r5 = t3_self_heal.maybe_trigger(
            self.root, 'private_bot', 'disconnected', now=now + 700,
            start_thread_fn=fake_start, server_status_fn=ok_server,
            is_thread_running_fn=lambda root, tid: False,
        )
        self.assertTrue(r5['triggered'])
        self.assertEqual(r5['thread_id'], 'tid-2')

    def test_05_restricted_job_delegates_instead_of_spawning_doomed_child(self):
        with patch.dict(os.environ, {'FEIYU_INDEPENDENT_HOST': ''}, clear=False), \
             patch.object(win_runtime.subprocess, 'Popen', side_effect=PermissionError('Access denied')), \
             patch.object(win_runtime, 'in_kill_on_close_job', return_value=True), \
             patch.object(win_runtime, 'delegate_host_start', return_value='tray_event') as mock_del:
            mode, via = win_runtime.spawn_detached_or_delegate(
                ['python.exe', 'bridge_daemon.py', 'run'],
                cwd=self.root,
                root=self.root,
                service_name='bridge_daemon',
            )
            self.assertEqual(mode, 'delegated')
            self.assertEqual(via, 'tray_event')
            mock_del.assert_called_once()

        with patch.dict(os.environ, {'FEIYU_INDEPENDENT_HOST': ''}, clear=False), \
             patch.object(win_runtime.subprocess, 'Popen', side_effect=PermissionError('Access denied')), \
             patch.object(win_runtime, 'in_kill_on_close_job', return_value=True), \
             patch.object(win_runtime, 'delegate_host_start', return_value=None):
            mode, via = win_runtime.spawn_detached_or_delegate(
                ['python.exe', 'bridge_daemon.py', 'run'],
                cwd=self.root,
                root=self.root,
                service_name='bridge_daemon',
            )
            self.assertEqual(mode, 'blocked')
            self.assertEqual(via, 'restricted_job_no_host')

        fake_proc = MagicMock()
        with patch.dict(os.environ, {'FEIYU_INDEPENDENT_HOST': '1'}, clear=False), \
             patch.object(win_runtime.subprocess, 'Popen', side_effect=[PermissionError('Access denied'), fake_proc]) as mock_popen:
            mode, proc = win_runtime.spawn_detached_or_delegate(
                ['python.exe', 'bridge_daemon.py', 'run'],
                cwd=self.root,
                root=self.root,
                service_name='bridge_daemon',
            )
            self.assertEqual(mode, 'spawned')
            self.assertIs(proc, fake_proc)
            self.assertEqual(mock_popen.call_count, 2)

    def test_06_force_kill_never_collateral_kills_sibling_or_active_worker_by_ppid(self):
        fake_procs = [
            {'pid': 1001, 'ppid': 500, 'name': 'python.exe', 'cmdline': f'python.exe "{self.root}\\bridge_daemon.py" run'},
            {'pid': 1002, 'ppid': 1001, 'name': 'python.exe', 'cmdline': f'python.exe "{self.root}\\t3_dispatch.py" job --msgid w_1'},
            {'pid': 1003, 'ppid': 1001, 'name': 'python.exe', 'cmdline': f'python.exe "{self.root}\\private_bot_control.py" worker'},
            {'pid': 1004, 'ppid': 1001, 'name': 'python.exe', 'cmdline': f'python.exe "{self.root}\\watchdog.py" run'},
        ]
        terminated = []

        def fake_kill(pid, exit_code=1):
            terminated.append(pid)
            return True

        with patch.object(win_runtime, 'list_bridge_processes', return_value=fake_procs), \
             patch.object(win_runtime, 'pid_alive', side_effect=lambda p: p not in terminated), \
             patch.object(win_runtime, '_terminate_pid_nt', side_effect=fake_kill):
            killed = win_runtime.terminate_matching_processes(self.root, ['bridge_daemon.py'])
            self.assertEqual(killed, [1001])
            self.assertEqual(terminated, [1001])

    def test_07_post_restart_verify_clears_sibling_backoff_for_immediate_reconnect(self):
        now = 1795260500.0
        comps = {
            'bridge_daemon': {'attempts': 1, 'retry_at': now + 30},
            'private_bot': {'attempts': 2, 'retry_at': now + 120},
            'feiyu_receiver': {'attempts': 0, 'retry_at': 0},
        }
        snapshot = {
            'bridge_daemon': {'health': 'healthy', 'pid': 2001},
            'private_bot': {'health': 'disconnected', 'pid': 2002, 'configured': True},
            'feiyu_receiver': {'health': 'healthy', 'pid': 2003},
        }
        res = watchdog._post_restart_verify(self.root, 'bridge_daemon', comps, snapshot, now)
        self.assertTrue(res['verified_ok'])
        self.assertEqual(len(res['sibling_issues']), 1)
        self.assertEqual(res['sibling_issues'][0]['component'], 'private_bot')
        # Sibling backoff cleared immediately so private_bot reconnects on the very next tick
        self.assertEqual(comps['private_bot']['retry_at'], 0)

    def test_08_daemon_startup_verify_reconnects_disconnected_siblings(self):
        import bridge_daemon
        reconnected_cmds = []

        def fake_run(cmd, **kwargs):
            reconnected_cmds.append(cmd)
            return MagicMock(returncode=0)

        with patch.object(bridge_daemon.t3_dispatch, 'reconcile') as mock_rec, \
             patch('bridge_control.live', side_effect=[False, True]), \
             patch('private_bot_control.is_configured', return_value=True), \
             patch('private_bot_control.status', side_effect=[{'authenticated': False}, {'authenticated': True}]), \
             patch.object(bridge_daemon, '_read_json', return_value={'state': 'authenticated'}), \
             patch.object(bridge_daemon, 'SingleInstance') as mock_si, \
             patch.object(bridge_daemon.subprocess, 'run', side_effect=fake_run):
            mock_si.return_value.__enter__.return_value.acquired = False  # watchdog already running
            rep = bridge_daemon._startup_verify_and_reconnect(self.root)
            mock_rec.assert_called_once()
            self.assertTrue(rep['t3_reconciled'])
            self.assertEqual(rep['checks']['feiyu_receiver'], 'reconnected')
            self.assertEqual(rep['checks']['private_bot'], 'reconnected')
            self.assertEqual(rep['checks']['watchdog'], 'ok')
            self.assertEqual(sorted(rep['reconnected']), ['feiyu_receiver', 'private_bot'])
            self.assertEqual(len(reconnected_cmds), 2)

    def test_09_read_json_retry_survives_windows_replace_lock(self):
        p = self.root / 'status.json'
        p.write_text(json.dumps({'state': 'running', 'ok': True}), encoding='utf-8')
        real_read = Path.read_text
        calls = {'n': 0}

        def flaky_read(self_path, *args, **kwargs):
            if self_path == p:
                calls['n'] += 1
                if calls['n'] <= 2:
                    raise PermissionError(13, 'Access is denied')
            return real_read(self_path, *args, **kwargs)

        with patch.object(Path, 'read_text', flaky_read), patch.object(win_runtime.time, 'sleep') as mock_sleep:
            data = win_runtime.read_json_retry(p)
            self.assertEqual(data, {'state': 'running', 'ok': True})
            self.assertEqual(calls['n'], 3)
            self.assertEqual(mock_sleep.call_count, 2)

        # Genuinely missing file must return immediately with zero sleeps
        with patch.object(win_runtime.time, 'sleep') as mock_sleep:
            missing = win_runtime.read_json_retry(self.root / 'does-not-exist.json')
            self.assertEqual(missing, {})
            mock_sleep.assert_not_called()

    def test_10_health_guard_ignores_transient_post_check_blip_without_false_escalation(self):
        import health_guard
        # Step 4 passes (True), step 6 first probe fails transiently (False), second probe succeeds (True)
        daemon_seq = iter([True, False, True])
        with patch.object(health_guard, '_ensure_tray', return_value={'service': 'WecomBridge.exe', 'action': 'healthy'}), \
             patch.object(health_guard, '_is_receiver_healthy', return_value=True), \
             patch.object(health_guard, '_is_watchdog_healthy', return_value=True), \
             patch.object(health_guard, '_is_daemon_healthy', side_effect=lambda root: next(daemon_seq)), \
             patch.object(health_guard, '_run_start') as mock_start, \
             patch.object(health_guard.t3_self_heal, 'maybe_trigger') as mock_heal, \
             patch.object(health_guard.time, 'sleep'):
            rep = health_guard.run_once(self.root)
            self.assertEqual(rep['still_bad'], [])
            self.assertIsNone(rep['self_heal'])
            mock_start.assert_not_called()
            mock_heal.assert_not_called()

    def test_11_health_guard_attempts_recovery_before_escalating_post_check_drop(self):
        import health_guard
        # Healthy on initial check (True), then drops persistently (False x 5) so post-check must recover before escalating
        daemon_seq = iter([True, False, False, False, False, False])
        with patch.object(health_guard, '_ensure_tray', return_value={'service': 'WecomBridge.exe', 'action': 'healthy'}), \
             patch.object(health_guard, '_is_receiver_healthy', return_value=True), \
             patch.object(health_guard, '_is_watchdog_healthy', return_value=True), \
             patch.object(health_guard, '_is_daemon_healthy', side_effect=lambda root: next(daemon_seq, False)), \
             patch.object(health_guard, '_run_start', return_value={'script': 'bridge_daemon.py', 'exit': 1}) as mock_start, \
             patch.object(health_guard.t3_self_heal, 'maybe_trigger', return_value={'triggered': True, 'thread_id': 't-1'}) as mock_heal, \
             patch.object(health_guard.time, 'sleep'):
            rep = health_guard.run_once(self.root)
            self.assertEqual(rep['still_bad'], ['bridge_daemon'])
            mock_start.assert_called_once()
            mock_heal.assert_called_once()
            self.assertIn('diagnostics', rep)
            self.assertIn('bridge_daemon', rep['diagnostics'])


if __name__ == '__main__':
    unittest.main()
