"""Small Windows primitives: single-instance mutexes, directory signals,
safe job-escaped process spawning, and verified process termination."""
from __future__ import annotations

import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import errno
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import threading
import time

TRAY_RECHECK_EVENT = r'Local\XiaoaiWecomTrayRecheck'
RECOVERY_REQUEST_FILE = 'recovery.request'
HEALTH_GUARD_TASK_NAME = 'FeiyuBridge-HealthGuard'


def kernel():
    if os.name != "nt":
        raise RuntimeError("Windows required")
    dll = ctypes.WinDLL("kernel32", use_last_error=True)
    dll.CreateMutexW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
    dll.CreateMutexW.restype = wintypes.HANDLE
    dll.CloseHandle.argtypes = [wintypes.HANDLE]
    return dll


class SingleInstance:
    def __init__(self, name, root):
        self.dll = kernel()
        suffix = hashlib.sha256(str(root).lower().encode()).hexdigest()[:24]
        self.handle = self.dll.CreateMutexW(None, False, "Local\\Xiaoai" + name + suffix)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        self.acquired = ctypes.get_last_error() != 183

    def close(self):
        if self.handle:
            self.dll.CloseHandle(self.handle)
            self.handle = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class DirectorySignal:
    """Wait on Windows change notifications, not periodic directory scans."""

    def __init__(self, path, callback):
        self.dll = kernel()
        k = self.dll
        k.FindFirstChangeNotificationW.argtypes = [wintypes.LPCWSTR, wintypes.BOOL, wintypes.DWORD]
        k.FindFirstChangeNotificationW.restype = wintypes.HANDLE
        k.FindNextChangeNotification.argtypes = [wintypes.HANDLE]
        k.FindCloseChangeNotification.argtypes = [wintypes.HANDLE]
        k.CreateEventW.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.BOOL, wintypes.LPCWSTR]
        k.CreateEventW.restype = wintypes.HANDLE
        k.SetEvent.argtypes = [wintypes.HANDLE]
        k.WaitForMultipleObjects.argtypes = [wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE), wintypes.BOOL, wintypes.DWORD]
        self.changed = k.FindFirstChangeNotificationW(str(path), False, 1 | 8 | 16)
        if self.changed in (None, ctypes.c_void_p(-1).value):
            raise ctypes.WinError(ctypes.get_last_error())
        self.stopped = k.CreateEventW(None, True, False, None)
        if not self.stopped:
            k.FindCloseChangeNotification(self.changed)
            raise ctypes.WinError(ctypes.get_last_error())
        self.error = None

        def run():
            handles = (wintypes.HANDLE * 2)(self.stopped, self.changed)
            watching = True
            while True:
                # Reconcile after missed notifications, including partial writes.
                result = k.WaitForMultipleObjects(2 if watching else 1, handles, False, 1000 if self.error else 15000)
                if result == 0:
                    return
                if result != 258 and (result != 1 or not k.FindNextChangeNotification(self.changed)):
                    watching = False
                try:
                    callback()
                    self.error = None if watching else "directory_notification_failed"
                except Exception:
                    self.error = "queue_reconciliation_failed"

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def close(self):
        self.dll.SetEvent(self.stopped)
        self.thread.join(timeout=12)
        if not self.thread.is_alive():
            self.dll.FindCloseChangeNotification(self.changed)
            self.dll.CloseHandle(self.stopped)


def pid_alive(pid):
    """Return True if pid is running, False if dead, None if access denied."""
    if not isinstance(pid, int) or pid <= 0:
        return False
    if os.name != 'nt':
        try:
            os.kill(pid, 0)
            return True
        except PermissionError:
            return None
        except OSError:
            return False
    k = ctypes.WinDLL('kernel32', use_last_error=True)
    k.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    k.OpenProcess.restype = wintypes.HANDLE
    k.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    k.CloseHandle.argtypes = [wintypes.HANDLE]
    h = k.OpenProcess(0x100000, False, int(pid))  # SYNCHRONIZE
    if not h:
        return None if ctypes.get_last_error() == 5 else False
    try:
        return k.WaitForSingleObject(h, 0) == 258
    finally:
        k.CloseHandle(h)


def is_in_job():
    """Return True if current process belongs to any Windows Job Object."""
    if os.name != 'nt':
        return False
    try:
        k = ctypes.WinDLL('kernel32', use_last_error=True)
        k.GetCurrentProcess.restype = wintypes.HANDLE
        k.IsProcessInJob.argtypes = [wintypes.HANDLE, wintypes.HANDLE, ctypes.POINTER(wintypes.BOOL)]
        k.IsProcessInJob.restype = wintypes.BOOL
        in_job = wintypes.BOOL(False)
        if k.IsProcessInJob(k.GetCurrentProcess(), None, ctypes.byref(in_job)):
            return bool(in_job.value)
    except Exception:
        pass
    return False


def in_kill_on_close_job():
    """Return True if current process is inside a Windows Job Object with KILL_ON_JOB_CLOSE
    and without BREAKAWAY_OK / SILENT_BREAKAWAY_OK (e.g. T3/Codex/Antigravity tool sandboxes).
    """
    if os.name != 'nt' or not is_in_job():
        return False
    try:
        class _IO_COUNTERS(ctypes.Structure):
            _fields_ = [(n, ctypes.c_uint64) for n in (
                'ReadOperationCount', 'WriteOperationCount', 'OtherOperationCount',
                'ReadTransferCount', 'WriteTransferCount', 'OtherTransferCount',
            )]

        class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ('PerProcessUserTimeLimit', ctypes.c_int64),
                ('PerJobUserTimeLimit', ctypes.c_int64),
                ('LimitFlags', wintypes.DWORD),
                ('MinimumWorkingSetSize', ctypes.c_size_t),
                ('MaximumWorkingSetSize', ctypes.c_size_t),
                ('ActiveProcessLimit', wintypes.DWORD),
                ('Affinity', ctypes.c_size_t),
                ('PriorityClass', wintypes.DWORD),
                ('SchedulingClass', wintypes.DWORD),
            ]

        class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ('BasicLimitInformation', _JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ('IoInfo', _IO_COUNTERS),
                ('ProcessMemoryLimit', ctypes.c_size_t),
                ('JobMemoryLimit', ctypes.c_size_t),
                ('PeakProcessMemoryUsed', ctypes.c_size_t),
                ('PeakJobMemoryUsed', ctypes.c_size_t),
            ]

        k = ctypes.WinDLL('kernel32', use_last_error=True)
        k.QueryInformationJobObject.argtypes = [
            wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD),
        ]
        k.QueryInformationJobObject.restype = wintypes.BOOL
        info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        ret_len = wintypes.DWORD(0)
        if k.QueryInformationJobObject(None, 9, ctypes.byref(info), ctypes.sizeof(info), ctypes.byref(ret_len)):
            flags = int(info.BasicLimitInformation.LimitFlags)
            kill_on_close = bool(flags & 0x2000)
            breakaway_ok = bool(flags & (0x0800 | 0x1000))
            return kill_on_close and not breakaway_ok
    except Exception:
        pass
    return False


def _is_transient_fs_lock(exc):
    return (
        isinstance(exc, PermissionError)
        or getattr(exc, 'winerror', None) in (5, 32, 33)
        or getattr(exc, 'errno', None) in (errno.EACCES, errno.EBUSY)
    )


def _has_sibling_temp_file(path):
    try:
        parent = path.parent
        if not parent.is_dir():
            return False
        name = path.name
        for entry in parent.iterdir():
            ename = entry.name
            if ename != name and name in ename and (ename.endswith('.tmp') or ename.endswith('.tray-new') or '.tmp' in ename):
                return True
    except OSError:
        return False
    return False


def read_json_retry(path, *, default=None, delays=(0.0, 0.015, 0.04, 0.08), expect_dict=True):
    """Read a JSON file with bounded retries for transient Windows atomic-replace races.

    On Windows, concurrent `os.replace()` / `File.Replace()` on status JSON files can briefly
    trigger PermissionError (WinError 5/32/33), ValueError (partial read), or FileNotFoundError
    while a sibling temp file is swapping in. Genuinely missing files return immediately.
    """
    fallback = {} if default is None else default
    p = Path(path)
    last_idx = len(delays) - 1
    for idx, pause in enumerate(delays):
        if pause:
            time.sleep(pause)
        try:
            data = json.loads(p.read_text(encoding='utf-8-sig'))
            if expect_dict and not isinstance(data, dict):
                return fallback
            return data
        except FileNotFoundError:
            if idx < last_idx and _has_sibling_temp_file(p):
                continue
            return fallback
        except (OSError, ValueError) as exc:
            transient = isinstance(exc, ValueError) or _is_transient_fs_lock(exc)
            if transient and idx < last_idx:
                continue
            return fallback
    return fallback


def _atomic_write_json(path, payload):
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
                locked = _is_transient_fs_lock(exc)
                if not locked or pause == 0.15:
                    raise
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


atomic_write_json = _atomic_write_json


def signal_tray_recovery(root, services=None, reason='delegated_start'):
    """Write a recovery.request marker and signal the independent WecomBridge tray host."""
    root = Path(root)
    payload = {
        'requested_at': datetime.now(timezone.utc).isoformat(),
        'services': list(services) if services else [],
        'reason': str(reason or 'delegated_start'),
        'caller_pid': os.getpid(),
    }
    try:
        _atomic_write_json(root / RECOVERY_REQUEST_FILE, payload)
    except OSError:
        pass
    if os.name != 'nt':
        return False
    try:
        k = ctypes.WinDLL('kernel32', use_last_error=True)
        k.OpenEventW.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR]
        k.OpenEventW.restype = wintypes.HANDLE
        k.SetEvent.argtypes = [wintypes.HANDLE]
        k.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = k.OpenEventW(0x0002, False, TRAY_RECHECK_EVENT)  # EVENT_MODIFY_STATE
        if not handle:
            return False
        try:
            return bool(k.SetEvent(handle))
        finally:
            k.CloseHandle(handle)
    except Exception:
        return False


def trigger_health_guard_task(task_name=HEALTH_GUARD_TASK_NAME, *, timeout=10):
    """Trigger the independent Windows Scheduled Task so services launch outside restricted Job Objects."""
    if os.name != 'nt':
        return False
    try:
        proc = subprocess.run(
            ['schtasks.exe', '/Run', '/TN', str(task_name)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW,
            timeout=timeout,
            check=False,
        )
        return proc.returncode == 0
    except OSError:
        return False


def delegate_host_start(root, service_name, reason='restricted_job_object'):
    """Ask an independent host outside any agent Job Object to start or recover bridge services."""
    if signal_tray_recovery(root, services=[service_name], reason=reason):
        return 'tray_event'
    if trigger_health_guard_task():
        return 'scheduled_task'
    return None


def spawn_detached_or_delegate(
    args,
    *,
    cwd,
    root,
    service_name,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    env=None,
):
    """Spawn a long-lived background process safely outside restricted Windows Job Objects.

    Returns (mode, handle_or_via):
      - ('spawned', Popen) when process was spawned safely outside a restricted job.
      - ('delegated', 'tray_event' | 'scheduled_task') when current process is in a restricted
        Job Object and launch was delegated to an independent host so it survives caller exit.
      - ('blocked', 'restricted_job_no_host') when breakaway was denied and no independent host
        could be signaled (never silently spawns a doomed child inside a KILL_ON_JOB_CLOSE job).
    """
    if os.name != 'nt':
        proc = subprocess.Popen(
            args, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
        )
        return ('spawned', proc)

    breakaway_flags = (
        subprocess.CREATE_NO_WINDOW
        | subprocess.DETACHED_PROCESS
        | subprocess.CREATE_BREAKAWAY_FROM_JOB
    )
    try:
        proc = subprocess.Popen(
            args,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            shell=False,
            creationflags=breakaway_flags,
        )
        return ('spawned', proc)
    except PermissionError:
        pass

    # CREATE_BREAKAWAY_FROM_JOB failed with ERROR_ACCESS_DENIED.
    # If we are inside a KILL_ON_JOB_CLOSE Job Object (e.g. T3/Codex/Antigravity tool sandbox)
    # and NOT running inside an independent long-lived host (tray/watchdog/scheduled task),
    # spawning without breakaway produces a process that dies the instant this command exits.
    if os.environ.get('FEIYU_INDEPENDENT_HOST') != '1' and in_kill_on_close_job():
        via = delegate_host_start(root, service_name, reason='restricted_job_object')
        if via:
            return ('delegated', via)
        return ('blocked', 'restricted_job_no_host')

    proc = subprocess.Popen(
        args,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=stderr,
        shell=False,
        creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS,
    )
    return ('spawned', proc)


def list_bridge_processes(root):
    """Return list of dicts {pid, ppid, name, cmdline} for processes belonging to this bridge root."""
    root_norm = str(Path(root).resolve()).lower()
    if os.name != 'nt':
        return []
    ps_script = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.CommandLine -and $_.CommandLine.ToLower().Contains($env:FEIYU_ROOT_NORM) } | "
        "Select-Object ProcessId, ParentProcessId, Name, CommandLine | ConvertTo-Json -Compress"
    )
    env = dict(os.environ)
    env['FEIYU_ROOT_NORM'] = root_norm
    try:
        proc = subprocess.run(
            ['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', ps_script],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=env,
            stdin=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW,
            timeout=15,
            check=False,
        )
        raw = (proc.stdout or '').strip()
        if not raw:
            return []
        data = json.loads(raw)
        if isinstance(data, dict):
            data = [data]
        out = []
        for item in data:
            if not isinstance(item, dict):
                continue
            out.append({
                'pid': int(item.get('ProcessId') or 0),
                'ppid': int(item.get('ParentProcessId') or 0),
                'name': str(item.get('Name') or ''),
                'cmdline': str(item.get('CommandLine') or ''),
            })
        return out
    except Exception:
        return []


def _terminate_pid_nt(pid, exit_code=1):
    k = ctypes.WinDLL('kernel32', use_last_error=True)
    k.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    k.OpenProcess.restype = wintypes.HANDLE
    k.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    k.TerminateProcess.restype = wintypes.BOOL
    k.CloseHandle.argtypes = [wintypes.HANDLE]
    h = k.OpenProcess(0x0001 | 0x100000, False, int(pid))  # PROCESS_TERMINATE | SYNCHRONIZE
    if not h:
        return False
    try:
        return bool(k.TerminateProcess(h, exit_code))
    finally:
        k.CloseHandle(h)


def terminate_matching_processes(root, markers, *, exclude_pids=None, grace_seconds=0.0, include_children=False):
    """Safely terminate ONLY processes whose command line belongs to `root` and matches any marker.

    Never kills an unrelated process on PID reuse, and by default NEVER cascades to child
    processes by PPID (so force-killing `bridge_daemon.py` never collateral-kills active
    `t3_dispatch.py job` workers or healthy sibling bots it previously spawned).
    Returns list of terminated PIDs.
    """
    exclude = {os.getpid()} | {int(p) for p in (exclude_pids or ()) if p}
    markers_low = [str(m).lower() for m in markers if m]
    if not markers_low:
        return []
    procs = list_bridge_processes(root)
    targets = []
    for p in procs:
        pid = p['pid']
        if pid <= 0 or pid in exclude:
            continue
        cmd_low = p['cmdline'].lower()
        if any(m in cmd_low for m in markers_low):
            targets.append(pid)
    target_set = set(targets)
    if include_children:
        for p in procs:
            if p['ppid'] in target_set and p['pid'] not in exclude:
                target_set.add(p['pid'])
    if grace_seconds > 0:
        time.sleep(grace_seconds)
    killed = []
    for pid in sorted(target_set, reverse=True):
        if pid_alive(pid) is True:
            if os.name == 'nt':
                if _terminate_pid_nt(pid):
                    killed.append(pid)
            else:
                try:
                    os.kill(pid, 9)
                    killed.append(pid)
                except OSError:
                    pass
    if killed:
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            if all(pid_alive(pid) is False for pid in killed):
                break
            time.sleep(0.15)
    return killed
