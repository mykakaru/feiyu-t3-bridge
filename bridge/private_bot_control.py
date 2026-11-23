"""Lifecycle & bidirectional WebSocket IPC client for the owner's dedicated API-mode Private Bot."""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

import bridge_control
from identity import CLI_OWNER

ROOT = Path(__file__).resolve().parent
CRED_FILE = 'private-bot-credentials.dpapi'
STATUS_FILE = 'private-bot-status.json'
STOP_FILE = 'private-bot.stop'
OUTBOX_DIR = 'private-bot-outbox'
RESULTS_DIR = 'private-bot-results'
SCRIPT = Path(__file__).with_name('private_bot_receiver.cjs')


def is_configured(root=ROOT):
    return (Path(root) / CRED_FILE).is_file()


def read_state(root=ROOT):
    from win_runtime import read_json_retry
    return read_json_retry(Path(root) / STATUS_FILE)


def live(state):
    return bridge_control.live(state)


def status(root=ROOT):
    root = Path(root)
    st = read_state(root)
    running = live(st)
    return {
        'configured': is_configured(root),
        'running': running,
        'authenticated': running and st.get('state') == 'authenticated',
        **st,
    }


def owner_ws_userid(root=ROOT):
    root = Path(root)
    try:
        mapped = json.loads((root / 'identity_mapping.json').read_text(encoding='utf-8-sig'))
        ids = mapped.get('owner_ws_userids') or []
        if ids and isinstance(ids[0], str) and ids[0].strip():
            return ids[0].strip()
    except (OSError, ValueError, TypeError):
        pass
    return CLI_OWNER


def _atomic_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.' + secrets.token_hex(6) + '.tmp')
    try:
        with tmp.open('x', encoding='utf-8', newline='\n') as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def _send_ipc(root, payload, timeout=25.0):
    root = Path(root)
    st = status(root)
    if not st['authenticated']:
        return {'success': False, 'errcode': -2, 'errmsg': 'private_bot_not_authenticated'}
    req_id = 'pb_' + secrets.token_hex(10)
    payload = {**payload, 'req_id': req_id}
    outbox = root / OUTBOX_DIR
    results = root / RESULTS_DIR
    outbox.mkdir(parents=True, exist_ok=True)
    results.mkdir(parents=True, exist_ok=True)
    _atomic_json(outbox / f'{req_id}.json', payload)
    res_path = results / f'{req_id}.json'
    deadline = time.monotonic() + float(timeout)
    while time.monotonic() < deadline:
        if res_path.is_file():
            try:
                data = json.loads(res_path.read_text(encoding='utf-8-sig'))
                try:
                    res_path.unlink(missing_ok=True)
                except OSError:
                    pass
                return data
            except (OSError, ValueError):
                pass
        time.sleep(0.1)
    return {'success': False, 'errcode': -3, 'errmsg': 'private_bot_send_timeout', 'req_id': req_id}


def send_markdown(root, chatid, text, *, timeout=25.0):
    if not chatid:
        chatid = owner_ws_userid(root)
    return _send_ipc(root, {'kind': 'markdown', 'chatid': str(chatid), 'text': str(text)}, timeout=timeout)


def ask_owner(root, thread_id, text, *, title='', chatid='', dry_run=False, timeout=25.0):
    """Send an Agent-initiated message to the owner via Claude with embedded threadId/session code.

    Automatically appends `—— 来自 Agent〔A-XXXX〕... threadId: <id>` and registers the outbound
    mapping in `business.sqlite3` (`agent_outbound_asks`) so owner replies route back into the
    originating T3 thread.
    """
    import agent_relay
    from business_queue import db

    root = Path(root)
    tid = str(thread_id or '').strip()
    if not agent_relay.valid_thread_id(tid):
        return {'success': False, 'errcode': -10, 'errmsg': 'invalid_thread_id'}
    body_text = str(text or '').strip()
    if not body_text:
        return {'success': False, 'errcode': -11, 'errmsg': 'empty_text'}

    target = str(chatid or '').strip() or owner_ws_userid(root)
    clean_t = agent_relay.clean_title(title)
    if dry_run:
        code = agent_relay.derive_session_code(tid)
        if (root / 'business.sqlite3').is_file():
            try:
                with db(root) as c:
                    if c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_outbound_asks'").fetchone():
                        row = c.execute(
                            'SELECT session_code FROM agent_outbound_asks WHERE thread_id=? ORDER BY id DESC LIMIT 1',
                            (tid,),
                        ).fetchone()
                        if row and row['session_code']:
                            code = row['session_code'].upper()
            except Exception:
                pass
        full_text, footer = agent_relay.decorate_message(body_text, code, tid, title=clean_t)
        return {
            'success': True,
            'dry_run': True,
            'errcode': 0,
            'errmsg': 'dry_run',
            'chatid': target,
            'session_code': code,
            'thread_id': tid,
            'title': clean_t,
            'footer': footer,
            'text': full_text,
            'registered': False,
        }

    with db(root, True) as c:
        code = agent_relay.allocate_session_code(c, tid)
    full_text, footer = agent_relay.decorate_message(body_text, code, tid, title=clean_t)
    res = send_markdown(root, target, full_text, timeout=timeout)
    registered = False
    if res.get('success') or res.get('errcode') == -3:
        agent_relay.register_ask(
            root,
            thread_id=tid,
            body=full_text,
            chatid=target,
            session_code=code,
            title=clean_t,
            req_id=res.get('req_id'),
            ws_req_id=res.get('ws_req_id'),
            msgid=res.get('msgid'),
            bot_channel='private_bot',
        )
        registered = True
    return {
        **res,
        'chatid': target,
        'session_code': code,
        'thread_id': tid,
        'title': clean_t,
        'footer': footer,
        'registered': registered,
    }


def send_file(root, chatid, file_path, *, media_type='file', filename=None, title=None, description=None, timeout=180.0):
    if not chatid:
        chatid = owner_ws_userid(root)
    p = Path(file_path).resolve()
    payload = {
        'kind': 'media',
        'chatid': str(chatid),
        'file_path': str(p),
        'media_type': media_type,
        'filename': filename or p.name,
    }
    if title:
        payload['title'] = str(title)
    if description:
        payload['description'] = str(description)
    return _send_ipc(root, payload, timeout=timeout)


def start(root=ROOT):
    root = Path(root).resolve()
    cred = root / CRED_FILE
    if not cred.is_file():
        return {'configured': False, 'running': False, 'error': 'private_bot_credentials_missing'}
    st = read_state(root)
    if live(st) and st.get('state') == 'authenticated':
        return {'configured': True, 'running': True, 'authenticated': True, **st}
    (root / STOP_FILE).unlink(missing_ok=True)
    launched_at = datetime.now(timezone.utc)
    from win_runtime import spawn_detached_or_delegate
    delegated_via = None
    with (root / 'private-bot-launcher.log').open('ab') as logf:
        cmd = [sys.executable, str(Path(__file__).resolve()), 'worker', '--root', str(root)]
        mode, via = spawn_detached_or_delegate(
            cmd, cwd=root, root=root, service_name='private_bot',
            stdout=subprocess.DEVNULL, stderr=logf,
        )
    if mode == 'blocked':
        return {'configured': True, 'running': False, 'authenticated': False, 'error': via, **st}
    if mode == 'delegated':
        delegated_via = via
    for _ in range(45):
        time.sleep(0.4)
        st = read_state(root)
        try:
            if datetime.fromisoformat(st['started_at'].replace('Z', '+00:00')) < launched_at:
                continue
        except (KeyError, ValueError, TypeError):
            continue
        if live(st) and st.get('state') == 'authenticated':
            break
        if st.get('state') == 'stopped' and st.get('reason') != 'local stop request':
            break
    out = status(root)
    if delegated_via:
        out['delegated_via'] = delegated_via
    return out


def stop(root=ROOT):
    root = Path(root).resolve()
    (root / STOP_FILE).touch()
    for _ in range(25):
        if not live(read_state(root)):
            break
        time.sleep(0.3)
    st = status(root)
    st['stopped'] = not st['running']
    return st


def run_worker(root=ROOT):
    root = Path(root).resolve()
    cred = root / CRED_FILE
    config = bridge_control.protect(cred.read_bytes(), decrypt=True)
    for attempt in range(7):
        with (root / 'private-bot-worker.log').open('ab') as logf:
            child = subprocess.Popen(
                [str(bridge_control.NODE), str(SCRIPT), str(root), 'private_bot'],
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=logf,
                shell=False, creationflags=subprocess.CREATE_NO_WINDOW,
            )
            child.communicate(config)
        st = read_state(root)
        if child.returncode == 0 or st.get('error_type') == 'WSAuthFailureError' or 'replaced' in str(st.get('reason', '')).lower():
            return child.returncode
        for _ in range(min(30, 2 ** attempt)):
            if (root / STOP_FILE).exists():
                return 0
            time.sleep(1)
    return 1


def _read_cli_text(args):
    if getattr(args, 'text_file', ''):
        return Path(args.text_file).read_text(encoding='utf-8-sig')
    if getattr(args, 'stdin', False) or args.text == '-':
        if sys.stdin is None:
            return ''
        try:
            sys.stdin.reconfigure(encoding='utf-8')
        except Exception:
            pass
        return sys.stdin.read()
    return args.text or ''


def main():
    if sys.stdout is not None:
        sys.stdout.reconfigure(encoding='utf-8')
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['start', 'status', 'stop', 'worker', 'send', 'ask'])
    p.add_argument('--root', default=str(ROOT))
    p.add_argument('--chatid', default='')
    p.add_argument('--text', default='')
    p.add_argument('--text-file', default='')
    p.add_argument('--stdin', action='store_true')
    p.add_argument('--thread-id', default='')
    p.add_argument('--title', default='')
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()
    root = Path(args.root)
    from win_runtime import SingleInstance
    guard = SingleInstance('PrivateBot' + args.action, root) if args.action in ('start', 'worker') else nullcontext()
    with guard as inst:
        if inst is not None and not inst.acquired:
            print(json.dumps({'already_running_or_starting': True}))
            return 0
        if args.action == 'worker':
            return run_worker(root)
        if args.action == 'start':
            res = start(root)
            print(json.dumps(res, ensure_ascii=False))
            return 0 if res.get('authenticated') else 1
        if args.action == 'stop':
            res = stop(root)
            print(json.dumps(res, ensure_ascii=False))
            return 0 if res.get('stopped') else 1
        if args.action == 'ask':
            text = _read_cli_text(args)
            res = ask_owner(
                root,
                args.thread_id,
                text,
                title=args.title,
                chatid=args.chatid or owner_ws_userid(root),
                dry_run=args.dry_run,
            )
            print(json.dumps(res, ensure_ascii=False))
            return 0 if res.get('success') else 1
        if args.action == 'send':
            text = _read_cli_text(args)
            if args.thread_id:
                res = ask_owner(
                    root,
                    args.thread_id,
                    text,
                    title=args.title,
                    chatid=args.chatid or owner_ws_userid(root),
                    dry_run=args.dry_run,
                )
            else:
                res = send_markdown(root, args.chatid or owner_ws_userid(root), text)
            print(json.dumps(res, ensure_ascii=False))
            return 0 if res.get('success') else 1
        print(json.dumps(status(root), ensure_ascii=False))
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
