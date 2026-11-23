"""Bounded GUI backend for Feiyu T3 Bridge: health, cases, T3 settings, and zero-LLM conclusion notes."""
from __future__ import annotations

from datetime import datetime, timezone
import heapq
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent


def read(name):
    try:
        return json.loads((ROOT / name).read_text(encoding='utf-8-sig'))
    except (OSError, ValueError):
        return {}


def age(s, key='updated_at'):
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(s[key].replace('Z', '+00:00'))).total_seconds()
    except (KeyError, ValueError, TypeError):
        return 999999


def cases():
    from business_queue import panel_cases
    return panel_cases(ROOT)


def case_detail(case_id):
    row = next((r for r in cases() if r.get('id') == case_id), None)
    if row is None:
        raise ValueError('case_not_found')
    result = dict(row)
    result['result_text'] = ''
    candidate = row.get('result_path')
    if candidate:
        path = Path(candidate)
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(ROOT.resolve())
            if not path.is_absolute() or str(path).startswith('\\\\') or resolved.suffix.lower() not in ('.txt', '.md'):
                raise ValueError('unsupported_result')
            if not resolved.is_file() or resolved.stat().st_size > 2 * 1024 * 1024:
                raise ValueError('result_size')
            result['result_text'] = resolved.read_text(encoding='utf-8-sig')
        except (OSError, ValueError, UnicodeError):
            result['result_text'] = '结果文件不可读取：仅支持桥接目录内不超过 2 MiB 的 UTF-8 文本或 Markdown。'
    return result


def case_counts(rows):
    counts = {'queued': 0, 'running': 0, 'clarifying': 0, 'attention': 0, 'completed': 0, 'expired': 0, 'other': 0}
    for row in rows:
        state = row.get('state', '')
        if state in ('awaiting_clarification', 'colleague_followup'):
            bucket = 'clarifying'
        elif state in ('queued', 'pending', 'ready', 'collecting', 'resume_queued', 'heavy_queued', 'heavy_resume_queued'):
            bucket = 'queued'
        elif state in ('launching', 'running', 'processing'):
            bucket = 'running'
        elif state in ('blocked', 'failed', 'uncertain', 'needs_attention'):
            bucket = 'attention'
        elif state in ('completed', 'done', 'closed'):
            bucket = 'completed'
        elif state == 'expired':
            bucket = 'expired'
        else:
            bucket = 'other'
        counts[bucket] += 1
    counts['actionable'] = counts['queued'] + counts['running'] + counts['clarifying'] + counts['attention']
    return counts


def watchdog_history(root, limit=16):
    archive = root / 'watchdog-incidents'
    if not archive.is_dir():
        return []

    def modified(path):
        try:
            return path.stat().st_mtime_ns
        except OSError:
            return 0

    latest = heapq.nlargest(limit * 2, archive.glob('*.json'), key=modified)
    labels = {
        'dead_or_stale': '收件心跳中断',
        'disconnected': '接收器连接中断',
        'blocked': '接收器鉴权或连接受阻',
    }
    rows, seen = [], set()
    for path in latest:
        try:
            record = json.loads(path.read_text(encoding='utf-8'))
            key, kind = record['incident'], record['kind']
            if key in seen or record.get('test') or path.stem != key + '-' + kind:
                continue
            if kind not in ('down', 'recovered'):
                continue
            other = 'recovered' if kind == 'down' else 'down'
            pair = {kind: record}
            other_path = path.with_name(key + '-' + other + '.json')
            if other_path.is_file():
                part = json.loads(other_path.read_text(encoding='utf-8'))
                if part.get('incident') == key and part.get('kind') == other and not part.get('test'):
                    pair[other] = part
            current = pair.get('recovered', record)
            start = pair.get('down', record)
            reason = start.get('health') or start.get('reason')
            detail = labels.get(reason, reason or '本机检查')
            title = '已恢复' if 'recovered' in pair else '异常'
            if 'down' in pair and 'recovered' in pair:
                elapsed = datetime.fromisoformat(current['at']) - datetime.fromisoformat(start['at'])
                detail += '，异常到恢复 %.0f 秒' % max(0, elapsed.total_seconds())
            rows.append({'at': current['at'], 'kind': '守护记录', 'detail': '企微收件 · ' + title + ' · ' + detail, 'msgid': key[:7]})
            seen.add(key)
            if len(rows) >= limit:
                break
        except (KeyError, OSError, ValueError, TypeError):
            continue
    return rows


def snapshot():
    from command_routing import enabled, select
    import conclusion_notes
    from model_catalog import options as model_options
    from settings import read as prefs
    import t3_client
    from t3_dispatch import snapshot as t3_tasks

    status = read('status.json')
    guard = read('watchdog-state.json')
    daemon = read('bridge-daemon-status.json')
    dispatch_st = read('local-dispatch-status.json')
    t3_st = t3_client.server_status(ROOT)

    business = cases()
    by_source = {mid: case for case in business for mid in case.get('own_source_ids', ())}
    t = t3_tasks(ROOT)
    rows = []
    raw_pending = 0
    for p in (ROOT / 'queue').glob('*.json'):
        try:
            r = json.loads(p.read_text(encoding='utf-8'))
            raw_pending += r.get('delivery') == 'pending'
            route = select(r, ROOT)
            mid = r.get('msgid', '')
            case = by_source.get(mid)
            if case:
                lane = {'t3': 'T3 调度', 'conclusion_note': '群静态说明', 'colleague_followup': '同事待补充',
                        'agent_relay': 'Agent传话', 'control': '指令控制', 'local': '历史本机', 'dot': '历史', 'history': '历史'}.get(case['mode'], case['mode'])
                states = {
                    'completed': '已完成', 'running': '执行中', 'expired': '已过期',
                    'queued': '已排队', 'blocked': '受阻', 'uncertain': '待核实',
                    'resume_queued': '等待轻槽续办', 'heavy_queued': '等待重槽', 'heavy_resume_queued': '等待重槽续办',
                    'collecting': '收集中', 'cancelled': '已取消', 'historical': '历史记录',
                    'awaiting_clarification': '等待确认', 'colleague_followup': '待人工补充',
                }
                status_name = states.get(case['state'], case['state'])
            else:
                lane = 'T3' if route['target'] == 't3' else '群静默'
                status_name = {'pending': '已收件', 'acknowledged': '已完成', 'processing': '接收中'}.get(r.get('delivery'), '')
            if any(a.get('state') == 'unavailable' for a in r.get('attachments', [])):
                status_name += ' / 附件不可读'
            preview = route['original_text'].replace('\n', ' ')[:70]
            rows.append({'at': r.get('received_at', ''), 'kind': 'received', 'detail': lane + ' / ' + status_name + ' / ' + preview, 'msgid': mid})
        except (OSError, ValueError):
            pass

    rows.extend(t['log'])
    rows.extend(watchdog_history(ROOT))

    counts = case_counts(business)
    for case in business:
        if case['mode'] in ('t3', 'local', 'conclusion_note', 'colleague_followup') and case['state'] not in ('historical', 'material'):
            rows.append({
                'at': case['updated'],
                'kind': '业务事项',
                'detail': case['title'] + ' · ' + (case.get('model_summary') or case['mode']) + ' · ' + case['state'],
                'msgid': case['id'],
            })

    def timestamp(row):
        try:
            return datetime.fromisoformat(row['at'].replace('Z', '+00:00')).timestamp()
        except (KeyError, AttributeError, TypeError, ValueError):
            return 0

    rows.sort(key=timestamp, reverse=True)
    connected = status.get('state') == 'authenticated' and 0 <= age(status) < 15
    daemon_ok = daemon.get('state') == 'running' and 0 <= age(daemon) < 20
    dispatch_ok = enabled(ROOT) and 0 <= age(dispatch_st) < 15
    watchdog_ok = 0 <= age(guard) < 30 and not guard.get('stopped_at')
    pb_status = read('private-bot-status.json')
    private_bot_ok = pb_status.get('state') == 'authenticated' and 0 <= age(pb_status) < 15

    return {
        'engine': 't3',
        'cases': business,
        'case_counts': counts,
        'receiver_ok': connected,
        'receiver_state': status.get('state', 'unknown'),
        'receiver_updated': status.get('updated_at', ''),
        'private_bot_ok': private_bot_ok,
        'private_bot_state': pb_status.get('state', 'not_configured'),
        't3_ok': bool(t3_st['ok']),
        't3_status': t3_st,
        'daemon_ok': daemon_ok,
        'routing_enabled': dispatch_ok,
        'regular_slots': dispatch_st.get('regular_slots', t['regular_slots']),
        'light_slots': dispatch_st.get('light_slots', t['light_slots']),
        'light_burst_slots': dispatch_st.get('light_burst_slots', t['light_burst_slots']),
        'watchdog_ok': watchdog_ok,
        'pending': counts['actionable'],
        'raw_pending': raw_pending,
        'settings': prefs(ROOT),
        'model_catalog': model_options(ROOT),
        'jobs': t['jobs'],
        'log': rows[:180],
        'conclusions': conclusion_notes.list_panel_data(ROOT),
    }


def main():
    action = sys.argv[1]
    if action == 'health':
        import t3_client
        return {'engine': 't3', 't3': t3_client.server_status(ROOT)}
    if action == 'snapshot':
        return snapshot()
    if action == 'case_detail':
        return case_detail(json.load(sys.stdin)['id'])
    if action == 'save_t3_settings':
        from settings import write_t3_preferences
        from t3_dispatch import log
        data = json.load(sys.stdin)
        res = write_t3_preferences(
            ROOT,
            data.get('entry_model'),
            data.get('escalation_model'),
            data.get('clarification_mode', 'strict'),
        )
        log(ROOT, '', 'settings_saved', 'T3 模型与追问设置已保存，对新事项生效')
        return res
    if action == 'save':
        from settings import write
        from t3_dispatch import log
        data = json.load(sys.stdin)
        res = write(ROOT, data.get('personal_model'), data.get('group_readonly_model'))
        log(ROOT, '', 'settings_saved', '设置已保存')
        return res
    if action == 'conclusion_data':
        import conclusion_notes
        return conclusion_notes.list_panel_data(ROOT)
    if action == 'save_conclusion':
        import conclusion_notes
        data = json.load(sys.stdin)
        return conclusion_notes.panel_save_note(
            ROOT, data.get('chat', ''), data.get('title', ''), data.get('body', ''),
            note_id=data.get('note_id'), anchor_text=data.get('anchor_text', ''),
            source_case_id=data.get('source_case_id'),
        )
    if action == 'delete_conclusion':
        import conclusion_notes
        return conclusion_notes.panel_delete_note(ROOT, json.load(sys.stdin).get('note_id', ''))
    if action == 'resolve_followup':
        import conclusion_notes
        return conclusion_notes.panel_resolve_followup(ROOT, json.load(sys.stdin).get('id', ''))
    if action == 'start':
        import os
        env = dict(os.environ)
        env['FEIYU_INDEPENDENT_HOST'] = '1'
        results = []
        scripts = ['bridge_control.py', 'watchdog.py', 'bridge_daemon.py']
        if (ROOT / 'private-bot-credentials.dpapi').is_file():
            scripts.append('private_bot_control.py')
        for script in scripts:
            args = [sys.executable, str(ROOT / script), 'start']
            p = subprocess.run(
                args, cwd=ROOT, env=env, capture_output=True, timeout=100,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            results.append({'service': script, 'exit': p.returncode})
        from t3_dispatch import log
        log(ROOT, '', 'services_checked', json.dumps(results))
        return {'results': results}
    raise ValueError('unknown_action')


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stdin.reconfigure(encoding='utf-8')
    try:
        print(json.dumps(main(), ensure_ascii=False))
    except Exception as e:
        print(json.dumps({'error': type(e).__name__, 'detail': str(e)[:200]}, ensure_ascii=False))
        sys.exit(1)
