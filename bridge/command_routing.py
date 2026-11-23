"""T3-only owner routing. Legacy Dot/Xiaoai and 【agent】 prefixes are retired."""
from __future__ import annotations

import json
from pathlib import Path
import re

from identity import resolve

RETIRED_PREFIX = re.compile(r'^\s*【(dot|小爱|agent|任务|本地任务)】', re.IGNORECASE)


def route_history(root):
    try:
        return json.loads((Path(root) / 'private-route-history.json').read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return []


def private_default(root, at=None):
    return 't3'


def set_private_default(root, target):
    if target != 't3':
        raise ValueError('dot_routing_retired_t3_only')
    return 't3'


def select(row, root):
    parts = [
        p['text'] for p in row.get('parts', [])
        if p.get('type') in ('text', 'voice_transcript') and not p.get('quoted') and isinstance(p.get('text'), str)
    ]
    original = '\n'.join(parts)
    from group_replies import is_group
    routing_text = re.sub(r'^\s*[@＠]大肥鱼[\s\u2005\u200b]*', '', original, count=1) if is_group(root, row) else original
    retired = RETIRED_PREFIX.match(routing_text)
    owner = resolve(root, row.get('sender'))['is_owner']
    return {
        'target': 't3' if owner else 'colleague_static',
        'explicit': False,
        'prefix': None,
        'retired_prefix': retired.group(0).strip() if (retired and owner) else None,
        'original_text': original,
        'version': 2,
    }


def timeout_eligible(root, row):
    return False


def enabled(root):
    root = Path(root)
    for name in ('t3-bridge-config.json', 'command-routing.json'):
        try:
            data = json.loads((root / name).read_text(encoding='utf-8-sig'))
            if isinstance(data, dict) and 'enabled' in data:
                return data['enabled'] is True
        except (OSError, ValueError):
            continue
    return False
