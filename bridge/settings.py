"""Validated T3 bridge model and clarification preferences."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import tempfile

EFFORTS = {'low', 'medium', 'high', 'xhigh', 'max', 'ultra'}
SERVICE_TIERS = {'default', 'priority', 'ultrafast'}
MODEL_ID = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}')
PROVIDER_ID = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9._-]{0,39}')

DEFAULT_ENTRY = {'provider': 'antigravity', 'model': 'gemini-4-hydrogen', 'effort': 'high'}
DEFAULT_ESCALATION = {'provider': 'antigravity', 'model': 'gemini-4-neon', 'effort': 'xhigh'}

DEFAULT = {
    'entry_model': dict(DEFAULT_ENTRY),
    'escalation_model': dict(DEFAULT_ESCALATION),
    'clarification_mode': 'strict',
    # Legacy compatibility fields retained as read-only nulls
    'personal_model': None,
    'group_readonly_model': None,
    'model_dispatch_implemented': True,
    'context_visibility': 'owner_private_and_static_group_notes',
}


def model_spec(value, *, allow_none=False):
    if value is None:
        if allow_none:
            return None
        raise ValueError('model specification required')
    if not isinstance(value, dict):
        raise ValueError('model specification must be an object')
    model = value.get('model')
    if not isinstance(model, str) or not MODEL_ID.fullmatch(model):
        raise ValueError('invalid model identifier')
    provider = value.get('provider')
    if provider is None:
        provider = 'antigravity' if model.lower().startswith(('gemini', 'claude')) else 'codex'
    if not isinstance(provider, str) or not PROVIDER_ID.fullmatch(provider):
        raise ValueError('invalid provider identifier')
    effort = value.get('effort', value.get('reasoning_effort'))
    if effort is not None and (not isinstance(effort, str) or effort not in EFFORTS):
        raise ValueError('unsupported reasoning effort')
    out = {'provider': provider, 'model': model}
    if effort:
        out['effort'] = effort
    tier = value.get('service_tier')
    if tier is not None:
        if not isinstance(tier, str) or tier not in SERVICE_TIERS:
            raise ValueError('unsupported service tier')
        out['service_tier'] = tier
    return out


def preference(value):
    if value is None:
        return None
    if not isinstance(value, dict) or not {'model', 'reasoning_effort'} <= set(value):
        raise ValueError('model and reasoning_effort required')
    if not isinstance(value['model'], str) or not MODEL_ID.fullmatch(value['model']):
        raise ValueError('invalid model identifier')
    if not isinstance(value['reasoning_effort'], str) or value['reasoning_effort'] not in EFFORTS:
        raise ValueError('unsupported reasoning effort')
    return {'model': value['model'], 'reasoning_effort': value['reasoning_effort']}


def read(root):
    root = Path(root)
    result = {
        **DEFAULT,
        'entry_model': dict(DEFAULT_ENTRY),
        'escalation_model': dict(DEFAULT_ESCALATION),
    }
    cfg_path = root / 't3-bridge-config.json'
    if cfg_path.is_file():
        try:
            cfg = json.loads(cfg_path.read_text(encoding='utf-8-sig'))
            if isinstance(cfg, dict):
                if 'entry_model' in cfg:
                    result['entry_model'] = model_spec(cfg['entry_model'])
                if 'escalation_model' in cfg:
                    result['escalation_model'] = model_spec(cfg['escalation_model'])
                if cfg.get('clarification_mode') in ('strict', 'standard'):
                    result['clarification_mode'] = cfg['clarification_mode']
        except (OSError, ValueError, TypeError):
            result['settings_error'] = 'invalid_t3_config_using_defaults'
    legacy_path = root / 'routing_settings.json'
    if legacy_path.is_file():
        try:
            data = json.loads(legacy_path.read_text(encoding='utf-8-sig'))
            if isinstance(data, dict):
                if 'entry_model' in data and not cfg_path.is_file():
                    result['entry_model'] = model_spec(data['entry_model'])
                if 'escalation_model' in data and not cfg_path.is_file():
                    result['escalation_model'] = model_spec(data['escalation_model'])
                if 'clarification_mode' in data and data['clarification_mode'] in ('strict', 'standard') and not cfg_path.is_file():
                    result['clarification_mode'] = data['clarification_mode']
                if data.get('personal_model') is not None:
                    result['personal_model'] = preference(data['personal_model'])
        except (OSError, ValueError, TypeError):
            pass
    return result


def _atomic_write_json(path, payload):
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.' + path.stem + '-', suffix='.tmp')
    temp = Path(name)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def write_t3_preferences(root, entry_model, escalation_model, clarification_mode='strict'):
    root = Path(root)
    entry = model_spec(entry_model)
    escalation = model_spec(escalation_model)
    if clarification_mode not in ('strict', 'standard'):
        raise ValueError('invalid clarification mode')
    path = root / 't3-bridge-config.json'
    from win_runtime import SingleInstance
    with SingleInstance('ModelPreferences', root) as writer:
        if not writer.acquired:
            raise RuntimeError('model_settings_busy')
        try:
            existing = json.loads(path.read_text(encoding='utf-8-sig'))
        except FileNotFoundError:
            existing = {}
        if not isinstance(existing, dict):
            raise ValueError('invalid_settings_object')
        existing.update(
            entry_model=entry,
            escalation_model=escalation,
            clarification_mode=clarification_mode,
        )
        _atomic_write_json(path, existing)
    return read(root)


def write(root, personal, group=None):
    """Backward-compatible wrapper preserved for legacy callers/tests."""
    root = Path(root)
    selected = {'personal_model': preference(personal), 'group_readonly_model': preference(group) if group is not None else None}
    path = root / 'routing_settings.json'
    from win_runtime import SingleInstance
    with SingleInstance('ModelPreferences', root) as writer:
        if not writer.acquired:
            raise RuntimeError('model_settings_busy')
        try:
            existing = json.loads(path.read_text(encoding='utf-8-sig'))
        except FileNotFoundError:
            existing = {}
        if not isinstance(existing, dict):
            raise ValueError('invalid_settings_object')
        value = {**existing, **selected}
        _atomic_write_json(path, value)
    return value


def choose(root, row, owner_verified, explicit_owner_choice=None):
    if explicit_owner_choice is not None:
        if not owner_verified:
            raise ValueError('only owner may override dispatch preference')
        return dict(explicit_owner_choice)
    if not owner_verified:
        return None
    return dict(read(root)['entry_model'])
