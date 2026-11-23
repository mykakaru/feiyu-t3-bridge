"""Multi-provider T3 live catalog loader and owner explicit-model parser.

Reads live T3 provider caches (antigravity + codex) with local offline fallback.
Supports both 【模型：...】 tags and natural-language requests like:
  - "用 6.2 astra max 处理..."
  - "用 6.2solmax ultrafast 分析..."
  - "让 neon 来看看..."
  - "用 hydrogen high / carbon high ..."
Never reads quotes, attachments, or negated model mentions.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re

from settings import EFFORTS, MODEL_ID, SERVICE_TIERS
import t3_client


class UnavailableChoice(ValueError):
    """A recognized explicit model or option is unavailable in the live T3 catalog."""


class ConflictingChoice(ValueError):
    """Multiple conflicting explicit models were specified without multi-agent discussion intent."""


def options(root):
    """Return selectable models across enabled T3 providers (plus offline fallback)."""
    live = t3_client.live_catalog(root)
    choices = list(live.get('models', []))
    seen = {(c['provider'], c['id'].lower()) for c in choices}

    # Offline fallback for unit tests or missing T3 cache
    if not choices:
        home = Path(os.environ.get('CODEX_HOME') or Path.home() / '.codex')
        sources = (
            home / 'models_cache.json',
            home / 'models_cache.codex_work_desktop.json',
            Path(root) / 'bridge-models.json',
        )
        for path in sources:
            try:
                data = json.loads(path.read_text(encoding='utf-8-sig'))
            except (OSError, ValueError):
                continue
            rows = data.get('models') if isinstance(data, dict) else None
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                model = row.get('slug')
                if not isinstance(model, str) or not MODEL_ID.fullmatch(model):
                    continue
                provider = row.get('provider') or ('antigravity' if model.lower().startswith(('gemini', 'claude')) else 'codex')
                if (provider, model.lower()) in seen:
                    continue
                if row.get('visibility') not in (None, 'list'):
                    if row.get('visibility') in ('hide', 'hidden', 'none'):
                        seen.add((provider, model.lower()))
                    continue
                levels = row.get('supported_reasoning_levels')
                if not isinstance(levels, list):
                    continue
                efforts = list(dict.fromkeys(
                    lv['effort'] for lv in levels
                    if isinstance(lv, dict) and isinstance(lv.get('effort'), str) and lv['effort'] in EFFORTS
                ))
                if not efforts:
                    continue
                seen.add((provider, model.lower()))
                label = row.get('display_name') or model
                default = row.get('default_reasoning_level')
                option_id = 'thought_level' if provider == 'antigravity' else 'reasoningEffort'
                choices.append({
                    'provider': provider,
                    'id': model,
                    'label': label if isinstance(label, str) and label else model,
                    'option_id': option_id,
                    'efforts': efforts,
                    'default': default if default in efforts else efforts[0],
                    'service_tiers': ['default', 'priority', 'ultrafast'] if model.lower() == 'gpt-6.2-sol' else (['default', 'priority'] if provider == 'codex' else []),
                    'default_service_tier': 'default' if provider == 'codex' else None,
                })
    return choices


# Flexible leading tag stripping (no 【agent】/【dot】/【小爱】 routing prefixes!)
LEADING_TAGS = re.compile(
    r'^(?:(?:[@＠]大肥鱼[\s\u2005\u200b]*)|(?:【回报\s*[:：]\s*[^】]{1,60}】\s*))+',
    re.I,
)
TAG_SPEC = re.compile(r'【模型\s*[:：]\s*([^】]{1,80})】', re.I)
MULTI_AGENT_INTENT = re.compile(
    r'对比|比较|商量|共同|一起|讨论|互审|评审|交叉|双模型|多模型|多\s*agent|协作|联手|分别',
    re.I,
)
NEGATION_PREFIX = re.compile(r'(?:不要用|别用|不用|勿用|禁[用止]|不是用|别走|不走)\s*$', re.I)

# Natural-language imperative model request
IMPERATIVE_MODEL = re.compile(
    r'(?:^|[，,。；;！!\n\s])(?:请)?(?:直接)?(?:用|使用|采用|换用?|切到?|转给|交给|让|由|模型使用|模型选)\s*'
    r'(?P<spec>(?:'
    r'(?:gpt[- ]?)?[56](?:\.[0-9]+)?\s*[-]?\s*(?:astra|sol|terra|luna)'
    r'|(?:gemini[- ]?(?:4|3\.8)[- ]?)?(?:neon|hydrogen|carbon|argon|flash)'
    r'|gpt-[a-z0-9.-]+'
    r'|gemini-[a-z0-9.-]+'
    r')'
    r'(?:[\s/·-]*(?:xhigh|ultra|max|high|medium|low))?'
    r'(?:[\s/·-]*(?:ultrafast|priority|fast))?)',
    re.I,
)

# Tokenized parser for a single model spec string like "6.2solmax ultrafast", "gpt-6.2-astra max", "neon", "hydrogen high"
FUSED_GPT = re.compile(
    r'^(?:gpt[- ]?)?(?P<ver>[56](?:\.[0-9]+)?)\s*[-]?\s*(?P<fam>astra|sol|terra|luna)'
    r'(?:[\s/·-]*(?P<eff>xhigh|ultra|max|high|medium|low))?'
    r'(?:[\s/·-]*(?P<tier>ultrafast|priority|fast|default))?$',
    re.I,
)
FUSED_GEMINI = re.compile(
    r'^(?:gemini[- ]?(?P<ver>4|3\.8)[- ]?)?(?P<fam>neon|hydrogen|carbon|argon|flash)'
    r'(?:[\s/·-]*(?P<eff>xhigh|high|medium|low))?$',
    re.I,
)
GENERIC_SPEC = re.compile(
    r'^(?P<model>(?:gpt|gemini|claude)-[a-z0-9._-]+)'
    r'(?:[\s/·]+(?P<eff>[a-z]+))?'
    r'(?:[\s/·]+(?P<tier>ultrafast|priority|fast|default))?$',
    re.I,
)


def _normalize_spec(raw):
    text = ' '.join((raw or '').strip().split())
    if not text:
        return None
    m = FUSED_GPT.fullmatch(text)
    if m:
        ver, fam = m.group('ver'), m.group('fam').lower()
        tier = (m.group('tier') or '').lower() or None
        if tier == 'fast':
            tier = 'priority'
        return {
            'model_id': f'gpt-{ver}-{fam}',
            'effort': (m.group('eff') or '').lower() or None,
            'service_tier': tier,
        }
    m = FUSED_GEMINI.fullmatch(text)
    if m:
        fam = m.group('fam').lower()
        ver = m.group('ver') or ('3.8' if fam == 'flash' else '4')
        return {
            'model_id': f'gemini-{ver}-{fam}',
            'effort': (m.group('eff') or '').lower() or None,
            'service_tier': None,
        }
    m = GENERIC_SPEC.fullmatch(text)
    if m:
        tier = (m.group('tier') or '').lower() or None
        if tier == 'fast':
            tier = 'priority'
        return {
            'model_id': m.group('model').lower(),
            'effort': (m.group('eff') or '').lower() or None,
            'service_tier': tier,
        }
    return None


def resolve_spec(root, raw_spec):
    parsed = _normalize_spec(raw_spec)
    if not parsed:
        raise UnavailableChoice(f'模型标签「{raw_spec}」无法识别。请确认模型名和档位，例如 gpt-6.2-astra max、neon、hydrogen high。')
    catalog = options(root)
    choice = next((row for row in catalog if row['id'].lower() == parsed['model_id']), None)
    if choice is None:
        avail = '、'.join(row['id'] for row in catalog[:10])
        raise UnavailableChoice(f'指定的模型「{parsed["model_id"]}」未在当前可用 T3 目录中列出（当前可选：{avail}）。')
    effort = parsed['effort'] or choice.get('default')
    if parsed['effort'] and parsed['effort'] not in (choice.get('efforts') or []):
        levels = '/'.join(choice.get('efforts') or [])
        raise UnavailableChoice(f'模型 {choice["id"]} 不支持档位「{parsed["effort"]}」（支持：{levels}）。')
    tier = parsed['service_tier']
    if tier:
        supported_tiers = choice.get('service_tiers') or []
        if tier not in supported_tiers:
            raise UnavailableChoice(f'模型 {choice["id"]} 不支持加速档位「{tier}」。')
    option_id = choice.get('option_id') or ('thought_level' if choice['provider'] == 'antigravity' else 'reasoningEffort')
    options_dict = {}
    if effort:
        options_dict[option_id] = effort
    if tier and tier != choice.get('default_service_tier'):
        options_dict['serviceTier'] = tier
    matches_defaults = (
        (effort is None or effort == choice.get('default'))
        and (tier is None or tier == choice.get('default_service_tier'))
    )
    return {
        'provider': choice['provider'],
        'model': choice['id'],
        'label': choice.get('label') or choice['id'],
        'option_id': option_id,
        'effort': effort,
        'reasoning_effort': effort,
        'default_effort': choice.get('default'),
        'service_tier': tier,
        'options': options_dict,
        'matches_defaults': matches_defaults,
        'explicit': True,
    }


def owner_override(root, anchor):
    """Parse explicit model choice ONLY from the authenticated owner's direct text."""
    direct = (anchor.get('direct') or '').strip()
    if not direct:
        return None
    stripped = LEADING_TAGS.sub('', direct).strip()

    # 1. Explicit 【模型：...】 tags anywhere in direct text (allows swapping order with 【回报：...】)
    tag_matches = TAG_SPEC.findall(direct)
    if tag_matches:
        resolved = [resolve_spec(root, t.strip()) for t in tag_matches]
        unique_keys = {(r['provider'], r['model'], r['effort'], r['service_tier']) for r in resolved}
        if len(unique_keys) > 1:
            if MULTI_AGENT_INTENT.search(direct):
                return None  # Multi-agent request handled by orchestration, not single lock
            names = ' 与 '.join(f'{r["model"]}({r["effort"]})' for r in resolved)
            raise ConflictingChoice(f'同一条消息中指定了多个冲突的模型标签：{names}。请确认只用一个，还是需要多模型共同讨论。')
        return resolved[0]

    if re.search(r'【模型\s*[:：]', direct):
        raise UnavailableChoice('模型标签格式不完整，请写成例如【模型：gpt-6.2-astra max】或【模型：neon】。')

    # 2. Natural-language explicit request (skip negations and multi-agent discussions)
    found = []
    for m in IMPERATIVE_MODEL.finditer(stripped):
        prefix_window = stripped[max(0, m.start() - 10):m.start() + 6]
        if NEGATION_PREFIX.search(prefix_window):
            continue
        spec_raw = m.group('spec').strip()
        if not spec_raw:
            continue
        # Only treat bare "flash"/"carbon"/"neon" etc. as explicit model when attached to an imperative verb
        try:
            res = resolve_spec(root, spec_raw)
        except UnavailableChoice:
            # Re-raise only when the spec clearly looks like a model ID (gpt-/gemini-/6.2)
            if re.search(r'(?:gpt|gemini|[56]\.[0-9])', spec_raw, re.I):
                raise
            continue
        found.append(res)

    if not found:
        return None
    unique = []
    seen = set()
    for r in found:
        k = (r['provider'], r['model'], r['effort'], r['service_tier'])
        if k not in seen:
            seen.add(k)
            unique.append(r)
    if len(unique) > 1:
        if MULTI_AGENT_INTENT.search(direct):
            # Multi-agent discussion requested by owner (e.g., "你和 6.2 astra max 一起商量"):
            # do not hard-lock to a single model; let T3 escalation/multi-agent handle it!
            return None
        names = '、'.join(f'{r["model"]}({r["effort"]})' for r in unique)
        raise ConflictingChoice(f'指令中提到了多个模型（{names}），但未说明是要多模型联合讨论还是单选其一。')
    if MULTI_AGENT_INTENT.search(direct) and re.search(r'(?:和|跟|与)\s*(?:gpt|6\.|5\.|neon|carbon|astra|sol)', direct, re.I):
        return None
    return unique[0]
