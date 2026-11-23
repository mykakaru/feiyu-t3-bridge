"""One-way sanitized sync from a live local bridge directory into this public repo.

Usage:
    python scripts/sync_from_live.py --source "C:\\Path\\To\\Live\\bridge"

Optional local-only redaction map (gitignored, never committed):
    Create `.sync-redact.local.json` in the repo root with:
    {
      "literal_replacements": {
        "SensitiveName": "ExampleName"
      }
    }

Always runs `scripts/sanitize_audit.py` at the end and fails non-zero if any
leak pattern remains.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BRIDGE_DIR = REPO_ROOT / "bridge"
TESTS_DIR = REPO_ROOT / "tests"
LOCAL_REDACT = REPO_ROOT / ".sync-redact.local.json"


def load_local_replacements() -> dict[str, str]:
    if not LOCAL_REDACT.is_file():
        return {}
    data = json.loads(LOCAL_REDACT.read_text(encoding="utf-8"))
    repl = data.get("literal_replacements", {})
    return {str(k): str(v) for k, v in repl.items() if k}


def sanitize_text(text: str, local_repls: dict[str, str], wrk_map: dict[str, str], tid_map: dict[str, str]) -> str:
    def _map_wrk(m: re.Match[str]) -> str:
        val = m.group(0)
        if val not in wrk_map:
            wrk_map[val] = f"wrk_fixture_group_{len(wrk_map) + 1:02d}xxxxxxxxxx"[: max(len(val), 24)]
        return wrk_map[val]

    def _map_tid(m: re.Match[str]) -> str:
        val = m.group(0)
        if val not in tid_map:
            tid_map[val] = f"U_FIXTURE_{len(tid_map) + 1:04d}"
        return tid_map[val]

    text = re.sub(r"\bwrk(?!_fixture|_example|_other|Group|Test|FAKE|-)[A-Za-z0-9_-]{18,}\b", _map_wrk, text)
    text = re.sub(r"\bT\d{6,}[A-Z]\b", _map_tid, text)
    text = re.sub(r"'woa\.com',\s*", "", text)
    text = re.sub(r'"woa\.com",\s*', "", text)
    text = re.sub(r"\b[a-z0-9.-]*woa\.com\b", "example.internal", text, flags=re.I)

    for old, new in local_repls.items():
        text = text.replace(old, new)
    return text


def main() -> int:
    parser = argparse.ArgumentParser(description="Sanitize and sync active bridge files into this repository.")
    parser.add_argument("--source", required=True, help="Path to your live local bridge directory.")
    args = parser.parse_args()

    src = Path(args.source).resolve()
    if not src.is_dir():
        print(f"Source directory not found: {src}", file=sys.stderr)
        return 2

    local_repls = load_local_replacements()
    wrk_map: dict[str, str] = {}
    tid_map: dict[str, str] = {}

    synced = 0
    for target_dir in (BRIDGE_DIR, TESTS_DIR):
        for existing in sorted(target_dir.iterdir()):
            if not existing.is_file() or existing.name == "__init__.py":
                continue
            live_file = src / existing.name
            if not live_file.is_file():
                continue
            raw = live_file.read_text(encoding="utf-8", errors="replace")
            cleaned = sanitize_text(raw, local_repls, wrk_map, tid_map)
            existing.write_text(cleaned, encoding="utf-8", newline="\n")
            synced += 1

    print(f"Synced {synced} tracked files from {src}.")
    from sanitize_audit import main as run_audit

    return run_audit()


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    raise SystemExit(main())
