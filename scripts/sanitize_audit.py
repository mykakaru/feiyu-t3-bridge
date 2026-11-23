"""Zero-leak security & sanitization audit gate for feiyu-t3-bridge.

Run before every commit or push:
    python scripts/sanitize_audit.py

Exits 0 only when zero forbidden runtime files, secrets, real WeCom IDs,
hardcoded local usernames, or internal corporate domains exist in the repo.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

FORBIDDEN_EXTENSIONS = {
    ".dpapi",
    ".sqlite3",
    ".sqlite3-shm",
    ".sqlite3-wal",
    ".db",
    ".log",
    ".exe",
    ".dll",
    ".tmp",
    ".signal",
    ".request",
}

FORBIDDEN_FILENAMES = {
    "identity_mapping.json",
    "t3-bridge-config.json",
    "command-routing.json",
    "routing_settings.json",
    "task-navigation-settings.json",
    "bridge-models.json",
    ".env",
    ".sync-redact.local.json",
}

TEXT_EXTENSIONS = {
    ".py", ".cjs", ".js", ".mjs", ".ts", ".cs", ".ps1",
    ".md", ".json", ".toml", ".yaml", ".yml", ".txt", ".sh",
}

# Purely structural patterns (no real PII literals stored in public repo)
FORBIDDEN_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("hardcoded_windows_user_path", re.compile(r"[A-Za-z]:[/\\]Users[/\\](?!YOUR_USER\b|fixture\b|<YOU>)[A-Za-z0-9._-]{3,}", re.I)),
    ("real_wecom_short_tid", re.compile(r"\bT\d{6,}[A-Z]\b")),
    ("real_wecom_chatid", re.compile(r"\bwrk(?!_fixture|_example|_other|Group|Test|FAKE|-)[A-Za-z0-9_-]{18,}\b")),
    ("github_token", re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}\b|\bgithub_pat_[A-Za-z0-9_]{20,}\b")),
    ("private_key_header", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("internal_woa_domain", re.compile(r"\b[a-z0-9.-]*woa\.com\b", re.I)),
    ("tencent_cos_numeric_bucket", re.compile(r"ww-aibot-img-\d{6,}\.cos\.", re.I)),
    ("hardcoded_drive_dev_workspace", re.compile(r"\b[DF]:[/\\]Development2?\b", re.I)),
]


def audit(root: Path = REPO_ROOT) -> list[str]:
    violations: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in (".git", "node_modules", "__pycache__", ".venv", ".pytest_cache")]
        for fn in filenames:
            fp = Path(dirpath) / fn
            rel = fp.relative_to(root).as_posix()

            if fn in FORBIDDEN_FILENAMES:
                violations.append(f"[FORBIDDEN_FILE] {rel}")
                continue
            if fp.suffix.lower() in FORBIDDEN_EXTENSIONS:
                violations.append(f"[FORBIDDEN_EXT] {rel}")
                continue
            if fp.suffix.lower() not in TEXT_EXTENSIONS:
                continue
            if rel == "scripts/sanitize_audit.py":
                continue

            try:
                text = fp.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                violations.append(f"[READ_ERR] {rel}: {exc}")
                continue

            for lineno, line in enumerate(text.splitlines(), 1):
                for name, pat in FORBIDDEN_PATTERNS:
                    if pat.search(line):
                        violations.append(f"[{name}] {rel}:{lineno}")
    return violations


def main() -> int:
    violations = audit()
    if violations:
        print(f"AUDIT FAILED: {len(violations)} potential leak(s) found:", file=sys.stderr)
        for v in violations[:50]:
            print("  -", v, file=sys.stderr)
        if len(violations) > 50:
            print(f"  ... and {len(violations) - 50} more", file=sys.stderr)
        return 1
    print("AUDIT PASSED: 0 leaks / forbidden artifacts detected.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
