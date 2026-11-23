"""Thin wrapper around the official wecom-unified CLI for verified outbound delivery."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

from bridge_console import background_console

LOCAL = Path(os.environ.get('LOCALAPPDATA', str(Path.home() / 'AppData/Local')))
CLI = Path(os.environ.get('WECOM_CLI_SCRIPT', str(Path.home() / '.codex/skills/wecom-unified/scripts/run_cli.py')))


def cli_call(args, *, upload_directory=None, download_directory=None):
    if upload_directory is not None and download_directory is not None:
        raise ValueError('scope_conflict')
    if upload_directory is not None:
        if len(args) != 4 or args[:3] != ['media', 'upload', '--json']:
            raise ValueError('scope_requires_media_upload')
        source = Path(json.loads(args[3])['file_path'])
        if not source.is_absolute() or source.parent != upload_directory:
            raise ValueError('upload_source_directory_mismatch')
    if download_directory is not None:
        if len(args) != 6 or args[:3] != ['media', 'download', '--output-dir'] or args[4] != '--json':
            raise ValueError('scope_requires_media_download')
        target_dir = Path(args[3])
        if not target_dir.is_absolute() or target_dir != Path(download_directory):
            raise ValueError('download_target_directory_mismatch')
    timeout = 300 if args[:2] in (['media', 'upload'], ['media', 'download']) else 40
    process = subprocess.run(
        [sys.executable, str(CLI), *args],
        capture_output=True,
        encoding='utf-8',
        timeout=timeout,
        cwd=upload_directory or download_directory,
        env=dict(os.environ, LOCALAPPDATA=str(LOCAL), PYTHONIOENCODING='utf-8'),
        **background_console(),
    )
    if process.returncode:
        try:
            data = json.loads(process.stdout)
        except ValueError:
            data = None
        if isinstance(data, dict) and data.get('success') is False and type(data.get('errcode')) is int:
            return {'success': False, 'errcode': data['errcode']}
        if args[:2] in (['media', 'upload'], ['media', 'download']) and isinstance(data, dict):
            error = data.get('error')
            if isinstance(error, dict) and type(error.get('code')) is int:
                return {'success': False, 'errcode': error['code']}
        raise RuntimeError('wecom_cli_exit_' + str(process.returncode))
    return json.loads(process.stdout)
