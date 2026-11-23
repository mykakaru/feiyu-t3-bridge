"""Convert file references in a verified owner's private result to native WeCom files."""
import hashlib
import ipaddress
from contextlib import contextmanager
from functools import partial
from itertools import groupby
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import subprocess
from tempfile import TemporaryDirectory
import time
from urllib.parse import unquote, urlsplit
import zipfile

from business_queue import db
from identity import CLI_OWNER

TRANSPORT = "native_file_with_download_sha256"
WECOM_FILE_MAX_BYTES = 19_500_000
WECOM_IMAGE_MAX_BYTES = 9_500_000
WECOM_VIDEO_TARGET_BYTES = 17_500_000
IMAGE_EXTS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp"})
VIDEO_EXTS = frozenset({".mp4", ".mov", ".mkv", ".webm", ".avi"})


# Match file destinations as authored, including ordinary parentheses and
# Markdown's angle form for spaces. Keep unrelated remote URLs intact.
LOCAL_PREFIX = r"(?:/?[a-z]:[/\\]|\\\\|(?:file|sandbox|blob|vscode):|codex://(?:open|file)[/?]|/(?:mnt/data|Users|home|tmp)/|(?:\.\.?[/\\]|outputs[/\\]))"
WEB = r"https?://"
PAREN = r"(?:[^()\r\n]|\((?:[^()\r\n]|\([^()\r\n]*\))*\))*"
TOKEN = re.compile(
    r"(?P<fence>^[ \t]{0,3}(?P<delimiter>\x60{3,}|~{3,})[^\r\n]*\r?\n[\s\S]*?^(?P=delimiter)[ \t]*$)"
    r"|(?P<inline>!?\[(?:\\.|[^\]\r\n])*\]\(\s*(?:<(?P<angle>[^>\r\n]+)>|(?P<dest>" + PAREN + r"))\s*\))"
    r"|(?P<definition>^[ \t]*\[[^\]\r\n]+\]:[ \t]*(?:<(?P<ref_angle>[^>\r\n]+)>|(?P<ref>\S+))[^\r\n]*)"
    r"|(?P<wrapped><(?P<autolink>(?:" + LOCAL_PREFIX + "|" + WEB + r")[^>\r\n]+)>)"
    r"|(?P<tick>\x60(?P<codepath>" + LOCAL_PREFIX + r"[^\x60\r\n]+)\x60)"
    r"|(?P<naked>(?<![\w/])(?:" + LOCAL_PREFIX + "|" + WEB + r")[^\s<>）。，；！？\x60\]\x22]+)",
    re.IGNORECASE | re.MULTILINE,
)
LEGACY_TYPES = frozenset((".pdf .doc .docx .ppt .pptx .xls .xlsx .csv .tsv .md .png .jpg .jpeg .webp .gif "
                          ".mp4 .mov .webm .wav .mp3 .fbx .blend .ma .mb .zip .7z").split())
PRIVATE_NAMES = {"agents.md", "request.txt", "result.txt", "events.jsonl", "manifest.json", "source.json",
                 "completion.json", "worker.json", "stderr.log", "task-control.json", "task-control-ack.json",
                 "task-navigation.json", "wecom-readback.json"}
SOURCE_LINE = re.compile(r"^(?:file://)?(?:/?[a-z]:[/\\]|/(?:Users|home|mnt/data|tmp)/)"
                         r"(?:.*[/\\])?(?P<location>[^/\\:\r\n]+\.[^/\\:\r\n]+:\d+(?::\d+)?)$", re.I)
DELIVERY_CLAIM = re.compile(r"(?:附件|原件|文件|卡片).{0,15}(?:已(?:经|成功)?(?:发送|发出|送达|交付|上传)|发送成功)|"
                            r"(?:已(?:经|成功)?(?:发送|发出|送达|交付|上传)|发送成功).{0,15}(?:附件|原件|文件|卡片)")


def local_target(target):
    decoded = unquote(target.strip()).replace(r"\(", "(").replace(r"\)", ")")
    if re.match(LOCAL_PREFIX, decoded, re.I):
        return decoded
    try:
        url = urlsplit(decoded)
        if url.scheme.lower() in ("http", "https"):
            host = (url.hostname or "").rstrip(".").lower()
            if host == "localhost" or host.endswith(".localhost"):
                return decoded
            try:
                address = ipaddress.ip_address(host)
                if address.is_loopback or address.is_unspecified:
                    return decoded
            except ValueError:
                pass
            return None
        if url.scheme or decoded.startswith(("#", "//")):
            return None
    except ValueError:
        return decoded
    return decoded if decoded else None


def references(text):
    """Return offsets and normalized local destinations, in message order."""
    result = []
    for match in TOKEN.finditer(text):
        if match["fence"] is not None:
            continue
        value = next(match[k] for k in ("angle", "dest", "ref_angle", "ref", "autolink", "codepath", "naked")
                     if match[k] is not None).strip()
        if match["dest"] is not None:
            value = re.sub(r"""\s+(?:"[^"]*"|'[^']*')$""", "", value)
        target = local_target(value)
        if target and (match["naked"] is not None or match["codepath"] is not None):
            # A path mentioned in UE instructions or a shell example can itself
            # be the answer. Only treat a plain path as an offered artifact.
            first = text.rfind("\n", 0, match.start()) + 1
            last = text.find("\n", match.end())
            tail = text[match.end():None if last < 0 else last].strip(" \t\r。，.、")
            before = text[first:match.start()].strip()
            offered = re.search(r"附件|打开|下载|交付|领取|原件|源文件|文件(?:在|见|如下)|\b(?:download|attachment|open)\b",before,re.I)
            explicit_uri = re.match(r"(?:file:|sandbox:|blob:|vscode:|https?:|codex://(?:open|file))", target, re.I)
            extension = Path(target.rstrip('.,')).suffix.lower()
            if not explicit_uri and (not extension or (not offered and (before or tail))):
                continue
        if target:
            result.append((match.start(), match.end(), target))
    return result


def plain_source_citations(text):
    """Keep editor line links readable in WeCom without offering their files."""
    def plain(match):
        if match['inline'] is None:
            return match[0]
        target = local_target(match['angle'] or match['dest'] or '')
        source = SOURCE_LINE.fullmatch(target or '')
        if not source:
            return match[0]
        label = re.match(r'!?\[((?:\\.|[^\]\r\n])*)\]', match[0])[1]
        return f"{label}（{source['location']}）" if label else source['location']

    return TOKEN.sub(plain, text)


class FileIssue(Exception):
    """A safe user-facing description. Never include a private path or media ID."""


@contextmanager
def checked_stage(phase):
    try:
        yield
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, sqlite3.Error, subprocess.SubprocessError) as exc:
        # Publish only the phase, exception type and, if present, a numeric system error.
        code = getattr(exc, "winerror", None) or getattr(exc, "errno", None)
        detail = type(exc).__name__ + (f" {code}" if type(code) is int else "")
        raise FileIssue(f"{phase}失败（{detail}）") from None


def workspace(root, cid):
    root = Path(root)
    base_str = None
    for name in ("t3-bridge-config.json", "command-routing.json"):
        p = root / name
        if p.is_file():
            try:
                base_str = json.loads(p.read_text(encoding="utf-8-sig")).get("tasks_root")
                if base_str:
                    break
            except (OSError, ValueError):
                continue
    if not base_str:
        raise FileIssue("任务文件目录不可用")
    base = Path(base_str)
    if not base.is_absolute():
        raise FileIssue("任务文件目录不可用")
    return base / ("business-" + hashlib.sha256(cid.encode()).hexdigest()[:24])


def bound_path(work, target):
    if re.match(r"^\\\\\?\\[a-z]:[/\\]", target, re.I):
        target = target[4:]
    if target.lower().startswith("file:"):
        url = urlsplit(target)
        if url.netloc and url.netloc.lower() != "localhost":
            raise FileIssue("网络共享路径无法作为企微附件来源")
        target = url.path
    if re.match(r"^/[a-z]:[/\\]", target, re.I):
        target = target[1:]
    if re.match(r"^(?:(?:sandbox|blob|vscode|codex|https?):|[/\\]{2}|/(?:mnt/data|home|tmp)/)", target, re.I):
        raise FileIssue("该引用没有提供本事项可上传的源文件")
    # A Codex line anchor is metadata, never a Windows alternate data stream.
    target = re.sub(r"(?<=\w):\d+(?::\d+)?$", "", target)
    p = Path(target)
    if not p.is_absolute():
        p = work / p
    if not p.is_relative_to(work):
        raise FileIssue("源文件不在本事项的交付目录")
    relative = p.relative_to(work)
    if not relative.parts or any(part.startswith(".") or ":" in part or part.lower() in PRIVATE_NAMES
                                 for part in relative.parts):
        raise FileIssue("该引用不是本事项的交付文件")
    explicit = len(relative.parts) > 1 and relative.parts[0].lower() == "outputs"
    if not explicit and (len(relative.parts) != 1 or p.suffix.lower() not in LEGACY_TYPES):
        raise FileIssue("请将要交付的原件放入本事项的 outputs 目录")
    return p


def source_path(work, target):
    p = bound_path(work, target)
    relative = p.relative_to(work)
    current = work
    for part in ("", *relative.parts):
        if part:
            current = current / part
        info = current.lstat()
        if getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT or stat.S_ISLNK(info.st_mode):
            raise FileIssue("源文件引用了任务目录以外的存储位置")
    if not stat.S_ISREG(info.st_mode) or not info.st_size:
        raise FileIssue("源文件为空或不是普通文件")
    return p


def previously_delivered(root, cid, target, work, dest):
    path = bound_path(work, dest)
    key = (cid, hashlib.sha256(target.encode()).hexdigest(), os.path.normcase(str(path.relative_to(work))))
    with db(root) as c:
        exists = c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='private_file_receipts'").fetchone()
        if exists and c.execute("""SELECT 1 FROM private_file_receipts
                        WHERE case_id=? AND audience=? AND path_key=? AND state='accepted'""",key).fetchone():
            return path
    raise FileIssue("找不到本事项要交付的源文件")


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def recent_sources(c, root, case):
    """An object hint for “刚刚的文件”, scoped to prior finals in this exact private chat."""
    rows = c.execute("""SELECT b.id, l.result FROM cases b
        JOIN chat_receipts r ON r.case_id=b.id AND r.phase='final' AND r.state IN ('accepted','file_incomplete')
        JOIN lanes l ON l.case_id=b.id AND l.state='completed'
        WHERE b.chat=? AND b.sender=? AND b.chat=b.sender AND b.id<>? AND r.finished BETWEEN ? AND ?
        ORDER BY r.finished DESC, b.id, l.target LIMIT 12""",
                     (case['chat'], case['sender'], case['id'], case['epoch']-3600, case['epoch']))
    for cid, lanes in groupby(rows, key=lambda row: row['id']):
        refs = [dest for lane in lanes for _, _, dest in references(lane['result'] or '')]
        if not refs:
            continue
        candidates = {}
        for dest in refs:
            try:
                path = source_path(workspace(root, cid), dest)
                candidates[str(path)] = {"case_id": cid, "name": path.name, "source": str(path)}
            except (FileIssue, OSError, KeyError, TypeError, ValueError):
                continue
        # An unavailable newer reference must never silently pick an older version.
        return list(candidates.values())
    return []


def api(call, args, payload):
    return call([*args, "--json", json.dumps(payload, ensure_ascii=False)])


def format_wecom_media_error(code, errmsg='', *, prefix='企微发送附件失败'):
    if code == 40006:
        return f"{prefix}（服务码 40006：文件大小不符合企微限制，单文件上限 20MB、图片上限 10MB、需大于 5 字节）"
    if code == -2:
        return f"{prefix}（服务码 -2：Bot 未连接或未鉴权）"
    if code == -3:
        return f"{prefix}（服务码 -3：上传或发送超时）"
    if code == 893006:
        return f"{prefix}（服务码 893006：CLI 目录权限校验未通过）"
    if type(code) is int:
        detail = f"服务码 {code}"
        cleaned = str(errmsg or '').strip()
        if cleaned and len(cleaned) <= 120 and 'media_id' not in cleaned.lower():
            detail += f"：{cleaned}"
        return f"{prefix}（{detail}）"
    return f"{prefix}（{errmsg or '未知错误'}）"


def _find_ffmpeg_tool(name):
    found = shutil.which(name)
    if found:
        return found
    winget_link = Path.home() / 'AppData/Local/Microsoft/WinGet/Links' / f'{name}.exe'
    if winget_link.is_file():
        return str(winget_link)
    return None


def _probe_video(path, ffprobe_exe):
    if not ffprobe_exe:
        return None, False
    try:
        proc = subprocess.run(
            [ffprobe_exe, '-v', 'error', '-show_entries', 'format=duration:stream=codec_type',
             '-of', 'json', str(path)],
            capture_output=True, text=True, encoding='utf-8', timeout=20, check=False,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
        )
        if proc.returncode != 0:
            return None, False
        data = json.loads(proc.stdout or '{}')
        dur_raw = (data.get('format') or {}).get('duration')
        duration = float(dur_raw) if dur_raw is not None else None
        has_audio = any((s.get('codec_type') == 'audio') for s in (data.get('streams') or []))
        return (duration if duration and duration > 0 else None), has_audio
    except (OSError, ValueError, subprocess.SubprocessError):
        return None, False


def _compress_video(path, temp_dir):
    ffmpeg = _find_ffmpeg_tool('ffmpeg')
    if not ffmpeg:
        raise FileIssue(f"视频《{path.name}》超过企微 20 MB 上限，且本机未找到 ffmpeg 自动压制")
    ffprobe = _find_ffmpeg_tool('ffprobe')
    duration, has_audio = _probe_video(path, ffprobe)
    if not duration:
        duration = 35.0
    out_path = Path(temp_dir) / (path.stem + '.mp4')
    passlog = Path(temp_dir) / 'ffmpeg2pass'
    audio_kbps = 96 if (has_audio and duration <= 120) else (64 if has_audio else 0)
    total_kbps = int((WECOM_VIDEO_TARGET_BYTES * 8) / max(duration, 1.0) / 1000)
    video_kbps = max(120, total_kbps - audio_kbps)
    vf_args = []
    if video_kbps < 350:
        vf_args = ['-vf', 'scale=1280:-2']
    if video_kbps < 180:
        vf_args = ['-vf', 'scale=960:-2']
    audio_args = ['-c:a', 'aac', '-b:a', f'{audio_kbps}k'] if has_audio else ['-an']
    flags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
    common = [ffmpeg, '-y', '-i', str(path), '-c:v', 'libx264', '-preset', 'fast',
              '-pix_fmt', 'yuv420p', *vf_args, '-b:v', f'{video_kbps}k']
    null_sink = 'NUL' if os.name == 'nt' else '/dev/null'
    try:
        p1 = subprocess.run(
            [*common, '-pass', '1', '-passlogfile', str(passlog), '-an', '-f', 'mp4', null_sink],
            capture_output=True, text=True, timeout=180, check=False, creationflags=flags,
        )
        if p1.returncode != 0:
            raise FileIssue(f"视频《{path.name}》自动压制失败（阶段 1 退出码 {p1.returncode}）")
        p2 = subprocess.run(
            [*common, '-pass', '2', '-passlogfile', str(passlog), *audio_args, '-movflags', '+faststart', str(out_path)],
            capture_output=True, text=True, timeout=180, check=False, creationflags=flags,
        )
        if p2.returncode != 0 or not out_path.is_file():
            raise FileIssue(f"视频《{path.name}》自动压制失败（阶段 2 退出码 {p2.returncode}）")
    except subprocess.TimeoutExpired:
        raise FileIssue(f"视频《{path.name}》超过企微 20 MB 上限，自动压制超时") from None
    out_size = out_path.stat().st_size
    if out_size <= 0 or out_size > WECOM_FILE_MAX_BYTES:
        raise FileIssue(
            f"视频《{path.name}》原体积 {path.stat().st_size / (1024 * 1024):.1f} MB，"
            f"自动压制后仍为 {out_size / (1024 * 1024):.1f} MB，超过企微 20 MB 上限"
        )
    return out_path


def _compress_image(path, temp_dir):
    ffmpeg = _find_ffmpeg_tool('ffmpeg')
    if ffmpeg:
        flags = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
        for scale in (None, '2560:-2', '1920:-2'):
            out_path = Path(temp_dir) / (path.stem + '.jpg')
            cmd = [ffmpeg, '-y', '-i', str(path)]
            if scale:
                cmd.extend(['-vf', f'scale={scale}'])
            cmd.extend(['-q:v', '3', str(out_path)])
            try:
                proc = subprocess.run(cmd, capture_output=True, timeout=60, check=False, creationflags=flags)
                if proc.returncode == 0 and out_path.is_file() and 0 < out_path.stat().st_size <= WECOM_FILE_MAX_BYTES:
                    return out_path
            except (OSError, subprocess.SubprocessError):
                break
    return _compress_zip(path, temp_dir)


def _compress_zip(path, temp_dir):
    out_path = Path(temp_dir) / (path.name + '.zip')
    with zipfile.ZipFile(out_path, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.write(path, arcname=path.name)
    out_size = out_path.stat().st_size
    if out_size <= 0 or out_size > WECOM_FILE_MAX_BYTES:
        raise FileIssue(
            f"文件《{path.name}》原体积 {path.stat().st_size / (1024 * 1024):.1f} MB"
            f"（压缩后 {out_size / (1024 * 1024):.1f} MB），超过企微单文件 20 MB 上限"
        )
    return out_path


def prepare_deliverable(path, temp_dir, *, allow_image_as_file=True, allow_lossy_video_compress=False):
    """Ensure a file fits WeCom media limits without destructively re-encoding videos by default."""
    size = path.stat().st_size
    if size <= 0:
        raise FileIssue("源文件为空或不是普通文件")
    suffix = path.suffix.lower()
    if suffix in IMAGE_EXTS:
        if size <= WECOM_IMAGE_MAX_BYTES:
            return path, 'image', None
        if allow_image_as_file and size <= WECOM_FILE_MAX_BYTES:
            return path, 'file', None
        compressed = _compress_image(path, temp_dir)
        out_mb = compressed.stat().st_size / (1024 * 1024)
        src_mb = size / (1024 * 1024)
        mtype = 'image' if (compressed.suffix.lower() in IMAGE_EXTS and compressed.stat().st_size <= WECOM_IMAGE_MAX_BYTES) else 'file'
        return compressed, mtype, f"原图 {src_mb:.1f} MB 超企微上限，已自动压缩至 {out_mb:.1f} MB"
    if size <= WECOM_FILE_MAX_BYTES:
        return path, 'file', None
    src_mb = size / (1024 * 1024)
    if suffix in VIDEO_EXTS:
        if not allow_lossy_video_compress:
            raise FileIssue(
                f"视频《{path.name}》（{src_mb:.1f} MB）超过企微 Bot 单文件 20 MB 上限；"
                "按保原画策略已跳过自动压制并保留本地原片，请由 GPT Computer Use 通过本人企微发至【视频验收群】，"
                "或在生成端按完整段落拆分为 ≤19 MB 分段后重发"
            )
        compressed = _compress_video(path, temp_dir)
        out_mb = compressed.stat().st_size / (1024 * 1024)
        return compressed, 'file', f"原视频 {src_mb:.1f} MB 超企微 20 MB 上限，已自动压制至 {out_mb:.1f} MB"
    compressed = _compress_zip(path, temp_dir)
    out_mb = compressed.stat().st_size / (1024 * 1024)
    return compressed, 'file', f"原文件 {src_mb:.1f} MB 超企微 20 MB 上限，已自动打包压缩至 {out_mb:.1f} MB"


def transfer(call, operation, args, payload, authorized):
    """Retry only temporary-media operations before any visible card is reserved."""
    for attempt in range(3):
        if not authorized():
            raise FileIssue("本次私聊投递授权已变更")
        try:
            data = api(call, args, payload)
            if not isinstance(data, dict):
                raise ValueError("invalid media response")
            code = data.get("errcode", 0)
            if type(code) is not int or data.get("success") is False and not code:
                raise FileIssue(f"企微未确认{operation}")
            if code:
                if code not in (429, 45009, 45011):
                    raise FileIssue(format_wecom_media_error(code, data.get("errmsg", ""), prefix=f"企微{operation}失败"))
                reason = f"服务限流 {code}"
            else:
                return data
        except (TimeoutError, subprocess.TimeoutExpired, ConnectionError) as exc:
            reason = "请求超时" if isinstance(exc, (TimeoutError, subprocess.TimeoutExpired)) else "连接中断"
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
            # Exception messages and CLI output may contain media IDs or local paths.
            cli_exit = re.fullmatch(r"wecom_cli_exit_(-?\d+)", str(exc)) if isinstance(exc, RuntimeError) else None
            detail = "工具退出码 " + cli_exit[1] if cli_exit else type(exc).__name__
            raise FileIssue(f"企微{operation}失败（{detail}）") from None
        if attempt == 2:
            raise FileIssue(f"企微{operation}失败（{reason}，已尝试 3 次）")
        time.sleep(0.3 * 2**attempt)


def install(c):
    c.execute("""CREATE TABLE IF NOT EXISTS private_file_receipts(
        case_id TEXT, audience TEXT, path_key TEXT, name TEXT, sha256 TEXT, bytes INTEGER,
        state TEXT, updated REAL, PRIMARY KEY(case_id,audience,path_key))""")
    c.execute("""CREATE TABLE IF NOT EXISTS private_card_attempts(
        case_id TEXT, audience TEXT, path_key TEXT, attempt INTEGER, state TEXT, errcode INTEGER, at REAL,
        PRIMARY KEY(case_id,audience,path_key,attempt))""")


def file_card(root, cid, target, work, path, call, authorized, *, auth_error="本次私聊投递授权已变更"):
    with checked_stage("源文件读取"):
        size = path.stat().st_size
        digest = sha(path)
    key = (cid, hashlib.sha256(target.encode()).hexdigest(), os.path.normcase(str(path.relative_to(work))))
    with db(root, True) as c:
        install(c)
        previous = c.execute("SELECT sha256,state FROM private_file_receipts WHERE case_id=? AND audience=? AND path_key=?",key).fetchone()
        if previous:
            if previous["sha256"] != digest:
                raise FileIssue("同一文件在上次投递后发生了变化")
            if previous["state"] == "accepted":
                return
            raise FileIssue("上次投递未确认，请核对文件卡片后再补发")
    if not authorized():
        raise FileIssue(auth_error)
    from local_dispatch import cli_call as legacy_cli_call
    from wecom_cli import cli_call as wecom_cli_call
    with TemporaryDirectory(prefix="wecom-file-prep-") as prep_dir:
        send_path, _, _ = prepare_deliverable(path, prep_dir, allow_image_as_file=True)
        send_size = size if send_path == path else send_path.stat().st_size
        send_digest = digest if send_path == path else sha(send_path)
        # The official CLI scopes media input to its child process's working directory.
        # Bind only the previously validated source directory for this upload.
        if call in (wecom_cli_call, legacy_cli_call):
            if send_path == path:
                send_path = source_path(work, str(path))
            upload_call = partial(call, upload_directory=send_path.parent)
        else:
            upload_call = call
        upload = transfer(upload_call, "上传", ["media", "upload"], {"type": "file", "file_path": str(send_path)}, authorized)
        media = upload.get("media_id")
        if upload.get("type") != "file" or not isinstance(media, str) or not media or upload.get("errcode", 0) != 0:
            raise FileIssue("企微未确认上传源文件")
        with checked_stage("回下载文件核验"), TemporaryDirectory(prefix="wecom-file-readback-") as folder:
            download_call = partial(wecom_cli_call, download_directory=Path(folder)) if call is wecom_cli_call else call
            downloaded = transfer(download_call, "回下载", ["media", "download", "--output-dir", folder], {"media_id": media}, authorized)
            saved = downloaded.get("file_path")
            if downloaded.get("errcode", 0) != 0 or not isinstance(saved, str) or not saved:
                raise FileIssue("企微回下载校验失败")
            retrieved = Path(saved)
            if (not retrieved.is_absolute() or not retrieved.parent.samefile(folder)
                    or retrieved.name != send_path.name
                    or not retrieved.is_file() or retrieved.stat().st_size != send_size
                    or sha(retrieved) != send_digest or sha(send_path) != send_digest):
                raise FileIssue("企微回下载内容与源文件不一致")
    if not authorized():
        raise FileIssue(auth_error)
    # Reserve only immediately before the visible send. Network failures after
    # this point are ambiguous; retries must never create a second file card.
    with db(root, True) as c:
        inserted = c.execute("INSERT OR IGNORE INTO private_file_receipts VALUES(?,?,?,?,?,?,'sending',?)",
                             (*key, path.name, digest, size, time.time())).rowcount
        if not inserted:
            state = c.execute("SELECT sha256,state FROM private_file_receipts WHERE case_id=? AND audience=? AND path_key=?",key).fetchone()
            if state["sha256"] == digest and state["state"] == "accepted":
                return
            raise FileIssue("文件投递已登记，结果尚待确认")
    payload = {"chat_id": target, "msg_type": "file", "file": {"media_id": media}}
    for attempt in range(1, 5):
        if not authorized():
            with db(root, True) as c:
                c.execute("UPDATE private_file_receipts SET state='disabled',updated=? WHERE case_id=? AND audience=? AND path_key=?",
                          (time.time(), *key))
            raise FileIssue(auth_error)
        with db(root, True) as c:
            c.execute("INSERT INTO private_card_attempts VALUES(?,?,?,?,'sending',NULL,?)", (*key, attempt, time.time()))
        state, code = "unknown", None
        try:
            response = api(call, ["message", "aibot", "send"], payload)
            if isinstance(response, dict):
                value = response.get("errcode")
                code = value if type(value) is int else None
                if response.get("success") is True and response.get("errcode", 0) == 0:
                    state = "accepted"
                elif response.get("success") is False:
                    state = "throttled" if code in (429, 45009, 45011) and attempt < 4 else "rejected"
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
            # A timeout after the visible send may already have delivered the card.
            pass
        finally:
            with db(root, True) as c:
                c.execute("UPDATE private_card_attempts SET state=?,errcode=?,at=? WHERE case_id=? AND audience=? AND path_key=? AND attempt=?",
                          (state, code, time.time(), *key, attempt))
                if state != "throttled":
                    c.execute("UPDATE private_file_receipts SET state=?, updated=? WHERE case_id=? AND audience=? AND path_key=?",
                              (state, time.time(), *key))
        if state == "accepted":
            return
        if state != "throttled":
            raise FileIssue("企微未确认文件卡片发送成功" + (f"（服务码 {code}）" if code else ""))
        time.sleep(2**attempt)


def _deliver_references(root, cid, target, text, found, call, authorized, *, outputs_only=False, auth_error="本次私聊投递授权已变更"):
    parts = []
    position = 0
    delivered = {}
    definitions = {}
    errors = []
    for start, end, dest in found:
        parts.append(text[position:start])
        definition = re.match(r"^[ \t]*\[([^\]]+)\]:", text[start:end])
        try:
            with checked_stage("交付目录读取"):
                work = workspace(root, cid)
            try:
                path = source_path(work, dest)
                if outputs_only:
                    rel = path.relative_to(work)
                    if not (len(rel.parts) > 1 and rel.parts[0].lower() == "outputs"):
                        raise FileIssue("群聊只允许交付 outputs 目录内的文件")
            except FileNotFoundError:
                path = previously_delivered(root, cid, target, work, dest)
                if outputs_only:
                    rel = path.relative_to(work)
                    if not (len(rel.parts) > 1 and rel.parts[0].lower() == "outputs"):
                        raise FileIssue("群聊只允许交付 outputs 目录内的文件")
            else:
                if path not in delivered:
                    file_card(root, cid, target, work, path, call, authorized, auth_error=auth_error)
            delivered[path] = path.name
            label = "附件《" + path.name + "》"
        except FileIssue as exc:
            errors.append(str(exc))
            label = "（附件未交付）"
        except (OSError, KeyError, TypeError, ValueError, RuntimeError, subprocess.SubprocessError, sqlite3.Error) as exc:
            errors.append(f"交付文件准备失败（{type(exc).__name__}）")
            label = "（附件未交付）"
        if definition:
            definitions[definition[1]] = label
        else:
            parts.append(label)
        position = end
    parts.append(text[position:])
    result = "".join(parts)
    for key, label in definitions.items():
        name = re.escape(key)
        pattern = r"!?\[[^\]\r\n]*\]\[" + name + r"\]|!?\[" + name + r"\](?:\[\]|(?![\[(]))"
        result = re.sub(pattern, lambda _: label, result, flags=re.I)
    if errors:
        # Keep the answer, with the transport ledger as the source of delivery claims.
        result = '\n'.join(line for line in result.splitlines() if not DELIVERY_CLAIM.search(line)).strip()
        sent = ("已发送的文件：" + "、".join(delivered.values()) + "。\n") if delivered else ""
        notice = f"事项〔{cid[-7:]}〕文件交付未完成。\n{sent}" + "。".join(dict.fromkeys(errors)) + "。"
        return notice + ('\n\n' + result if result else ''), False
    return result, True


def private_result(root, cid, target, text, found, call, authorized):
    """Only invoke for a reserved result from the original authenticated owner DM."""
    if not found:
        return text, True
    if target != CLI_OWNER or not authorized():
        raise FileIssue("本次私聊投递授权已变更")
    return _deliver_references(
        root, cid, target, text, found, call, authorized,
        outputs_only=False, auth_error="本次私聊投递授权已变更",
    )


def group_result(root, cid, target, text, found, call, authorized):
    """Deliver verified outputs/ file cards for an owner-authorized group reply."""
    if not found:
        return text, True
    if not isinstance(target, str) or not target or target == CLI_OWNER or not authorized():
        raise FileIssue("本次群聊投递授权已变更")
    return _deliver_references(
        root, cid, target, text, found, call, authorized,
        outputs_only=True, auth_error="本次群聊投递授权已变更",
    )
