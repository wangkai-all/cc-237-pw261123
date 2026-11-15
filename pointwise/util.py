"""文件系统与通用小工具。"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

WRITE_BITS = stat_WUSR = 0o222  # 任意写位（owner/group/other）


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    """同目录临时文件 + os.replace 原子落位，读取方只会看到完整文件。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def atomic_write_json(path: str | Path, obj) -> None:
    atomic_write_bytes(path, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"))


def is_writable(path: str | Path) -> bool:
    """真实写位判断：mode 中任一写位即视为来源仍可改写。"""
    return bool(Path(path).stat().st_mode & WRITE_BITS)


def set_readonly(path: str | Path) -> None:
    """去掉所有写位；immutable 来源靠它（配合硬链接）构成可靠引用。"""
    os.chmod(path, Path(path).stat().st_mode & ~WRITE_BITS)


def set_writable(path: str | Path) -> None:
    Path(path).chmod(Path(path).stat().st_mode | 0o200)


def same_fs(a: str | Path, b: str | Path) -> bool:
    try:
        return Path(a).stat().st_dev == Path(b).stat().st_dev
    except OSError:
        return False
