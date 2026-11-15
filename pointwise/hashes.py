"""哈希工具：统一使用 SHA-256 作为完整性指纹。

判齐原则：文件名 + 大小只是必要条件，最终以清单中声明的 sha256
与实际字节流哈希一致为准（见 watcher / storage）。
"""
from __future__ import annotations

import hashlib
from pathlib import Path

_CHUNK = 1024 * 1024


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str | Path) -> str:
    """流式计算文件 SHA-256，避免大文件整体读入内存。"""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def verify_file(path: str | Path, expected_sha256: str) -> bool:
    return sha256_file(path) == expected_sha256.lower()
