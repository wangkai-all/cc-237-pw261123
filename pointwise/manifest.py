"""交付清单（目录通知）的构造与校验，以及模拟器的确定性内容生成。

清单 JSON 结构::

    {
      "schema": "pointwise-manifest/1",
      "batch_id": "batch-a",
      "title": "批次 A",
      "epoch": 2,                       # 0 起，目录更正每次 +1
      "source_mode": "mutable",        # mutable | immutable
      "created_at": "...",
      "files": [
        {"relpath": "a/one.txt", "size": 120, "sha256": "..."}
      ]
    }

数据文件落在 dropzone/<batch_id>/data/<relpath>；清单落在
dropzone/<batch_id>/manifest-<epoch>.json，经临时文件原子改名发布。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .util import atomic_write_bytes

SCHEMA_NAME = "pointwise-manifest/1"
VALID_MODES = ("mutable", "immutable")
HEX64 = set("0123456789abcdef")


class ManifestError(ValueError):
    pass


def deterministic_content(batch_id: str, relpath: str, epoch: int, size: int) -> bytes:
    """可重复生成的确定性字节：同一参数两次生成哈希必然相同。"""
    out = bytearray()
    counter = 0
    header = f"pointwise|{batch_id}|{relpath}|epoch={epoch}|".encode("utf-8")
    while len(out) < size:
        out.extend(hashlib.sha256(header + counter.to_bytes(8, "little")).digest())
        counter += 1
    return bytes(out[:size])


def build_manifest(*, batch_id: str, title: str, epoch: int,
                   source_mode: str, files: list[dict]) -> dict:
    payload = {
        "schema": SCHEMA_NAME,
        "batch_id": batch_id,
        "title": title,
        "epoch": epoch,
        "source_mode": source_mode,
        "created_at": "",
        "files": files,
    }
    validate_manifest(payload)
    return payload


def validate_manifest(obj) -> dict:
    """校验解析后的清单；非法时抛 ManifestError，返回归一化说明。"""
    if not isinstance(obj, dict):
        raise ManifestError("清单根节点必须是 JSON 对象")
    if obj.get("schema") != SCHEMA_NAME:
        raise ManifestError(f"不支持的 schema: {obj.get('schema')!r}")
    batch_id = obj.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id:
        raise ManifestError("batch_id 缺失或非法")
    epoch = obj.get("epoch")
    if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 0:
        raise ManifestError("epoch 必须是非负整数")
    mode = obj.get("source_mode")
    if mode not in VALID_MODES:
        raise ManifestError(f"source_mode 必须是 {VALID_MODES} 之一")
    files = obj.get("files")
    if not isinstance(files, list):
        raise ManifestError("files 必须是数组")
    seen: set[str] = set()
    total = 0
    for i, item in enumerate(files):
        if not isinstance(item, dict):
            raise ManifestError(f"files[{i}] 必须是对象")
        rel = item.get("relpath")
        if not isinstance(rel, str) or not rel or rel.startswith("/") or ".." in Path(rel).parts:
            raise ManifestError(f"files[{i}].relpath 非法: {rel!r}")
        if rel in seen:
            raise ManifestError(f"files[{i}].relpath 重复: {rel}")
        seen.add(rel)
        size = item.get("size")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ManifestError(f"files[{i}].size 非法")
        digest = item.get("sha256")
        if not isinstance(digest, str) or len(digest) != 64 or not set(digest) <= HEX64:
            raise ManifestError(f"files[{i}].sha256 必须是 64 位小写十六进制")
        total += size
    return {"file_count": len(files), "total_bytes": total}


def manifest_path(dropzone: str | Path, batch_id: str, epoch: int) -> Path:
    return Path(dropzone) / batch_id / f"manifest-{epoch}.json"


def data_path(dropzone: str | Path, batch_id: str, relpath: str) -> Path:
    return Path(dropzone) / batch_id / "data" / relpath


def write_manifest(path: str | Path, manifest: dict) -> None:
    """原子写清单：先写 .tmp 再 rename，观察方永远不会读到半个 JSON。"""
    atomic_write_bytes(path, json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"))


def try_load_manifest(path: str | Path) -> tuple[dict | None, str]:
    """尝试读取清单。返回 (对象, 状态说明)；写到一半时对象为 None。"""
    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        return None, f"读取失败: {exc}"
    try:
        obj = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, "JSON 不完整或非法（清单仍在写入）"
    try:
        validate_manifest(obj)
    except ManifestError as exc:
        return obj, f"清单模式非法: {exc}"
    return obj, "ok"
