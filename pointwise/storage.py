"""接收侧内容寻址对象存储。

两种入库方式（由来源是否还可能被改写决定）：

* 独立复制（copy）：来源可写 / 声明为 mutable / 不允许硬链接时，把字节
  完整复制进存储。空间成本 = 文件大小，但来源之后被改写或删除都与
  接收方无关，完整性由入库前后两次 SHA-256 校验保证。
* 可靠引用（hardlink）：清单声明 immutable 且来源文件确实只读、且与存储
  在同一文件系统时，建立硬链接。不额外占用数据块（额外空间 ≈ 0），
  只读位 + 内容寻址 + 哈希校验保证引用可靠；若跨文件系统则自动降级为
  独立复制并在记录中注明。

对象路径按哈希两级分片：objects/ab/cdef...，对象本身不可变，同名内容
天然去重；所有写入都先落 tmp/ 再原子改名，存储目录中永不出现半成品。
"""
from __future__ import annotations

import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

from .hashes import sha256_file


class ObjectError(RuntimeError):
    pass


@dataclass
class PutResult:
    sha256: str
    size: int
    mode: str            # "copy" | "hardlink"
    extra_bytes: int     # 本次入库额外占用的物理字节（硬链接为 0，去重命中也为 0）
    reused: bool         # 是否命中了已有对象（内容去重）
    fallback: str | None  # 降级说明，例如 "cross-device -> copy"


class ObjectStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.objects = self.root / "objects"
        self.tmp = self.root / "tmp"
        self.objects.mkdir(parents=True, exist_ok=True)
        self.tmp.mkdir(parents=True, exist_ok=True)

    # ---------- 路径 ----------
    def object_path(self, sha256: str) -> Path:
        sha256 = sha256.lower()
        return self.objects / sha256[:2] / sha256[2:]

    def exists(self, sha256: str) -> bool:
        return self.object_path(sha256).is_file()

    # ---------- 入库 ----------
    def put(
        self,
        src: str | Path,
        expected_sha256: str,
        *,
        allow_hardlink: bool,
    ) -> PutResult:
        """把 src 收入存储。expected_sha256 来自清单，是完整性的唯一裁决。"""
        src = Path(src)
        expected_sha256 = expected_sha256.lower()
        size = src.stat().st_size

        # 入库前：来源必须先与清单哈希一致（watcher 已算过，这里是防御性复核）
        if sha256_file(src) != expected_sha256:
            raise ObjectError(f"来源哈希与清单不符，拒绝入库: {src}")

        final = self.object_path(expected_sha256)
        if final.exists():
            # 内容寻址去重：同一哈希即同一内容，校验后直接复用，不占额外空间
            if sha256_file(final) != expected_sha256:
                raise ObjectError(f"存储对象损坏（哈希不符）: {final}")
            return PutResult(
                expected_sha256, size,
                _mode_of(final, src), 0, True, None,
            )

        final.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.tmp / f"put-{uuid.uuid4().hex}"
        mode = "copy"
        fallback = None
        try:
            if allow_hardlink:
                try:
                    os.link(src, tmp_path)
                    mode = "hardlink"
                except OSError as exc:
                    # 典型为 EXDEV（跨设备/文件系统），硬链接不成立 -> 降级复制
                    fallback = f"hardlink 不可用({exc.__class__.__name__}) -> 降级独立复制"
                    shutil.copyfile(src, tmp_path)
                    mode = "copy"
            else:
                shutil.copyfile(src, tmp_path)

            # 入库后：对落位对象再做一次哈希校验，然后才原子发布
            if sha256_file(tmp_path) != expected_sha256:
                raise ObjectError(f"落位对象哈希校验失败: {src}")
            os.replace(tmp_path, final)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

        extra = 0 if mode == "hardlink" else size
        return PutResult(expected_sha256, size, mode, extra, False, fallback)

    # ---------- 领取 ----------
    def open_verified(self, sha256: str):
        """打开对象并确认其哈希仍然吻合，返回可读文件对象。"""
        path = self.object_path(sha256)
        if not path.is_file():
            raise ObjectError(f"对象不存在: {sha256}")
        if sha256_file(path) != sha256.lower():
            raise ObjectError(f"对象哈希损坏: {sha256}")
        return open(path, "rb")

    # ---------- 空间统计 ----------
    def stats(self) -> dict:
        """物理口径：按 inode 去重求和（硬链接不重复计），另给逻辑口径对照。"""
        seen_inodes: set[tuple[int, int]] = set()
        physical = 0
        logical = 0
        count = 0
        for p in self.objects.rglob("*"):
            if not p.is_file():
                continue
            st = p.stat()
            count += 1
            logical += st.st_size
            key = (st.st_dev, st.st_ino)
            if key not in seen_inodes:
                seen_inodes.add(key)
                physical += st.st_size
        return {
            "objects": count,
            "logical_bytes": logical,   # 按对象名累加（硬链接会被重复计）
            "physical_bytes": physical,  # 按 inode 去重后的真实占用
        }


def _mode_of(obj: Path, src: Path) -> str:
    """判断已存在对象与来源是否共享 inode（用于去重命中时标注方式）。"""
    try:
        if obj.stat().st_dev == src.stat().st_dev and obj.stat().st_ino == src.stat().st_ino:
            return "hardlink"
    except OSError:
        pass
    return "copy"
