"""目录观察器：把“目录通知 + 文件观察 + 接收存储”串起来。

判齐不靠固定等待时间，也不靠文件名/大小：
  1. 清单必须先能解析并通过模式校验（writing 与 corrupt 区分）；
  2. 数据文件大小与清单一致后，还要连续 STABLE_TICKS 个扫描周期
     （mtime_ns + size 都不变）才算“稳定”，避免读到写到一半的文件；
  3. 稳定后实际计算 SHA-256，与清单声明一致才 complete 并入对象存储；
     大小一致而哈希不符是 corrupt（目录更正与写到一半因此是两类状态）。

epoch 围栏：只处理批次当前 epoch 的清单与文件；更旧的更正目录标记为
stale，绝不覆盖当前目录。

入库方式：immutable 清单 + 来源真实只读 + 同文件系统 => 硬链接可靠引用；
其余一律独立复制（mutable 来源随时可能被改写）。
"""
from __future__ import annotations

import re
import threading
import time
from pathlib import Path

from .hashes import sha256_file
from .manifest import data_path, try_load_manifest
from .models import DB
from .storage import ObjectStore, ObjectError
from .util import is_writable

MANIFEST_RE = re.compile(r"^manifest-(\d+)\.json$")
STABLE_TICKS = 2          # 连续几个扫描周期 size+mtime 不变才允许哈希
TICK_SECONDS = 1.0


class Watcher:
    def __init__(self, db: DB, store: ObjectStore, dropzone: str | Path,
                 tick_seconds: float = TICK_SECONDS):
        self.db = db
        self.store = store
        self.dropzone = Path(dropzone)
        self.tick_seconds = tick_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # 稳定性观察的内存账本：key -> (size, mtime_ns, 连续不变次数)
        self._stable: dict[str, tuple[int, int, int]] = {}
        # 已判齐版本的来源 inode 账本：version_id -> (dev, ino)，用于识别同名替换
        self._inode_ledger: dict[int, tuple[int, int]] = {}

    # ---------- 线程控制 ----------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="pointwise-watcher",
                                        daemon=True)
        self._thread.start()

    def stop_thread(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.db.meta_get("state") == "running":
                    self.scan_once()
            except Exception as exc:  # 观察器不能因单次异常死掉
                self.db.add_event("watcher_error", f"扫描异常: {exc!r}")
            self._stop.wait(self.tick_seconds)

    # ---------- 单次扫描 ----------
    def scan_once(self) -> None:
        scan_id = self.db.next_scan_id()
        self.dropzone.mkdir(parents=True, exist_ok=True)
        for batch_dir in sorted(p for p in self.dropzone.iterdir() if p.is_dir()):
            self._scan_batch(batch_dir, scan_id)

    def _scan_batch(self, batch_dir: Path, scan_id: int) -> None:
        batch_id = batch_dir.name
        manifest_files = sorted(
            (p for p in batch_dir.iterdir() if p.is_file() and MANIFEST_RE.match(p.name)),
            key=lambda p: int(MANIFEST_RE.match(p.name).group(1)),
        )
        if not manifest_files:
            return

        current_epoch = None
        batch = self.db.get_batch(batch_id)
        if batch:
            current_epoch = batch["current_epoch"]

        for mp in manifest_files:
            epoch = int(MANIFEST_RE.match(mp.name).group(1))
            self._scan_manifest(batch_id, epoch, mp, scan_id, current_epoch)
            # 刷新围栏：本轮可能刚把某个 epoch 置为当前
            batch = self.db.get_batch(batch_id)
            current_epoch = batch["current_epoch"] if batch else current_epoch

        batch = self.db.get_batch(batch_id)
        if not batch or batch["current_epoch"] < 0:
            return
        self._scan_data(batch_dir, batch, scan_id)

    # ---------- 清单 ----------
    def _scan_manifest(self, batch_id: str, epoch: int, path: Path,
                       scan_id: int, current_epoch: int | None) -> None:
        st = path.stat()
        obj, note = try_load_manifest(path)

        if obj is None:
            # JSON 还不完整：清单写到一半（与“目录更正”明确区分）
            row, created = self.db.upsert_manifest(
                batch_id=batch_id, epoch=epoch, path=str(path), size=st.st_size,
                sha256="", source_mode="unknown", state="writing", note=note,
            )
            if created:
                self.db.add_event("manifest_writing",
                                  f"批次 {batch_id} epoch={epoch} 清单正在写入",
                                  batch_id=batch_id, epoch=epoch)
            else:
                self.db.set_manifest_state(row["id"], "writing", note)
            self.db.add_observation(kind="manifest", ref_id=row["id"], scan_id=scan_id,
                                    observed_size=st.st_size, expected_size=-1,
                                    mtime_ns=st.st_mtime_ns, note=note)
            return

        if note != "ok":
            # 能解析但模式非法
            row, created = self.db.upsert_manifest(
                batch_id=batch_id, epoch=epoch, path=str(path), size=st.st_size,
                sha256=sha256_file(path), source_mode=obj.get("source_mode", "unknown"),
                state="corrupt", note=note,
            )
            if created:
                self.db.add_event("manifest_corrupt",
                                  f"批次 {batch_id} epoch={epoch} 清单模式非法: {note}",
                                  batch_id=batch_id, epoch=epoch)
            return

        mode = obj["source_mode"]
        digest = sha256_file(path)
        row, created = self.db.upsert_manifest(
            batch_id=batch_id, epoch=epoch, path=str(path), size=st.st_size,
            sha256=digest, source_mode=mode, state="received", note="",
        )

        # epoch 围栏：更旧的更正目录只能成为 stale
        if current_epoch is not None and epoch < current_epoch:
            if row["state"] != "stale":
                self.db.set_manifest_state(
                    row["id"], "stale",
                    f"旧批次更正（epoch={epoch}）到达时当前已为 epoch={current_epoch}，拒绝覆盖",
                )
                self.db.add_event(
                    "manifest_stale",
                    f"批次 {batch_id} epoch={epoch} 为过期更正，当前 epoch={current_epoch}，已忽略",
                    batch_id=batch_id, epoch=epoch,
                )
            return

        if created:
            self.db.get_or_create_batch(batch_id, mode, obj.get("title", ""))
            self.db.set_batch_current(batch_id, epoch, row["id"])
            self.db.add_event(
                "manifest_received",
                f"批次 {batch_id} 目录通知 epoch={epoch} 生效（{mode}，"
                f"{len(obj['files'])} 个文件声明）",
                batch_id=batch_id, epoch=epoch,
                detail={"files": obj["files"]},
            )
            # 为清单中每个声明建立文件与 writing 版本行
            for item in obj["files"]:
                f = self.db.get_or_create_file(batch_id, item["relpath"])
                if not self.db.get_open_version(f["id"], epoch):
                    self.db.create_version(
                        file_id=f["id"], epoch=epoch, size=item["size"],
                        sha256=item["sha256"], state="writing",
                        manifest_id=row["id"], note="已声明，等待数据到达",
                    )

    # ---------- 数据文件 ----------
    def _scan_data(self, batch_dir: Path, batch, scan_id: int) -> None:
        batch_id = batch["batch_id"]
        epoch = batch["current_epoch"]
        manifest_row = self.db.get_manifest(batch_id, epoch)
        manifest_obj, _ = try_load_manifest(manifest_row["path"])
        if not manifest_obj:
            return
        declared = {f["relpath"]: f for f in manifest_obj["files"]}

        for relpath, item in declared.items():
            f = self.db.get_or_create_file(batch_id, relpath)
            version = self.db.get_open_version(f["id"], epoch)
            if version is None:
                version = self.db.create_version(
                    file_id=f["id"], epoch=epoch, size=item["size"],
                    sha256=item["sha256"], state="writing",
                    manifest_id=manifest_row["id"], note="清单补登记",
                )

            src = data_path(self.dropzone, batch_id, relpath)
            # 发送方慢写时字节落在 .<name>.writing 临时文件中：把它也纳入观察，
            # 这样“写到一半”体现为观察到的大小逐步增长，而不是笼统的“尚未到达”。
            tmp_src = src.parent / f".{src.name}.writing"
            observed = src if src.exists() else (tmp_src if tmp_src.exists() else None)
            st = observed.stat() if observed is not None else None
            key = f"{batch_id}/{relpath}@{epoch}"

            # 已判齐的版本仍持续监督：同名替换（新 inode）必须重新判齐，
            # 但在新版本 complete 前，旧发布指针保持不动，下游仍拿到旧完整版。
            if version["state"] == "complete" and st is not None:
                observed_key = (st.st_dev, st.st_ino)
                stored_key = self._inode_ledger.get(version["id"])
                if stored_key is None:
                    self._inode_ledger[version["id"]] = observed_key  # 首次登记
                elif stored_key != observed_key:
                    self.db.add_event(
                        "file_replaced",
                        f"文件 {relpath} 检测到同名替换（inode 变化），重新判齐；"
                        "替换完成前下游仍领取旧完整版本",
                        batch_id=batch_id, epoch=epoch, file_id=f["id"],
                        version_id=version["id"],
                    )
                    self._stable.pop(key, None)
                    self.db.supersede_versions(f["id"], epoch, version["id"])
                    version = self.db.create_version(
                        file_id=f["id"], epoch=epoch, size=item["size"],
                        sha256=item["sha256"], state="writing",
                        manifest_id=manifest_row["id"],
                        note="检测到同名替换（新 inode），重新观察判齐",
                    )
                    # 落到下面的统一观察流程
                else:
                    continue  # inode 未变，已入库对象不受影响

            if st is None:
                self._stable.pop(key, None)
                if version["state"] != "writing":
                    self.db.update_version(version["id"], state="writing",
                                           note="来源文件消失，等待重新到达")
                self._note_version(version, "数据文件尚未到达",
                                   scan_id, None, item["size"])
                continue

            writing_tmp = observed != src
            if st.st_size != item["size"]:
                # 大小不符：还在写（含写到一半的临时文件），重置稳定性
                self._stable.pop(key, None)
                note = (f"写到一半：临时文件 {st.st_size}/{item['size']} 字节"
                        if writing_tmp
                        else f"大小 {st.st_size}/{item['size']}，仍在写入")
                if version["state"] != "writing":
                    self.db.update_version(version["id"], state="writing",
                                           note="文件被重写，大小与清单不符")
                self._note_version(version, note, scan_id, st, item["size"])
                continue

            # 大小一致：进入稳定性观察（连续 STABLE_TICKS 周期 size+mtime 不变）
            prev = self._stable.get(key)
            if prev and prev[0] == st.st_size and prev[1] == st.st_mtime_ns:
                ticks = prev[2] + 1
            else:
                ticks = 1
            self._stable[key] = (st.st_size, st.st_mtime_ns, ticks)
            self._note_version(version, f"大小吻合，稳定性 {ticks}/{STABLE_TICKS}",
                               scan_id, st, item["size"])
            if ticks < STABLE_TICKS:
                if version["state"] != "writing":
                    self.db.update_version(version["id"], state="writing",
                                           note="重写后重新观察稳定性")
                continue

            # 稳定 -> 哈希裁决（文件名+大小不算数）
            actual = sha256_file(src)
            if actual != item["sha256"]:
                self.db.update_version(
                    version["id"], state="corrupt",
                    note=f"大小正确但 SHA-256 不符（期望 {item['sha256'][:12]}…，"
                         f"实际 {actual[:12]}…）",
                )
                self.db.add_event(
                    "file_corrupt",
                    f"文件 {relpath} 内容损坏：哈希与清单不符",
                    batch_id=batch_id, epoch=epoch, file_id=f["id"],
                    version_id=version["id"],
                )
                continue

            # 完整 -> 决定入库方式
            allow_hardlink, reason = self._hardlink_allowed(manifest_obj, src)
            try:
                result = self.store.put(src, item["sha256"],
                                        allow_hardlink=allow_hardlink)
            except ObjectError as exc:
                self.db.add_event("store_error", f"入库失败 {relpath}: {exc}",
                                  batch_id=batch_id, epoch=epoch)
                continue

            note = reason if result.mode == "hardlink" else (
                result.fallback or "来源可改写（或声明 mutable），采用独立复制"
            )
            self.db.update_version(
                version["id"], state="complete", storage_mode=result.mode,
                extra_bytes=result.extra_bytes, fallback_note=result.fallback or "",
                note=note,
            )
            self.db.supersede_versions(f["id"], epoch, version["id"])
            self._stable.pop(key, None)
            self._inode_ledger[version["id"]] = (st.st_dev, st.st_ino)
            self.db.add_event(
                "file_complete",
                f"文件 {relpath} 已判齐入库（{result.mode}，"
                f"额外空间 {result.extra_bytes} 字节）",
                batch_id=batch_id, epoch=epoch, file_id=f["id"],
                version_id=version["id"],
                detail={"storage_mode": result.mode, "reused": result.reused,
                        "fallback": result.fallback, "sha256": actual},
            )

    def _hardlink_allowed(self, manifest_obj: dict, src: Path) -> tuple[bool, str]:
        if manifest_obj["source_mode"] != "immutable":
            return False, "清单声明 mutable：来源可能被改写"
        if is_writable(src):
            return False, "来源仍带写位：immutable 尚未封口，不能可靠引用"
        return True, "immutable 且来源只读：硬链接可靠引用（空间≈0）"

    def _note_version(self, version, note: str, scan_id: int, st, expected_size: int) -> None:
        self.db.update_version(version["id"], note=note)
        self.db.add_observation(
            kind="data", ref_id=version["id"], scan_id=scan_id,
            observed_size=st.st_size if st else -1, expected_size=expected_size,
            mtime_ns=st.st_mtime_ns if st else -1, note=note,
        )
