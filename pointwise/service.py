"""接收领域服务：发布、撤回、接收器控制、下游领取。

关键并发保证
------------
* publish 在单事务内切换 publishes 指针，claim 读取指针与对象在同一把
  服务锁内完成：替换进行中的领取要么拿到旧完整版本，要么拿到新完整
  版本，绝不会拿到半个文件或空指针。
* epoch 围栏：只允许发布当前 epoch 的 complete 版本；旧批次更正产生的
  stale 目录没有可发布版本，天然不能覆盖正在发布的新批次。
* 撤回删除整个批次的发布指针并标记 withdrawn；后续更正目录到达会把
  批次重新置为 open（重新逐个发布）。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from .models import DB, now_iso
from .storage import ObjectStore, ObjectError


class ServiceError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass
class ClaimResult:
    stream: object
    relpath: str
    sha256: str
    size: int
    epoch: int
    storage_mode: str
    evidence: dict


class ReceiverService:
    def __init__(self, db: DB, store: ObjectStore):
        self.db = db
        self.store = store
        self._claim_lock = threading.RLock()

    # ---------- 接收器控制 ----------
    def receiver_state(self) -> str:
        return self.db.meta_get("state") or "running"

    def stop_receiver(self) -> str:
        self.db.meta_set("state", "stopped")
        self.db.add_event("receiver", "接收器已停止：观察器暂停扫描，已发布内容仍可领取")
        return "stopped"

    def resume_receiver(self) -> str:
        self.db.meta_set("state", "running")
        self.db.add_event("receiver", "接收器已恢复：观察器继续扫描投递区")
        return "running"

    # ---------- 发布（逐个放出文件） ----------
    def publish_file(self, file_id: int) -> dict:
        with self._claim_lock:
            f = self.db.get_file(file_id)
            if not f:
                raise ServiceError(404, f"文件不存在: id={file_id}")
            batch = self.db.get_batch(f["batch_id"])
            if batch["state"] == "withdrawn":
                raise ServiceError(409, "批次已撤回，不能发布；等待新的更正目录")
            version = self.db.get_open_version(file_id, batch["current_epoch"])
            if not version:
                raise ServiceError(409, "当前 epoch 没有可发布版本")
            if version["state"] != "complete":
                raise ServiceError(
                    409,
                    f"文件尚未判齐（当前状态 {version['state']}），只有 complete 版本可发布",
                )
            old = self.db.get_publish(file_id)
            evidence = {
                "basis": "SHA-256 与当前生效清单一致 + 连续稳定观察 + 对象入库复核",
                "batch_id": f["batch_id"],
                "relpath": f["relpath"],
                "epoch": batch["current_epoch"],
                "manifest_id": version["manifest_id"],
                "version_id": version["id"],
                "sha256": version["sha256"],
                "size": version["size"],
                "storage_mode": version["storage_mode"],
                "extra_bytes": version["extra_bytes"],
                "fallback_note": version["fallback_note"],
                "published_at": now_iso(),
                "replaced": bool(old),
                "previous_epoch": old["epoch"] if old else None,
            }
            self.db.publish(
                file_id=file_id, version_id=version["id"],
                batch_id=f["batch_id"], relpath=f["relpath"],
                epoch=batch["current_epoch"], sha256=version["sha256"],
                storage_mode=version["storage_mode"] or "copy",
                evidence=evidence,
            )
            self.db.add_event(
                "publish",
                f"发布 {f['relpath']} epoch={batch['current_epoch']}"
                + (f"（替换 epoch={old['epoch']} 旧发布）" if old else "（首次发布）"),
                batch_id=f["batch_id"], epoch=batch["current_epoch"],
                file_id=file_id, version_id=version["id"], detail=evidence,
            )
            return evidence

    def withdraw_batch(self, batch_id: str, reason: str) -> None:
        with self._claim_lock:
            batch = self.db.get_batch(batch_id)
            if not batch:
                raise ServiceError(404, f"批次不存在: {batch_id}")
            if batch["state"] == "withdrawn":
                raise ServiceError(409, "批次已处于撤回状态")
            self.db.withdraw_batch(batch_id, reason)
            self.db.add_event(
                "withdraw",
                f"撤回批次 {batch_id} 的全部目录发布（原因：{reason}）；"
                "历史记录保留，下游不再可领取",
                batch_id=batch_id, epoch=batch["current_epoch"],
                detail={"reason": reason},
            )

    # ---------- 下游领取 ----------
    def claim(self, file_id: int) -> ClaimResult:
        with self._claim_lock:
            pub = self.db.get_publish(file_id)
            if not pub:
                raise ServiceError(404, "该文件尚未发布（或发布已随批次撤回）")
            batch = self.db.get_batch(pub["batch_id"])
            if batch["state"] == "withdrawn":
                # 防御：withdraw 已删指针，正常走不到这里
                raise ServiceError(410, "批次已撤回，发布不可领取")
            self.db.add_event(
                "claim",
                f"下游领取 {pub['relpath']} epoch={pub['epoch']} "
                f"（{pub['storage_mode']}，sha256={pub['sha256'][:12]}…）",
                batch_id=pub["batch_id"], epoch=pub["epoch"], file_id=file_id,
                version_id=pub["version_id"],
            )
            try:
                stream = self.store.open_verified(pub["sha256"])
            except ObjectError as exc:
                raise ServiceError(500, f"存储对象校验失败: {exc}")
            import json
            import os
            return ClaimResult(
                stream=stream, relpath=pub["relpath"], sha256=pub["sha256"],
                size=os.fstat(stream.fileno()).st_size,
                epoch=pub["epoch"], storage_mode=pub["storage_mode"],
                evidence=json.loads(pub["evidence"]),
            )

    # ---------- 汇总视图 ----------
    def overview(self) -> dict:
        batches = []
        for b in self.db.list_batches():
            files = []
            for f in self.db.list_files(b["batch_id"]):
                versions = [dict(v) for v in self.db.list_versions(f["id"])]
                pub = self.db.get_publish(f["id"])
                files.append({
                    "file": dict(f),
                    "versions": versions,
                    "publish": dict(pub) if pub else None,
                })
            batches.append({
                "batch": dict(b),
                "manifests": [dict(m) for m in self.db.list_manifests(b["batch_id"])],
                "files": files,
            })
        return {
            "receiver_state": self.receiver_state(),
            "store": self.store.stats(),
            "batches": batches,
            "events": [dict(e) for e in self.db.list_events(120)],
        }
