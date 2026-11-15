"""接收存储：SQLite 状态库。

线程模型：HTTP 工作线程与观察器线程共用一个连接，所有公开方法在
RLock 内执行；写操作使用 WAL + 短事务，发布指针的切换在单事务内完成，
下游领取只会读到已提交的完整版本。

状态词汇
--------
清单 manifests.state：
  writing   清单文件还在写（JSON 不完整，无法解析）
  received  已解析、校验通过，是该批次当前生效的目录
  stale     旧 epoch 的更正/重复投递，不允许覆盖当前目录
  corrupt   能解析但模式非法（字段缺失/哈希长度错误等）

文件版本 file_versions.state：
  writing     未到齐：缺失、大小不符，或还没通过稳定性观察
  corrupt     大小已吻合但哈希与清单不符（内容损坏）
  complete    哈希校验通过并已收入对象存储
  superseded  同 (文件, epoch) 被后续观察结论替代
批次 batches.state：open / withdrawn
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    source_mode TEXT NOT NULL DEFAULT 'mutable',
    current_epoch INTEGER NOT NULL DEFAULT -1,
    current_manifest_id INTEGER,
    state TEXT NOT NULL DEFAULT 'open',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    withdrawn_at TEXT,
    withdraw_reason TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    source_mode TEXT NOT NULL,
    state TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    first_seen TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(batch_id, epoch)
);
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    relpath TEXT NOT NULL,
    current_version_id INTEGER,
    created_at TEXT NOT NULL,
    UNIQUE(batch_id, relpath)
);
CREATE TABLE IF NOT EXISTS file_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id),
    epoch INTEGER NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    state TEXT NOT NULL,
    storage_mode TEXT,
    extra_bytes INTEGER NOT NULL DEFAULT 0,
    fallback_note TEXT NOT NULL DEFAULT '',
    manifest_id INTEGER REFERENCES manifests(id),
    first_seen TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,              -- manifest | data
    ref_id INTEGER,                  -- manifests.id 或 file_versions.id
    scan_id INTEGER NOT NULL,
    observed_size INTEGER NOT NULL,
    expected_size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS publishes (
    file_id INTEGER PRIMARY KEY REFERENCES files(id),
    version_id INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    relpath TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    storage_mode TEXT NOT NULL,
    evidence TEXT NOT NULL,         -- 发布依据 JSON
    published_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    kind TEXT NOT NULL,
    batch_id TEXT,
    epoch INTEGER,
    file_id INTEGER,
    version_id INTEGER,
    message TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT ''
);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DB:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            if self.meta_get("state") is None:
                self.meta_set("state", "running")
                self.meta_set("created_at", now_iso())
                self.meta_set("scan_id", "0")

    # ---------- 基础 ----------
    def _execute(self, sql: str, params: tuple = ()):
        return self._conn.execute(sql, params)

    def meta_get(self, key: str) -> str | None:
        with self._lock:
            row = self._execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row["value"] if row else None

    def meta_set(self, key: str, value: str) -> None:
        with self._lock:
            self._execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def next_scan_id(self) -> int:
        with self._lock:
            cur = self._execute("UPDATE meta SET value=value+1 WHERE key='scan_id' "
                                "RETURNING value")
            return int(cur.fetchone()["value"])

    def add_event(self, kind: str, message: str, *, batch_id: str | None = None,
                  epoch: int | None = None, file_id: int | None = None,
                  version_id: int | None = None, detail: dict | None = None) -> None:
        with self._lock:
            self._execute(
                "INSERT INTO events(at, kind, batch_id, epoch, file_id, version_id, "
                "message, detail) VALUES(?,?,?,?,?,?,?,?)",
                (now_iso(), kind, batch_id, epoch, file_id, version_id, message,
                 json.dumps(detail or {}, ensure_ascii=False)),
            )

    def list_events(self, limit: int = 100) -> list[sqlite3.Row]:
        with self._lock:
            return self._execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    # ---------- 批次 ----------
    def get_or_create_batch(self, batch_id: str, source_mode: str, title: str) -> sqlite3.Row:
        with self._lock:
            row = self._execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if row:
                return row
            ts = now_iso()
            self._execute(
                "INSERT INTO batches(batch_id, title, source_mode, state, "
                "created_at, updated_at) VALUES(?,?,?,?,?,?)",
                (batch_id, title, source_mode, "open", ts, ts),
            )
            return self._execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()

    def get_batch(self, batch_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()

    def list_batches(self) -> list[sqlite3.Row]:
        with self._lock:
            return self._execute("SELECT * FROM batches ORDER BY created_at").fetchall()

    def set_batch_current(self, batch_id: str, epoch: int, manifest_id: int) -> None:
        with self._lock:
            self._execute(
                "UPDATE batches SET current_epoch=?, current_manifest_id=?, "
                "state='open', withdrawn_at=NULL, withdraw_reason='', updated_at=? "
                "WHERE batch_id=?",
                (epoch, manifest_id, now_iso(), batch_id),
            )

    def withdraw_batch(self, batch_id: str, reason: str) -> None:
        with self._lock:
            ts = now_iso()
            self._execute(
                "UPDATE batches SET state='withdrawn', withdrawn_at=?, "
                "withdraw_reason=?, updated_at=? WHERE batch_id=?",
                (ts, reason, ts, batch_id),
            )
            self._execute("DELETE FROM publishes WHERE batch_id=?", (batch_id,))

    # ---------- 清单 ----------
    def upsert_manifest(self, *, batch_id: str, epoch: int, path: str, size: int,
                        sha256: str, source_mode: str, state: str, note: str = ""
                        ) -> tuple[sqlite3.Row, bool]:
        """返回 (行, 是否新建)。同批次同 epoch 唯一。"""
        with self._lock:
            existing = self._execute(
                "SELECT * FROM manifests WHERE batch_id=? AND epoch=?", (batch_id, epoch)
            ).fetchone()
            ts = now_iso()
            if existing:
                return existing, False
            self._execute(
                "INSERT INTO manifests(batch_id, epoch, path, size, sha256, "
                "source_mode, state, note, first_seen, updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (batch_id, epoch, path, size, sha256, source_mode, state, note, ts, ts),
            )
            row = self._execute(
                "SELECT * FROM manifests WHERE batch_id=? AND epoch=?", (batch_id, epoch)
            ).fetchone()
            return row, True

    def set_manifest_state(self, manifest_id: int, state: str, note: str = "") -> None:
        with self._lock:
            self._execute(
                "UPDATE manifests SET state=?, note=?, updated_at=? WHERE id=?",
                (state, note, now_iso(), manifest_id),
            )

    def get_manifest(self, batch_id: str, epoch: int) -> sqlite3.Row | None:
        with self._lock:
            return self._execute(
                "SELECT * FROM manifests WHERE batch_id=? AND epoch=?", (batch_id, epoch)
            ).fetchone()

    def list_manifests(self, batch_id: str | None = None) -> list[sqlite3.Row]:
        with self._lock:
            if batch_id:
                return self._execute(
                    "SELECT * FROM manifests WHERE batch_id=? ORDER BY epoch", (batch_id,)
                ).fetchall()
            return self._execute("SELECT * FROM manifests ORDER BY id").fetchall()

    # ---------- 文件与版本 ----------
    def get_or_create_file(self, batch_id: str, relpath: str) -> sqlite3.Row:
        with self._lock:
            row = self._execute(
                "SELECT * FROM files WHERE batch_id=? AND relpath=?", (batch_id, relpath)
            ).fetchone()
            if row:
                return row
            ts = now_iso()
            self._execute(
                "INSERT INTO files(batch_id, relpath, created_at) VALUES(?,?,?)",
                (batch_id, relpath, ts),
            )
            return self._execute(
                "SELECT * FROM files WHERE batch_id=? AND relpath=?", (batch_id, relpath)
            ).fetchone()

    def get_file(self, file_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self._execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()

    def list_files(self, batch_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return self._execute(
                "SELECT * FROM files WHERE batch_id=? ORDER BY relpath", (batch_id,)
            ).fetchall()

    def get_open_version(self, file_id: int, epoch: int) -> sqlite3.Row | None:
        """取该 (文件, epoch) 尚未被替代的版本行。"""
        with self._lock:
            return self._execute(
                "SELECT * FROM file_versions WHERE file_id=? AND epoch=? "
                "AND state != 'superseded' ORDER BY id DESC LIMIT 1",
                (file_id, epoch),
            ).fetchone()

    def create_version(self, *, file_id: int, epoch: int, size: int, sha256: str,
                       state: str, manifest_id: int, note: str = "") -> sqlite3.Row:
        with self._lock:
            ts = now_iso()
            cur = self._execute(
                "INSERT INTO file_versions(file_id, epoch, size, sha256, state, "
                "manifest_id, note, first_seen, updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (file_id, epoch, size, sha256, state, manifest_id, note, ts, ts),
            )
            return self._execute(
                "SELECT * FROM file_versions WHERE id=?", (cur.lastrowid,)
            ).fetchone()

    def update_version(self, version_id: int, **fields: Any) -> None:
        if not fields:
            return
        with self._lock:
            fields["updated_at"] = now_iso()
            cols = ", ".join(f"{k}=?" for k in fields)
            self._execute(
                f"UPDATE file_versions SET {cols} WHERE id=?",
                (*fields.values(), version_id),
            )

    def supersede_versions(self, file_id: int, epoch: int, keep_id: int) -> None:
        with self._lock:
            self._execute(
                "UPDATE file_versions SET state='superseded', updated_at=? "
                "WHERE file_id=? AND epoch=? AND id != ? AND state != 'superseded'",
                (now_iso(), file_id, epoch, keep_id),
            )

    def list_versions(self, file_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return self._execute(
                "SELECT * FROM file_versions WHERE file_id=? ORDER BY id", (file_id,)
            ).fetchall()

    def add_observation(self, *, kind: str, ref_id: int, scan_id: int,
                        observed_size: int, expected_size: int, mtime_ns: int,
                        note: str) -> None:
        with self._lock:
            self._execute(
                "INSERT INTO observations(kind, ref_id, scan_id, observed_size, "
                "expected_size, mtime_ns, note, at) VALUES(?,?,?,?,?,?,?,?)",
                (kind, ref_id, scan_id, observed_size, expected_size, mtime_ns,
                 note, now_iso()),
            )

    def list_observations(self, kind: str, ref_id: int, limit: int = 20) -> list[sqlite3.Row]:
        with self._lock:
            return self._execute(
                "SELECT * FROM observations WHERE kind=? AND ref_id=? "
                "ORDER BY id DESC LIMIT ?", (kind, ref_id, limit)
            ).fetchall()[::-1]

    # ---------- 发布指针（下游可见性的唯一来源） ----------
    def publish(self, *, file_id: int, version_id: int, batch_id: str, relpath: str,
                epoch: int, sha256: str, storage_mode: str, evidence: dict) -> None:
        """单事务内替换指针：任何并发领取都只能读到替换前或替换后的完整版本。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._execute(
                    "INSERT INTO publishes(file_id, version_id, batch_id, relpath, "
                    "epoch, sha256, storage_mode, evidence, published_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(file_id) DO UPDATE SET version_id=excluded.version_id, "
                    "epoch=excluded.epoch, sha256=excluded.sha256, "
                    "storage_mode=excluded.storage_mode, evidence=excluded.evidence, "
                    "published_at=excluded.published_at",
                    (file_id, version_id, batch_id, relpath, epoch, sha256,
                     storage_mode, json.dumps(evidence, ensure_ascii=False), now_iso()),
                )
                self._execute(
                    "UPDATE files SET current_version_id=? WHERE id=?",
                    (version_id, file_id),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def get_publish(self, file_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self._execute(
                "SELECT * FROM publishes WHERE file_id=?", (file_id,)
            ).fetchone()

    def list_publishes(self, batch_id: str | None = None) -> list[sqlite3.Row]:
        with self._lock:
            if batch_id:
                return self._execute(
                    "SELECT * FROM publishes WHERE batch_id=? ORDER BY relpath", (batch_id,)
                ).fetchall()
            return self._execute("SELECT * FROM publishes ORDER BY batch_id, relpath").fetchall()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
