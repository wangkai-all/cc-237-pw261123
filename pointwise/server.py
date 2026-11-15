"""HTTP 服务：静态接收页 + JSON API + 下游领取流。

路由
----
GET  /                      接收页（web/index.html）
GET  /static/app.js         页面脚本
GET  /api/overview          全量视图（轮询）
POST /api/receiver/stop     停止接收器（观察器暂停，已发布仍可领）
POST /api/receiver/resume   恢复接收器
POST /api/files/<id>/publish   逐个发布
POST /api/batches/<id>/withdraw 撤回目录（body: {"reason": "..."}）
GET  /api/files/<id>/claim  下游领取（流式，带完整性响应头）
"""
from __future__ import annotations

import base64
import json
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

from .models import DB
from .service import ReceiverService, ServiceError
from .storage import ObjectStore
from .watcher import Watcher

WEB_ROOT = Path(__file__).resolve().parent.parent / "web"


class Handler(BaseHTTPRequestHandler):
    service: ReceiverService = None  # 由工厂注入
    db: DB = None

    # ---------- 工具 ----------
    def _json(self, obj, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: int, message: str) -> None:
        self._json({"error": message}, status)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError:
            raise ServiceError(400, "请求体不是合法 JSON")

    def log_message(self, fmt, *args):  # 静默默认访问日志，事件走 DB
        return

    # ---------- 路由 ----------
    def do_GET(self) -> None:
        path = urlparse(self.path).path
        try:
            if path == "/":
                self._serve_static(WEB_ROOT / "index.html", "text/html; charset=utf-8")
            elif path == "/static/app.js":
                self._serve_static(WEB_ROOT / "app.js", "application/javascript; charset=utf-8")
            elif path == "/static/styles.css":
                self._serve_static(WEB_ROOT / "styles.css", "text/css; charset=utf-8")
            elif path == "/api/overview":
                self._json(self.service.overview())
            elif path.startswith("/api/files/") and path.endswith("/claim"):
                self._claim(int(path.split("/")[3]))
            else:
                self._error(404, "路由不存在")
        except ServiceError as exc:
            self._error(exc.status, exc.message)
        except ValueError:
            self._error(400, "ID 必须是整数")

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            if path == "/api/receiver/stop":
                self._json({"state": self.service.stop_receiver()})
            elif path == "/api/receiver/resume":
                self._json({"state": self.service.resume_receiver()})
            elif path.startswith("/api/files/") and path.endswith("/publish"):
                evidence = self.service.publish_file(int(path.split("/")[3]))
                self._json({"ok": True, "evidence": evidence})
            elif path.startswith("/api/batches/") and path.endswith("/withdraw"):
                batch_id = unquote(path.split("/")[3])
                reason = (self._read_json() or {}).get("reason") or "页面手动撤回"
                self.service.withdraw_batch(batch_id, reason)
                self._json({"ok": True})
            else:
                self._error(404, "路由不存在")
        except ServiceError as exc:
            self._error(exc.status, exc.message)
        except ValueError:
            self._error(400, "ID 必须是整数")

    # ---------- 静态资源与领取流 ----------
    def _serve_static(self, path: Path, content_type: str) -> None:
        if not path.is_file():
            self._error(404, f"资源不存在: {path.name}")
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _claim(self, file_id: int) -> None:
        result = self.service.claim(file_id)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(result.size))
            # 完整性与版本依据随响应头交给下游（头只能 latin-1，依据用 base64）
            self.send_header("X-Pointwise-Sha256", result.sha256)
            self.send_header("X-Pointwise-Epoch", str(result.epoch))
            self.send_header("X-Pointwise-Storage-Mode", result.storage_mode)
            evidence_b64 = base64.b64encode(
                json.dumps(result.evidence, ensure_ascii=False).encode("utf-8")
            ).decode("ascii")
            self.send_header("X-Pointwise-Evidence-B64", evidence_b64)
            from urllib.parse import quote
            fname = Path(result.relpath).name
            self.send_header(
                "Content-Disposition",
                f"attachment; filename=download; filename*=UTF-8''{quote(fname)}",
            )
            self.end_headers()
            while True:
                chunk = result.stream.read(65536)
                if not chunk:
                    break
                self.wfile.write(chunk)
        finally:
            result.stream.close()


def build_server(runtime_dir: str | Path, port: int = 0, *,
                 tick_seconds: float = 1.0,
                 start_watcher: bool = True) -> tuple[ThreadingHTTPServer, Watcher, DB]:
    runtime = Path(runtime_dir)
    dropzone = runtime / "dropzone"
    store_root = runtime / "store"
    db_path = runtime / "receiver.db"
    dropzone.mkdir(parents=True, exist_ok=True)

    db = DB(db_path)
    store = ObjectStore(store_root)
    service = ReceiverService(db, store)
    watcher = Watcher(db, store, dropzone, tick_seconds=tick_seconds)

    handler = _make_handler_class(service, db)
    httpd = ThreadingHTTPServer(("127.0.0.1", port), handler)
    httpd.db = db
    httpd.store = store
    httpd.service = service
    httpd.watcher = watcher
    httpd.runtime_dir = runtime
    if start_watcher:
        watcher.start()
    return httpd, watcher, db


def _make_handler_class(service: ReceiverService, db: DB):
    """生成绑定了 service/db 的 Handler 子类（类属性在构造前就位）。"""
    class BoundHandler(Handler):
        pass

    BoundHandler.service = service
    BoundHandler.db = db
    return BoundHandler


def serve_forever(runtime_dir: str | Path, port: int = 8000,
                  tick_seconds: float = 1.0) -> None:
    httpd, watcher, db = build_server(runtime_dir, port=port,
                                      tick_seconds=tick_seconds)
    host, bound_port = httpd.server_address
    print(f"pointwise 接收页: http://{host}:{bound_port}/")
    print(f"运行目录: {Path(runtime_dir).resolve()}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        watcher.stop_thread()
        httpd.server_close()
        db.close()
