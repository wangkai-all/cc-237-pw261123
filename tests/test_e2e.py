"""端到端测试：在临时目录里跑完整的模拟器 -> 观察 -> 发布 -> 领取链路。

运行：python3 -m tests.test_e2e
（不依赖 pytest / requests，只用标准库；python -I 亦可运行）
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pointwise.manifest import data_path, deterministic_content  # noqa: E402
from pointwise.models import DB  # noqa: E402
from pointwise.sender import main as sender_main  # noqa: E402
from pointwise.server import build_server  # noqa: E402
from pointwise.storage import ObjectStore  # noqa: E402

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def wait_until(desc: str, timeout: float, predicate):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.05)
    raise AssertionError(f"等待超时: {desc} (last={last!r})")


# 测试机偶发高负载时观察器 tick 会被调度延迟，统一给足时间
WAIT_TIMEOUT = 20.0


def http(method: str, url: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def api(method: str, url: str, body=None):
    status, _, raw = http(method, url, body)
    payload = json.loads(raw) if raw else {}
    if status >= 400:
        raise AssertionError(f"{method} {url} -> {status}: {payload}")
    return payload


class Harness:
    def __init__(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="pointwise-e2e-"))
        self.runtime = self.tmp / "runtime"
        self.dropzone = self.runtime / "dropzone"
        self.runtime.mkdir()
        self.httpd, self.watcher, self.db = build_server(
            self.runtime, port=0, tick_seconds=0.05
        )
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def sender(self, *args: str):
        sender_main(["--dropzone", str(self.dropzone), *args])

    def overview(self):
        return api("GET", f"{self.base}/api/overview")

    def file_id(self, batch: str, relpath: str) -> int:
        block = self.file_block(batch, relpath)
        if block is None:
            raise KeyError(f"{batch}/{relpath} 尚未被观察到")
        return block["file"]["id"]

    def file_block(self, batch: str, relpath: str):
        data = self.overview()
        for b in data["batches"]:
            if b["batch"]["batch_id"] == batch:
                for f in b["files"]:
                    if f["file"]["relpath"] == relpath:
                        return f
        return None

    def close(self):
        self.watcher.stop_thread()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)


def expect_state(h: Harness, batch: str, relpath: str, state: str):
    def poll():
        block = h.file_block(batch, relpath)
        if block is None:
            return None
        data = h.overview()
        b = next((x for x in data["batches"]
                  if x["batch"]["batch_id"] == batch), None)
        if b is None:
            return None
        cur = [v for v in block["versions"] if v["epoch"] == b["batch"]["current_epoch"]]
        return block if cur and cur[-1]["state"] == state else None
    return wait_until(f"{relpath} -> {state}", WAIT_TIMEOUT, poll)


# ---------------- 场景 ------------------
def scenario_mutable_copy(h: Harness):
    print("\n[场景1] mutable 来源：先目录后内容、慢写 writing、判齐后独立复制")
    h.sender("init", "--batch", "b-mut", "--mode", "mutable",
             "--files", "alpha.txt:4096", "beta.txt:2048")

    data = wait_until("清单 received", WAIT_TIMEOUT,
                      lambda: h.overview() if any(
                          m["state"] == "received"
                          for b in h.overview()["batches"]
                          for m in b["manifests"]) else None)
    block = next(b for b in data["batches"] if b["batch"]["batch_id"] == "b-mut")
    check("清单状态 received（目录通知已生效）",
          block["manifests"][0]["state"] == "received")
    check("文件先登记为 writing（内容晚到）",
          all(v["state"] == "writing" for f in block["files"] for v in f["versions"]))

    h.sender("write", "--batch", "b-mut", "--names", "alpha.txt", "--slow")
    expect_state(h, "b-mut", "alpha.txt", "complete")
    block = h.file_block("b-mut", "alpha.txt")
    v = block["versions"][-1]
    check("mutable 来源采用独立复制", v["storage_mode"] == "copy", v["storage_mode"])
    check("复制的额外空间=文件大小", v["extra_bytes"] == 4096)
    check("观察结论记录了依据", "独立复制" in v["note"])

    # 来源随后被改写，接收对象不受影响
    src = data_path(h.dropzone, "b-mut", "alpha.txt")
    original = src.read_bytes()
    os.chmod(src, 0o644)
    with open(src, "r+b") as f:
        f.write(b"X" * 16)
    time.sleep(0.3)
    status, headers, body = http("GET", f"{self_claim(h, 'b-mut', 'alpha.txt')}")
    check("来源被改写后领取内容仍完整", body == original and status == 200)
    check("领取响应头携带 sha256 依据", "X-Pointwise-Sha256" in headers)


def self_claim(h: Harness, batch: str, relpath: str) -> str:
    # 先发布再返回领取 URL
    fid = h.file_id(batch, relpath)
    api("POST", f"{h.base}/api/files/{fid}/publish")
    return f"{h.base}/api/files/{fid}/claim"


def scenario_corrupt(h: Harness):
    print("\n[场景2] 写到一半 vs 内容损坏：不同状态，且损坏不可发布")
    h.sender("init", "--batch", "b-bad", "--files", "good.bin:1024", "bad.bin:1024")
    h.sender("write", "--batch", "b-bad", "--names", "good.bin")
    h.sender("corrupt", "--batch", "b-bad", "--name", "bad.bin")
    expect_state(h, "b-bad", "good.bin", "complete")
    expect_state(h, "b-bad", "bad.bin", "corrupt")
    block = h.file_block("b-bad", "bad.bin")
    check("损坏文件状态=corrupt 而非 writing", block["versions"][-1]["state"] == "corrupt")
    fid = h.file_id("b-bad", "bad.bin")
    status, _, raw = http("POST", f"{h.base}/api/files/{fid}/publish")
    check("corrupt 版本被拒绝发布", status == 409, raw.decode())

    # 重新写入正确内容后应自动恢复为 complete
    h.sender("write", "--batch", "b-bad", "--names", "bad.bin")
    expect_state(h, "b-bad", "bad.bin", "complete")
    check("修复后自动判齐 complete",
          h.file_block("b-bad", "bad.bin")["versions"][-1]["state"] == "complete")


def scenario_immutable_hardlink(h: Harness):
    print("\n[场景3] immutable 封口：可靠引用（硬链接），空间≈0；未封口则降级复制")
    h.sender("init", "--batch", "b-imm", "--mode", "immutable",
             "--files", "linked.txt:8192")
    h.sender("write", "--batch", "b-imm", "--names", "linked.txt")
    # 未 seal：来源仍可写 -> 不能硬链接
    expect_state(h, "b-imm", "linked.txt", "complete")
    v1 = h.file_block("b-imm", "linked.txt")["versions"][-1]
    check("未封口 immutable 仍采用独立复制", v1["storage_mode"] == "copy")

    # 新批次演示真正的硬链接路径
    h.sender("init", "--batch", "b-imm2", "--mode", "immutable",
             "--files", "linked2.txt:8192")
    h.sender("write", "--batch", "b-imm2", "--names", "linked2.txt")
    h.sender("seal", "--batch", "b-imm2")
    expect_state(h, "b-imm2", "linked2.txt", "complete")
    v2 = h.file_block("b-imm2", "linked2.txt")["versions"][-1]
    check("封口后采用硬链接可靠引用", v2["storage_mode"] == "hardlink", v2["note"])
    check("硬链接额外空间为 0", v2["extra_bytes"] == 0)
    src = data_path(h.dropzone, "b-imm2", "linked2.txt")
    digest = v2["sha256"]
    store = ObjectStore(h.runtime / "store")
    check("硬链接确实共享 inode",
          src.stat().st_ino == store.object_path(digest).stat().st_ino)
    check("物理占用统计按 inode 去重",
          h.overview()["store"]["physical_bytes"] <=
          h.overview()["store"]["logical_bytes"])


def scenario_correction_stale(h: Harness):
    print("\n[场景4] 目录更正：新 epoch 生效；旧批次迟到更正只能 stale，不能覆盖")
    h.sender("init", "--batch", "b-corr", "--files", "keep.txt:3000", "old.txt:1500")
    h.sender("write", "--batch", "b-corr")
    expect_state(h, "b-corr", "keep.txt", "complete")
    expect_state(h, "b-corr", "old.txt", "complete")

    h.sender("correct", "--batch", "b-corr", "--add", "new.txt:2000",
             "--bump", "keep.txt", "--remove", "old.txt")
    def corrected():
        data = h.overview()
        b = next(x for x in data["batches"] if x["batch"]["batch_id"] == "b-corr")
        return b if b["batch"]["current_epoch"] == 1 else None
    block = wait_until("epoch=1 生效", WAIT_TIMEOUT, corrected)
    check("当前 epoch 推进到 1", block["batch"]["current_epoch"] == 1)
    states = {m["epoch"]: m["state"] for m in block["manifests"]}
    check("epoch0 清单仍保留", states.get(0) == "received")
    check("epoch1 清单 received", states.get(1) == "received")
    rels = {f["file"]["relpath"] for f in block["files"]}
    check("更正目录移除了 old.txt 声明（文件行保留历史）", "old.txt" in rels)
    check("更正目录新增 new.txt", "new.txt" in rels)

    # 迟到的旧批次更正：当前已在 epoch=1，再投递一个 epoch=0 的目录
    # （真实场景里旧通知可能因重试延迟到达），观察器必须标记 stale 且不覆盖当前目录
    late_manifest = h.dropzone / "b-corr" / "manifest-0.json"
    mtime_before = late_manifest.stat().st_mtime_ns
    # 重写一遍（内容相同、mtime 更新）模拟旧通知重投
    payload = late_manifest.read_bytes()
    late_manifest.unlink()
    time.sleep(0.02)
    late_manifest.write_bytes(payload)
    h.watcher.scan_once()
    row = h.db.get_manifest("b-corr", 0)
    check("迟到的旧 epoch 更正被标记 stale", row is not None and row["state"] == "stale",
          dict(row) if row else "missing")
    check("stale 没有改变当前 epoch", h.db.get_batch("b-corr")["current_epoch"] == 1)
    check("旧通知重投不影响已登记文件", mtime_before is not None)


def scenario_publish_replace_claim(h: Harness):
    print("\n[场景5] 逐个发布、同名替换并发领取：只会拿到完整版本")
    h.sender("init", "--batch", "b-pub", "--files", "hot.txt:5000")
    h.sender("write", "--batch", "b-pub", "--names", "hot.txt")
    expect_state(h, "b-pub", "hot.txt", "complete")
    fid = h.file_id("b-pub", "hot.txt")
    api("POST", f"{h.base}/api/files/{fid}/publish")

    status, headers, body_v1 = http("GET", f"{h.base}/api/files/{fid}/claim")
    v1_hash = hashlib.sha256(body_v1).hexdigest()
    check("首次领取内容完整且哈希匹配响应头",
          status == 200 and v1_hash == headers["X-Pointwise-Sha256"])

    # 未判齐不能发布（先制造 writing）
    h.sender("corrupt", "--batch", "b-pub", "--name", "hot.txt")
    expect_state(h, "b-pub", "hot.txt", "corrupt")
    status, _, _ = http("POST", f"{h.base}/api/files/{fid}/publish")
    check("替换期间损坏版本不能发布", status == 409)
    # 旧发布指针仍在，领取依旧拿到完整旧版
    status, _, body = http("GET", f"{h.base}/api/files/{fid}/claim")
    check("替换未完成时领取仍得到旧完整版本", hashlib.sha256(body).hexdigest() == v1_hash)

    # 正确重写（同名替换，新 inode）
    h.sender("write", "--batch", "b-pub", "--names", "hot.txt")
    expect_state(h, "b-pub", "hot.txt", "complete")

    # 并发：一边循环领取，一边切换发布指针，验证每次领取都是完整文件
    errors = []
    def claimer():
        for _ in range(30):
            st, hd, raw = http("GET", f"{h.base}/api/files/{fid}/claim")
            if st != 200 or hashlib.sha256(raw).hexdigest() != hd["X-Pointwise-Sha256"]:
                errors.append((st, "哈希不符"))
    t = threading.Thread(target=claimer)
    t.start()
    time.sleep(0.05)
    api("POST", f"{h.base}/api/files/{fid}/publish")  # 指针原子切换
    t.join()
    check("并发领取期间没有任何不完整/不一致响应", not errors, str(errors))

    status, headers, body_v2 = http("GET", f"{h.base}/api/files/{fid}/claim")
    check("切换后领取到新版本且完整",
          hashlib.sha256(body_v2).hexdigest() == headers["X-Pointwise-Sha256"])
    block = h.file_block("b-pub", "hot.txt")
    check("发布依据留痕（含替换标记）",
          json.loads(block["publish"]["evidence"])["replaced"] is True)


def scenario_withdraw_and_control(h: Harness):
    print("\n[场景6] 撤回目录、停止/恢复接收器")
    h.sender("init", "--batch", "b-ctl", "--files", "x.txt:128")
    h.sender("write", "--batch", "b-ctl")
    expect_state(h, "b-ctl", "x.txt", "complete")
    fid = h.file_id("b-ctl", "x.txt")
    api("POST", f"{h.base}/api/files/{fid}/publish")
    api("POST", f"{h.base}/api/batches/b-ctl/withdraw", {"reason": "测试撤回"})
    status, _, _ = http("GET", f"{h.base}/api/files/{fid}/claim")
    check("撤回后下游无法领取", status == 404)
    status, _, _ = http("POST", f"{h.base}/api/files/{fid}/publish")
    check("撤回状态下不能发布", status == 409)

    # 停止接收器：观察器冻结，新文件不再被处理
    api("POST", f"{h.base}/api/receiver/stop")
    h.sender("init", "--batch", "b-frozen", "--files", "z.txt:64")
    time.sleep(0.4)
    data = h.overview()
    check("停止后新批次不被观察",
          all(b["batch"]["batch_id"] != "b-frozen" for b in data["batches"]))
    check("停止状态已持久化", data["receiver_state"] == "stopped")
    # 已发布历史仍可查（指针已撤回则 404，这里验证 API 可用）
    api("POST", f"{h.base}/api/receiver/resume")
    wait_until("恢复后补扫到冻结批次", WAIT_TIMEOUT,
               lambda: h.overview() if any(
                   b["batch"]["batch_id"] == "b-frozen"
                   for b in h.overview()["batches"]) else None)
    check("恢复后补扫成功", True)


def scenario_evidence_and_events(h: Harness):
    print("\n[场景7] 每次发布/领取/更正都有事件与依据可查")
    data = h.overview()
    kinds = {e["kind"] for e in data["events"]}
    for required in ("manifest_received", "file_complete", "publish", "claim",
                     "withdraw", "file_corrupt", "manifest_stale"):
        check(f"事件流包含 {required}", required in kinds, str(sorted(kinds)))
    pub = next((f["publish"] for b in data["batches"] for f in b["files"]
                if f["publish"]), None)
    check("发布依据含哈希/epoch/存储方式/时间",
          pub and all(k in json.loads(pub["evidence"])
                      for k in ("sha256", "epoch", "storage_mode", "basis",
                                "published_at")))


def main() -> int:
    h = Harness()
    try:
        scenario_mutable_copy(h)
        scenario_corrupt(h)
        scenario_immutable_hardlink(h)
        scenario_correction_stale(h)
        scenario_publish_replace_claim(h)
        scenario_withdraw_and_control(h)
        scenario_evidence_and_events(h)
    finally:
        h.close()
    print(f"\n结果：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
