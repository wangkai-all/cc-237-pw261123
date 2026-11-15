"""本地交付模拟器（发送方）。

所有动作都作用在 dropzone 目录上，接收器只通过观察该目录感知交付。
子命令：

  init      创建批次并发布第 0 版目录通知（manifest-0.json 原子落位）
  write     按当前目录写数据文件；--slow 分块慢写制造“写到一半”
  seal     immutable 批次封口：数据文件去写位
  correct   发布更正目录（epoch+1），可调整文件清单
  replace   同名文件替换：写新 inode 再原子改名（旧硬链接保持有效）
  corrupt   写一个大小正确但内容损坏的文件（哈希对不上）
  list      查看批次状态
  reset     清空 dropzone

注意：清单与数据都先写临时文件再 rename，因此接收方看到的清单要么
不存在/不完整（writing），要么是完整 JSON，绝不会读到“半个清单”。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from .hashes import sha256_bytes
from .manifest import (
    VALID_MODES,
    build_manifest,
    data_path,
    deterministic_content,
    manifest_path,
    write_manifest,
)
from .util import atomic_write_bytes, set_readonly


def _state_path(dropzone: Path, batch_id: str) -> Path:
    return dropzone / batch_id / ".sender-state.json"


def _load_state(dropzone: Path, batch_id: str) -> dict:
    p = _state_path(dropzone, batch_id)
    if not p.exists():
        raise SystemExit(f"批次不存在或未 init: {batch_id}")
    return json.loads(p.read_text("utf-8"))


def _save_state(dropzone: Path, state: dict) -> None:
    p = _state_path(dropzone, Path(state["batch_id"]))
    atomic_write_bytes(p, json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8"))


def _file_specs(entries: list[str], *, size_optional: bool = False) -> list[dict]:
    """解析 name[:size[:version]] 形式的文件参数。"""
    specs = []
    for e in entries:
        parts = e.split(":")
        if len(parts) not in (1, 2, 3):
            raise SystemExit(f"文件参数格式应为 name[:size[:version]]，得到: {e}")
        name = parts[0]
        size = None
        if len(parts) >= 2:
            try:
                size = int(parts[1])
            except ValueError:
                raise SystemExit(f"大小必须是整数: {e}")
            if size < 0:
                raise SystemExit(f"大小不能为负: {e}")
        if size is None and not size_optional:
            raise SystemExit(f"缺少大小: {e}")
        version = int(parts[2]) if len(parts) == 3 else 0
        specs.append({"relpath": name, "size": size, "version": version})
    return specs


def _describe(spec: dict, batch_id: str, epoch: int) -> dict:
    content = deterministic_content(
        batch_id, spec["relpath"], spec.get("version", epoch), spec["size"]
    )
    return {
        "relpath": spec["relpath"],
        "size": spec["size"],
        "sha256": sha256_bytes(content),
        "version": spec.get("version", epoch),
    }


def _write_data_file(path: Path, batch_id: str, desc: dict, *,
                     slow: bool, corrupt: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = bytearray(
        deterministic_content(batch_id, desc["relpath"], desc["version"], desc["size"])
    )
    if corrupt and content:
        content[0] ^= 0xFF  # 翻转首字节：大小不变，哈希必然不符
    import os
    tmp = path.parent / f".{path.name}.writing"
    if slow:
        # 分块慢写：观察器在此期间会看到大小 < 清单大小 -> writing
        with open(tmp, "wb") as f:
            if not content:
                pass
            else:
                chunk = max(1, len(content) // 5)
                for i in range(0, len(content), chunk):
                    f.write(bytes(content[i:i + chunk]))
                    f.flush()
                    time.sleep(0.25)
    else:
        atomic_write_bytes(tmp, bytes(content))
    os.replace(tmp, path)  # 原子改名：同名替换时旧 inode（可能已被硬链接）不动


# ---------- 子命令 ----------
def cmd_init(args) -> None:
    dropzone = Path(args.dropzone)
    if args.mode not in VALID_MODES:
        raise SystemExit(f"mode 必须是 {VALID_MODES} 之一")
    batch_dir = dropzone / args.batch_id
    if batch_dir.exists():
        raise SystemExit(f"批次目录已存在: {batch_dir}")
    (batch_dir / "data").mkdir(parents=True)
    descs = [_describe(s, args.batch_id, 0) for s in _file_specs(args.files)]
    manifest = build_manifest(
        batch_id=args.batch_id, title=args.title, epoch=0,
        source_mode=args.mode,
        files=[{"relpath": d["relpath"], "size": d["size"], "sha256": d["sha256"]}
               for d in descs],
    )
    write_manifest(manifest_path(dropzone, args.batch_id, 0), manifest)
    state = {
        "batch_id": args.batch_id,
        "title": args.title,
        "mode": args.mode,
        "epoch": 0,
        "files": descs,
    }
    _save_state(dropzone, state)
    print(f"[init] 批次 {args.batch_id} 目录通知 epoch=0 已发布，"
          f"含 {len(descs)} 个文件声明（数据尚未到达）")


def cmd_write(args) -> None:
    dropzone = Path(args.dropzone)
    state = _load_state(dropzone, args.batch_id)
    epoch = state["epoch"]
    by_name = {d["relpath"]: d for d in state["files"]}
    names = args.names or list(by_name)
    for name in names:
        if name not in by_name:
            raise SystemExit(f"当前目录未声明该文件: {name}")
        desc = by_name[name]
        _write_data_file(
            data_path(dropzone, args.batch_id, name),
            args.batch_id, desc, slow=args.slow, corrupt=False,
        )
        print(f"[write] {name} 已写入（{desc['size']} 字节，epoch={epoch}，"
              f"{'慢写' if args.slow else '原子写'}）")


def cmd_corrupt(args) -> None:
    dropzone = Path(args.dropzone)
    state = _load_state(dropzone, args.batch_id)
    desc = next((d for d in state["files"] if d["relpath"] == args.name), None)
    if desc is None:
        raise SystemExit(f"当前目录未声明该文件: {args.name}")
    _write_data_file(
        data_path(dropzone, args.batch_id, args.name),
        args.batch_id, desc, slow=args.slow, corrupt=True,
    )
    print(f"[corrupt] {args.name} 写入了大小正确但内容损坏的版本"
          f"（{'慢写' if args.slow else '原子写'}）")


def cmd_seal(args) -> None:
    dropzone = Path(args.dropzone)
    state = _load_state(dropzone, args.batch_id)
    if state["mode"] != "immutable":
        raise SystemExit("seal 仅用于 immutable 批次")
    n = 0
    for desc in state["files"]:
        p = data_path(dropzone, args.batch_id, desc["relpath"])
        if p.exists():
            set_readonly(p)
            n += 1
    print(f"[seal] {n} 个已到达文件已去除写位（只读，满足可靠引用条件）")


def cmd_correct(args) -> None:
    dropzone = Path(args.dropzone)
    state = _load_state(dropzone, args.batch_id)
    new_epoch = state["epoch"] + 1

    existing = {d["relpath"]: dict(d) for d in state["files"]}
    for spec in _file_specs(args.add or []):
        spec.setdefault("version", new_epoch)
        existing[spec["relpath"]] = _describe(spec, args.batch_id, new_epoch)
    for spec in _file_specs(args.bump or [], size_optional=True):
        if spec["relpath"] not in existing:
            raise SystemExit(f"bump 的文件不在原目录中: {spec['relpath']}")
        spec["version"] = new_epoch  # 更正版本：同名文件新内容
        spec["size"] = spec["size"] or existing[spec["relpath"]]["size"]
        existing[spec["relpath"]] = _describe(spec, args.batch_id, new_epoch)
    for name in args.remove or []:
        existing.pop(name, None)

    descs = list(existing.values())
    manifest = build_manifest(
        batch_id=args.batch_id, title=state["title"], epoch=new_epoch,
        source_mode=state["mode"],
        files=[{"relpath": d["relpath"], "size": d["size"], "sha256": d["sha256"]}
               for d in descs],
    )
    write_manifest(manifest_path(dropzone, args.batch_id, new_epoch), manifest)
    state["epoch"] = new_epoch
    state["files"] = descs
    _save_state(dropzone, state)
    print(f"[correct] 更正目录 epoch={new_epoch} 已发布，"
          f"声明 {len(descs)} 个文件（旧 epoch 目录仍留在投递区）")


def cmd_replace(args) -> None:
    """同名文件替换：新字节写到新 inode，再原子改名覆盖目录项。"""
    dropzone = Path(args.dropzone)
    state = _load_state(dropzone, args.batch_id)
    desc = next((d for d in state["files"] if d["relpath"] == args.name), None)
    if desc is None:
        raise SystemExit(f"当前目录未声明该文件: {args.name}")
    target = data_path(dropzone, args.batch_id, args.name)
    old_inode = target.stat().st_ino if target.exists() else None
    _write_data_file(target, args.batch_id, desc, slow=args.slow, corrupt=False)
    new_inode = target.stat().st_ino
    print(f"[replace] {args.name} 同名替换完成：inode {old_inode} -> {new_inode}；"
          f"旧 inode 若已被硬链接引用则继续有效")


def cmd_list(args) -> None:
    dropzone = Path(args.dropzone)
    state = _load_state(dropzone, args.batch_id)
    print(json.dumps(state, ensure_ascii=False, indent=2))
    base = dropzone / args.batch_id
    print("\n投递区内容:")
    for p in sorted(base.rglob("*")):
        if p.is_file() and not p.name.startswith("."):
            print(" ", p.relative_to(base), p.stat().st_size, "字节")


def cmd_reset(args) -> None:
    import shutil
    dropzone = Path(args.dropzone)
    if dropzone.exists():
        shutil.rmtree(dropzone)
    dropzone.mkdir(parents=True)
    print(f"[reset] 投递区已清空: {dropzone}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="sender", description="本地交付模拟器")
    p.add_argument("--dropzone", required=True, help="投递区目录")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("init", help="创建批次并发布 epoch=0 目录")
    sp.add_argument("--batch", dest="batch_id", required=True)
    sp.add_argument("--title", default="")
    sp.add_argument("--mode", default="mutable", choices=VALID_MODES)
    sp.add_argument("--files", nargs="*", default=[], help="name:size[:version] ...")
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("write", help="按当前目录写数据文件")
    sp.add_argument("--batch", dest="batch_id", required=True)
    sp.add_argument("--names", nargs="*", help="不填则写全部")
    sp.add_argument("--slow", action="store_true", help="分块慢写（制造 writing）")
    sp.set_defaults(func=cmd_write)

    sp = sub.add_parser("corrupt", help="写大小正确但哈希损坏的文件")
    sp.add_argument("--batch", dest="batch_id", required=True)
    sp.add_argument("--name", required=True)
    sp.add_argument("--slow", action="store_true")
    sp.set_defaults(func=cmd_corrupt)

    sp = sub.add_parser("seal", help="immutable 批次数据文件去写位")
    sp.add_argument("--batch", dest="batch_id", required=True)
    sp.set_defaults(func=cmd_seal)

    sp = sub.add_parser("correct", help="发布更正目录（epoch+1）")
    sp.add_argument("--batch", dest="batch_id", required=True)
    sp.add_argument("--add", nargs="*", default=[], help="新增 name:size ...")
    sp.add_argument("--bump", nargs="*", default=[], help="换内容 name[:size] ...")
    sp.add_argument("--remove", nargs="*", default=[])
    sp.set_defaults(func=cmd_correct)

    sp = sub.add_parser("replace", help="同名文件原子替换（新 inode）")
    sp.add_argument("--batch", dest="batch_id", required=True)
    sp.add_argument("--name", required=True)
    sp.add_argument("--slow", action="store_true")
    sp.set_defaults(func=cmd_replace)

    sp = sub.add_parser("list", help="查看发送方状态与投递区")
    sp.add_argument("--batch", dest="batch_id", required=True)
    sp.set_defaults(func=cmd_list)

    sp = sub.add_parser("reset", help="清空投递区")
    sp.set_defaults(func=cmd_reset)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
