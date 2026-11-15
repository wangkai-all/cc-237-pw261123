"""命令行入口。

接收服务：  python -m pointwise receiver --runtime ./runtime --port 8000
发送模拟：  python -m pointwise sender --dropzone ./runtime/dropzone <子命令>
"""
from __future__ import annotations

import argparse
import sys

from .sender import build_parser as sender_parser
from .server import serve_forever


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        argv = ["--help"]

    if argv[0] == "receiver":
        parser = argparse.ArgumentParser(prog="pointwise receiver")
        parser.add_argument("--runtime", default="runtime")
        parser.add_argument("--port", type=int, default=8000)
        parser.add_argument("--tick", type=float, default=1.0)
        args = parser.parse_args(argv[1:])
        serve_forever(args.runtime, port=args.port, tick_seconds=args.tick)
        return 0

    if argv[0] == "sender":
        return _run_sender(argv[1:])

    print(f"未知命令: {argv[0]}（支持 receiver / sender）", file=sys.stderr)
    return 2


def _run_sender(rest: list[str]) -> int:
    args = sender_parser().parse_args(rest)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
