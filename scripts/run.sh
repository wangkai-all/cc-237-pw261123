#!/usr/bin/env bash
# 启动接收服务：观察器 + 接收存储 + 网页（默认 http://127.0.0.1:8000/）
set -euo pipefail
cd "$(dirname "$0")/.."
RUNTIME="${RUNTIME_DIR:-runtime}"
PORT="${PORT:-8000}"
TICK="${TICK:-1.0}"
mkdir -p "$RUNTIME/dropzone"
exec python3 -m pointwise receiver --runtime "$RUNTIME" --port "$PORT" --tick "$TICK"
