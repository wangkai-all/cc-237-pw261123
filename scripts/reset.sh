#!/usr/bin/env bash
# 清空全部运行数据（投递区、对象存储、状态库），回到空接收方。
set -euo pipefail
cd "$(dirname "$0")/.."
RUNTIME="${RUNTIME_DIR:-runtime}"
rm -rf "$RUNTIME"
mkdir -p "$RUNTIME/dropzone" "$RUNTIME/store"
echo "[reset] 已清空 $RUNTIME"
