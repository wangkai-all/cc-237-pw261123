#!/usr/bin/env bash
# 一键演示：重置运行目录 -> 起接收服务 -> 用模拟器制造目录先行、慢写、
# 损坏、同名替换、目录更正、撤回、停止/恢复等全部场景。
# 用法：bash scripts/demo.sh   （结束后服务保持运行，网页 http://127.0.0.1:8000/）
set -euo pipefail
cd "$(dirname "$0")/.."
PORT="${PORT:-8000}"
RUNTIME="${RUNTIME_DIR:-runtime}"
DZ="$RUNTIME/dropzone"

bash scripts/reset.sh
PORT="$PORT" bash scripts/run.sh > /tmp/pointwise-demo.log 2>&1 &
SERVER_PID=$!
trap 'echo; echo "服务仍在运行 (PID $SERVER_PID)，停止: kill $SERVER_PID"' EXIT
sleep 1.2
BASE="http://127.0.0.1:$PORT"
S() { python3 -m pointwise sender --dropzone "$DZ" "$@"; }
fid() {  # 按文件路径取 id
  curl -s "$BASE/api/overview" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for b in d['batches']:
    for f in b['files']:
        if f['file']['relpath']=='$1': print(f['file']['id'])
"
}
wait_state() {  # wait_state <batch> <relpath> <state>
  for _ in $(seq 1 100); do
    curl -s "$BASE/api/overview" | python3 -c "
import json,sys
d=json.load(sys.stdin)
ep=next((b['batch']['current_epoch'] for b in d['batches'] if b['batch']['batch_id']=='$1'),-1)
for b in d['batches']:
    if b['batch']['batch_id']=='$1':
        for f in b['files']:
            if f['file']['relpath']=='$2':
                vs=[v for v in f['versions'] if v['epoch']==ep]
                sys.exit(0 if vs and vs[-1]['state']=='$3' else 1)
sys.exit(1)
" && return 0
    sleep 0.2
  done
  echo "等待 $2 -> $3 超时" >&2; return 1
}

echo "== 1. 目录先行（mutable），数据晚到 =="
S init --batch demo --mode mutable --title "演示批次" --files report.csv:3000 notes.txt:500
S write --batch demo --names report.csv --slow
wait_state demo report.csv complete
S write --batch demo --names notes.txt
wait_state demo notes.txt complete

echo "== 2. 逐个发布并下游领取 =="
curl -s -X POST "$BASE/api/files/$(fid report.csv)/publish" > /dev/null
curl -s -X POST "$BASE/api/files/$(fid notes.txt)/publish" > /dev/null
curl -s -D - -o /tmp/pointwise-claimed.bin "$BASE/api/files/$(fid report.csv)/claim" \
  | grep -i "x-pointwise-sha256"

echo "== 3. 损坏版本不可发布，旧发布仍可领 =="
S corrupt --batch demo --name notes.txt
wait_state demo notes.txt corrupt
curl -s -o /dev/null -w "  损坏版发布 HTTP %{http_code}（期望 409）\n" \
  -X POST "$BASE/api/files/$(fid notes.txt)/publish"

echo "== 4. 同名替换后重新判齐，再发布新版本 =="
S write --batch demo --names notes.txt
wait_state demo notes.txt complete
curl -s -X POST "$BASE/api/files/$(fid notes.txt)/publish" > /dev/null

echo "== 5. 目录更正 epoch=1（旧清单变 stale）=="
S correct --batch demo --bump report.csv --add extra.log:700 --remove notes.txt
S write --batch demo --names report.csv extra.log
wait_state demo extra.log complete

echo "== 6. immutable 批次演示硬链接可靠引用 =="
S init --batch fixed --mode immutable --files frozen.bin:4096
S write --batch fixed
S seal  --batch fixed
wait_state fixed frozen.bin complete

echo "== 7. 撤回目录 + 停止/恢复接收器 =="
curl -s -X POST -H "Content-Type: application/json" -d '{"reason":"demo"}' \
  "$BASE/api/batches/demo/withdraw" > /dev/null
curl -s -X POST "$BASE/api/receiver/stop" > /dev/null
curl -s -X POST "$BASE/api/receiver/resume" > /dev/null

echo
echo "全部场景执行完毕。打开 $BASE/ 查看接收页；事件流含每次发布依据。"
