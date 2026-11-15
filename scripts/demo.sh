#!/bin/sh
# demo.sh —— 生成一批日志，跑一次采集，打印结果。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/logforge
mkdir -p /tmp/logforge/out

echo "== 1. 生成 200 条日志（含 JSON、logfmt、多行堆栈、坏行）=="
./bin/mocklog -path /tmp/logforge/app.log -lines 200 -interval-ms 1

echo
echo "== 2. 运行采集（4 秒后优雅退出）=="
./bin/logforge -config configs/logforge.yaml -metrics-out /tmp/logforge/metrics.json &
pid=$!
sleep 4
kill -TERM "$pid" 2>/dev/null || true
wait "$pid" || true

echo
echo "== 3. 输出文件行数 =="
wc -l /tmp/logforge/out/all.log

echo
echo "== 4. 指标 =="
cat /tmp/logforge/metrics.json

echo
echo "== 5. 位点文件 =="
cat /tmp/logforge/offset.json

echo
echo "== 6. 前 3 条输出 =="
head -3 /tmp/logforge/out/all.log
