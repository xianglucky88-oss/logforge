#!/bin/sh
# demo.sh —— 生成一批日志，跑一次采集，打印结果。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/logforge
mkdir -p /tmp/logforge/out

echo "== 1. 生成 200 条日志（JSON、logfmt、多行堆栈、坏行、未换行片段）=="
./bin/mocklog -path /tmp/logforge/app.log -lines 200 -interval-ms 1 \
  -fragment-every 25 -manifest /tmp/logforge/manifest.json

echo
echo "== 2. 运行采集（4 秒后优雅退出）=="
./bin/logforge -config configs/logforge.yaml -metrics-out /tmp/logforge/metrics.json &
pid=$!
sleep 4
kill -TERM "$pid" 2>/dev/null || true
wait "$pid" || true

echo
echo "== 3. 记录清单（生成端对账依据）=="
cat /tmp/logforge/manifest.json

echo
echo "== 4. 输出行数（all=解析成功，bad=解析失败）=="
wc -l /tmp/logforge/out/all.log /tmp/logforge/out/bad.log

echo
echo "== 5. 指标 =="
cat /tmp/logforge/metrics.json

echo
echo "== 6. 位点文件 =="
cat /tmp/logforge/offset.json

echo
echo "== 7. 前 2 条正常输出 =="
head -2 /tmp/logforge/out/all.log

echo
echo "== 8. 前 2 条死信输出 =="
head -2 /tmp/logforge/out/bad.log
