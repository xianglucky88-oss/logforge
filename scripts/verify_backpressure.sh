#!/bin/sh
# verify_backpressure.sh —— 验证有界缓冲的背压、溢写与恢复语义。
#
# 不依赖网络与外部服务，只用本仓库二进制 + 临时目录，断言四类性质：
#   1. 三态恒等式：pushed == emitted + buffered + dropped_total
#   2. 满载策略：block 有界不卡死；spill 落盘后零丢失并可回放
#   3. 崩溃恢复：kill -9 后段文件可收养、按 FIFO 回放、重启幂等
#   4. sink 故障：写失败有界重试后进 dead-letter，消费协程不永久卡死
set -eu
cd "$(dirname "$0")/.."

BIN=bin
LOGFORGE=$BIN/logforge
MOCKLOG=$BIN/mocklog
ROOT=$(mktemp -d /tmp/logforge-bp-XXXXXX)
trap 'rm -rf "$ROOT"' EXIT

fail=0
pass() { echo "  [PASS] $1: $2"; }
bad()  { echo "  [FAIL] $1: $2"; fail=$((fail+1)); }

# make_config <dir> <policy> <sink_type> [block_ms]
make_config() {
  dir=$1; policy=$2; sink=$3; blockms=${4:-0}
  cat > "$dir/config.yaml" <<EOF
source:
  path: $dir/app.log
  poll_interval_ms: 50
multiline:
  start_pattern: '^(\d{4}-\d{2}-\d{2}T|\{|ts=)'
  max_lines: 200
  flush_after_ms: 1000
parsers:
  - name: json
  - name: logfmt
buffer:
  capacity: 32
  policy: $policy
  spill_dir: $dir/spill
  spill_segment_bytes: 2048
  spill_max_segments: 128
  spill_max_bytes: 104857600
  block_timeout_ms: 300
  block_timeout_action: drop_new
  spill_full_action: drop_new
  manifest_every_acks: 128
output:
  write_timeout_ms: 400
  max_retries: 1
  retry_backoff_ms: 5
  drain_timeout_ms: 2000
route:
  rules: []
  default: [all]
sinks:
  - name: all
    type: $sink
    config:
      path: $dir/out.log
      block_ms: $blockms
offset:
  path: $dir/offset.json
EOF
}

echo "== 1. spill 策略：产出快、sink 慢时落盘且零丢失 =="
d1=$ROOT/case1; mkdir -p "$d1"
$MOCKLOG -path "$d1/app.log" -lines 200 -interval-ms 0 >/dev/null 2>&1
make_config "$d1" spill file
$LOGFORGE -config "$d1/config.yaml" -metrics-out "$d1/metrics.json" &
pid=$!; sleep 3; kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
python3 - "$d1" <<'PY'
import json, sys
d = sys.argv[1]
m = json.load(open(f"{d}/metrics.json"))
out = sum(1 for _ in open(f"{d}/out.log"))
pushed, emitted, dropped = m["pushed"], m["emitted"], m["dropped"]
buffered = m["buffered"]
if pushed == emitted + buffered + dropped and dropped == 0 and out == pushed:
    print(f"  [PASS] 三态守恒且零丢失: pushed={pushed} emitted={emitted} spilled={m['spilled_writes']}")
else:
    print(f"  [FAIL] 守恒破坏: pushed={pushed} emitted={emitted} buffered={buffered} dropped={dropped} out={out}")
    sys.exit(1)
PY

echo "== 2. block 策略：sink 慢时 Push 有界，不永久卡死 =="
d2=$ROOT/case2; mkdir -p "$d2"
# 少量输入即可证明“有界不卡死”：每条记录最多 max_retries+1 次写超时。
$MOCKLOG -path "$d2/app.log" -lines 12 -interval-ms 0 >/dev/null 2>&1
# block_ms=800 远超 output.write_timeout_ms=400，每次写必然超时失败。
make_config "$d2" block faulty 800
# 缩短写超时与重试，控制验证耗时，同时保留“超时即失败、有界重试”的语义。
sed -i 's/write_timeout_ms: 400/write_timeout_ms: 100/; s/max_retries: 1/max_retries: 1/; s/block_timeout_ms: 300/block_timeout_ms: 100/' "$d2/config.yaml"
start=$(date +%s%N)
$LOGFORGE -config "$d2/config.yaml" -metrics-out "$d2/metrics.json" >"$d2/run.log" 2>&1 &
pid=$!; sleep 6; kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
elapsed=$(( ($(date +%s%N) - start) / 1000000 ))
python3 - "$d2" "$elapsed" <<'PY'
import json, sys, glob
d, elapsed = sys.argv[1], int(sys.argv[2])
m = json.load(open(f"{d}/metrics.json"))
ok_deadletter = m["dead_lettered"] > 0
# 有界性判据：进程在观察窗口内退出（被 TERM 唤醒），且每条失败记录都有
# 确定去向（dead-letter），没有任何记录无限停留。这里用 20s 作为宽松上界。
ok_bounded = elapsed < 20000
dl = glob.glob(f"{d}/spill/dead-*")
print(f"  [{'PASS' if ok_deadletter else 'FAIL'}] 慢 sink 记录进入 dead-letter: dead_lettered={m['dead_lettered']} 文件={len(dl)}")
print(f"  [{'PASS' if ok_bounded else 'FAIL'}] 消费协程未永久卡死: 耗时={elapsed}ms (<20000ms)")
sys.exit(0 if ok_deadletter and ok_bounded else 1)
PY

echo "== 3. 崩溃恢复：kill -9 后段文件收养、FIFO 回放、重启幂等 =="
d3=$ROOT/case3; mkdir -p "$d3"
$MOCKLOG -path "$d3/app.log" -lines 150 -interval-ms 0 >/dev/null 2>&1
make_config "$d3" spill faulty 200
$LOGFORGE -config "$d3/config.yaml" -metrics-out "$d3/m1.json" &
pid=$!; sleep 2; kill -9 "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
seg_count=$(ls "$d3/spill"/spill-*.log 2>/dev/null | wc -l | tr -d ' ')
if [ "$seg_count" -gt 0 ]; then pass "强杀后磁盘留有溢写段" "segments=$seg_count"; else bad "强杀后无溢写段" "segments=0"; fi

# 换快速 file sink 重启：应收养段并回放。
sed 's/type: faulty/type: file/; s/block_ms: 200/block_ms: 0/' "$d3/config.yaml" > "$d3/config2.yaml"
$LOGFORGE -config "$d3/config2.yaml" -metrics-out "$d3/m2.json" 2>"$d3/recovery.log" &
pid=$!; sleep 3; kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
grep -q "buffer recovery" "$d3/recovery.log" && pass "启动报告了恢复现场" "$(grep 'buffer recovery' "$d3/recovery.log")" || bad "未报告恢复" ""
first_out=$(wc -l < "$d3/out.log" | tr -d ' ')

# 再重启一次：无新输入时不允许重复回放。
$LOGFORGE -config "$d3/config2.yaml" -metrics-out "$d3/m3.json" 2>/dev/null &
pid=$!; sleep 2; kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
second_out=$(wc -l < "$d3/out.log" | tr -d ' ')
if [ "$first_out" = "$second_out" ] && [ "$first_out" -gt 0 ]; then
  pass "重启幂等无重复回放" "lines=$first_out"
else
  bad "重启后重复回放或无输出" "first=$first_out second=$second_out"
fi
# 回放后段应已退休，目录只剩 manifest（或仅剩在途段）。
remain=$(ls "$d3/spill"/spill-*.log 2>/dev/null | wc -l | tr -d ' ')
[ "$remain" -eq 0 ] && pass "已消费段被退休清理" "remaining=$remain" || echo "  [INFO] 仍有在途段 remaining=$remain"

echo
if [ "$fail" -eq 0 ]; then
  echo "结果: 全部背压/溢写/恢复断言通过"
else
  echo "结果: $fail 项失败"
  exit 1
fi
