#!/usr/bin/env bash
# verify-recovery.sh —— 端到端验证可恢复跟踪协议：
#   S1 运行中轮转（旧文件仍被写入）
#   S2 kill -9 后重启（重放窗口有界、不丢、已提交不重复）
#   S3 半行 + SIGTERM（跨重启拼成完整记录）
#   S4 运行中截断（copytruncate）
#   S5 位点文件损坏（.bak 回退 / 双损坏兜底）
#   S6 停机期间轮转（启动扫描按 inode 找回 backlog）
set -u
cd "$(dirname "$0")/.."

ROOT=/tmp/logforge-recovery
BIN=./bin/logforge
FAILURES=0

rm -rf "$ROOT"
mkdir -p "$ROOT"

# gen_config <dir> —— 写一份快轮询、快提交的测试配置。
gen_config() {
  local d=$1
  mkdir -p "$d"
  cat > "$d/config.yaml" <<EOF
source:
  path: $d/app.log
  poll_interval_ms: 50
  rotate_drain_quiet_polls: 3
  rotate_drain_max_ms: 5000
multiline:
  start_pattern: '^{'
  max_lines: 50
  flush_after_ms: 500
parsers:
  - name: json
buffer:
  capacity: 10000
  policy: drop_oldest
route:
  rules: []
  default: [all]
sinks:
  - name: all
    type: file
    config:
      path: $d/out.log
offset:
  path: $d/offset.json
  commit_interval_ms: 200
EOF
}

# start_collector <dir> <run#> —— PID 放在全局变量 CPID 里
# （不能用命令替换：子 shell 启动的后台进程无法被当前 shell wait）。
start_collector() {
  "$BIN" -config "$1/config.yaml" -metrics-out "$1/metrics.$2.json" \
    >"$1/stderr.$2.log" 2>&1 &
  CPID=$!
}

# stop_collector —— 优雅停止并等待真正退出（最终提交落盘后才返回）。
stop_collector() {
  kill -TERM "$CPID" 2>/dev/null
  wait "$CPID" 2>/dev/null
}

# wait_lines <file> <n> —— 等输出文件达到 n 行。
wait_lines() {
  local f=$1 n=$2 i
  for i in $(seq 1 1500); do
    if [ -f "$f" ] && [ "$(wc -l < "$f")" -ge "$n" ]; then
      return 0
    fi
    sleep 0.02
  done
  echo "TIMEOUT waiting for $n lines in $f (have $(wc -l < "$f" 2>/dev/null || echo 0))" >&2
  return 1
}

# emit <file> <from> <to> [interval_s] —— 追加编号 JSON 行。
emit() {
  local f=$1 from=$2 to=$3 interval=${4:-0}
  local i
  for i in $(seq "$from" "$to"); do
    printf '{"n":%d}\n' "$i" >> "$f"
    [ "$interval" != "0" ] && sleep "$interval"
  done
}

# check <scenario> <name> <ok> <detail>
check() {
  if [ "$3" = "0" ]; then
    echo "  [PASS] $1: $2 ($4)"
  else
    echo "  [FAIL] $1: $2 ($4)"
    FAILURES=$((FAILURES + 1))
  fi
}

# assert_unique <dir> <expected_count> —— 输出恰好是 1..N 各一次。
# 返回重复条数到全局 DUPS。
assert_unique() {
  DUPS=$(python3 - "$1/out.log" "$2" <<'PY'
import json, sys
ns = []
for line in open(sys.argv[1]):
    line = line.strip()
    if not line:
        continue
    rec = json.loads(line)
    ns.append(rec.get("n"))
want = int(sys.argv[2])
got = sorted(x for x in ns if isinstance(x, int))
missing = [i for i in range(1, want + 1) if i not in set(got)]
dups = len(got) - len(set(got))
if missing:
    print(f"MISSING:{missing[:5]}", file=sys.stderr)
print(dups)
PY
)
}

metric() { # metric <file> <name> → 值（缺省 0）
  python3 - "$1" "$2" <<'PY'
import json, sys
try:
    print(json.load(open(sys.argv[1])).get(sys.argv[2], 0))
except Exception:
    print(0)
PY
}

echo "== S1 运行中轮转（旧文件仍被写入）=="
D=$ROOT/s1; gen_config "$D"
emit "$D/app.log" 1 150
start_collector "$D" 1
wait_lines "$D/out.log" 150 || check S1 "首批 150 条" 1 "timeout"
mv "$D/app.log" "$D/app.log.1"
emit "$D/app.log.1" 151 160   # 旧文件描述符上的迟到写入
emit "$D/app.log" 161 200     # 新文件
wait_lines "$D/out.log" 200 || check S1 "全部 200 条" 1 "timeout"
stop_collector
assert_unique "$D" 200
check S1 "200 条恰好一次" "$([ "$DUPS" = "0" ] && echo 0 || echo 1)" "dups=$DUPS"
check S1 "检测到轮转" "$([ "$(metric "$D/metrics.1.json" rotations_detected)" -ge 1 ] && echo 0 || echo 1)" \
  "rotations=$(metric "$D/metrics.1.json" rotations_detected)"
check S1 "旧文件被封闭" "$([ "$(metric "$D/metrics.1.json" rotated_sources_sealed)" -ge 1 ] && echo 0 || echo 1)" \
  "sealed=$(metric "$D/metrics.1.json" rotated_sources_sealed)"

echo "== S2 kill -9 后重启 =="
D=$ROOT/s2; gen_config "$D"
start_collector "$D" 1
emit "$D/app.log" 1 500 0.005 &
writer=$!
sleep 1.2
kill -9 "$CPID" 2>/dev/null; wait "$CPID" 2>/dev/null
wait "$writer" 2>/dev/null
start_collector "$D" 2
ok=0
for i in $(seq 1 1500); do
  n=$(python3 -c "
import json
try:
    print(len({json.loads(l).get('n') for l in open('$D/out.log') if l.strip()}))
except Exception:
    print(0)" 2>/dev/null)
  [ "$n" -ge 500 ] && { ok=1; break; }
  sleep 0.02
done
[ "$ok" = "1" ] || check S2 "收齐 500 条" 1 "timeout, have $n"
stop_collector
assert_unique "$D" 500
check S2 "500 条零丢失" "$([ "$DUPS" != "MISSING"* ] && echo 0 || echo 1)" "unique 检查"
# 重放窗口 ≤ 提交间隔(200ms ≈ 40 条) + 调度余量
check S2 "重复条数有界 (≤150)" "$([ "$DUPS" -le 150 ] && echo 0 || echo 1)" "dups=$DUPS"
check S2 "重启后从位点续读" "$([ "$(metric "$D/metrics.2.json" offset_resumes)" -ge 1 ] && echo 0 || echo 1)" \
  "resumes=$(metric "$D/metrics.2.json" offset_resumes)"

echo "== S3 半行 + SIGTERM =="
D=$ROOT/s3; gen_config "$D"
emit "$D/app.log" 1 10
printf '{"n":11' >> "$D/app.log"   # 没有换行的半行
start_collector "$D" 1
wait_lines "$D/out.log" 10 || check S3 "前 10 条" 1 "timeout"
stop_collector
pending=$(python3 - "$D/offset.json" <<'PY'
import base64, json, sys
env = json.load(open(sys.argv[1]))
for ps in env["data"]["paths"].values():
    for st in ps.get("identities", {}).values():
        p = st.get("pending")
        if p:
            print(base64.b64decode(p).decode(), end="")
PY
)
check S3 "半行已持久化到位点" "$([ "$pending" = '{"n":11' ] && echo 0 || echo 1)" "pending=$pending"
printf ',"tail":true}\n' >> "$D/app.log"   # 停机期间写者补完这一行
emit "$D/app.log" 12 16
start_collector "$D" 2
wait_lines "$D/out.log" 16 || check S3 "全部 16 条" 1 "timeout"
stop_collector
joined=$(python3 - "$D/out.log" <<'PY'
import json, sys
frag, joined = 0, 0
for line in open(sys.argv[1]):
    rec = json.loads(line)
    if rec.get("n") == 11 and rec.get("tail") is True:
        joined += 1
    if rec.get("parse_error") and "n\":11" in rec.get("raw", ""):
        frag += 1
print(f"{joined} {frag}")
PY
)
check S3 "半行跨重启拼成完整记录" "$([ "${joined% *}" = "1" ] && echo 0 || echo 1)" "joined=$joined"
check S3 "没有半行碎片" "$([ "${joined#* }" = "0" ] && echo 0 || echo 1)" "fragments=${joined#* }"
assert_unique "$D" 16
check S3 "16 条恰好一次" "$([ "$DUPS" = "0" ] && echo 0 || echo 1)" "dups=$DUPS"

echo "== S4 运行中截断（copytruncate）=="
D=$ROOT/s4; gen_config "$D"
emit "$D/app.log" 1 50
start_collector "$D" 1
wait_lines "$D/out.log" 50 || check S4 "前 50 条" 1 "timeout"
: > "$D/app.log"              # 同 inode 截断
emit "$D/app.log" 51 80
wait_lines "$D/out.log" 80 || check S4 "全部 80 条" 1 "timeout"
stop_collector
check S4 "检测到截断" "$([ "$(metric "$D/metrics.1.json" truncations_detected)" -ge 1 ] && echo 0 || echo 1)" \
  "truncations=$(metric "$D/metrics.1.json" truncations_detected)"
assert_unique "$D" 80
check S4 "80 条恰好一次" "$([ "$DUPS" = "0" ] && echo 0 || echo 1)" "dups=$DUPS"

echo "== S5 位点文件损坏 =="
D=$ROOT/s5; gen_config "$D"
emit "$D/app.log" 1 20
start_collector "$D" 1
wait_lines "$D/out.log" 20 || check S5 "前 20 条" 1 "timeout"
stop_collector
echo 'this is not json' > "$D/offset.json"   # 主文件损坏，.bak 还在
emit "$D/app.log" 21 30
start_collector "$D" 2
wait_lines "$D/out.log" 30 || check S5 "bak 恢复后 30 条" 1 "timeout"
stop_collector
check S5 "从 .bak 恢复" "$([ "$(metric "$D/metrics.2.json" offset_recovered_from_bak)" -ge 1 ] && echo 0 || echo 1)" \
  "from_bak=$(metric "$D/metrics.2.json" offset_recovered_from_bak)"
assert_unique "$D" 30
check S5 "bak 恢复后 30 条恰好一次" "$([ "$DUPS" = "0" ] && echo 0 || echo 1)" "dups=$DUPS"
# 双损坏：主文件和 .bak 都写坏
echo garbage > "$D/offset.json"; echo garbage > "$D/offset.json.bak"
emit "$D/app.log" 31 35
start_collector "$D" 3
wait_lines "$D/out.log" 65 || check S5 "双损坏后仍采集" 1 "timeout"
stop_collector
check S5 "双损坏有指标且进程存活" "$([ "$(metric "$D/metrics.3.json" offset_corrupt_total)" -ge 1 ] && echo 0 || echo 1)" \
  "corrupt=$(metric "$D/metrics.3.json" offset_corrupt_total)"
check S5 "双损坏后新内容不丢" "$(python3 -c "
import json
ns = {json.loads(l).get('n') for l in open('$D/out.log') if l.strip()}
print(0 if all(i in ns for i in range(31, 36)) else 1)")" "n=31..35"

echo "== S6 停机期间轮转 =="
D=$ROOT/s6; gen_config "$D"
emit "$D/app.log" 1 100
start_collector "$D" 1
wait_lines "$D/out.log" 100 || check S6 "前 100 条" 1 "timeout"
stop_collector
mv "$D/app.log" "$D/app.log.1"
emit "$D/app.log.1" 101 120   # 停机期间旧文件的迟到写入
emit "$D/app.log" 121 150     # 新文件
start_collector "$D" 2
wait_lines "$D/out.log" 150 || check S6 "全部 150 条" 1 "timeout"
stop_collector
check S6 "启动扫描找回 backlog" "$([ "$(metric "$D/metrics.2.json" backlog_files_recovered)" -ge 1 ] && echo 0 || echo 1)" \
  "backlog=$(metric "$D/metrics.2.json" backlog_files_recovered)"
assert_unique "$D" 150
check S6 "150 条恰好一次" "$([ "$DUPS" = "0" ] && echo 0 || echo 1)" "dups=$DUPS"

echo "== S7 v1 位点格式迁移 =="
D=$ROOT/s7; gen_config "$D"
emit "$D/app.log" 1 30
size30=$(wc -c < "$D/app.log")   # 前 30 行的字节数，作为 v1 位点
printf '{"version":1,"offsets":{"%s":%s}}\n' "$D/app.log" "$size30" > "$D/offset.json"
emit "$D/app.log" 31 50
start_collector "$D" 1
wait_lines "$D/out.log" 20 || check S7 "迁移后只读新增 20 条" 1 "timeout"
stop_collector
check S7 "v1 迁移有指标" "$([ "$(metric "$D/metrics.1.json" offset_migrated)" -ge 1 ] && echo 0 || echo 1)" \
  "migrated=$(metric "$D/metrics.1.json" offset_migrated)"
assert_unique "$D" 50 2>/dev/null || true
only_new=$(python3 -c "
import json
ns = sorted(json.loads(l).get('n') for l in open('$D/out.log') if l.strip())
print(0 if ns == list(range(31, 51)) else 1)")
check S7 "恰好是 31..50 各一次" "$only_new" "out 只含新行"

echo
if [ "$FAILURES" -eq 0 ]; then
  echo "恢复场景全部通过"
else
  echo "恢复场景失败: $FAILURES 项"
  exit 1
fi
