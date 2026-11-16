#!/bin/sh
# verify.sh —— 对一次 demo 的结果做断言。
set -eu
cd "$(dirname "$0")/.."

# Keep verify standalone: a clean checkout has no prior demo artifacts.
mkdir -p /tmp/logforge
sh scripts/demo.sh >/tmp/logforge-verify-demo.log 2>&1

python3 - <<'PY'
import json, sys

metrics = json.load(open("/tmp/logforge/metrics.json"))
lines = sum(1 for _ in open("/tmp/logforge/out/all.log"))
envelope = json.load(open("/tmp/logforge/offset.json"))
get = lambda name: metrics.get(name, 0)

checks = []
def check(name, ok, detail):
    checks.append((name, ok, detail))

# v2 位点格式：信封 + 按文件身份的状态。
data = envelope.get("data", {})
paths = data.get("paths", {})
src = paths.get("/tmp/logforge/app.log", {})
identities = src.get("identities", {})

check("有输入被读取", get("lines_read") > 0, f"lines_read={get('lines_read')}")
check("形成了记录块", get("blocks_formed") > 0, f"blocks_formed={get('blocks_formed')}")
check("有解析成功", get("parse_ok") > 0, f"parse_ok={get('parse_ok')}")
check("输出非空", lines > 0, f"output_lines={lines}")
check("没有写失败", get("write_failed") == 0, f"write_failed={get('write_failed')}")
check("位点信封是 v2", envelope.get("version") == 2 and "crc32" in envelope,
      f"version={envelope.get('version')}")
check("位点按身份记录", len(identities) > 0 and src.get("current") in identities,
      f"current={src.get('current')} identities={list(identities)}")
check("位点有提交", get("offset_commits_total") > 0,
      f"offset_commits_total={get('offset_commits_total')}")
check("确认位点大于 0", any(s.get("offset", 0) > 0 for s in identities.values()),
      f"offsets={[s.get('offset') for s in identities.values()]}")

failed = 0
print("== 断言 ==")
for name, ok, detail in checks:
    if not ok:
        failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
