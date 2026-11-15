#!/bin/sh
# verify.sh —— 对一次 demo 的结果做断言。
set -eu
cd "$(dirname "$0")/.."

# Keep verify standalone: a clean checkout has no prior demo artifacts.
mkdir -p /tmp/logforge
sh scripts/demo.sh >/tmp/logforge-verify-demo.log 2>&1

python3 - <<'PY'
import json, os, sys

metrics = json.load(open("/tmp/logforge/metrics.json"))
lines = sum(1 for _ in open("/tmp/logforge/out/all.log"))
offset = json.load(open("/tmp/logforge/offset.json"))

checks = []
def check(name, ok, detail):
    checks.append((name, ok, detail))

check("有输入被读取", metrics["lines_read"] > 0, f"lines_read={metrics['lines_read']}")
check("形成了记录块", metrics["blocks_formed"] > 0, f"blocks_formed={metrics['blocks_formed']}")
check("有解析成功", metrics["parse_ok"] > 0, f"parse_ok={metrics['parse_ok']}")
check("输出非空", lines > 0, f"output_lines={lines}")
check("位点已保存", bool(offset.get("offsets")), f"offsets={offset.get('offsets')}")
check("没有写失败", metrics["write_failed"] == 0, f"write_failed={metrics['write_failed']}")

failed = 0
print("== 断言 ==")
for name, ok, detail in checks:
    if not ok:
        failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
