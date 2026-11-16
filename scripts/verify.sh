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
offset = json.load(open("/tmp/logforge/offset.json"))

def rows(name):
    path = f"/tmp/logforge/out/{name}.log"
    if not os.path.exists(path):
        return []
    out = []
    for line in open(path):
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out

archive, audit, alert = rows("archive"), rows("audit"), rows("alert")

def has(r, **want):
    return all(r.get(k) == v for k, v in want.items())

checks = []
def check(name, ok, detail):
    checks.append((name, ok, detail))

check("有输入被读取", metrics["lines_read"] > 0, f"lines_read={metrics['lines_read']}")
check("形成了记录块", metrics["blocks_formed"] > 0, f"blocks_formed={metrics['blocks_formed']}")
check("有解析成功", metrics["parse_ok"] > 0, f"parse_ok={metrics['parse_ok']}")

# 规则命中：ERROR -> alert；billing -> audit。
check("告警文件非空", len(alert) > 0, f"alert={len(alert)}")
check("告警文件只含 ERROR", all(r.get("level") == "ERROR" for r in alert),
      f"alert levels={sorted({r.get('level') for r in alert})}")
check("审计文件非空", len(audit) > 0, f"audit={len(audit)}")
check("审计文件只含 billing", all(r.get("service") == "billing" for r in audit),
      f"audit services={sorted({r.get('service') for r in audit})}")

# 合并语义：billing 的 ERROR 记录同时进入 audit 与 alert。
billing_errors = [r for r in alert if r.get("service") == "billing"]
check("billing 的 ERROR 也进审计",
      all(any(has(a, level="ERROR", service="billing") for a in audit) for r in billing_errors),
      f"billing_errors={len(billing_errors)}")

# 默认回退：未命中任何规则的记录进 archive。
check("归档文件非空", len(archive) > 0, f"archive={len(archive)}")
check("归档只含未命中规则的记录",
      all(r.get("level") != "ERROR" and r.get("service") != "billing" for r in archive),
      f"archive sample={archive[:1]}")

# 计数语义：记录数按条，投递数按出口。
check("记录路由计数合理",
      metrics["records_matched"] + metrics["records_default"] == metrics["records_routed"],
      f"matched={metrics['records_matched']} default={metrics['records_default']} "
      f"routed={metrics['records_routed']}")
check("投递计数等于各文件行数之和",
      metrics["emitted"] == len(archive) + len(audit) + len(alert),
      f"emitted={metrics['emitted']} files={len(archive)+len(audit)+len(alert)}")
check("没有写失败", metrics["write_failed"] == 0, f"write_failed={metrics['write_failed']}")
check("没有丢记录", metrics["records_lost"] == 0, f"records_lost={metrics['records_lost']}")

check("位点已保存", bool(offset.get("offsets")), f"offsets={offset.get('offsets')}")

failed = 0
print("== 断言 ==")
for name, ok, detail in checks:
    if not ok:
        failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
