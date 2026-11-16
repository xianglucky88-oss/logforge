#!/bin/sh
# verify.sh —— 对一次 demo 的结果做断言。
#
# 核心是对账：生成端的记录清单（manifest）、采集端的计数（metrics）
# 与输出端的行数（all.log / bad.log）三方必须一致。
set -eu
cd "$(dirname "$0")/.."

# Keep verify standalone: a clean checkout has no prior demo artifacts.
mkdir -p /tmp/logforge
sh scripts/demo.sh >/tmp/logforge-verify-demo.log 2>&1

python3 - <<'PY'
import json, sys

metrics = json.load(open("/tmp/logforge/metrics.json"))
manifest = json.load(open("/tmp/logforge/manifest.json"))
out_all = sum(1 for _ in open("/tmp/logforge/out/all.log"))
out_bad = sum(1 for _ in open("/tmp/logforge/out/bad.log"))
offset = json.load(open("/tmp/logforge/offset.json"))

def m(key):
    return metrics.get(key, 0)

checks = []
def check(name, ok, detail):
    checks.append((name, ok, detail))

# 物理行：每条 JSON/logfmt/坏行/片段记录 1 行，每条堆栈 4 行。
check("物理行全部读取", m("lines_read") == manifest["lines"],
      f"lines_read={m('lines_read')} manifest.lines={manifest['lines']}")

# 每条逻辑记录恰好形成一条记录块：坏行单独成条，片段粘连在宿主记录内。
check("记录块与清单一一对应", m("blocks_formed") == manifest["records"],
      f"blocks_formed={m('blocks_formed')} records={manifest['records']}")

# 收尾原因可解释：各原因之和等于总块数。
closed = {k: v for k, v in metrics.items() if k.startswith("blocks_closed_")}
check("收尾原因合计对账", sum(closed.values()) == m("blocks_formed"),
      f"reasons={closed}")

# 解析结果：正常记录 = json + logfmt + 堆栈(regex)；失败 = 坏行 + 片段。
check("解析成败合计对账", m("parse_ok") + m("parse_failed") == m("blocks_formed"),
      f"parse_ok={m('parse_ok')} parse_failed={m('parse_failed')}")
check("JSON 记录全部解析", m("parse_ok_json") == manifest["json"],
      f"parse_ok_json={m('parse_ok_json')} json={manifest['json']}")
check("logfmt 记录全部解析", m("parse_ok_logfmt") == manifest["logfmt"],
      f"parse_ok_logfmt={m('parse_ok_logfmt')} logfmt={manifest['logfmt']}")
check("堆栈记录全部解析", m("parse_ok_regex") == manifest["stacks"],
      f"parse_ok_regex={m('parse_ok_regex')} stacks={manifest['stacks']}")
check("坏行与片段进入失败", m("parse_failed") == manifest["bad"] + manifest["fragments"],
      f"parse_failed={m('parse_failed')} bad={manifest['bad']} fragments={manifest['fragments']}")

# 输出端：正常记录进 all.log，失败记录进 bad.log，每条恰好写一次。
check("正常输出行数对账", out_all == m("parse_ok"),
      f"all.log={out_all} parse_ok={m('parse_ok')}")
check("死信输出行数对账", out_bad == m("parse_failed"),
      f"bad.log={out_bad} parse_failed={m('parse_failed')}")
check("写出总量对账", m("emitted") == m("blocks_formed"),
      f"emitted={m('emitted')} blocks_formed={m('blocks_formed')}")
check("没有丢弃与写失败", m("dropped") == 0 and m("write_failed") == 0,
      f"dropped={m('dropped')} write_failed={m('write_failed')}")

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
