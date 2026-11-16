# logforge

单进程的日志采集代理：跟踪文件、合并多行、解析成结构化记录、按规则分发。

## 快速开始

```bash
make build
make demo     # 生成 200 条日志（含坏行与未换行片段）并采集一次
make verify   # 三方对账：生成清单 ↔ 采集计数 ↔ 输出行数
make test     # 单元测试
```

## 结构

```
cmd/logforge        采集进程入口
cmd/mocklog         日志生成器（测试用，支持轮转、坏行与片段注入、对账清单）
internal/tail       文件追踪
internal/multiline  多行合并（边界判定与收尾原因）
internal/parser     解析器（json / logfmt / regex）
internal/plugin     解析器与输出端的注册表
internal/buffer     有界缓冲
internal/router     按规则分发
internal/sink       输出端（file / stdout）
internal/offset     位点持久化
internal/metrics    进程内计数器
configs/            配置示例
```

## 多行边界与解析的协作

每一行先分类，再决定合并行为：

| 行的分类 | 判定 | 行为 |
| --- | --- | --- |
| 起始行 | 匹配 `start_pattern` | 收尾当前记录（`start`），开启新记录 |
| 续行 | 匹配 `continuation_pattern` | 并入当前记录 |
| 外来行 | 两者都不匹配 | 当前记录必然已结束，先收尾（`foreign`），该行按 `orphan` 策略处理 |

未设置 `continuation_pattern` 时，所有非起始行默认是续行（旧行为）；
此时可打开 `flush_on_complete`，让解析器链参与边界判断：当前块已经
能被完整解析（如一条单行 JSON），遇到非起始行就先收尾（`complete`），
孤立坏行不会粘进上一条记录。

每条收尾的记录都带原因，计数为 `blocks_closed_<reason>`：
`start` / `foreign` / `complete` / `max_lines` / `timeout` / `shutdown` /
`orphan` / `passthrough`。

### 取舍说明

- **flush_after_ms**：太小会劈开写得慢的堆栈（残余续行变成孤立行）；
  太大则文件末尾的记录延迟可见。退出时一定会以 `shutdown` 收尾，不会丢。
- **max_lines + on_overflow**：`truncate` 丢弃超长记录的剩余续行并计数
  （`overflow_lines_dropped`），记录边界保持干净；`split` 保留全部数据，
  但会产生没有起始行的"幻影记录"（通常解析失败进死信）。
- **坏起始行**（半行 JSON 粘了下一条首行）：无法可靠猜拆，整块保留
  `raw` 计入 `parse_failed` 并路由到死信；影响范围限于这一条记录，
  不会吞掉后续记录。
- **flush_on_complete 与解析器的配合**：解析链不能把多行记录的首行
  判为完整（例如 regex 恰好匹配堆栈首行），否则堆栈会在首行后被劈开。
  堆栈场景请设置 `continuation_pattern` 保护续行——设置后外来行一律
  收尾，`flush_on_complete` 不再参与。
- **字段冲突**：解析器顺序即优先级，胜出的解析器名写入 `_parser`
  （`parse.annotate_parser` 可改名或置空关闭）。JSON 的数字是
  float64，logfmt/regex 经 coerce 可能是 int64；路由匹配前统一
  字符串化，因此 `500` 与 `500.0` 等价。logfmt 的键收紧为标识符，
  粘连了半行 JSON 的日志不会"侥幸解析成功"。
- **退出收尾**：停机时合并器里未完成的记录以 `shutdown` 收尾；
  文件末尾未换行的半行不计入位点，下次运行时从行首重读——
  宁可重读一段，也不把截断的半行当作记录发出去。

## 对账关系

`make verify` 断言以下等式（生成端清单 ↔ 采集计数 ↔ 输出行数）：

```
blocks_formed == Σ blocks_closed_<reason>
blocks_formed == parse_ok + parse_failed
parse_ok      == Σ parse_ok_<parser>
单出口时      blocks_formed == emitted + write_failed + dropped
all.log 行数  == parse_ok        bad.log 行数 == parse_failed
```

## 配置

见 `configs/logforge.yaml`。解析器按 `parsers` 顺序尝试，第一个成功即生效；
路由规则按顺序评估，第一条全部字段命中的规则生效，`route.default`
是未命中任何规则时的出口。

## 扩展方式

- 新增日志格式：实现 `plugin.Parser`，在 `internal/parser` 里注册
- 新增输出目标：实现 `plugin.Sink`，在 `internal/sink` 里注册
- 新增路由规则：改配置即可，不需要动代码
