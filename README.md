# logforge

单进程的日志采集代理：跟踪文件、合并多行、解析成结构化记录、按规则分发。

## 快速开始

```bash
make build
make demo     # 生成 200 条日志并采集一次
make verify   # 断言输出与计数
make test     # 单元测试
```

## 结构

```
cmd/logforge        采集进程入口
cmd/mocklog         日志生成器（测试用，支持轮转与坏行注入）
internal/tail       文件追踪
internal/multiline  多行合并
internal/parser     解析器（json / logfmt / regex）
internal/plugin     解析器与输出端的注册表
internal/buffer     有界缓冲
internal/router     按规则分发
internal/sink       输出端（file / stdout）
internal/offset     位点持久化
internal/metrics    进程内计数器
configs/            配置示例
```

## 配置

见 `configs/logforge.yaml`。解析器按 `parsers` 顺序尝试，第一个成功即生效。

### 路由

`route.rules` 按书写顺序匹配。一条规则的 `match` 里所有字段都相等时命中：

```yaml
route:
  rules:
    - name: errors-to-alert
      match: {level: ERROR}
      sinks: [alert]
    - name: billing-audit
      match: {service: billing}
      sinks: [audit]
  default: [archive]
```

**多规则命中：合并。** 一条记录命中多条规则时，所有命中规则的 `sinks`
都会收到它（fan-out）；同一出口只投递一次，顺序按规则顺序、规则内
`sinks` 书写顺序去重合并。例如 service=billing 且 level=ERROR 的记录会
同时进入 `alert` 与 `audit`。

**默认回退。** 一条规则都没命中时才使用 `route.default`。命中任何规则
都不会再追加默认出口。`route.default` 必填且至少一个出口，避免未命中
的记录被静默丢弃。

**字段类型。** `match` 值保持 YAML 原始类型（字符串 / 布尔 / 数字）。
比较是类型严格的：

- 字符串只与字符串相等，布尔只与布尔相等；
- 数字之间按数值比较，`int64` 与 `float64` 可以互相匹配
  （JSON 解析出的 `507.0` 能匹配配置里的 `507`）；
- 字段缺失＝不命中，不算类型错误；
- 字段存在但类型不一致＝不命中，并计入 `route_type_mismatch` 指标。
  它不会阻止记录进入其他规则或默认出口。

### 启动期校验

以下配置错误在 `config.Load` / `pipeline.New` 阶段直接报错退出，错误信息
带配置文件行号：

- 规则名缺失或重复；
- `match` 为空（否则会匹配所有记录）、`match` 值不是标量或为 null；
- 规则未指定出口、出口重复；
- 规则或 `route.default` 引用不存在的输出端（错误里会列出已配置的出口）；
- `route.default` 为空；
- 输出端重名、类型未知。

### 指标

计数分成"按记录"和"按投递"两类，fan-out 时不会混淆：

| 指标 | 含义 |
| --- | --- |
| `records_routed` | 进入路由器的记录数（每条只计一次） |
| `records_matched` / `records_default` | 命中规则 / 回退默认，两者之和等于 `records_routed` |
| `emitted` | 成功的出口投递次数；一条记录发往 N 个出口且都成功则 +N |
| `write_failed` | 失败的出口投递次数 |
| `records_delivered` / `records_lost` | 至少一个出口成功 / 所有出口都失败的记录数 |
| `sink_written.<name>` / `sink_write_failed.<name>` | 按出口拆分的投递与失败计数 |
| `route_type_mismatch` | 字段存在但类型与配置值不一致的次数 |

**单个出口写失败不会影响其他出口**：同一条记录发往其余出口的投递照常完成，
失败会写入 stderr 并计入该出口的失败计数；只有所有出口都失败时
`records_lost` 才增加。退出时各出口按名字排序依次 flush/close，单个
flush/close 失败同样只影响自己。

## 扩展方式

- 新增日志格式：实现 `plugin.Parser`，在 `internal/parser` 里注册
- 新增输出目标：实现 `plugin.Sink`，在 `internal/sink` 里注册
- 新增路由规则：改配置即可，不需要动代码
