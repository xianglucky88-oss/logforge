# logforge

单进程的日志采集代理：跟踪文件、合并多行、解析成结构化记录、按规则分发。

## 快速开始

```bash
make build
make demo       # 生成 200 条日志并采集一次
make verify     # 断言输出与计数
make verify-bp  # 断言背压/溢写/崩溃恢复/死信语义
make test       # 单元测试
```

## 有界缓冲：背压、溢写与三态对账

采集速度短时超过 sink 写入速度时，缓冲在**有限内存**下继续工作。
每条记录分配单调递增的 `Seq`，任意时刻恰好处于三态之一，恒等式

```
pushed == emitted + buffered + dropped_total
```

在同一把互斥锁内维护，`Stats()`/`DeltaStats()` 快照与真实丢弃数严格同步，
不存在“指标看到的 dropped 与队列实际不符”的窗口。

### 满载策略（buffer.policy）

| policy | 满载行为 | 数据语义 |
| --- | --- | --- |
| `drop_new` | 直接拒绝新记录 | 保全已有数据，丢弃计入 `dropped_new` |
| `drop_oldest` | 踢出最老记录、保留最新 | 保新，丢弃计入 `dropped_oldest` |
| `block` | 阻塞生产者直到有空位 | 保全；`block_timeout_ms` 到期后按 `block_timeout_action` 兜底，`Close` 必唤醒，不会永久卡死 |
| `spill` | 内存环整批落盘到滚动段 | 内存严格有界；磁盘预算耗尽或 I/O 失败时按 `spill_full_action` 分级降级 |

### 溢写与恢复

- 段文件采用 `magic + 长度 + body + CRC32` 自描述帧，损坏后可按 magic 字节级重同步；
  完全不可读的段移入 `quarantine/` 而非静默删除。
- `manifest.json` 以临时文件 + fsync + rename 原子落盘，记录提交水位、段清单、
  dead-letter 序号与历史计数；manifest 丢失时未登记的段作为孤儿被收养。
- 重启后按 `Seq` 幂等去重：`Seq <= committed_seq` 的帧跳过（at-least-once 重放），
  恢复顺序严格 FIFO；新记录的序号接在历史最大值之后，不复用。
- `Close` 先把内存环与重试队列刷盘再收尾，强杀后磁盘段仍可完整回放。

### sink 失败处理

- 单次 `Write` 受 `output.write_timeout_ms` 约束，慢/挂死 sink 不会永久卡住消费协程；
  panic 由消费侧 recover 兜底。
- 失败记录按指数退避有界重试（`max_retries`），仍失败转入 **dead-letter 段**
  （`dead-*.log`，可人工检查、永不自动回放），损失显式计入 `dead_lettered`。

### 关键指标

`pushed / emitted / buffered(in_memory+in_retry+on_disk) / dropped_* /
dead_lettered / spilled_writes / spill_reads / corrupt_frames /
quarantined_segs / replayed_dupes / requeued`，丢弃按原因拆分，可直接对账。

## 结构

```
cmd/logforge        采集进程入口
cmd/mocklog         日志生成器（测试用，支持轮转与坏行注入）
internal/tail       文件追踪
internal/multiline  多行合并
internal/parser     解析器（json / logfmt / regex）
internal/plugin     解析器与输出端的注册表
internal/buffer     有界缓冲：内存环 + 重试队列 + 磁盘溢写（frame.go/spill.go/stats.go）
internal/router     按规则分发
internal/sink       输出端（file / stdout / faulty 故障注入）
internal/offset     位点持久化
internal/metrics    进程内计数器
configs/            配置示例
scripts/            demo 与 verify/verify-bp 验证脚本
```

## 配置

见 `configs/logforge.yaml`。解析器按 `parsers` 顺序尝试，第一个成功即生效；
`route.default` 是未命中任何规则时的出口。

## 扩展方式

- 新增日志格式：实现 `plugin.Parser`，在 `internal/parser` 里注册
- 新增输出目标：实现 `plugin.Sink`，在 `internal/sink` 里注册
- 新增路由规则：改配置即可，不需要动代码
