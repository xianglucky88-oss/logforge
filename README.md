# logforge

单进程的日志采集代理：跟踪文件、合并多行、解析成结构化记录、按规则分发。

## 快速开始

```bash
make build
make demo             # 生成 200 条日志并采集一次
make verify           # 断言输出与计数
make verify-recovery  # 端到端验证可恢复跟踪协议（6 个场景）
make test             # 单元测试
```

## 结构

```
cmd/logforge        采集进程入口
cmd/mocklog         日志生成器（测试用，支持轮转与坏行注入）
internal/tail       文件追踪（身份识别、轮转 drain、截断检测、半行缓存）
internal/multiline  多行合并
internal/parser     解析器（json / logfmt / regex）
internal/plugin     解析器与输出端的注册表
internal/buffer     有界缓冲
internal/router     按规则分发
internal/sink       输出端（file / stdout）
internal/offset     位点持久化（v2 格式、校验、备份、提交状态跟踪）
internal/metrics    进程内计数器
configs/            配置示例
```

## 可恢复跟踪协议

采集进程长期跟踪一个持续写入的文件，必须面对轮转、截断、路径复用和
随时被终止。协议围绕四件事设计：

### 1. 文件身份，而不是路径

每个物理文件用 `dev:inode` 标识。位点按身份持久化；同一路径被轮转、
重建、快速复用产生的是不同身份，各自独立计位。只有身份匹配，位点才有意义。

### 2. 位点提交时机：先确认输出，再提交位点

持久化的 `offset` 永远只指向**已确认输出**的记录边界——记录写入所有路由
sink 成功（或被缓冲策略丢弃）后才推进。提交发生在：

- 每个提交周期（`offset.commit_interval_ms`，默认 1s）；
- 优雅退出时（排空管线、flush sink 之后的最终提交）。

因此：**优雅重启零重复、零丢失**；被 `kill -9` 时重放窗口不超过一个提交
间隔，且只重复"已输出但尚未提交"的尾部——已提交的记录不会再次输出。

### 3. 轮转与截断识别

- **运行中轮转**：`stat(path)` 发现身份变化，旧文件句柄保留为 rotated
  source 继续读取（覆盖"轮转后旧文件仍被写入"），连续静默
  `rotate_drain_quiet_polls` 轮或超过 `rotate_drain_max_ms` 后封闭；
  新身份从 0 或其持久化位点开始。
- **运行中截断**（copytruncate）：`fstat` 发现 `size < 已读位置`，
  回到 0 重读，该身份的已确认位点同步归零。
- **停机期间轮转**：启动时按 `path*`（可用 `backlog_glob` 覆盖）扫描目录，
  用 inode 找回旧物理文件，从其持久化位点把未读内容读完；
  找不回的未读字节计入 `backlog_bytes_lost`。
- **停机期间截断**：`size < 持久化位点` 时从 0 开始。

### 4. 半行缓存

读到 EOF 的半行（没有换行结尾）在内存 pending 里等待补齐。优雅退出时，
若半行起点恰好等于已确认位点，半行字节会持久化进位点文件；重启时逐字节
校验文件内容后预填，写者补完该行后拼成**一条完整记录**——不产生碎片，
也不重复。校验失败（内容已被改写）按截断处理并暴露指标。

### 位点文件（v2）

```json
{"version": 2, "crc32": 1630512480,
 "data": {"version": 2, "paths": {"/path/app.log": {
   "current": "3d:2a56097",
   "identities": {"3d:2a56097": {"offset": 18399, "size": 18399,
                                  "pending": "...", "updated": ...}}}}}}
```

- CRC32 校验信封；`.bak` 是同代镜像，任一文件损坏都能零重放恢复；
- 双文件都损坏：隔离为 `*.corrupt.<时间戳>`，从空状态开始并计
  `offset_corrupt_total`（可观察的兜底，而不是静默"从零重读"）；
- v1 格式（`{"offsets":{path:N}}`）自动迁移为未验证位点，
  仅在当前文件大小足够时采用一次。

### 可观察指标

`rotations_detected`、`rotated_sources_sealed`、`truncations_detected`、
`backlog_files_recovered`、`backlog_bytes_lost`、`offset_commits_total`、
`offset_commit_failed`、`offset_corrupt_total`、`offset_recovered_from_bak`、
`offset_migrated`、`offset_resumes`、`pending_lines_persisted`、
`pending_lines_resumed`、`pending_dropped`、`bytes_read`、
`commit_lag_records`（已成块未确认的记录数）、`open_sources`。

### 已知边界

- 两次轮询之间连续轮转两次以上、且中间文件被立即删除：中间文件的内容
  物理上无法找回（句柄只能钉住打开过的 inode）；
- inode 复用（删除后新文件分到同一 inode）且新文件更大：可能按旧位点
  续读。`size < offset` 的截断情形总能被识别；
- sink 写失败不重试（计 `write_failed`），位点照常推进。

## 配置

见 `configs/logforge.yaml`。解析器按 `parsers` 顺序尝试，第一个成功即生效；
`route.default` 是未命中任何规则时的出口。

## 扩展方式

- 新增日志格式：实现 `plugin.Parser`，在 `internal/parser` 里注册
- 新增输出目标：实现 `plugin.Sink`，在 `internal/sink` 里注册
- 新增路由规则：改配置即可，不需要动代码
