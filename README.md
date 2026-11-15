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

见 `configs/logforge.yaml`。解析器按 `parsers` 顺序尝试，第一个成功即生效；
`route.default` 是未命中任何规则时的出口。

## 扩展方式

- 新增日志格式：实现 `plugin.Parser`，在 `internal/parser` 里注册
- 新增输出目标：实现 `plugin.Sink`，在 `internal/sink` 里注册
- 新增路由规则：改配置即可，不需要动代码
