# my2sql-plus AGENTS

## 项目定位

本文件是 `my2sql-plus` 的根级协作入口，面向 AI coding agent 与工程协作者。

`my2sql-plus` 是 Go 项目，用于解析 MySQL row binlog，生成正向 SQL、回滚 SQL、JSON、DML 统计、大事务分析，以及可选的 flashback binlog 和 rollback summary。

| 项目 | 说明 |
| --- | --- |
| 模块名 | `github.com/SisyphusSQ/my2sql` |
| 语言 | Go |
| `go.mod` | `go 1.22` |
| 默认分支 | `main` |
| 主入口 | `my2sql.go`、`cmd/root.go`、`cmd/run.go`、`cmd/version.go` |

## 控制面导航

| 主题 | 入口 |
| --- | --- |
| 仓库说明 | `README.md` |
| 主流程、gate、计划 contract | `docs/harness/control-plane.md` |
| Issue Workflow 与模板 | `docs/harness/issue-workflow.md` |
| Linear 兼容 profile | `docs/harness/linear.md` |
| 仓库内 issue 存储 | `docs/issues/` |
| 项目级机械约束登记 | `docs/harness/project-constraints.md` |
| 测试 runbook 模板 | `docs/test/RUNBOOK_TEMPLATE.md` |
| 计划协议 | `.agents/PLANS.md` |
| 计划主模板 | `.agents/plans/TEMPLATE.md` |
| 实现型示例 | `.agents/plans/EXAMPLE-implementation.md` |
| 默认技能层 | `.agents/skills/` |
| 本地恢复面 | `.agents/state/TEMPLATE.md` |
| 本地结果面 | `.agents/runs/TEMPLATE.md` |
| Prompt 层 | `.agents/prompts/README.md` |
| 主 thread 编排 Prompt | `.agents/prompts/orchestrator-thread.md` |
| 维护循环 Prompt | `.agents/prompts/maintenance-loop.md` |
| Guide 层 | `.agents/guides/` |

固定阅读顺序：

1. 先读 `AGENTS.md`
2. 再读 `docs/harness/control-plane.md`
3. 复杂任务读 `.agents/PLANS.md`、`.agents/plans/TEMPLATE.md`、`.agents/plans/EXAMPLE-implementation.md`
4. 按 issue 推进时读 `docs/harness/issue-workflow.md` 和 `docs/harness/linear.md`
5. 需要测试记录时读 `docs/test/RUNBOOK_TEMPLATE.md`

## 真相边界

| 路径 | 负责内容 |
| --- | --- |
| `docs/harness/` | 控制面规则、Issue Workflow、Issue Tracker profile 与项目级机械约束登记 |
| `docs/issues/` | `issue-provider=repo` 时的仓库 issue 存储 |
| `.agents/PLANS.md` + `.agents/plans/` | 计划协议、计划主模板和实现型示例 |
| `.agents/skills/` | base 默认 repo-local workflow skill：计划归档、版本发布边界、测试 runbook 执行与回写 |
| `.agents/state/` + `.agents/runs/` | repo-local 恢复点与结果摘要面 |
| `.agents/prompts/` | Prompt 模板，当前使用 `full` 模式 |
| `.agents/guides/` | review / linter 说明，当前使用 `full` 模式 |
| `scripts/harness/` | base harness 的最小 gate 脚本与共享 helper |

固定解释：

- `Issue Tracker 是主协作真相`
- `repo 是主执行真相`
- `PR / MR 是次级代码叙事面`
- `.agents/state/` 与 `.agents/runs/` 只补充本地恢复和结果细节，不替代 Issue Tracker

## 代码导航

| 任务 | 入口 |
| --- | --- |
| 看 CLI 入口与命令注册 | `my2sql.go`、`cmd/root.go`、`cmd/run.go`、`cmd/version.go` |
| 看主执行链路 | `internal/core/life_cycle.go`、`cmd/run.go` |
| 看实时拉取 binlog | `internal/extractor/repl_extract.go` |
| 看本地文件解析 binlog | `internal/extractor/file_extract.go` |
| 看 SQL / JSON 转换 | `internal/transformer/transform.go` |
| 看 SQL / JSON / 统计输出 | `internal/loader/sql_load.go`、`internal/loader/stats_load.go` |
| 看配置解析与校验 | `internal/config/config.go` |
| 看表结构与事件模型 | `internal/models/` |
| 看事务顺序控制 | `internal/locker/trx_lock.go` |
| 看常量、错误与选项枚举 | `internal/vars/` |
| 看工具函数 | `internal/utils/` |
| 看现有测试 | `cmd/*_test.go`、`internal/**/*_test.go` |

## 目录职责

| 路径 | 职责 |
| --- | --- |
| `cmd/` | Cobra 命令注册、参数定义、应用启动与退出处理 |
| `internal/config/` | 命令行参数汇总、过滤条件解析、时间与文件位置校验 |
| `internal/core/` | `Extractor` / `Transformer` / `Loader` 工厂与生命周期装配 |
| `internal/extractor/` | `repl` 和 `file` 两种模式下的 binlog 读取与事件投递 |
| `internal/transformer/` | 行事件转 SQL / JSON、主键与唯一键选择、事务顺序协同 |
| `internal/loader/` | SQL、JSON、统计结果落盘或输出到屏幕 |
| `internal/models/` | binlog 事件、表结构、结果结构、统计结构等模型 |
| `internal/locker/` | 事务内事件序控制，保证输出顺序稳定 |
| `internal/log/` | 日志封装 |
| `internal/utils/` | 通用工具、binlog 辅助、时间与字符串处理 |
| `internal/vars/` | 常量、默认值、错误定义、合法选项集合 |
| `bin/` | 本地构建产物 |
| `test/` | 手工验证或样例输入输出目录 |

## 核心执行链路

默认链路如下：

1. `cmd/run.go` 收集 flag 并调用 `config.ParseConfig`
2. `internal/core.NewExtractor` 创建 `repl` 或 `file` 抽取器
3. `internal/extractor/*` 将 binlog 事件写入 `eventChan` / `statsChan`
4. `internal/core.NewTransformer` 将行事件转换为 SQL / JSON
5. `internal/core.NewLoader` 将结果输出到文件、终端或统计结果文件

复杂改动默认沿 `config -> core -> extractor -> transformer -> loader` 的边界定位，不要跳过中间层直接在入口拼逻辑。

## 开发约束

- 优先保持 `cmd/*` 只做命令组装、flag 注册和生命周期编排，不把业务逻辑塞进命令层。
- 涉及 binlog 输入模式时，同时检查 `internal/config/`、`internal/core/` 与 `internal/extractor/`。
- 涉及 SQL 生成、回滚逻辑、主键 / 唯一键选择、输出格式时，同时检查 `internal/transformer/`、`internal/models/`、`internal/loader/`。
- 涉及统计、大事务分析时，同时检查 `internal/extractor/` 的 `statsChan` 生产和 `internal/loader/stats_load.go` 的消费。
- 涉及事件顺序或多线程输出时，不要绕开 `internal/locker/` 直接改并发顺序。
- 涉及 flag、默认值、合法枚举时，同时更新 `cmd/run.go` 与 `internal/vars/` / `internal/config/`。
- 非必要不要改 `bin/`、压缩包、样例输出等本地产物。
- 模板配置可提交，真实环境配置不提交；如需要环境配置，优先提交 `.env.example`、`settings.example.yaml` 这类示例文件。

## 推荐命令

| 用途 | 命令 |
| --- | --- |
| 验证 harness | `make harness-verify` |
| 计划 review gate | `make harness-review-gate PLAN=path/to/plan.md` |
| 构建全部包 | `go build ./...` |
| 跑全部测试 | `go test ./...` |
| 跑内部包测试 | `go test ./internal/...` |
| 跑指定包测试 | `go test ./internal/utils/...` |
| 查看版本 | `go run . version` |
| 查看运行参数 | `go run . run --help` |
| 实时模式运行 | `go run . run --mode=repl ...` |
| 文件模式运行 | `go run . run --mode=file --start-file=<binlog> ...` |

## 测试建议

- 改 `internal/config/`、`cmd/` 时，先验证参数解析、帮助信息和最小启动路径。
- 改 `internal/extractor/` 时，优先区分 `repl` 与 `file` 两种模式，避免只验证其一。
- 改 `internal/transformer/` 时，重点验证 SQL 类型过滤、回滚 SQL、主键 / 唯一键选择、时间与字段类型转换。
- 改 `internal/loader/` 时，重点验证输出文件命名、分表输出、额外信息输出和统计文件格式。
- 需要端到端验证时，优先使用本地 binlog 文件；`repl` 模式默认视为外部 MySQL 环境依赖。
- 若任务从局部修复扩大到跨 `config`、`extractor`、`transformer`、`loader` 多边界联动，应先按 `.agents/PLANS.md` 补计划。

## 本地工件规则

- `.agents/state/*` 与 `.agents/runs/*` 的真实运行文件默认不提交，只保留 `TEMPLATE.md`。
- `.agents/plans/` 下真实计划实例默认不提交，只保留 `TEMPLATE.md` 和 `EXAMPLE-implementation.md`。
- `docs/harness/*.md`、`docs/issues/*.md`、`docs/test/RUNBOOK_TEMPLATE.md` 默认应提交。
- 原始命令输出、真实凭据、数据库主机、临时目录、完整下载 URL、token、行主键、本机路径等敏感或机器本地痕迹不提交。
- `AGENTS.md` 是提交版协作入口；旧 `.agent/` 已废弃，不再保留。

## 交付前检查

在结束当前任务前，至少自查以下问题：

- 改动是否落在正确目录边界，没有把业务逻辑塞进 `cmd/*`
- 是否运行了与改动匹配的目标测试或 `go test ./...`
- 是否运行了 `make harness-verify`
- 涉及复杂任务时，是否按 `.agents/PLANS.md` 使用新计划协议
- 是否错误提交了真实环境配置、真实运行面、真实计划实例或敏感信息
