# Project Mechanical Constraints

## 文档定位

本文件登记当前项目的项目级机械约束：哪些工程边界已经变成可执行检查，哪些还只是文档约束，哪些计划后续接入。

它不定义通用 lint 规则，也不预设某个业务项目的架构边界。初始化后，项目维护者需要基于真实代码、架构文档、运行入口和协作规则补齐本文件。

固定原则：

- 没有可执行命令或 gate 时，不得假装 `enforced`
- `enforced` 必须能对应到本地命令、CI、linter、script、test、contract diff、E2E 或 review gate
- `documented` 只表示已有文档规则，不表示机器会拦截
- `partial` 必须说明哪些部分已机械化，哪些仍需人工 review
- 项目专属规则不要写进 base harness 模板本身，先登记到本文件，再按项目选择 linter / script / test / E2E 载体

## 状态枚举

| Status | 含义 |
| --- | --- |
| `enforced` | 已有可执行命令或 gate 会在违反时失败 |
| `partial` | 部分已机械化，仍有人工 review 或后续补齐项 |
| `documented` | 只有文档约束，尚无可执行检查 |
| `planned` | 已决定后续接入，但当前没有规则或命令 |
| `not_applicable` | 当前项目明确不适用 |

## 分类枚举

| Category | 典型内容 |
| --- | --- |
| `architecture` | 分层、依赖方向、目录职责、模块边界 |
| `contract` | API / schema / DTO / OpenAPI / provider-consumer contract |
| `runtime` | 配置、环境变量、日志、指标、trace、启动方式 |
| `verification` | 测试矩阵、E2E、live self-test、构建和验证入口 |
| `docs` | 设计文档、runbook、计划、结果摘要和链接同步 |
| `security` | secret、权限、副作用、脱敏、危险命令 |
| `cross-repo` | provider / consumer / shared truth 分层与同步 |

## 维护循环关联

Maintenance loop 默认扫描本文件，用来判断项目规则是否仍停留在文档层、是否需要建 issue，或是否已具备升级为机械检查的条件。

| Maintenance Tag | 含义 |
| --- | --- |
| `maintenance_candidate` | 维护循环应定期扫描该规则是否漂移，但当前不一定适合机械化 |
| `rule_promotion_candidate` | 重复 review finding 或已有稳定命令，适合评估升级为机械检查 |
| `human_decision_required` | 涉及产品、API、安全、数据或跨团队取舍，需要人类确认后才能修改 |

固定规则：

- maintenance loop 发现 `documented` 规则长期未机械化时，只能报告或建议建 issue，不得自动把它改成 `enforced`。
- repeated review finding 可以升级为 `project-check`、linter、contract diff、E2E 或 harness check，但必须先写清 evidence、目标 `Rule ID`、执行命令、回归验证和回滚方式。
- `rule_promotion_candidate` 只是候选标签，不代表已经允许自动新增检查脚本或 CI。

## 约束登记表

| Rule ID | Category | Rule | Source | Enforcement | Command | Status | Maintenance Tag | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `ARCH-001` | `architecture` | `cmd/` 只做 Cobra 命令组装、flag 注册和生命周期编排；业务逻辑沿 `config -> core -> extractor -> transformer -> loader` 定位 | `AGENTS.md` | review | 人工 review | `documented` | `maintenance_candidate` | 当前没有项目级架构 linter，不得标记为 `enforced` |
| `ARCH-002` | `architecture` | 涉及 binlog 输入模式时，同时检查 `internal/config/`、`internal/core/` 与 `internal/extractor/` | `AGENTS.md` | review | 人工 review | `documented` | `maintenance_candidate` | 用于避免只改入口或单一 extractor 的范围遗漏 |
| `ARCH-003` | `architecture` | 涉及 SQL / JSON / rollback / 主键唯一键选择 / 输出格式时，同时检查 `internal/transformer/`、`internal/models/`、`internal/loader/` | `AGENTS.md` | review + tests | `go test ./internal/transformer/... ./internal/loader/...` | `partial` | `rule_promotion_candidate` | 包测试可覆盖部分行为，跨层范围仍需人工 review |
| `VERIFY-001` | `verification` | base harness 必须可验证 | `Makefile` / `scripts/harness/` | script | `make harness-verify` | `enforced` | `maintenance_candidate` | 检查 harness 文件、技能、Makefile 目标和关键文档模式 |
| `VERIFY-002` | `verification` | Go 包测试应保持通过 | `go.mod` / `README.md` | test | `go test ./...` | `enforced` | `rule_promotion_candidate` | 当前本地基线通过；真实 MySQL / binlog 环境不包含在此命令内 |
| `VERIFY-003` | `verification` | Go 包应保持可构建 | `go.mod` / `Makefile` | build | `go build ./...` | `enforced` | `maintenance_candidate` | 不替代目标平台二进制构建验证 |
| `RUNTIME-001` | `runtime` | `repl` 模式、真实 binlog 文件、表结构读取和 flashback 可读性验证依赖外部 MySQL、binlog 样例或 `mysqlbinlog` | `README.md` / `AGENTS.md` | manual gate | 按 `docs/test/RUNBOOK_TEMPLATE.md` 记录环境、步骤和脱敏结果 | `documented` | `human_decision_required` | 未执行真实环境步骤时不得写成已通过 |
| `SEC-001` | `security` | 不提交真实环境配置、数据库主机、账号密码、token、完整连接串、原始命令输出或含敏感行数据的材料 | `AGENTS.md` / `.gitignore` | ignore + review | `git status --short` + 人工 review | `partial` | `maintenance_candidate` | `.gitignore` 覆盖常见本地配置；敏感内容仍需人工 review |

## `project-check` 挂载协议

base harness 不默认生成 `project-check`，也不生成永远 pass 的占位脚本。

当项目已有稳定的项目级机械约束后，可以按需补充：

```text
scripts/project-checks/
  check.sh
  check-architecture.sh
  check-contracts.sh
  check-runtime.sh
  check-docs.sh
```

推荐 Makefile 入口：

```makefile
project-check:
	bash scripts/project-checks/check.sh
```

固定要求：

- 一旦某条规则标记为 `enforced`，`Command` 必须指向真实可执行入口
- `project-check` 可以汇总项目专属检查，但不替代 `make harness-check`
- `make harness-check` 只校验本文件作为登记入口存在且结构完整，不替项目臆造项目规则
- 违反规则时，失败信息应说明违反了哪条 `Rule ID`、参考哪个 `Source`、应运行或修复哪个 `Command`

## 初始化后补齐步骤

1. 从 `AGENTS.md`、目录级 `AGENTS.md`、README、架构文档和现有 Makefile 里提取项目不可违反的规则。
2. 先把规则登记到上方表格，并诚实标注 `Status`。
3. 已有命令或 gate 的规则，补齐 `Enforcement` 和 `Command`。
4. 只有文档约束的规则，保持 `documented`，不要写成 `enforced`。
5. 后续把稳定规则逐步接入 linter、script、test、contract diff、E2E 或 CI。
6. 为每条规则补齐 `Maintenance Tag`，让 maintenance loop 能区分扫描、升级和人工决策边界。
