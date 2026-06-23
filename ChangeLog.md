## Unreleased

### v1.0.0(20260623)
#### feature:
1. 新增 repo-local harness 控制面，接入 `AGENTS.md`、`docs/harness/`、`docs/issues/`、`docs/test/`、`.agents/` 和 `scripts/harness/`，统一计划、验证、issue workflow 与本地 agent 协作入口。
2. 新增 v1.0.0 发布构建入口，支持 Linux、macOS、Windows 三个平台的 `amd64` 与 `arm64` release binary 输出。

#### optimization:
1. 调整 `Makefile` 版本注入方式，使用 `APP_VERSION ?= v1.0.0` 统一写入 `vars.AppVersion`，支持发布时通过 `make APP_VERSION=<version>` 覆盖。
2. 保留原有 `build` 与 `darwin` 目标的本地构建习惯，同时新增 `build-all` 和平台定向目标，避免影响现有 `bin/my2sql` 使用路径。

#### note:
1. 本版本作为 `my2sql-plus` 首个稳定发版准备版本；真实 MySQL、真实 binlog、`repl` 模式和 flashback 可读性验证仍需按环境 runbook 执行并记录脱敏结果。
