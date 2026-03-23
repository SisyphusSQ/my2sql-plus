# my2sql-plus

`my2sql-plus` 用于解析 MySQL row binlog，面向 DML 场景生成 SQL、JSON、统计结果，以及可选的反向 binlog 和 rollback summary。

## 项目定位

| 能力 | 说明 | 当前状态 |
| --- | --- | --- |
| `2sql` | 将 row binlog 转成正向 SQL | 已支持 |
| `rollback` | 生成回滚 SQL | 已支持 |
| JSON 输出 | 为 DML 事件额外生成 JSON 行结果 | 已支持，`2sql` / `rollback` 默认都会产出 |
| 事务统计 | 输出 DML 统计与大事务/长事务分析 | 已支持 |
| flashback binlog | 为 `rollback` 额外生成可读的反向 binlog 文件 | 已支持，需显式开启 |
| rollback summary | 汇总回滚结果的表级影响行数与类型映射 | 已支持，需显式开启 |
| DDL 回滚 | 回滚 DDL 或保证 DDL 中途切换安全 | 不承诺 |
| statement binlog 解析 | 将 statement binlog 作为主能力完整支持 | 不承诺 |

这是一个 row binlog DML 工具。README 不把 DDL 回滚或 statement binlog 作为主能力介绍。

## 命令概览

推荐先通过 Makefile 编译，再使用生成的二进制 `my2sql`。

| 命令 | 作用 |
| --- | --- |
| `./bin/my2sql version` | 输出版本、Go 版本、构建时间、提交信息 |
| `./bin/my2sql run --help` | 查看主运行命令和全部 flag |
| `./bin/my2sql completion --help` | 查看 shell 自动补全命令 |
| `./bin/my2sql --help` | 查看根命令帮助 |

## 前置条件

- 需要读取的是 MySQL row binlog。
- `repl` 模式需要可连接源库，并使用唯一的 `--server-id` 以从 binlog 拉流。
- `file` 模式虽然读取本地 binlog 文件，但当前 SQL / JSON 生成阶段仍会连接 MySQL，通过 `SHOW COLUMNS` / `SHOW INDEX` 获取表结构；它不是纯离线模式。
- 需要保证 `--output-dir` 有足够磁盘空间，尤其是在批量导出 SQL、JSON 和 flashback 文件时。
- 如果要校验 `.flashback` 文件是否可读，可选安装 `mysqlbinlog`。

## 快速开始

先编译二进制：

```bash
make darwin
```

编译完成后，会得到 `bin/my2sql`。如果需要 Linux `amd64` 产物，可使用：

```bash
make build
```

查看版本：

```bash
./bin/my2sql version
```

查看帮助：

```bash
./bin/my2sql --help
./bin/my2sql run --help
```

`file` 模式生成 rollback SQL：

```bash
./bin/my2sql run \
  --mode=file \
  --work-type=rollback \
  --host=127.0.0.1 \
  --port=3306 \
  --user=root \
  --password=secret \
  --local-binlog-file=/path/to/mysql-bin.000001
```

`repl` 模式生成正向 SQL：

```bash
./bin/my2sql run \
  --mode=repl \
  --work-type=2sql \
  --host=127.0.0.1 \
  --port=3306 \
  --user=root \
  --password=secret \
  --server-id=223306 \
  --start-file=mysql-bin.000001 \
  --start-pos=4
```

统计模式：

```bash
./bin/my2sql run \
  --mode=file \
  --work-type=stats \
  --host=127.0.0.1 \
  --port=3306 \
  --user=root \
  --password=secret \
  --local-binlog-file=/path/to/mysql-bin.000001 \
  --output-dir=/tmp/my2sql-plus-out
```

`rollback` 同时生成 SQL、flashback binlog 和 summary：

```bash
./bin/my2sql run \
  --mode=file \
  --work-type=rollback \
  --host=127.0.0.1 \
  --port=3306 \
  --user=root \
  --password=secret \
  --local-binlog-file=/path/to/mysql-bin.000001 \
  --flashback-binlog \
  --flashback-binlog-base=rollback_out \
  --summary \
  --summary-file=rollback-summary.txt
```

按表拆分文件：

```bash
./bin/my2sql run \
  --mode=file \
  --work-type=2sql \
  --host=127.0.0.1 \
  --port=3306 \
  --user=root \
  --password=secret \
  --local-binlog-file=/path/to/mysql-bin.000001 \
  --file-per-table
```

校验生成的 flashback 文件：

```bash
mysqlbinlog rollback_out.flashback
```

## 输出结果说明

### 结果类型

| `work-type` | 默认产物 | 命名规则 | 备注 |
| --- | --- | --- | --- |
| `2sql` | 正向 SQL 文件 + JSON 文件 | 当前 CLI 实际会产出 `2sql.<idx>.sql` 与 `json.<idx>.sql` | `idx` 来自 binlog 序号 |
| `rollback` | 回滚 SQL 文件 + JSON 文件 | `rollback.<idx>.sql` 与 `json.<idx>.sql` | 开启额外能力后会再产出 flashback / summary |
| `stats` | `binlog_status.txt` + `biglong_trx.txt` | 固定文件名 | 不生成 SQL / JSON 文件 |

### `--file-per-table` 命名差异

| 开关 | SQL / JSON 命名 |
| --- | --- |
| 未开启 | `<type>.<idx>.sql` |
| 开启 | `<schema>.<table>.<type>.<idx>.sql` |

这里的 `<type>` 以当前 loader 类型为准。对 CLI 来说，常见值是 `2sql`、`rollback`、`json`。

### rollback 额外产物

| 开关 | 产物 | 说明 |
| --- | --- | --- |
| `--flashback-binlog` | `<base>.flashback` | 反向 binlog 二进制文件 |
| `--summary` | 屏幕 summary | 运行结束后打印汇总 |
| `--summary-file` | summary 文件 + 屏幕 summary | 写文件的同时仍会打印到屏幕 |

### summary 内容

summary 固定包含以下列：

| 列名 | 说明 |
| --- | --- |
| `database` | 数据库名 |
| `table` | 表名 |
| `affected_rows` | 受影响行数 |
| `original_sql_type` | 原始 DML 类型 |
| `rollback_sql_type` | 对应回滚类型 |

### `--add-extraInfo` 的效果

开启后，SQL 文件中的每条 SQL 之前会额外写入一行注释，包含：

- `datetime`
- `database`
- `table`
- `binlog`
- `startpos`
- `stoppos`

这个附加信息只作用于 SQL 文件输出，不作用于 JSON 文件。

### `--output-toScreen` 的当前行为

- 会把 SQL 结果直接输出到屏幕，而不是落 SQL 文件。
- 这个开关更适合快速查看 SQL 结果。
- 在 `2sql` / `rollback` 下，当前实现会同时跑 SQL loader 和 JSON loader，因此屏幕上可能看到重复 SQL 输出。
- 当前实现不会把 JSON 结果以独立 JSON 形式输出到屏幕，因此它不适合作为 JSON 消费方式。
- `stats` 产物仍然会写入 `binlog_status.txt` 和 `biglong_trx.txt`。

## 参数说明

### 连接与来源

| 参数 | 说明 | 默认 / 备注 |
| --- | --- | --- |
| `--mode` | 输入模式，`repl` 或 `file` | 默认 `repl` |
| `--host` | MySQL 地址 | 默认 `127.0.0.1` |
| `--port` | MySQL 端口 | 默认 `3306` |
| `--user` | MySQL 用户 | 必填场景下自行提供 |
| `--password` | MySQL 密码 | 必填场景下自行提供 |
| `--mysql-type` | MySQL 类型，`mysql` 或 `mariadb` | 默认 `mysql` |
| `--server-id` | `repl` 模式下作为复制 slave 的 server id | 默认 `1113306` |
| `--local-binlog-file` | `file` 模式要解析的本地 binlog 文件 | `file` 模式必填 |

### 过滤范围

| 参数 | 说明 | 备注 |
| --- | --- | --- |
| `--databases` | 仅解析这些库 | 逗号分隔 |
| `--tables` | 仅解析这些表 | 不带 schema，逗号分隔 |
| `--ignore-databases` | 忽略这些库 | 逗号分隔 |
| `--ignore-tables` | 忽略这些表 | 不带 schema，逗号分隔 |
| `--sql` | 仅解析指定 DML 类型 | 可选 `insert,update,delete` |

### 时间与位点

| 参数 | 说明 | 默认 / 备注 |
| --- | --- | --- |
| `--start-file` | 开始读取的 binlog 文件 | `file` 模式可由 `--local-binlog-file` 自动推导 |
| `--start-pos` | 开始位置 | 默认 `4` |
| `--stop-file` | 停止读取的 binlog 文件 | 可选 |
| `--stop-pos` | 停止位置 | 默认 `4`，与 `--stop-file` 配合 |
| `--start-datetime` | 起始时间 | 格式 `YYYY-MM-DD HH:MM:SS` |
| `--stop-datetime` | 截止时间 | 格式 `YYYY-MM-DD HH:MM:SS` |
| `--tl` | binlog 中时间列解析使用的时区 | 默认 `Local` |

### SQL 生成行为

| 参数 | 说明 | 默认 / 备注 |
| --- | --- | --- |
| `--work-type` | `2sql`、`rollback`、`stats` | 默认 `2sql` |
| `--full-columns` | update 时带上未变更列；update / delete 的 where 尽量使用全列条件 | 默认关闭 |
| `--do-not-add-prefixDb` | 不给表名加数据库前缀 | 默认会输出 `db.table` |
| `--U` | delete / update 优先使用唯一键而不是主键构造 where | 默认关闭 |
| `--ignore-primaryKey-forInsert` | `2sql` 的 insert 生成时忽略主键列 | 默认关闭 |
| `--threads` | `2sql` / `rollback` 的并发线程数 | 默认 `2` |

### 输出与落盘

| 参数 | 说明 | 默认 / 备注 |
| --- | --- | --- |
| `--output-dir` | 结果输出目录 | 默认当前工作目录 |
| `--file-per-table` | 每张表单独输出一个文件 | 默认关闭 |
| `--output-toScreen` | 直接输出到屏幕 | 更适合看 SQL |
| `--add-extraInfo` | 在 SQL 前加注释行 | 仅影响 SQL 文件输出 |
| `--cpuprofile` | 写 CPU profile 文件 | 调试用 |
| `--memprofile` | 写内存 profile 文件 | 调试用 |

### rollback 专属

| 参数 | 说明 | 默认 / 备注 |
| --- | --- | --- |
| `--flashback-binlog` | 额外生成反向 binlog 文件 | 仅 `rollback` 可用 |
| `--flashback-binlog-base` | 反向 binlog 输出前缀 | 依赖 `--flashback-binlog` |
| `--summary` | 打印 rollback summary | 仅 `rollback` 可用 |
| `--summary-file` | 写 rollback summary 文件 | 会隐式开启 `--summary` |

### stats 专属

| 参数 | 说明 | 默认 / 备注 |
| --- | --- | --- |
| `--print-interval` | 周期性刷新统计结果 | 默认 `30` 秒 |
| `--big-trx-row-limit` | 认定为大事务的影响行数阈值 | 默认 `10` |
| `--long-trx-seconds` | 认定为长事务的持续时间阈值 | 默认 `1` 秒 |

## 参数关系与常见误区

- `--summary-file` 会隐式开启 `--summary`。
- `--flashback-binlog-base` 必须和 `--flashback-binlog` 一起使用。
- `--flashback-binlog` 和 `--summary` 只允许在 `--work-type=rollback` 下使用。
- `--flashback-binlog-base` 和 `--summary-file` 如果给的是相对路径，会基于 `--output-dir` 解析。
- `file` 模式下如果没显式给 `--start-file`，当前实现会自动用 `--local-binlog-file` 的 basename 作为起点 binlog 名。
- `2sql` 和 `rollback` 当前都会额外产出 JSON 文件，不是只产出 SQL。
- `--output-toScreen` 不能等价替代完整落盘结果。它主要适合快速查看 SQL，当前实现不会把 JSON 单独渲染到屏幕，而且在 `2sql` / `rollback` 下可能看到重复 SQL 输出。
- `--add-extraInfo` 只影响 SQL 文件输出，不影响 JSON 文件，也不会改变当前的屏幕输出格式。
- `--do-not-add-prefixDb` 名字是否定式开关。默认行为是带数据库前缀，打开它以后才是不带前缀。
- `file` 模式不是“给一个 binlog 文件就能纯离线出 SQL”。当前实现仍需要连接 MySQL 读取表结构。

## 限制与边界

- 本项目面向 row binlog 的 DML 处理，不承诺完整支持 statement binlog。
- 不承诺 DDL 回滚能力，也不保证 DDL 穿插时的回滚结果一定安全。
- 当前 SQL / JSON 生成依赖实时获取表结构，因此不承诺“无数据库连接的离线 SQL 生成”。
- README 仅描述当前 CLI 暴露出来的能力，不覆盖未暴露的遗留配置字段。
