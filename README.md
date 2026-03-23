# my2sql-plus

解析 MySQL row binlog，可生成原始 SQL、回滚 SQL、去主键 INSERT SQL，并支持 DML 统计、大事务分析、可选反向 binlog 二进制输出以及回滚 summary 概览。

## 关键行为

- 保留 `--work-type=2sql|rollback|stats`
- `--work-type=rollback` 默认仍生成 rollback SQL
- 显式开启 `--flashback-binlog` 后，会额外生成 `<base>.flashback` 反向 binlog 文件
- 开启 `--summary` 后，会在解析结束时输出回滚结果概览
- 指定 `--summary-file` 时，会隐式开启 `--summary`，同时输出到屏幕和文件
- `--work-type=stats` 现有 `binlog_status.txt` / `biglong_trx.txt` 保持不变

## 常用示例

查看参数：

```bash
go run . run --help
```

`file` 模式默认只生成 rollback SQL：

```bash
go run . run \
  --mode=file \
  --work-type=rollback \
  --local-binlog-file=/path/to/mysql-bin.000001
```

`file` 模式同时生成 rollback SQL、反向 binlog 和 summary：

```bash
go run . run \
  --mode=file \
  --work-type=rollback \
  --local-binlog-file=/path/to/mysql-bin.000001 \
  --flashback-binlog \
  --flashback-binlog-base=rollback_out \
  --summary \
  --summary-file=rollback-summary.txt
```

验证反向 binlog 是否可读：

```bash
mysqlbinlog rollback_out.flashback
```

## 参数关系

- `--flashback-binlog` 仅对 `--work-type=rollback` 生效
- `--flashback-binlog-base` 仅在 `--flashback-binlog` 开启时生效；相对路径会基于 `--output-dir`
- `--summary` 仅对 `--work-type=rollback` 生效
- `--summary-file` 会隐式开启 `--summary`；相对路径会基于 `--output-dir`
- `file` 模式下若未显式给 `--start-file`，会自动从 `--local-binlog-file` 推导 basename 作为解析起点
