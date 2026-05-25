# S3 Tables Snapshot 查看指南

S3 Tables 底层是 Apache Iceberg 格式，因此快照（snapshot）管理遵循 Iceberg 的元数据模型。以下是几种查看快照的方式。

## 1. Athena 查询元数据表（最常用）

Iceberg 表自带 `$snapshots`、`$history`、`$files`、`$manifests` 等元数据表：

```sql
-- 查看所有快照（snapshot_id, committed_at, operation, summary）
SELECT * FROM "sales_iceberg$snapshots" ORDER BY committed_at DESC;

-- 查看历史（每次提交的 snapshot_id 与父快照）
SELECT * FROM "sales_iceberg$history";

-- 查看某个快照对应的数据文件
SELECT * FROM "sales_iceberg$files";

-- 查看 manifest 列表
SELECT * FROM "sales_iceberg$manifests";
```

> 注意：必须用双引号包住表名，否则 `$` 会被当成变量。

## 2. Spark / Glue Job 中查询

```python
spark.sql("SELECT * FROM s3tablesbucket.poc_ns.sales.snapshots").show()
spark.sql("SELECT * FROM s3tablesbucket.poc_ns.sales.history").show()
spark.sql("SELECT * FROM s3tablesbucket.poc_ns.sales.files").show()
```

## 3. 直接读取 metadata.json

S3 Tables 的 metadata 位置可以通过 API 拿到：

```bash
aws s3tables get-table \
  --table-bucket-arn arn:aws:s3tables:ap-northeast-1:964727750692:bucket/<bucket-name> \
  --namespace poc_ns \
  --name sales \
  --query 'metadataLocation'
```

然后用 `aws s3 cp` 下载那个 `metadata/00002-xxx.metadata.json`，里面的 `snapshots` 数组列出了所有快照：

- `snapshot-id`
- `parent-snapshot-id`
- `timestamp-ms`
- `summary`（操作类型、行数、文件数等）
- `manifest-list`

## 4. 时间旅行查询某个快照

```sql
-- 按 snapshot id
SELECT * FROM sales_iceberg FOR VERSION AS OF 1234567890123456789;

-- 按时间点
SELECT * FROM sales_iceberg FOR TIMESTAMP AS OF TIMESTAMP '2026-05-19 07:09:48';
```

## 元数据表说明

| 元数据表 | 用途 |
|---------|------|
| `$snapshots` | 所有快照列表，含时间、操作类型、统计摘要 |
| `$history` | 提交历史链（snapshot_id ← parent_snapshot_id） |
| `$files` | 当前快照引用的所有数据文件 |
| `$manifests` | 当前快照的 manifest 文件列表 |
| `$partitions` | 分区级别的统计信息 |
| `$refs` | 命名引用（branches、tags） |

## POC 场景参考

本 POC 中跑过初始 load + incremental append，因此 `$snapshots` 至少包含 2 条记录，operation 分别为 `append`。可通过以下查询确认：

```sql
SELECT
  snapshot_id,
  committed_at,
  operation,
  summary['added-records'] AS added_rows,
  summary['total-records'] AS total_rows
FROM "sales_iceberg$snapshots"
ORDER BY committed_at;
```
