# S3 Tables 大量数据客户交付方案

向客户交付从 S3 Tables 检索出的大量数据，方案选型主要看 **数据量级**、**客户技术栈**、**是否需要持续更新**。

## 一、按数据量级划分

### 小规模（< 10 GB）— 直接文件交付

**Athena UNLOAD → S3 → 预签名 URL**

```sql
UNLOAD (SELECT * FROM sales_iceberg WHERE ...)
TO 's3://delivery-bucket/customer-a/2026-05-26/'
WITH (format = 'PARQUET', compression = 'SNAPPY')
```

然后用 `aws s3 presign` 生成限时下载链接，邮件/API 发给客户。

- **优点**：简单、零运维、按需付费
- **缺点**：链接最长 7 天，单文件下载

### 中规模（10 GB ~ 数 TB）— S3 跨账户共享

- **S3 Access Point + Bucket Policy 跨账户授权**：客户用自己的 AWS 账户直接 `aws s3 sync` 拉取
- **S3 Batch Operations**：批量生成、复制、加密
- **AWS Transfer Family（SFTP/FTPS）**：给非 AWS 客户

### 大规模（数 TB ~ PB）

- **AWS DataSync**：跨账户/跨区域/跨云高速同步，断点续传，校验
- **AWS Snowball Edge**：物理设备邮寄，适合 PB 级或网络受限客户

## 二、按客户技术能力划分

### 客户有数据团队 / Spark / Iceberg 能力

**直接共享 Iceberg 表**（推荐）

- 用 **Lake Formation 跨账户授权** 把 S3 Tables / Glue Catalog 的表共享给客户账户
- 客户用自己的 Athena / EMR / Glue 读取，**无需复制数据**
- 增量更新自动可见

### 客户用数仓（Snowflake / Databricks / BigQuery）

- **Snowflake Iceberg External Table**：直接挂载 S3 上的 Iceberg metadata，零复制
- **Databricks Unity Catalog Federation**：联邦查询 Glue Catalog
- **BigQuery Omni / BigLake**：跨云查询 S3 Iceberg

### 客户只有 BI 工具 / 业务人员

- **预导出 CSV/Excel/Parquet 到 S3** + 预签名 URL
- **QuickSight 共享**：把分析结果以 Dashboard 形式交付，不暴露原始数据

## 三、持续/增量交付场景

| 场景 | 推荐方案 |
|------|---------|
| 每日批量增量 | EventBridge 定时触发 Glue Job → UNLOAD 到客户 S3 |
| 近实时 | Iceberg 表共享（客户自行读取最新 snapshot） |
| 准实时事件流 | S3 Tables → Kinesis Firehose / MSK → 客户消费 |
| 按需 API 拉取 | API Gateway + Lambda + Athena 查询封装为 REST 接口 |

## 四、安全与合规要点

- **加密**：S3 SSE-KMS，跨账户共享 KMS Key
- **权限最小化**：Lake Formation 列/行级权限、S3 Access Point 限定前缀
- **审计**：CloudTrail + S3 Server Access Log 记录客户访问
- **数据脱敏**：交付前在 Athena CTAS 中做 mask（PII 字段哈希/截断）
- **链接时效**：预签名 URL 不超过 7 天，敏感数据建议 < 24 小时

## 五、推荐组合（按典型场景）

| 场景 | 推荐方案 |
|------|---------|
| **一次性交付报表数据** | Athena UNLOAD + 预签名 URL |
| **长期合作客户、AWS 用户** | Lake Formation 跨账户共享 Iceberg 表 |
| **非 AWS 客户、定期交付** | Glue Job 导出 Parquet → Transfer Family SFTP |
| **PB 级历史数据迁移** | Snowball Edge |
| **客户用 Snowflake/Databricks** | Iceberg External Table 直接挂载 |

## 六、方案对比矩阵

| 方案 | 数据量 | 客户门槛 | 实时性 | 成本 | 运维 |
|------|--------|---------|--------|------|------|
| Athena UNLOAD + 预签名 URL | < 10 GB | 低（HTTP 下载） | 按需 | 低 | 极低 |
| S3 跨账户 + Access Point | TB 级 | 中（需 AWS 账户） | 实时 | 低 | 低 |
| Transfer Family SFTP | < TB | 低（SFTP 客户端） | 按需 | 中 | 中 |
| DataSync | TB ~ PB | 中 | 计划任务 | 中 | 低 |
| Snowball Edge | PB+ | 低（物理交付） | T+几天 | 高（一次性） | 高 |
| Lake Formation 跨账户 | 无限 | 高（需数据团队） | 实时 | 极低 | 低 |
| Iceberg External Table | 无限 | 高（Snowflake/DBX） | 实时 | 极低 | 极低 |
| QuickSight 共享 | 任意 | 低（浏览器） | 实时 | 中 | 低 |
