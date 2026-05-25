# AWS リソース指標 設定ガイド（10TB シナリオ）

S3 Tables 10TB データ配信タスクを正常に完了させるための、主要 AWS リソースの推奨設定値。コンポーネント別に具体的な数値、算出根拠、事前申請が必要なクォータをまとめる。

## 一、Glue Job 設定

### Plan Job（軽量）

| 指標 | 推奨値 | 根拠 |
|------|--------|------|
| Worker Type | **G.1X** | メタデータスキャンのみ、CPU/メモリ要求小 |
| Worker 数 | **2–3** | Plan フェーズはシングルスレッド中心 |
| Timeout | **30 分** | メタデータスキャン + DynamoDB バッチ書き込みは分単位 |
| Max Retries | **2** | 再実行コスト低 |

### Execute Job（中核）

| 指標 | 推奨値 | 根拠 |
|------|--------|------|
| Worker Type | **G.2X**（8 vCPU / 32GB / 128GB disk） | 10TB Iceberg スキャン、shuffle と row group 解凍にメモリ必要 |
| Worker 数 | **30–50** | 下記計算参照 |
| Total DPU | **60–100** | G.2X = 2 DPU/worker |
| Timeout | **8 時間**（単回） | 余裕を持たせ、超過時は次回再実行 |
| Max Concurrent Runs | **3–5** | 複数配信タスクの並行処理を許容 |
| Max Retries | **1** | アプリ層にリース機構あり、Glue 層では過剰リトライしない |
| Job Bookmark | **Disable** | 独自の DynamoDB checkpoint を使用 |

**Worker 数算出根拠**：

```
生データ 10TB = 10,240 GB
Parquet 圧縮後 約 1.5–2 TB
G.2X worker 1 台あたり読込スループット ~200 MB/s（shuffle 含む）
30 worker × 200 MB/s = 6 GB/s
理論時間 = 10240 GB / 6 GB/s ≈ 28 分（IO のみ）
shuffle、書き込み、スケジューリングオーバーヘッドを加味し、実際は 6–10 時間
```

### Spark パラメータ（Job 内で設定）

```
--conf spark.sql.shuffle.partitions=2000
--conf spark.sql.files.maxPartitionBytes=256MB
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.advisoryPartitionSizeInBytes=512MB
--conf spark.sql.parquet.compression.codec=zstd
--conf spark.hadoop.fs.s3a.connection.maximum=200
--conf spark.hadoop.fs.s3a.threads.max=64
--conf spark.hadoop.fs.s3a.multipart.size=64M
```

## 二、DynamoDB 設定

### 容量モードとスループット

| 指標 | 推奨値 | 根拠 |
|------|--------|------|
| **容量モード** | **On-Demand** | chunk 数 500–2000、バースト書き込み、オンデマンドが運用上有利 |
| 代替 Provisioned WCU | **500** | プロビジョンドの場合、50 worker × 10 ops/s で見積もり |
| 代替 Provisioned RCU | **1000** | GSI クエリ頻度高く、読み込み > 書き込み |
| Auto Scaling | **有効** | プロビジョンドモード必須 |

### GSI 設計

| 指標 | 推奨値 | 根拠 |
|------|--------|------|
| GSI 名 | `status-index` | PK=`job_id`, SK=`status` |
| Projection | **KEYS_ONLY** または **INCLUDE(chunk_id, lease_until)** | GSI サイズ削減 |
| GSI 容量 | メインテーブルと同じ | GSI スロットリングのメインテーブルへの逆流防止 |

### TTL

| 指標 | 推奨値 | 根拠 |
|------|--------|------|
| TTL フィールド | `ttl_expire_at` | Job 完了 30 日後に checkpoint 自動クリーンアップ |

### 重要監視閾値

| 指標 | アラート閾値 |
|------|------------|
| `ThrottledRequests` | > 0 で即アラート |
| `UserErrors` | > 10/分 |
| `SystemErrors` | > 0 |
| `ConsumedWriteCapacityUnits` | 設定容量の 80% 超 |

## 三、S3 設定

### リクエスト速度

| 指標 | 制限 | 対応 |
|------|------|------|
| PUT/COPY/POST/DELETE | **3,500 req/s/prefix** | 出力パスを hash プレフィックスで分散 |
| GET/HEAD | **5,500 req/s/prefix** | 同上 |

**出力パス設計**（ホットプレフィックス回避）：

```
s3://delivery-bucket/
  └── jobs/<job_id>/
      └── data/
          └── <hash(chunk_id)[:2]>/<chunk_id>/   ← hash プレフィックスで分散
              └── part-*.parquet
```

### バケットレベル設定

| 指標 | 推奨値 |
|------|--------|
| Versioning | **Enabled**（誤削除防止） |
| Default Encryption | **SSE-S3** または **SSE-KMS（bucket key 有効）** | bucket key で KMS RPS ボトルネック回避 |
| Lifecycle ルール 1 | `_tmp/` プレフィックスを 7 日後自動削除 | 失敗残留物のクリーンアップ |
| Lifecycle ルール 2 | 未完了 multipart upload を 1 日後クリーンアップ | ゾンビ MPU 防止 |
| Lifecycle ルール 3 | `data/` を 30 日後 Standard-IA / 90 日後 Glacier へ | コスト最適化 |
| Object Ownership | **Bucket owner enforced** | ACL 無効化、権限モデル統一 |
| Block Public Access | **4 項目全 ON** | セキュリティベースライン |

### Transfer Acceleration

| 指標 | 推奨値 |
|------|--------|
| クロスリージョン顧客転送 | **有効** | 大陸跨ぎダウンロード高速化 |

## 四、KMS 設定

| 指標 | 推奨値 | 根拠 |
|------|--------|------|
| Key 種別 | **Customer Managed Key (CMK)** | クロスアカウント共有、監査、ローテーション |
| **S3 Bucket Key** | **Enabled** | **重要**：KMS 呼び出しを 99% 削減、RPS 制限回避 |
| Key Policy | Glue Role + 顧客アカウント Role に権限付与 | クロスアカウント復号 |
| Rotation | **Annual** | コンプライアンス要件 |
| KMS RPS クォータ（デフォルト） | **5,500–10,000 RPS** | Bucket Key 有効化で実質到達せず |

**Bucket Key を必ず有効化すべき理由**：

- 無効：各 S3 object のアップロード/ダウンロードで毎回 KMS 呼び出し
- 10TB / 512MB per file = 20,000 ファイル → 20,000 回 KMS 呼び出し → ピーク時 RPS 超過
- Bucket Key 有効：5 分窓 × バケット単位で 1 回 KMS 呼び出し → ほぼゼロ

## 五、Step Functions 設定

| 指標 | 推奨値 | 根拠 |
|------|--------|------|
| Workflow 種別 | **Standard** | 長時間実行（時間単位）、Express は 5 分上限で不足 |
| 最大実行時間 | **24 時間** | Plan + 複数回 Execute + Verify を含む |
| State Machine リトライ | Execute 状態は最大 **5 回**、指数バックオフ 60s–30min | 一時的障害への対応 |
| Input/Output Size | **≤ 256 KB** | job_id のみ渡し、詳細は DynamoDB 参照 |
| 並行実行数 | **10** | 複数顧客並行配信 |

## 六、ネットワーク（VPC / NAT / Endpoint）

| 指標 | 推奨値 | 根拠 |
|------|--------|------|
| **S3 Gateway Endpoint** | **必須** | Glue → S3 を内部経路で、料金ゼロ、NAT ボトルネックなし |
| **DynamoDB Gateway Endpoint** | **必須** | 同上 |
| KMS Interface Endpoint | オプション | トラフィック小、セキュリティ向上 |
| Glue Interface Endpoint | オプション | パブリック経由で可 |
| NAT Gateway 帯域 | **45 Gbps**（デフォルト） | S3 endpoint 未設定時のみ必要 |
| **推奨**：データトラフィックを NAT 経由にしない | — | すべて Gateway Endpoint 経由 |

## 七、CloudWatch 監視

### カスタムメトリクス（アプリ層計装）

| 指標 | 単位 | アラート閾値 |
|------|------|------------|
| `chunks_pending` | Count | タスク期間中 > 0 は正常、10h 以上減少なしでアラート |
| `chunks_in_progress` | Count | > worker 数 × 1.5 で異常 |
| `chunks_failed` | Count | 総数の 5% 超でアラート |
| `chunks_completed_per_minute` | Count/min | 期待スループットの 50% 未満でアラート |
| `avg_chunk_duration_sec` | Seconds | 予想値の 3 倍超でアラート（データ偏りの兆候） |
| `bytes_written_per_minute` | Bytes | < 100MB/min でアラート |

### ログ保持期間

| Log Group | 保持期間 |
|-----------|---------|
| `/aws-glue/jobs/output` | 30 日 |
| `/aws-glue/jobs/error` | 90 日 |
| Step Functions execution | 90 日 |

## 八、IAM Role 必須権限

### Glue Job Role 必須権限

```
- glue:GetTable, GetPartitions, GetDatabase
- s3tables:GetTable, GetTableData, GetTableMetadata, ListTableBuckets
- s3:GetObject, ListBucket (ソースバケット + delivery バケット)
- s3:PutObject, DeleteObject, AbortMultipartUpload (delivery バケット)
- dynamodb:GetItem, PutItem, UpdateItem, Query, BatchWriteItem
- kms:Decrypt, Encrypt, GenerateDataKey (ソース KMS Key + delivery KMS Key)
- logs:CreateLogStream, PutLogEvents
- cloudwatch:PutMetricData
- lakeformation:GetDataAccess (Lake Formation 使用時)
```

### 重要：`*` リソース回避、テーブル/バケット/Key ARN 単位の最小権限

## 九、Service Quotas（事前提出必須）

| サービス | クォータ項目 | デフォルト | 申請推奨値 |
|---------|-------------|-----------|-----------|
| **Glue** | DPU per region | 100 | **200–500** |
| **Glue** | Max concurrent job runs per job | 1000 | デフォルト維持 |
| **Glue** | Number of jobs per account | 1000 | デフォルト維持 |
| **DynamoDB** | Account-level write throughput | 40,000 WCU | オンデマンドモードは制限なし |
| **S3** | Buckets per account | 100 | 通常十分 |
| **Step Functions** | Standard workflow executions | 1,000,000/月 | デフォルト維持 |
| **KMS** | Cryptographic operations RPS | 5,500–10,000 | Bucket Key 有効化で提額不要 |
| **EC2**（Glue 基盤） | vCPU クォータ（On-Demand Standard） | アカウント依存 | **≥ 500 vCPU** |
| **CloudWatch Logs** | Ingestion rate | 5 MB/s | 通常十分 |

**提額申請タイミング**：**最低 1 週間前**（DPU 提額は 1–5 営業日かかる場合あり）

## 十、クォータ監視（実行時の上限到達防止）

| 監視項目 | CloudWatch メトリクス |
|---------|----------------------|
| Glue DPU 使用率 | `AWS/Glue` → `glue.driver.aggregate.numCompletedTasks` |
| DynamoDB スロットリング | `AWS/DynamoDB` → `ThrottledRequests` |
| S3 5xx | `AWS/S3` → `5xxErrors` |
| KMS スロットリング | `AWS/KMS` → `ThrottledRequests` |
| NAT Gateway 帯域 | `AWS/NATGateway` → `BytesOutToDestination` |

## 十一、主要パラメータ クイックリファレンス（10TB 一括設定）

| カテゴリ | パラメータ | 値 |
|---------|----------|-----|
| **Glue Plan** | Worker × Type | 2 × G.1X |
| **Glue Execute** | Worker × Type | 30 × G.2X |
| **Glue Execute** | Timeout / Retry | 8h / 1 |
| **Glue Verify** | Worker × Type | 2 × G.1X |
| **DynamoDB** | 容量モード | On-Demand |
| **DynamoDB** | GSI | status-index (KEYS_ONLY) |
| **Chunk 数** | 目標 | 500–2000 |
| **Chunk サイズ** | 生データ / Parquet | 5–20GB / 1–4GB |
| **Parquet ファイルサイズ** | 目標 | 512 MB |
| **Lease 期間** | — | 30 分 |
| **S3 出力プレフィックス** | 分散度 | 256 (hash[:2]) |
| **S3 Bucket Key** | — | Enabled |
| **VPC Endpoint** | S3 + DynamoDB | Gateway |
| **DPU クォータ** | 申請後 | ≥ 200 |
| **EC2 vCPU クォータ** | 申請後 | ≥ 500 |
| **Step Functions** | 種別 / タイムアウト | Standard / 24h |

## 十二、容量検証チェックリスト（タスク開始前必須確認）

- [ ] Glue DPU クォータ ≥ 実需要 × 1.5
- [ ] DynamoDB テーブル作成済み、GSI クエリ検証済み
- [ ] S3 Bucket Key 有効化済み
- [ ] S3 + DynamoDB Gateway Endpoint 設定済み
- [ ] IAM Role を小規模 chunk で読み書き検証済み
- [ ] KMS Key クロスアカウント認可テスト完了
- [ ] CloudWatch Alarms 設定済み（DPU、スロットリング、失敗率）
- [ ] SNS 通知トピック購読済み
- [ ] Lifecycle ルール設定済み（_tmp クリーンアップ、MPU クリーンアップ）
- [ ] Iceberg ソーステーブルの expire_snapshots 一時停止済み
- [ ] 100GB スケールの e2e テスト完走済み

## 十三、サイジング全体まとめ（一文）

> **Glue Execute は G.2X × 30 worker、DynamoDB は On-Demand + GSI、S3 は Bucket Key + hash プレフィックス分散、KMS は Bucket Key 有効化で RPS 回避、ネットワークは S3/DynamoDB Gateway Endpoint 必須、DPU と EC2 vCPU クォータを事前に 1 週間前に提額申請する。**
