# Spark SQL 長時間実行 + S3 出力に関する設計検討

## 1. 長時間 Spark SQL 実行 + S3 書き込み時に発生しやすい問題

長時間（数時間〜24時間以上）の Spark SQL 実行および S3 出力では、以下のような問題が発生しやすい。

---

## 1.1 Spark / Glue 実行基盤側の問題

| 問題 | 内容 |
|---|---|
| Glue Job Timeout | Glue Job の最大実行時間超過により Job が強制終了される |
| Driver OOM | Driver JVM のメモリ不足により Job 全体が停止する |
| Executor OOM | 特定 Task のデータ量が大きすぎて Executor が異常終了する |
| Executor Lost | Worker ノード障害や Container 異常終了 |
| Shuffle Failure | 大量 JOIN / GROUP BY / ORDER BY により Shuffle が失敗する |
| Task Retry Over | Task 再試行回数超過により Stage が失敗する |

---

## 1.2 データ量起因の問題

| 問題 | 内容 |
|---|---|
| Data Skew | 特定 Partition のデータ量が極端に偏る |
| Massive Shuffle | repartition / join による大量ネットワーク転送 |
| Spill 発生 | メモリ不足により Disk Spill が大量発生 |
| Small Files 問題 | 出力ファイル数が増えすぎて性能低下 |

---

## 1.3 S3 書き込み時の問題

| 問題 | 内容 |
|---|---|
| 出力途中失敗 | part ファイルが途中状態で残る |
| overwrite 中断 | 旧データ削除後に新データ書き込み失敗 |
| append 重複 | Job 再実行時に重複データ発生 |
| 単一巨大ファイル生成失敗 | coalesce(1) による巨大 Shuffle |
| Multipart Upload Failure | S3 Multipart Upload の途中失敗 |

---

## 1.4 データ整合性の問題

| 問題 | 内容 |
|---|---|
| 実行中データ更新 | 実行途中で元データが更新される |
| Schema Change | 実行中にテーブル定義変更 |
| Partition 更新 | 実行途中で Partition 差し替え |

---

# 2. どの問題が途中復旧可能か

Spark は「Task レベル」の復旧には強いが、「Job 全体の途中再開」は基本的に自動対応しない。

---

## 2.1 自動復旧可能なケース

| 問題 | 自動復旧可否 | 内容 |
|---|---|---|
| Task Failure | ○ | Spark が Task Retry 実施 |
| 一時的 Network Error | ○ | SDK / Spark Retry |
| Executor Lost | △ | 一部 Partition 再計算可能 |
| Shuffle Read Error | △ | Shuffle Retry 可能な場合あり |
| S3 PUT 一時失敗 | △ | SDK Retry |

---

## 2.2 自動復旧困難なケース

| 問題 | 自動復旧可否 | 内容 |
|---|---|---|
| Glue Job Timeout | × | Job 全体再実行が必要 |
| Driver Crash | × | DAG 情報消失 |
| overwrite 中断 | × | データ不整合リスク |
| append 重複 | × | 重複データ発生 |
| coalesce(1) 失敗 | × | 単一巨大ファイル生成失敗 |
| 24時間 Job 中断 | × | 中間位置からの Resume 不可 |

---

# 3. 長時間 Spark SQL + S3 出力で中断復旧を実現する設計案

---

# 3.1 推奨アーキテクチャ

```text
S3 / S3 Tables
        ↓
Batch 分割
（日付・ID範囲・Partition）
        ↓
Glue Spark Job
        ↓
一時出力（tmp）
        ↓
成功確認
        ↓
正式出力（final）
        ↓
状態管理テーブル更新
```

---

# 3.2 Batch 分割方式（推奨）

## 概要

全量を 1 Job で処理せず、小さな単位に分割する。

例：

```text
dt=2026-05-01
dt=2026-05-02
dt=2026-05-03
```

または：

```text
ID 1〜100万
ID 100万〜200万
```

---

## メリット

| メリット | 内容 |
|---|---|
| 障害影響縮小 | 失敗 Batch のみ再実行 |
| Resume 容易 | SUCCESS Batch をスキップ可能 |
| 並列化可能 | 複数 Batch 同時実行 |
| コスト制御容易 | 小規模単位で制御可能 |

---

# 3.3 状態管理テーブル方式

DynamoDB / RDS / S3 Manifest 等で Batch 状態を管理する。

---

## 状態管理例

| batch_id | status |
|---|---|
| 2026-05-01 | SUCCESS |
| 2026-05-02 | SUCCESS |
| 2026-05-03 | FAILED |

---

## Resume 時

```text
SUCCESS → Skip
FAILED → Retry
RUNNING Timeout → Retry 対象
```

---

# 3.4 tmp → final Commit 方式（重要）

直接 final ディレクトリへ書き込まない。

---

## NG

```text
s3://bucket/final/
```

へ直接出力。

---

## 推奨

```text
s3://bucket/tmp/run_id=001/batch=2026-05-01/
```

成功確認後：

```text
s3://bucket/final/batch=2026-05-01/
```

へ Commit。

---

## メリット

| メリット | 内容 |
|---|---|
| 半端データ防止 | 中断時に final 汚染しない |
| 再実行容易 | tmp 削除のみ |
| 整合性向上 | SUCCESS Batch のみ公開 |

---

# 3.5 overwrite 単位最小化

推奨：

```python
mode("overwrite")
```

ただし：

```text
batch 単位のみ overwrite
```

とする。

---

## NG

```text
全体 final overwrite
```

---

## OK

```text
final/batch=2026-05-01/
```

のみ overwrite。

---

# 3.6 Parquet 推奨

CSV より Parquet を推奨。

| 項目 | CSV | Parquet |
|---|---|---|
| サイズ | 大 | 小 |
| 圧縮 | なし | あり |
| Athena 性能 | 低 | 高 |
| Spark 性能 | 低 | 高 |
| Partition Pushdown | 不可 | 可 |

---

# 3.7 単一巨大ファイルを避ける

## 非推奨

```python
coalesce(1)
```

理由：

- 巨大 Shuffle
- OOM
- 超低速
- Resume 困難

---

## 推奨

```text
複数 part ファイル
```

例：

```text
part-0000.parquet
part-0001.parquet
part-0002.parquet
```

---

# 3.8 Step Functions による制御（推奨）

```text
Step Functions
    ↓
Batch List 作成
    ↓
Map State
    ↓
Glue Job 起動
    ↓
状態更新
    ↓
失敗 Batch Retry
```

---

## メリット

| メリット | 内容 |
|---|---|
| Retry 制御 | Batch 単位 |
| 並列制御 | Map State |
| 状態可視化 | AWS Console |
| 自動復旧 | Retry Policy |

---

# 4. 推奨結論

長時間 Spark SQL + S3 出力では、以下を推奨する。

---

## 推奨構成

```text
Batch 分割
+ 
状態管理
+ 
tmp 出力
+ 
SUCCESS Commit
+ 
Parquet
+ 
Step Functions 制御
```

---

## 避けるべき構成

```text
24時間単一 Job
+ 
coalesce(1)
+ 
巨大 CSV
+ 
直接 final overwrite
```

これは：

- 障害復旧困難
- データ不整合
- 超巨大 Shuffle
- OOM
- 長時間再実行

を引き起こしやすい。
