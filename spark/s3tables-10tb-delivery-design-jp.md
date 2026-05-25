# S3 Tables 10TB データ配信ソリューション設計書

S3 Tables から約 10TB のデータを読み出し、通常の S3 バケットへ出力して顧客に配信する Glue Job ベースの設計。中断時に失敗箇所から再開できる仕組みを提供する。

## 一、全体アーキテクチャ

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Step       │───▶│  Plan Job    │───▶│  Execute Job │───▶│  Verify Job  │
│  Functions  │    │  (分割計画)   │    │  (並列処理)  │    │  (検証+清單) │
└─────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                          │                   │                    │
                          ▼                   ▼                    ▼
                   ┌──────────────────────────────────────────────────┐
                   │   DynamoDB Checkpoint テーブル (中核となる状態)    │
                   └──────────────────────────────────────────────────┘
                                              │
                                              ▼
                   ┌──────────────────────────────────────────────────┐
                   │   S3 配信バケット (Parquet + manifest)             │
                   └──────────────────────────────────────────────────┘
```

3 つの Glue Job を Step Functions でオーケストレーションする：**Plan → Execute（再実行可能） → Verify**。

## 二、コア設計：再開可能性の鍵

### 1. Iceberg スナップショットのロック

- Plan フェーズで `current_snapshot_id` を一度取得し、Job パラメータと Checkpoint テーブルに記録
- すべての Execute タスクは統一的に `FOR VERSION AS OF <snapshot_id>` でクエリ
- **保証**：配信期間中にソーステーブルへの書き込みが継続しても、すべての再試行で同じバージョンのデータを読み込み、結果の一貫性が保たれる

### 2. 分割戦略（10TB をどう分けるか）

優先順に選択：

| 戦略 | 適用シーン | 分割粒度 |
|------|-----------|---------|
| **Iceberg パーティション単位** | テーブルがパーティション分けされている（例：`dt`、`region`） | 1 パーティション = 1 チャンク |
| **主キー範囲ハッシュバケット** | パーティションなしだが主キーあり | `MOD(hash(pk), N) = bucket_id` |
| **ファイル単位（高度）** | `$files` メタデータから直接データファイルを割り当て | 1 ～複数のデータファイル = 1 チャンク |

目標：**1 チャンク 5–20 GB の生データ**（圧縮後 1–4 GB の Parquet）、10TB で約 500–2000 チャンク。

### 3. DynamoDB Checkpoint テーブル設計

プライマリキー：
- **PK**：`job_id`（1 回の配信タスク ID）
- **SK**：`chunk_id`（チャンク ID）

属性：

| フィールド | 用途 |
|----------|------|
| `status` | `pending` / `in_progress` / `completed` / `failed` |
| `partition_filter` | チャンクの WHERE 条件（またはファイルリスト） |
| `snapshot_id` | ロックされた Iceberg snapshot |
| `output_prefix` | 出力 S3 パス（chunk_id 含む） |
| `row_count` | 書き込み行数（完了後に書き戻し） |
| `byte_size` | 書き込みバイト数 |
| `sha256` | ファイルチェックサム（オプション） |
| `attempt` | リトライ回数 |
| `worker_id` | 現在の保持者（並行性防止） |
| `lease_until` | リース期限（タイムアウト時自動解放） |
| `started_at` / `completed_at` | タイムスタンプ |
| `error_msg` | 失敗理由 |

**追加 GSI**：`job_id` + `status` で `pending` / `failed` のチャンクを高速に取得可能。

### 4. アトミックコミット（半完成状態の防止）

各チャンクの処理フロー：

```
1. Glue Worker が条件付き書き込みで DynamoDB を更新:
   ConditionExpression: status IN (pending, failed)
   SET status = in_progress, worker_id = ..., lease_until = now + 30min
   ↓
2. チャンクデータを読み込み → 一時パス s3://delivery/_tmp/<job_id>/<chunk_id>/ へ書き込み
   ↓
3. 書き込み完了後、正式パス s3://delivery/<job_id>/data/<chunk_id>/ へ移動
   (または _SUCCESS マーカーファイル)
   ↓
4. 条件付き書き込みで DynamoDB を更新:
   ConditionExpression: worker_id = <self> AND status = in_progress
   SET status = completed, row_count = ..., sha256 = ...
```

**重要ポイント**：
- 一時パスに書き込んでから正式パスへ移動 → **半完成ファイルの読み込みを防止**
- 条件付き書き込み → **並行処理またはリース期限後の不正書き込みを防止**
- 失敗時、Worker は能動的に状態変更せず、リース期限切れで他 Worker が引き継ぐ

## 三、3 つの Job の役割

### Plan Job（一回のみ実行、小規模 G.1X で十分）

- snapshot_id を取得しロック
- `$partitions` または `$files` をスキャンしてチャンクを決定
- DynamoDB へバッチ書き込み（全 chunks の初期状態 `pending`）
- 出力：job_id

### Execute Job（中核、再実行可能、G.2X または G.4X、複数 Worker）

- job_id パラメータを受け取る
- ループ：DynamoDB GSI から `pending` または `lease_until < now` のチャンクをバッチ取得
- Spark で Iceberg を指定 snapshot + チャンクフィルター条件で読む
- `repartition` で出力ファイルサイズを制御（256MB–1GB/ファイル推奨）
- Parquet (ZSTD) を `_tmp` へ書き込み → 正式パスへ rename
- DynamoDB を completed に更新
- GSI に pending がなくなるまで → 終了
- **再実行時**：同じ job_id を完全に再利用、pending/failed のチャンクのみ処理

### Verify Job

- すべての chunk が completed であることを確認
- 集計して `manifest.json` を生成：ファイルリスト、行数、バイト数、sha256
- 集計 `_SUCCESS` マーカー
- （オプション）プリサインド URL の生成または通知の送信

## 四、失敗回復シナリオ

| 失敗種類 | 回復メカニズム |
|---------|--------------|
| **単一チャンク失敗** | リース期限後、次回 Execute で自動リトライ |
| **Job 全体クラッシュ** | Step Functions が Execute を再実行、DynamoDB から継続 |
| **S3 への書き込み途中のファイル残留** | `_tmp` パスは顧客には見えない；定期クリーンアップ |
| **正式パスへの重複書き込み** | パスに chunk_id 含む；冪等上書き；DynamoDB 条件付き書き込みで並行性防止 |
| **ソースデータの変更** | snapshot_id ロックで常に同じバージョンを読む |
| **DynamoDB 書き込み失敗** | Spark task 自体がリトライ；複数回失敗で chunk failed |
| **全体タイムアウト** | リース機構で自動解放；max_attempts で無限ループ防止 |

## 五、Glue Job パラメータとスケーラビリティ

| パラメータ | 推奨値 |
|-----------|--------|
| Worker Type | G.2X（10TB シナリオ） |
| Worker 数 | 20–50（各 Worker が複数 chunk を処理） |
| Job タイムアウト | 単回 8–12 時間 |
| リトライ回数 | Glue 層 1 回、アプリ層は無制限（リースに依存） |
| 並行度 | DynamoDB lease で制御、調整可能 |
| Parquet ファイルサイズ | 目標 512MB（ZSTD 圧縮後） |

**水平スケーリング**：Execute Job の並列インスタンス数を増やすだけで線形に高速化、すべてのインスタンスが DynamoDB の状態を共有。

## 六、コストと時間の見積もり

| 項目 | 見積もり |
|------|---------|
| Glue DPU 時間（G.2X × 30 × 8h） | ~$200–400 |
| DynamoDB（オンデマンド、約 10 万回書き込み） | < $5 |
| S3 PUT/GET | < $20 |
| S3 ストレージ（Parquet 1.5TB） | $35/月 |
| **総コスト（顧客転送除く）** | **~$250–500** |
| **総所要時間（30 Worker 並列）** | **6–10 時間** |

## 七、配信方法の選択

Execute Job が配信バケットに書き込んだ後、顧客の能力に応じて選択：

| 顧客種別 | 配信方法 |
|---------|---------|
| AWS 顧客 | S3 クロスアカウント認可（推奨） |
| 非 AWS 顧客 | DataSync → 顧客のオブジェクトストレージ |
| 低帯域/PB 級 | Snowball Edge |
| 一時ダウンロード | プリサインド URL（各ファイル個別、manifest.json と組み合わせ） |

## 八、主要設計のトレードオフ

| 決定 | 理由 |
|------|------|
| **DynamoDB を使用（S3/Glue Bookmark の代替）** | 強整合性、条件付き書き込み、GSI クエリ、リース機構をネイティブサポート |
| **Snapshot ID のロック** | 複数回リトライ結果の一貫性を保証する唯一の方法 |
| **`_tmp` パス + rename** | 顧客パスに半完成品が現れない |
| **行数ではなくパーティション/ファイル単位の分割** | Iceberg メタデータと自然に整合、スキャンせずに分割可能 |
| **Plan/Execute/Verify を 3 つの Job に分離** | 各々独立して再実行可能、責任が明確、Plan は小規模で十分 |
| **Step Functions でオーケストレーション** | リトライ、タイムアウト、状態マシンの可視化が組み込み |

## 九、オプション拡張

- **レート制限**：Execute Job で token bucket を使い S3 書き込み速度を制限、他業務への影響回避
- **データマスキング**：Spark 読み込み後、出力前にフィールドを mask
- **暗号化**：配信バケットで SSE-KMS を有効化、クロスアカウント KMS Key を共有
- **監査**：CloudTrail + S3 Access Log で顧客ダウンロードを記録
- **通知**：Verify 完了後 SNS → メール/Slack で顧客に通知

## 十、設計の一行まとめ

> **Iceberg snapshot をロック → 10TB を 500–2000 個のチャンクに分割 → DynamoDB で状態管理 + リース機構 → `_tmp` 中間パスでアトミック書き込み → Step Functions が Plan/Execute/Verify を編成し、中断地点から確実に再開可能。**
