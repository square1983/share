# 実行時の失敗可能性 全体分析

S3 Tables 10TB 配信アーキテクチャにおいて、実行時に発生し得る失敗をレイヤー別に整理し、設計への影響をまとめる。

## 一、インフラ層（Infrastructure）

### 1. AWS サービスレベルの障害

| 失敗箇所 | 症状 | 影響範囲 |
|---------|------|---------|
| **Glue サービス利用不可** | Job 起動失敗、Worker が割り当てられない | Execute フェーズ全体が停止 |
| **Glue Worker ノードの消失** | Spark executor が突然消える（Spot 回収、基盤 EC2 障害） | 該当 Worker が処理中の chunk が中断 |
| **DynamoDB スロットリング** | `ProvisionedThroughputExceededException` | 状態更新失敗、chunk が in_progress でスタック |
| **DynamoDB 単一パーティションのホットスポット** | 多数 Worker が同時に GSI 同一パーティションを読む | 一部リクエストがタイムアウト |
| **S3 503 Slow Down** | 同一プレフィックスへの書き込み QPS が過大（>3500 PUT/s/prefix） | 書き込み失敗 / リトライ雪崩 |
| **S3 結果整合性（クロスリージョン）** | まれだが存在する | 直前に書いたファイルが List で見えない |
| **IAM / STS 認証情報の更新失敗** | Glue Job が長時間実行されてトークン失効 | 後半のタスクが全失敗 |
| **KMS API 制限** | SSE-KMS 暗号化時に各オブジェクトで KMS 呼び出し | PutObject 失敗 |
| **Step Functions 実行時間 1 年上限** | ほぼ発生しないが、Express モードは 5 分上限 | オーケストレーション中断 |
| **AZ レベル障害** | リージョン内の単一 AZ ダウン | Glue が自動移行、遅延の可能性 |
| **VPC Endpoint 異常** | Glue が VPC 経由で S3/DynamoDB アクセス時 | ネットワーク中断 |

### 2. ネットワークと依存関係

- **クロスリージョン S3 Tables 読み込み**：帯域課金 + レイテンシ、タイムアウト可能性
- **DNS 解決失敗**：低頻度だが endpoint 解決異常の発生例あり
- **NAT Gateway 帯域ボトルネック**：VPC 内 Glue の外向き通信制限
- **PrivateLink 設定ミス**：S3 Tables endpoint 不通

### 3. クォータ（Quota）

| クォータ | リスク |
|---------|--------|
| Glue 同時 Job 数（デフォルト 200） | 複数顧客並行時に上限到達 |
| Glue DPU 総クォータ | 大規模並行で不足 |
| S3 PUT/GET レート（プレフィックス単位） | 集中書き込みで 503 発生 |
| DynamoDB 書き込み容量（オンデマンドも上限あり） | バースト書き込みが制限 |
| KMS RPS（キーごとに 5500–10000/秒） | 暗号化オブジェクトで制限 |

## 二、データソース層（S3 Tables / Iceberg）

| 失敗箇所 | 症状 | 緩和策 |
|---------|------|--------|
| **Snapshot が expire 削除** | Iceberg の expire_snapshots がロック中の snapshot を削除、読み込み不可 | タスク期間中、ソーステーブルの expire_snapshots を無効化、または保持期間を延長 |
| **データファイルが compact/rewrite** | Snapshot メタデータが指す manifest/data file が書き換えられる | Iceberg は履歴 snapshot を保護するが設定不備で失う場合あり；`min-snapshots-to-keep` を明示設定 |
| **Schema 進化** | タスク開始後に ADD/DROP COLUMN | snapshot ロック後は schema も固定、理論上安全；下流の注意は必要 |
| **データ偏り（skew）** | 特定パーティションのデータ量が他の 100 倍 | Plan フェーズでサイズ推定、偏りパーティションを再分割 |
| **空パーティション / NULL データ** | あるチャンクが 0 行で読み出される | completed としてマーク、manifest に反映 |
| **メタデータ破損** | まれだが metadata.json 異常 | Iceberg テーブルヘルスチェック |
| **権限取り消し** | タスク中に Lake Formation 権限変更 | IAM 監視アラート |
| **S3 Tables バケットの誤削除/凍結** | 全読み込み失敗 | リソース保護ポリシー、アカウント警告 |

## 三、計算層（Glue / Spark）

| 失敗箇所 | 症状 | 緩和策 |
|---------|------|--------|
| **OOM（OutOfMemory）** | データ偏りや大 row group で executor クラッシュ | Worker サイズ拡大（G.2X→G.4X）、repartition で並列度増加 |
| **Driver OOM** | 大きな結果を driver に collect | collect を避け、foreachPartition 使用 |
| **GC ストーム** | 長時間の stop-the-world | Spark メモリ比率調整、G1GC 利用 |
| **Shuffle 失敗** | shuffle ファイル消失、ディスク満杯 | shuffle 削減、より大容量ディスクの Worker |
| **Iceberg 依存バージョン非互換** | S3 Tables Catalog JAR と Glue Iceberg バージョン衝突 | バージョン固定、POC で検証（既知の落とし穴） |
| **Job タイムアウト** | 単一 Job 8 時間以内に未完了 | バッチ退出、次回再実行で継続 |
| **Spark task リトライ上限** | デフォルト 4 回で stage 失敗 | `spark.task.maxFailures` 調整、本質はデータ修正 |
| **DPU 不足によるキュー待ち** | アカウントの DPU が他タスクで占有 | DPU 予約または時間帯スケジューリング |
| **Worker 起動の遅さ** | コールドスタート 1–2 分 | 現実を受け入れる、または streaming job 利用 |

## 四、状態管理層（DynamoDB Checkpoint）

| 失敗箇所 | 症状 | 緩和策 |
|---------|------|--------|
| **条件付き書き込み競合** | 2 つの Worker が同時に同じ chunk を取得 | 設計上想定済み、敗者は次の chunk を探す |
| **リース誤判定** | Worker は生きているがリース期限切れで他 Worker が引き継ぐ | 書き込みパスに chunk_id 含む、冪等上書き；条件付き書き込みで completed 状態の上書きを防止 |
| **DynamoDB 書き込み喪失** | ネットワーク揺らぎで ack 喪失 | SDK 自動リトライ、冪等設計 |
| **GSI 遅延** | GSI 書き込みに数百 ms 遅延、completed 直後でも pending クエリに出現 | 少量の重複処理を許容（冪等性で対応） |
| **状態遷移エラー** | コードバグで completed → in_progress | ConditionExpression で厳密に状態遷移制限 |
| **リース時間長さの不適切** | 短すぎ：誤判定、長すぎ：障害復旧遅延 | 単一 chunk の予想処理時間 × 3、Worker による能動的延長サポート |
| **DynamoDB 容量爆発** | 数百万 chunks のオンデマンド書き込み | Provisioned 容量に切替、BatchWriteItem 分割 |
| **GSI でデータが見つからない** | インデックス未構築または削除 | デプロイ前検証、監視 |

## 五、ファイル書き込み層（S3 Output）

| 失敗箇所 | 症状 | 緩和策 |
|---------|------|--------|
| **_tmp への書き込み途中失敗** | ゴミファイル残留 | `_tmp` プレフィックスを定期クリーンアップ、正確性に影響なし |
| **rename 失敗（S3 に真の rename なし）** | Spark の `_temporary` + commit、commit 段階で失敗 | S3A FileOutputCommitter v2 または Iceberg 形式の commit を使用；本方案は明示的 copy + delete |
| **copy 半成功** | 複数ファイル chunk の一部のみ copy 成功 | batch 操作 + ファイル数検証、失敗時は全削除リトライ |
| **正式パスへの重複書き込み** | Worker リトライで既存ファイル上書き | パスに chunk_id 含む、冪等；DynamoDB 条件付き書き込みで重複 completed 防止 |
| **正式パスのファイル誤削除** | 運用ミス、Lifecycle ルール | バージョニング + MFA Delete 有効化、Lifecycle ルールの精査 |
| **クロスアカウント書き込み失敗** | 顧客アカウントの Bucket Policy 拒否 | 事前テスト、失敗時は即時アラート |
| **小ファイル過多** | repartition 不適切で数万個の 1MB ファイル | 目標ファイルサイズ制御、repartition または coalesce |
| **5GB 超の単一 PUT** | 単一ファイル書き込み失敗 | Spark の自動 multipart、設定確認 |
| **S3 SSE-KMS 復号失敗** | 顧客クロスアカウントに KMS Key 権限なし | bucket key 利用または共有 KMS Key |
| **Multipart アップロード未完了** | Spark クラッシュで incomplete multipart 残存 | Bucket Lifecycle ルールで 7 日未完了 MPU を自動削除 |
| **チェックサム不一致** | ネットワーク破損による書き込みエラー（極稀） | S3 object integrity（CRC32C/SHA256）有効化 |

## 六、データ正確性層（Data Quality）

| 失敗箇所 | 症状 | 緩和策 |
|---------|------|--------|
| **行数不一致** | 実書き込み行数 ≠ Iceberg メタデータ宣言値 | Verify フェーズで `$snapshots.summary.total-records` と DynamoDB row_count 合計で対照 |
| **chunk 境界の漏れ** | 分割 WHERE 条件が全データをカバーしない | Plan フェーズで全 chunk の filter union = 全表 を検証必須 |
| **chunk 境界の重複** | 同一行が複数 chunk で読まれる | filter 排他設計、Verify で重複検出 |
| **NULL / 境界値の扱い** | hash 分割時に NULL 主キーが漏れる | 独立した NULL バケット |
| **データ型の精度損失** | Parquet 書き込み時に timestamp 精度損失 | INT64 timestamp 明示指定、タイムゾーン保持 |
| **文字エンコーディング不正** | 中国語/特殊文字の文字化け | UTF-8 全経路、CSV に BOM 付与 |
| **マスキング漏れ** | 一部フィールドのマスク忘れ | Spark 側で集中処理、単体テスト |

## 七、オーケストレーションとリトライ層（Step Functions）

| 失敗箇所 | 症状 | 緩和策 |
|---------|------|--------|
| **Step Functions 状態サイズ過大** | 状態マシン内に大データを渡す | job_id のみ渡し、詳細は DynamoDB |
| **無限リトライ** | Execute が繰り返し失敗するが SF が永遠にリトライ | max retry + max attempts の二重制限 |
| **デッドロック** | 一部 chunk が in_progress で永久スタック、Worker は既に死亡 | リース機構で必ず解放、stuck duration を監視 |
| **ゾンビ Job** | Glue Job 状態は RUNNING だが進展なし | Heartbeat 機構：Worker が定期的に lease_until 更新、長時間停止でアラート |
| **コールドスタート失敗の重複課金** | Glue Job 起動失敗でも課金 | Express Workflow + 失敗即停止 |

## 八、セキュリティとコンプライアンス層

| 失敗箇所 | 症状 | 緩和策 |
|---------|------|--------|
| **認証情報漏洩** | Glue Job ログに access key 出力 | 機密オブジェクトの print 厳禁 |
| **データ漏洩** | 誤った chunk filter で余分なデータ書き出し | Plan フェーズレビュー、最小権限 IAM |
| **プリサインド URL の悪用** | URL スクショ拡散 | 期限 < 24 時間、IP 制限、回数制限（STS 一時認証情報の代替推奨） |
| **監査の欠落** | 顧客ダウンロード記録なし | S3 Server Access Log 必須有効化 |
| **クロスアカウント権限過大** | Bucket Policy 設定ミスで public 化 | IAM Access Analyzer による継続スキャン |

## 九、運用可観測性層

| 失敗箇所 | 症状 | 緩和策 |
|---------|------|--------|
| **進捗不可視** | 処理済み chunk 数不明 | DynamoDB GSI のリアルタイム集計 + CloudWatch カスタムメトリクス |
| **失敗原因の喪失** | chunk が failed だがログが既にクリーン済み | error_msg フィールド保持、Glue Job ログを S3 へ保存 |
| **アラート漏れ** | 深夜タスク失敗で誰も対応せず | CloudWatch Alarm + SNS + PagerDuty |
| **コスト予算超過** | DPU 使用が制御不能 | CloudWatch コストアラート |

## 十、リスクレベル別 Top 10

| レベル | 失敗シナリオ | 防御メカニズム |
|--------|-------------|---------------|
| 🔴 高 | DynamoDB スロットリング | オンデマンドモード、指数バックオフ、並行度制御 |
| 🔴 高 | S3 同一プレフィックス 503 | 出力パスを hash プレフィックスで分散 |
| 🔴 高 | Snapshot expire 削除 | タスク期間中 expire_snapshots 停止 |
| 🔴 高 | Spark OOM（データ偏り） | Plan フェーズで偏り検出 + 二次分割 |
| 🟠 中 | Glue Worker 消失（Spot 回収） | リース + 自動リトライ |
| 🟠 中 | rename / commit 半失敗 | `_tmp` + chunk_id 冪等 + ファイル数検証 |
| 🟠 中 | クロスアカウント IAM/KMS 権限不足 | 全経路の事前テスト |
| 🟡 低 | データ型精度損失 | 型マッピング標準化 |
| 🟡 低 | Step Functions 状態過大 | job_id のみ渡す |
| 🟡 低 | 小ファイル過多 | repartition 制御 |

## 十一、全体の耐性まとめ

このアーキテクチャの耐性は 3 つの中核メカニズムの組み合わせから生まれる：

1. **Snapshot ロック** → "データが動いた" 問題を解決
2. **DynamoDB 条件付き書き込み + リース** → "タスク並行性とクラッシュ" 問題を解決
3. **`_tmp` 中間パス + chunk_id 冪等** → "ファイル書き込み途中" 問題を解決

**自動カバーされない、人手/監視介入が必要な失敗**：

- Snapshot expire 削除（設定/プロセスでの保護に依存）
- クロスアカウント権限変更（アラートに依存）
- データ正確性（Plan フェーズの分割設計と Verify フェーズの対照に依存）
- クォータ枯渇（容量計画に依存）

## 十二、設計総括（一文）

> **Snapshot ロック + DynamoDB 条件付き書き込み + `_tmp` 冪等パスにより、ほとんどのインフラ・計算・書き込み失敗から自動回復可能。残るリスクはデータソース設定（snapshot 保持）、クロスアカウント権限変更、データ正確性検証、クォータ管理であり、これらは運用プロセスと監視で補完する。**
