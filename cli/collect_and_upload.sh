#!/bin/bash
set -euo pipefail

# collect_and_upload.sh
# Step Functionsの実行データを収集し、S3にアップロードするオーケストレーションスクリプト。

EXECUTION_ARN=$1
S3_DESTINATION=$2
MOCK_MODE="${3:-false}" # "true" に設定するとモック履歴を使用

if [ -z "$EXECUTION_ARN" ] || [ -z "$S3_DESTINATION" ]; then
  echo "使用法: $0 <EXECUTION_ARN> <S3_DESTINATION> [true|false(mock_mode)]"
  echo "例: $0 arn:aws:states:us-east-1:123:execution:MySM:ID s3://my-bucket/path"
  exit 1
fi

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BASE_DIR="sf_data_${TIMESTAMP}"
METRICS_DIR="${BASE_DIR}/metrics"
mkdir -p "$METRICS_DIR"

echo "作業ディレクトリ: $BASE_DIR"

# ==========================================
# 関数定義
# ==========================================

get_lambda_insights() {
    local REQUEST_ID=$1
    local OUTPUT_FILE=$2

    if [ -z "$REQUEST_ID" ] || [ -z "$OUTPUT_FILE" ]; then
        echo "エラー: get_lambda_insights 引数が不足しています"
        return 1
    fi

    echo "   (Lambda) RequestID: $REQUEST_ID のインサイトを取得中..."
    aws logs filter-log-events \
      --log-group-name "/aws/lambda-insights" \
      --filter-pattern "{ $.request_id = \"$REQUEST_ID\" }" \
      --output json \
      > "$OUTPUT_FILE"
    
    echo "   (Lambda) 完了: $OUTPUT_FILE"
}

get_ecs_metric() {
    local CLUSTER=$1
    local TASK_ID=$2
    local OUTPUT_FILE=$3

    if [ -z "$CLUSTER" ] || [ -z "$TASK_ID" ] || [ -z "$OUTPUT_FILE" ]; then
        echo "エラー: get_ecs_metric 引数が不足しています"
        return 1
    fi

    local TEMP_DIR=$(dirname "$OUTPUT_FILE")/temp_ecs_${TASK_ID}
    mkdir -p "$TEMP_DIR"

    # 1. タスク詳細を取得して開始/終了時間を決定
    aws ecs describe-tasks \
      --cluster "$CLUSTER" \
      --tasks "$TASK_ID" \
      --output json \
      > "$TEMP_DIR/task_details.json"

    local START_TIME=$(jq -r '.tasks[0].startedAt // .tasks[0].createdAt' "$TEMP_DIR/task_details.json")
    local END_TIME=$(jq -r '.tasks[0].stoppedAt // empty' "$TEMP_DIR/task_details.json")

    if [ -z "$END_TIME" ] || [ "$END_TIME" == "null" ]; then
        END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    fi

    echo "   (ECS) タスク $TASK_ID ($START_TIME から $END_TIME) のメトリクスを取得中..."

    # 2. CloudWatch metrics
    aws cloudwatch get-metric-statistics \
        --namespace ECS/ContainerInsights \
        --metric-name CPUUtilization \
        --dimensions Name=TaskId,Value="$TASK_ID" Name=ClusterName,Value="$CLUSTER" \
        --statistics Average Maximum \
        --period 60 \
        --start-time "$START_TIME" \
        --end-time "$END_TIME" \
        --output json \
        > "$TEMP_DIR/cpu.json"

    aws cloudwatch get-metric-statistics \
        --namespace ECS/ContainerInsights \
        --metric-name MemoryUtilization \
        --dimensions Name=TaskId,Value="$TASK_ID" Name=ClusterName,Value="$CLUSTER" \
        --statistics Average Maximum \
        --period 60 \
        --start-time "$START_TIME" \
        --end-time "$END_TIME" \
        --output json \
        > "$TEMP_DIR/memory.json"

    # 3. 結合
    jq -n --slurpfile task "$TEMP_DIR/task_details.json" \
          --slurpfile cpu "$TEMP_DIR/cpu.json" \
          --slurpfile memory "$TEMP_DIR/memory.json" \
          '{ task: $task[0].tasks[0], metrics: { cpu: $cpu[0], memory: $memory[0] } }' \
          > "$OUTPUT_FILE"

    rm -rf "$TEMP_DIR"
    echo "   (ECS) 完了: $OUTPUT_FILE"
}

get_glue_job_metric() {
    local JOB_NAME=$1
    local RUN_ID=$2
    local OUTPUT_FILE=$3

    if [ -z "$JOB_NAME" ] || [ -z "$RUN_ID" ] || [ -z "$OUTPUT_FILE" ]; then
        echo "エラー: get_glue_job_metric 引数が不足しています"
        return 1
    fi

    local TEMP_DIR=$(dirname "$OUTPUT_FILE")/temp_glue_${RUN_ID}
    mkdir -p "$TEMP_DIR"

    # 1. Job Run 詳細取得
    aws glue get-job-run \
      --job-name "$JOB_NAME" \
      --run-id "$RUN_ID" \
      --output json \
      > "$TEMP_DIR/job_run.json"

    local START_TIME=$(jq -r '.JobRun.StartedOn' "$TEMP_DIR/job_run.json")
    local END_TIME=$(jq -r '.JobRun.CompletedOn // empty' "$TEMP_DIR/job_run.json")

    if [ -z "$END_TIME" ] || [ "$END_TIME" == "null" ]; then
        END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    fi

    echo "   (Glue) ジョブ $JOB_NAME / $RUN_ID ($START_TIME から $END_TIME) のメトリクスを取得中..."

    # 2. CloudWatch metrics
    aws cloudwatch get-metric-statistics \
        --namespace Glue \
        --metric-name glue.driver.cpuLoad \
        --dimensions Name=JobName,Value="$JOB_NAME" Name=JobRunId,Value="$RUN_ID" Name=Type,Value=gauge \
        --statistics Average Maximum \
        --period 300 \
        --start-time "$START_TIME" \
        --end-time "$END_TIME" \
        --output json \
        > "$TEMP_DIR/cpu_load.json"

    aws cloudwatch get-metric-statistics \
         --namespace Glue \
         --metric-name glue.driver.memoryUsed \
         --dimensions Name=JobName,Value="$JOB_NAME" Name=JobRunId,Value="$RUN_ID" Name=Type,Value=gauge \
         --statistics Average Maximum \
         --period 300 \
         --start-time "$START_TIME" \
         --end-time "$END_TIME" \
         --output json \
         > "$TEMP_DIR/memory_used.json"

    # 3. 結合
    jq -n --slurpfile job "$TEMP_DIR/job_run.json" \
          --slurpfile cpu "$TEMP_DIR/cpu_load.json" \
          --slurpfile memory "$TEMP_DIR/memory_used.json" \
          '{ jobRun: $job[0].JobRun, metrics: { cpuLoad: $cpu[0], memoryUsed: $memory[0] } }' \
          > "$OUTPUT_FILE"

    rm -rf "$TEMP_DIR"
    echo "   (Glue) 完了: $OUTPUT_FILE"
}

# ==========================================
# メイン処理
# ==========================================

# 1. 実行履歴の取得
echo "実行履歴を取得中..."
if [ "$MOCK_MODE" == "true" ]; then
    echo "MOCK モード: mock_sf_history.json を使用します"
    cp mock_sf_history.json "$BASE_DIR/history.json"
else
    # 既存のスクリプトを使用して履歴を取得 (ここはBash呼び出しのままにするか、インライン化可能だが一旦維持)
    bash get_step_functions_execution_history.sh "$EXECUTION_ARN" "$BASE_DIR/history.json"
fi

# 2. 履歴の解析
echo "実行履歴を解析中..."

DERIVED_SM_ARN=$(echo "$EXECUTION_ARN" | sed 's/:execution:/:stateMachine:/' | sed 's/:[^:]*$//')
if [ "$MOCK_MODE" == "true" ]; then
   DERIVED_SM_ARN="arn:aws:states:ap-northeast-1:123456789012:stateMachine:MockStateMachine"
fi

echo "推定された StateMachineArn: $DERIVED_SM_ARN"

jq -f sf_parser.jq --arg inputStateMachineArn "$DERIVED_SM_ARN" "$BASE_DIR/history.json" > "$BASE_DIR/index.json"

echo "インデックス作成完了: $BASE_DIR/index.json"

# 3. メトリクスの収集
echo "各ステップのメトリクスを収集内..."

jq -r '.steps[] | @base64' "$BASE_DIR/index.json" | while read -r step_b64; do
    _jq() {
     echo "$step_b64" | base64 --decode | jq -r "$1"
    }

    TYPE=$(_jq '.type')
    STATUS=$(_jq '.status')
    STEP_NAME=$(_jq '.stepName')
    
    SAFE_STEP_NAME=$(echo "$STEP_NAME" | sed 's/[^a-zA-Z0-9_-]/_/g')

    echo "   処理中 [$TYPE] $STEP_NAME ($STATUS)..."

    if [ "$STATUS" != "Succeeded" ] && [ "$STATUS" != "Failed" ] && [ "$STATUS" != "TimedOut" ]; then
        echo "      ステータスが対象外のためスキップ: $STATUS"
        continue
    fi
    
    if [ "$MOCK_MODE" == "true" ]; then
        echo "{\"mock\": true, \"step\": \"$STEP_NAME\"}" > "$METRICS_DIR/${TYPE}_${SAFE_STEP_NAME}.json"
        continue
    fi

    if [ "$TYPE" == "lambda" ]; then
        RESOURCE_ID=$(_jq '.executionId // .resource')
        get_lambda_insights "$RESOURCE_ID" "$METRICS_DIR/lambda_${SAFE_STEP_NAME}.json" || echo "      Lambdaメトリクスの取得に失敗しました"

    elif [ "$TYPE" == "ecs" ]; then
        CLUSTER=$(_jq '.clusterArn')
        TASK_ID=$(_jq '.taskId')
        get_ecs_metric "$CLUSTER" "$TASK_ID" "$METRICS_DIR/ecs_${SAFE_STEP_NAME}.json" || echo "      ECSメトリクスの取得に失敗しました"

    elif [ "$TYPE" == "glue" ]; then
        JOB_NAME=$(_jq '.jobName')
        RUN_ID=$(_jq '.jobRunId')
        get_glue_job_metric "$JOB_NAME" "$RUN_ID" "$METRICS_DIR/glue_${SAFE_STEP_NAME}.json" || echo "      Glueメトリクスの取得に失敗しました"
        
    elif [ "$TYPE" == "step_function" ]; then
        EXEC_ARN=$(_jq '.executionArn')
        echo "      ネストされた実行を検出: $EXEC_ARN"
    fi
done

# 4. S3 へのアップロード
echo "S3へアップロード中: $S3_DESTINATION/sf_data_${TIMESTAMP}/"
if [ "$MOCK_MODE" == "true" ]; then
    echo "MOCK モード: S3アップロードをスキップします。ファイル保存先: $BASE_DIR"
else
    aws s3 cp --recursive "$BASE_DIR" "$S3_DESTINATION/sf_data_${TIMESTAMP}/"
fi

echo "完了! データ収集が終了しました。"
